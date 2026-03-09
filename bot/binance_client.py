from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator

from .console import say
from .metrics import Metrics
from .models import BalanceSnapshot, MarketSnapshot, SymbolRules, utc_now


LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class BinanceClientConfig:
    api_key: str
    api_secret: str
    rest_base_url: str
    ws_base_url: str
    recv_window_ms: int = 5_000
    reconnect_max_delay: float = 30.0


@dataclass(slots=True)
class PaperTradingConfig:
    latency_min_ms: int = 50
    latency_max_ms: int = 250
    starting_quote_balance: float = 1_000.0
    starting_base_balance: float = 0.0
    replay_speed: float = 0.0
    random_seed: int | None = None


@dataclass(slots=True)
class _SimulatedOrder:
    order_id: int
    symbol: str
    side: str
    quantity: float
    price: float
    client_order_id: str
    order_type: str
    time_in_force: str
    maker_only: bool
    created_at: datetime
    updated_at: datetime
    active_at: datetime
    status: str = "NEW"
    executed_qty: float = 0.0
    cumulative_quote_qty: float = 0.0
    raw: dict[str, Any] = field(default_factory=dict)


class BinanceClient:
    def __init__(self, config: BinanceClientConfig, metrics: Metrics | None = None) -> None:
        self._config = config
        self._metrics = metrics
        self._book_ticker_cache: dict[str, tuple[float, float]] = {}

    async def get_symbol_rules(self, symbol: str) -> SymbolRules:
        payload = await self._request("GET", "/api/v3/exchangeInfo", {"symbol": symbol})
        symbols = payload.get("symbols", [])
        if not symbols:
            raise RuntimeError(f"Binance exchangeInfo returned no symbol metadata for {symbol}")

        info = symbols[0]
        filters = {item["filterType"]: item for item in info.get("filters", [])}
        if "PRICE_FILTER" not in filters:
            raise RuntimeError(f"Binance exchangeInfo missing PRICE_FILTER for {symbol}")
        if "LOT_SIZE" not in filters:
            raise RuntimeError(f"Binance exchangeInfo missing LOT_SIZE for {symbol}")
        notional_filter = filters.get("NOTIONAL", filters.get("MIN_NOTIONAL", {}))
        return SymbolRules(
            tick_size=float(filters["PRICE_FILTER"]["tickSize"]),
            step_size=float(filters["LOT_SIZE"]["stepSize"]),
            min_qty=float(filters["LOT_SIZE"]["minQty"]),
            min_notional=float(notional_filter.get("minNotional", 0.0)),
            base_asset=info["baseAsset"],
            quote_asset=info["quoteAsset"],
        )

    async def get_balances(self) -> list[BalanceSnapshot]:
        payload = await self._request("GET", "/api/v3/account", signed=True)
        balances = []
        for item in payload.get("balances", []):
            free = float(item["free"])
            locked = float(item["locked"])
            if free <= 0 and locked <= 0:
                continue
            balances.append(BalanceSnapshot(asset=item["asset"], free=free, locked=locked))
        return balances

    async def get_best_bid_ask(self, symbol: str) -> tuple[float, float]:
        payload = await self._request("GET", "/api/v3/ticker/bookTicker", {"symbol": symbol})
        return float(payload["bidPrice"]), float(payload["askPrice"])

    async def get_cached_best_bid_ask(self, symbol: str) -> tuple[float | None, float | None]:
        return self._book_ticker_cache.get(symbol, (None, None))

    async def place_limit_order(
        self,
        symbol: str,
        side: str,
        quantity: str,
        price: str,
        *,
        client_order_id: str,
        maker_only: bool,
        time_in_force: str = "GTC",
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "price": price,
            "newClientOrderId": client_order_id,
            "newOrderRespType": "RESULT",
        }
        if maker_only:
            params["type"] = "LIMIT_MAKER"
        else:
            params["type"] = "LIMIT"
            params["timeInForce"] = time_in_force
        return await self._request("POST", "/api/v3/order", params, signed=True)

    async def cancel_order(self, symbol: str, client_order_id: str) -> dict[str, Any]:
        return await self._request(
            "DELETE",
            "/api/v3/order",
            {"symbol": symbol, "origClientOrderId": client_order_id},
            signed=True,
        )

    async def get_order(self, symbol: str, client_order_id: str) -> dict[str, Any]:
        return await self._request(
            "GET",
            "/api/v3/order",
            {"symbol": symbol, "origClientOrderId": client_order_id},
            signed=True,
        )

    async def stream_market_data(self, symbol: str, stop_event: asyncio.Event) -> AsyncIterator[MarketSnapshot]:
        try:
            import websockets
            from websockets.exceptions import ConnectionClosed
        except ImportError as exc:
            raise RuntimeError("Missing dependency 'websockets'. Install project dependencies first.") from exc

        symbol_lower = symbol.lower()
        streams = [f"{symbol_lower}@bookTicker", f"{symbol_lower}@depth5@100ms"]
        url = self._combined_stream_url(streams)
        backoff = 1.0
        bid_price = ask_price = bid_qty = ask_qty = 0.0
        depth_bid_volume = depth_ask_volume = 0.0
        bid_update_time: datetime | None = None
        ask_update_time: datetime | None = None

        while not stop_event.is_set():
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as websocket:
                    LOGGER.info("Connected to market data stream %s", url)
                    say("Connected to market data stream for {symbol}.", symbol=symbol)
                    backoff = 1.0
                    while not stop_event.is_set():
                        raw_message = await asyncio.wait_for(websocket.recv(), timeout=30)
                        payload = json.loads(raw_message)
                        data = payload.get("data", payload)
                        message_time = datetime.now(timezone.utc)
                        if {"b", "a", "B", "A"}.issubset(data):
                            bid_price = float(data["b"])
                            ask_price = float(data["a"])
                            bid_qty = float(data["B"])
                            ask_qty = float(data["A"])
                            bid_update_time = message_time
                            ask_update_time = message_time
                            self._book_ticker_cache[symbol] = (bid_price, ask_price)
                        elif "bids" in data and "asks" in data:
                            depth_bid_volume = sum(float(level[1]) for level in data["bids"])
                            depth_ask_volume = sum(float(level[1]) for level in data["asks"])
                        else:
                            continue

                        if bid_price <= 0 or ask_price <= 0 or ask_price <= bid_price:
                            continue
                        use_bid_volume = depth_bid_volume or bid_qty
                        use_ask_volume = depth_ask_volume or ask_qty
                        if use_ask_volume <= 0:
                            continue
                        mid_price = (bid_price + ask_price) / 2
                        spread = ask_price - bid_price
                        yield MarketSnapshot(
                            symbol=symbol,
                            bid_price=bid_price,
                            ask_price=ask_price,
                            bid_volume=use_bid_volume,
                            ask_volume=use_ask_volume,
                            mid_price=mid_price,
                            spread=spread,
                            spread_bps=(spread / mid_price) * 10_000,
                            imbalance_ratio=use_bid_volume / use_ask_volume,
                            bid_update_time=bid_update_time or message_time,
                            ask_update_time=ask_update_time or message_time,
                            event_time=message_time,
                            raw=data,
                        )
            except (asyncio.TimeoutError, ConnectionClosed, OSError) as exc:
                if stop_event.is_set():
                    break
                LOGGER.warning("Market stream disconnected: %s", exc)
                if self._metrics:
                    self._metrics.record_ws_reconnect()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._config.reconnect_max_delay)

    def _combined_stream_url(self, streams: list[str]) -> str:
        base = self._config.ws_base_url.rstrip("/")
        if base.endswith("/stream"):
            return f"{base}?streams={'/'.join(streams)}"
        return f"{base}/stream?streams={'/'.join(streams)}"

    async def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        signed: bool = False,
    ) -> dict[str, Any]:
        params = dict(params or {})
        headers = {"X-MBX-APIKEY": self._config.api_key} if self._config.api_key else {}
        if signed:
            params["timestamp"] = int(time.time() * 1000)
            params["recvWindow"] = self._config.recv_window_ms
            query = urllib.parse.urlencode(params, doseq=True)
            signature = hmac.new(
                self._config.api_secret.encode("utf-8"),
                query.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()
            query = f"{query}&signature={signature}"
        else:
            query = urllib.parse.urlencode(params, doseq=True)

        url = f"{self._config.rest_base_url.rstrip('/')}{path}"
        if method in {"GET", "DELETE"}:
            if query:
                url = f"{url}?{query}"
            body = None
        else:
            body = query.encode("utf-8")
            headers["Content-Type"] = "application/x-www-form-urlencoded"

        request = urllib.request.Request(url=url, method=method, data=body, headers=headers)
        return await asyncio.to_thread(self._execute_request, request)

    @staticmethod
    def _execute_request(request: urllib.request.Request) -> dict[str, Any]:
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            payload = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Binance HTTP error {exc.code}: {payload}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Binance network error: {exc.reason}") from exc


class PaperTradingClient:
    def __init__(
        self,
        *,
        symbol: str,
        symbol_rules: SymbolRules,
        config: PaperTradingConfig,
        live_client: BinanceClient | None = None,
        replay_snapshots: list[MarketSnapshot] | None = None,
    ) -> None:
        self._symbol = symbol
        self._symbol_rules = symbol_rules
        self._config = config
        self._live_client = live_client
        self._replay_snapshots = replay_snapshots or []
        self._rng = random.Random(config.random_seed)
        self._current_market: MarketSnapshot | None = None
        self._orders: dict[str, _SimulatedOrder] = {}
        self._next_order_id = 1
        self._lock = asyncio.Lock()
        self._balances = {
            symbol_rules.base_asset: {"free": config.starting_base_balance, "locked": 0.0},
            symbol_rules.quote_asset: {"free": config.starting_quote_balance, "locked": 0.0},
        }

    async def get_symbol_rules(self, symbol: str) -> SymbolRules:
        if symbol != self._symbol:
            raise RuntimeError(f"Paper trading client only configured for {self._symbol}")
        return self._symbol_rules

    async def get_balances(self) -> list[BalanceSnapshot]:
        captured_at = self._sim_time()
        balances: list[BalanceSnapshot] = []
        for asset, amounts in self._balances.items():
            if amounts["free"] <= 0 and amounts["locked"] <= 0:
                continue
            balances.append(
                BalanceSnapshot(
                    asset=asset,
                    free=amounts["free"],
                    locked=amounts["locked"],
                    captured_at=captured_at,
                )
            )
        return balances

    async def get_best_bid_ask(self, symbol: str) -> tuple[float, float]:
        if symbol != self._symbol:
            raise RuntimeError(f"Paper trading client only configured for {self._symbol}")
        async with self._lock:
            if self._current_market is not None:
                return self._current_market.bid_price, self._current_market.ask_price
        if self._live_client is not None:
            return await self._live_client.get_best_bid_ask(symbol)
        raise RuntimeError("Paper trading best bid/ask unavailable before market data starts")

    async def get_cached_best_bid_ask(self, symbol: str) -> tuple[float | None, float | None]:
        if symbol != self._symbol:
            raise RuntimeError(f"Paper trading client only configured for {self._symbol}")
        async with self._lock:
            if self._current_market is None:
                return None, None
            return self._current_market.bid_price, self._current_market.ask_price

    async def place_limit_order(
        self,
        symbol: str,
        side: str,
        quantity: str,
        price: str,
        *,
        client_order_id: str,
        maker_only: bool,
        time_in_force: str = "GTC",
    ) -> dict[str, Any]:
        if symbol != self._symbol:
            raise RuntimeError(f"Paper trading client only configured for {self._symbol}")

        quantity_value = float(quantity)
        price_value = float(price)
        created_at = self._sim_time()
        active_at = created_at + timedelta(milliseconds=self._latency_ms())

        async with self._lock:
            if maker_only and self._current_market and self._would_cross_immediately(side, price_value):
                raise RuntimeError("Paper trading rejection: LIMIT_MAKER would cross the spread")
            self._reserve_balance(side, quantity_value, price_value)
            order = _SimulatedOrder(
                order_id=self._next_order_id,
                symbol=symbol,
                side=side,
                quantity=quantity_value,
                price=price_value,
                client_order_id=client_order_id,
                order_type="LIMIT_MAKER" if maker_only else "LIMIT",
                time_in_force=time_in_force,
                maker_only=maker_only,
                created_at=created_at,
                updated_at=created_at,
                active_at=active_at,
            )
            self._next_order_id += 1
            self._orders[client_order_id] = order
            self._maybe_fill_order(order, self._current_market)
            return self._order_payload(order)

    async def cancel_order(self, symbol: str, client_order_id: str) -> dict[str, Any]:
        if symbol != self._symbol:
            raise RuntimeError(f"Paper trading client only configured for {self._symbol}")

        async with self._lock:
            order = self._orders.get(client_order_id)
            if order is None:
                raise RuntimeError(f"Paper trading order not found: {client_order_id}")
            if order.status == "FILLED":
                raise RuntimeError("Paper trading cancel rejected: order already filled")
            if order.status in {"CANCELED", "REJECTED", "EXPIRED"}:
                return self._order_payload(order)
            self._release_balance(order.side, order.quantity - order.executed_qty, order.price)
            order.status = "CANCELED"
            order.updated_at = self._sim_time()
            return self._order_payload(order)

    async def get_order(self, symbol: str, client_order_id: str) -> dict[str, Any]:
        if symbol != self._symbol:
            raise RuntimeError(f"Paper trading client only configured for {self._symbol}")

        async with self._lock:
            order = self._orders.get(client_order_id)
            if order is None:
                raise RuntimeError(f"Paper trading order not found: {client_order_id}")
            return self._order_payload(order)

    async def stream_market_data(self, symbol: str, stop_event: asyncio.Event) -> AsyncIterator[MarketSnapshot]:
        if symbol != self._symbol:
            raise RuntimeError(f"Paper trading client only configured for {self._symbol}")

        if self._replay_snapshots:
            previous_event_time: datetime | None = None
            for market in self._replay_snapshots:
                if stop_event.is_set():
                    break
                if previous_event_time and self._config.replay_speed > 0:
                    delay_seconds = (market.event_time - previous_event_time).total_seconds() / self._config.replay_speed
                    if delay_seconds > 0:
                        await asyncio.sleep(delay_seconds)
                previous_event_time = market.event_time
                async with self._lock:
                    self._current_market = market
                    self._apply_market(market)
                yield market
            return

        if self._live_client is None:
            raise RuntimeError("Paper trading requires either live market data or replay snapshots")

        async for market in self._live_client.stream_market_data(symbol, stop_event):
            async with self._lock:
                self._current_market = market
                self._apply_market(market)
            yield market

    def _apply_market(self, market: MarketSnapshot) -> None:
        for order in self._orders.values():
            self._maybe_fill_order(order, market)

    def _maybe_fill_order(self, order: _SimulatedOrder, market: MarketSnapshot | None) -> None:
        if market is None:
            return
        if order.status != "NEW":
            return
        if market.event_time < order.active_at:
            return
        if not self._is_crossed(order, market):
            return

        fill_price = min(order.price, market.ask_price) if order.side == "BUY" else max(order.price, market.bid_price)
        order.executed_qty = order.quantity
        order.cumulative_quote_qty = fill_price * order.quantity
        order.status = "FILLED"
        order.updated_at = market.event_time

        if order.side == "BUY":
            reserved_quote = order.quantity * order.price
            self._balances[self._symbol_rules.quote_asset]["locked"] -= reserved_quote
            self._balances[self._symbol_rules.quote_asset]["free"] += max(0.0, reserved_quote - order.cumulative_quote_qty)
            self._balances[self._symbol_rules.base_asset]["free"] += order.quantity
        else:
            self._balances[self._symbol_rules.base_asset]["locked"] -= order.quantity
            self._balances[self._symbol_rules.quote_asset]["free"] += order.cumulative_quote_qty

    def _reserve_balance(self, side: str, quantity: float, price: float) -> None:
        if side == "BUY":
            required_quote = quantity * price
            quote_balance = self._balances[self._symbol_rules.quote_asset]
            if quote_balance["free"] < required_quote:
                raise RuntimeError("Paper trading rejection: insufficient quote balance")
            quote_balance["free"] -= required_quote
            quote_balance["locked"] += required_quote
            return

        base_balance = self._balances[self._symbol_rules.base_asset]
        if base_balance["free"] < quantity:
            raise RuntimeError("Paper trading rejection: insufficient base balance")
        base_balance["free"] -= quantity
        base_balance["locked"] += quantity

    def _release_balance(self, side: str, quantity: float, price: float) -> None:
        if quantity <= 0:
            return
        if side == "BUY":
            quote_amount = quantity * price
            quote_balance = self._balances[self._symbol_rules.quote_asset]
            quote_balance["locked"] -= quote_amount
            quote_balance["free"] += quote_amount
            return

        base_balance = self._balances[self._symbol_rules.base_asset]
        base_balance["locked"] -= quantity
        base_balance["free"] += quantity

    def _would_cross_immediately(self, side: str, price: float) -> bool:
        if self._current_market is None:
            return False
        if side == "BUY":
            return price >= self._current_market.ask_price
        return price <= self._current_market.bid_price

    @staticmethod
    def _is_crossed(order: _SimulatedOrder, market: MarketSnapshot) -> bool:
        if order.side == "BUY":
            return market.ask_price <= order.price
        return market.bid_price >= order.price

    def _order_payload(self, order: _SimulatedOrder) -> dict[str, Any]:
        return {
            "symbol": order.symbol,
            "orderId": order.order_id,
            "clientOrderId": order.client_order_id,
            "price": f"{order.price:.16f}".rstrip("0").rstrip("."),
            "origQty": f"{order.quantity:.16f}".rstrip("0").rstrip("."),
            "executedQty": f"{order.executed_qty:.16f}".rstrip("0").rstrip("."),
            "cummulativeQuoteQty": f"{order.cumulative_quote_qty:.16f}".rstrip("0").rstrip("."),
            "status": order.status,
            "type": order.order_type,
            "side": order.side,
            "timeInForce": order.time_in_force,
            "transactTime": int(order.updated_at.timestamp() * 1000),
            "workingTime": int(order.active_at.timestamp() * 1000),
        }

    def _latency_ms(self) -> int:
        low = min(self._config.latency_min_ms, self._config.latency_max_ms)
        high = max(self._config.latency_min_ms, self._config.latency_max_ms)
        return self._rng.randint(low, high)

    def _sim_time(self) -> datetime:
        if self._current_market is not None:
            return self._current_market.event_time
        return utc_now()
