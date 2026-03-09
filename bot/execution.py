from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN, ROUND_FLOOR

from .binance_client import BinanceClient
from .console import say
from .datastore import DataStore
from .metrics import Metrics
from .models import MarketSnapshot, OrderPurpose, OrderState, PositionState, Side, SymbolRules, utc_now
from .risk import RiskManager
from .strategy import SpreadCaptureStrategy


LOGGER = logging.getLogger(__name__)


def floor_to_tick(price: float, tick_size: float) -> float:
    tick = Decimal(str(tick_size))
    if tick <= 0:
        return float(price)
    price_dec = Decimal(str(price))
    ratio = price_dec / tick
    floored = ratio.to_integral_value(rounding=ROUND_FLOOR) * tick
    return float(floored)


def ceil_to_tick(price: float, tick_size: float) -> float:
    tick = Decimal(str(tick_size))
    if tick <= 0:
        return float(price)
    price_dec = Decimal(str(price))
    ratio = price_dec / tick
    ceiled = ratio.to_integral_value(rounding=ROUND_CEILING) * tick
    return float(ceiled)


@dataclass(slots=True)
class ExecutionConfig:
    symbol: str
    dry_run: bool
    entry_ttl_seconds: int
    exit_ttl_seconds: int
    maker_offset_ticks: int
    offset_ticks_min: int
    offset_ticks_max: int
    spread_tight_bps: float
    spread_wide_bps: float
    entry_attempt_interval_ms: int
    rejection_cooldown_ms: int
    max_consecutive_rejections: int
    allow_taker_exit: bool
    entry_max_requotes: int
    exit_max_requotes: int
    market_stale_after_ms: int
    max_spread_bps: float
    order_poll_interval: float
    balance_snapshot_interval: float
    market_data_source: str
    sync_orders_on_market_data: bool


class ExecutionEngine:
    _ENTRY_REPRICE_INTERVAL_SECONDS = 1.5
    _EXIT_REPRICE_INTERVAL_SECONDS = 1.5
    _ENTRY_REPRICE_RATE_LIMIT_SECONDS = 1.0
    _ENTRY_RETRY_BACKOFF_SECONDS = 5.0
    _DEFAULT_MARKET_STALE_AFTER = timedelta(milliseconds=500)
    _LOSS_STREAK_COOLDOWN = timedelta(minutes=10)
    _LOSS_STREAK_THRESHOLD = 3
    _METRICS_LOG_INTERVAL = timedelta(minutes=10)
    _HUMAN_STATUS_INTERVAL = timedelta(seconds=30)
    _REJECTION_PAUSE_SECONDS = 10.0

    def __init__(
        self,
        *,
        client: BinanceClient,
        strategy: SpreadCaptureStrategy,
        risk: RiskManager,
        datastore: DataStore,
        metrics: Metrics,
        config: ExecutionConfig,
        symbol_rules: SymbolRules,
    ) -> None:
        self._client = client
        self._strategy = strategy
        self._risk = risk
        self._datastore = datastore
        self._metrics = metrics
        self._config = config
        self._symbol_rules = symbol_rules
        self._current_market: MarketSnapshot | None = None
        self._last_decision = None
        self._entry_order: OrderState | None = None
        self._exit_order: OrderState | None = None
        self._position: PositionState | None = None
        self._stop_mode = False
        self._last_balance_snapshot = datetime.min.replace(tzinfo=timezone.utc)
        self._entry_attempt_started_at: datetime | None = None
        self._entry_backoff_until = datetime.min.replace(tzinfo=timezone.utc)
        self._next_entry_attempt_at = datetime.min.replace(tzinfo=timezone.utc)
        self._next_entry_reprice_at = datetime.min.replace(tzinfo=timezone.utc)
        self._last_entry_reprice_action_at = datetime.min.replace(tzinfo=timezone.utc)
        self._next_exit_reprice_at = datetime.min.replace(tzinfo=timezone.utc)
        self._rejection_cooldown_until = datetime.min.replace(tzinfo=timezone.utc)
        self._rejection_pause_until = datetime.min.replace(tzinfo=timezone.utc)
        self._cooldown_until = datetime.min.replace(tzinfo=timezone.utc)
        self._consecutive_losses = 0
        self._consecutive_rejections = 0
        self._next_metrics_log_at: datetime | None = None
        self._next_human_status_at: datetime | None = None
        self._last_best_bid: float | None = None
        self._last_best_ask: float | None = None
        self._last_quote_update_ts: datetime | None = None
        self._last_bid_update_ts: datetime | None = None
        self._last_ask_update_ts: datetime | None = None
        stale_after_ms = int(self._config.market_stale_after_ms)
        if stale_after_ms <= 0:
            self._market_stale_after = self._DEFAULT_MARKET_STALE_AFTER
        else:
            self._market_stale_after = timedelta(milliseconds=stale_after_ms)
        self.daily_realized_pnl = 0.0
        self._daily_realized_pnl_date = utc_now().date()
        self._entry_requote_count = 0
        self._exit_requote_count = 0
        self._entry_requotes_total = 0
        self._exit_requotes_total = 0
        self._exit_attempt_started_at: datetime | None = None
        self._exit_attempts = 0
        self._exit_requote_escalated = False
        self._count_2010 = 0
        self._count_1021 = 0
        self._entry_submit_attempts = 0
        self._entry_fills = 0
        self._skip_reason_counts: dict[str, int] = {
            "stale data": 0,
            "spread too small": 0,
            "rejection": 0,
            "ttl expired": 0,
        }
        self._last_entry_offset_ticks = max(0, self._config.maker_offset_ticks)
        self._session_started_at = utc_now()
        self._session_ended_at: datetime | None = None
        self._shutdown_requested = False
        self._lock = asyncio.Lock()

    async def run(self, stop_event: asyncio.Event) -> None:
        await self._capture_balances(force=True)
        poll_task = asyncio.create_task(self._poll_orders_loop(stop_event))
        try:
            async for market in self._client.stream_market_data(self._config.symbol, stop_event):
                async with self._lock:
                    self._current_market = market
                    self._ingest_market(market)
                    if stop_event.is_set():
                        self._shutdown_requested = True
                        LOGGER.info("Stop event received; skipping new decisions and shutting down.")
                        break
                    self._maybe_log_metrics_summary(market.event_time)
                    if self._config.sync_orders_on_market_data:
                        await self._refresh_open_orders()
                    await self._evaluate_signal(market)
                    await self._manage_entry_order(market)
                    await self._manage_timeouts(market)
                    await self._manage_position(market)
                    await self._maybe_enter(market)
                    self._maybe_emit_human_status(market.event_time)
                    await self._capture_balances(force=False)
                if stop_event.is_set():
                    break
        finally:
            self._shutdown_requested = True
            self._session_ended_at = utc_now()
            poll_task.cancel()
            await asyncio.gather(poll_task, return_exceptions=True)
            async with self._lock:
                await self.shutdown()

    async def shutdown(self) -> None:
        self._shutdown_requested = True
        self._session_ended_at = self._session_ended_at or utc_now()
        if self._config.dry_run:
            return
        for order in (self._entry_order, self._exit_order):
            if order and order.status in {"NEW", "PARTIALLY_FILLED"}:
                await self._cancel_order(order, "shutdown")

    def session_summary(self) -> dict[str, object]:
        metrics_summary = self._metrics.summary()
        winning_trades = int(metrics_summary.get("winning_trades", 0))
        losing_trades = int(metrics_summary.get("losing_trades", 0))
        breakeven_trades = int(metrics_summary.get("breakeven_trades", 0))
        total_trades = winning_trades + losing_trades + breakeven_trades
        if total_trades == 0:
            total_trades = int(metrics_summary.get("total_trades", 0))
        start_time = self._session_started_at
        end_time = self._session_ended_at or utc_now()
        runtime_seconds = max(0.0, (end_time - start_time).total_seconds())
        hours, remainder = divmod(int(runtime_seconds), 3600)
        minutes, seconds = divmod(remainder, 60)
        return {
            "symbol": self._config.symbol,
            "start_time": start_time,
            "end_time": end_time,
            "runtime": f"{hours:02d}:{minutes:02d}:{seconds:02d}",
            "total_trades": total_trades,
            "winning_trades": winning_trades,
            "losing_trades": losing_trades,
            "breakeven_trades": breakeven_trades,
            "win_rate": winning_trades / total_trades if total_trades > 0 else 0.0,
            "fill_rate": float(metrics_summary.get("fill_rate", 0.0)),
            "daily_pnl_usd": self.daily_realized_pnl,
            "max_drawdown_usd": float(metrics_summary.get("max_drawdown_usd", 0.0)),
            "avg_pnl_usd": float(metrics_summary.get("avg_pnl_usd", 0.0)),
            "count_2010": self._count_2010,
            "count_1021": self._count_1021,
            "entry_requotes": self._entry_requotes_total,
            "exit_requotes": self._exit_requotes_total,
        }

    @property
    def open_order_count(self) -> int:
        count = 0
        for order in (self._entry_order, self._exit_order):
            if order and order.status in {"NEW", "PARTIALLY_FILLED"}:
                count += 1
        return count

    async def _evaluate_signal(self, market: MarketSnapshot) -> None:
        await self._datastore.log_market_snapshot(self._config.symbol, market, self._config.market_data_source)
        self._last_decision = self._strategy.evaluate(market)
        self._metrics.record_signal()
        await self._datastore.log_signal(self._config.symbol, market, self._last_decision)

    async def _maybe_enter(self, market: MarketSnapshot) -> None:
        if self._shutdown_requested:
            return
        if not self._last_decision or self._entry_order or self._exit_order or self._position:
            return
        now = self._now()
        if now < self._entry_backoff_until:
            return
        if now < self._rejection_pause_until:
            return
        if now < self._rejection_cooldown_until:
            return
        if now < self._cooldown_until:
            self._human_skip("cooldown_active")
            return
        if now < self._next_entry_attempt_at:
            return
        if not self._last_decision.should_enter:
            if "spread_below_threshold" in self._last_decision.reasons:
                self._record_skip_bucket("spread too small")
            return
        if self._entry_attempt_expired(now):
            self._record_skip_bucket("ttl expired")
            self._reset_entry_attempt(now=now, backoff=True)
            return
        if not self._market_is_sane_for_order(reason_context="entry"):
            return

        quantity = self._quantize_quantity(self._risk.position_size_from_risk(market.bid_price))
        proposed_notional = quantity * market.bid_price
        risk_decision = self._risk.can_open_entry(
            position_notional_usd=0.0,
            open_orders=self.open_order_count,
            proposed_notional_usd=proposed_notional,
        )
        if not risk_decision.allowed:
            LOGGER.info("Entry blocked by risk: %s", ",".join(risk_decision.reasons))
            self._human_skip(",".join(risk_decision.reasons))
            return
        if quantity < self._symbol_rules.min_qty or proposed_notional < self._symbol_rules.min_notional:
            LOGGER.info("Entry skipped because quantity/notional is below exchange minimums")
            self._human_skip("exchange_minimums")
            return

        if self._config.dry_run:
            LOGGER.info(
                "Dry-run entry signal: spread=%.3f bps imbalance=%.3f volatility=%.3f bps qty=%.8f",
                self._last_decision.spread_bps,
                self._last_decision.imbalance_ratio,
                self._last_decision.volatility_bps,
                quantity,
            )
            return

        self._next_entry_attempt_at = now + timedelta(milliseconds=max(1, self._config.entry_attempt_interval_ms))
        say(
            "Trying to enter: placing a maker BUY near the best bid. Spread={spread_bps:.3f} bps.",
            spread_bps=market.spread_bps,
        )
        self._entry_submit_attempts += 1
        await self._submit_entry_order(
            quantity=quantity,
            best_bid=market.bid_price,
            best_ask=market.ask_price,
            spread_bps=market.spread_bps,
            vol_bps=self._last_decision.volatility_bps,
        )

    async def _manage_entry_order(self, market: MarketSnapshot) -> None:
        if not self._entry_order or self._entry_order.status not in {"NEW", "PARTIALLY_FILLED"}:
            return

        entry_order = self._entry_order
        now = self._now()
        if self._entry_attempt_expired(now):
            self._record_skip_bucket("ttl expired")
            await self._cancel_order(entry_order, "entry_ttl_expired")
            if entry_order.executed_quantity > 0:
                self._entry_order = entry_order
                await self._activate_position_from_entry()
            else:
                say("Entry did not fill in time, canceled to avoid stale orders.")
                self._reset_entry_attempt(now=now, backoff=True)
            return

        if entry_order.executed_quantity > 0 or now < self._next_entry_reprice_at:
            return
        self._next_entry_reprice_at = now + timedelta(seconds=self._ENTRY_REPRICE_INTERVAL_SECONDS)

        if not self._last_decision or not self._last_decision.should_enter:
            return

        best_bid, best_ask = await self._latest_cached_best_bid_ask()
        if best_bid is None or best_ask is None:
            return

        new_entry_price = self._quantize_price(best_bid)
        if abs(new_entry_price - entry_order.price) < self._symbol_rules.tick_size:
            return
        if not self._entry_reprice_rate_limit_allows(now):
            return

        remaining_qty = self._normalize_quantity(entry_order.quantity - entry_order.executed_quantity)
        if remaining_qty <= 0:
            return
        if self._entry_requote_count >= self._config.entry_max_requotes:
            LOGGER.warning(
                "Entry requote limit reached count=%d max=%d; skipping cycle",
                self._entry_requote_count,
                self._config.entry_max_requotes,
            )
            say(
                "Too many requotes ({count}). Skipping this cycle to avoid overtrading/spam.",
                count=self._entry_requote_count,
            )
            await self._cancel_order(entry_order, "entry_requote_limit_reached")
            self._reset_entry_attempt(now=now, backoff=True)
            return

        self._last_entry_reprice_action_at = now
        self._entry_requote_count += 1
        self._entry_requotes_total += 1
        old_price = entry_order.price
        await self._cancel_order(entry_order, "best_bid_moved")
        new_order = await self._submit_entry_order(
            quantity=remaining_qty,
            best_bid=best_bid,
            best_ask=best_ask,
            spread_bps=self._spread_bps(best_bid, best_ask),
            vol_bps=self._last_decision.volatility_bps,
        )
        if new_order is not None:
            LOGGER.info(
                "Repriced ENTRY BUY from %.8f to %.8f reason=best_bid_moved",
                old_price,
                new_order.price,
            )

    async def _manage_timeouts(self, market: MarketSnapshot) -> None:
        now = self._now()
        if self._exit_order and self._exit_order.status in {"NEW", "PARTIALLY_FILLED"}:
            if not self._stop_mode:
                await self._maybe_requote_exit_order(market)
            if not self._exit_order or self._exit_order.status not in {"NEW", "PARTIALLY_FILLED"}:
                return
            exit_order = self._exit_order
            age = (now - exit_order.created_at).total_seconds()
            if age >= self._config.exit_ttl_seconds:
                remaining_qty = max(0.0, exit_order.quantity - exit_order.executed_quantity)
                await self._cancel_order(exit_order, "exit_ttl_expired")
                if remaining_qty > 0 and self._position and not self._shutdown_requested:
                    await self._place_exit_order(market, remaining_qty, stop_mode=self._stop_mode)

    async def _manage_position(self, market: MarketSnapshot) -> None:
        if self._shutdown_requested:
            return
        if not self._position:
            return
        if self._risk.stop_loss_triggered(self._position.entry_price, market.mid_price):
            if not self._stop_mode:
                LOGGER.warning("Stop-loss triggered at mid %.8f", market.mid_price)
                say(
                    "Stop-loss triggered: price moved against us. Canceling profit exit and exiting to limit loss."
                )
            self._stop_mode = True
            if self._exit_order and self._exit_order.status in {"NEW", "PARTIALLY_FILLED"}:
                await self._cancel_order(self._exit_order, "stop_loss_reprice")
            if self.open_order_count == 0:
                await self._place_exit_order(market, self._position.quantity, stop_mode=True)
            return

        if not self._exit_order and self.open_order_count == 0:
            await self._place_exit_order(market, self._position.quantity, stop_mode=False)

    async def _poll_orders_loop(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await asyncio.sleep(self._config.order_poll_interval)
                async with self._lock:
                    await self._refresh_open_orders()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.exception("Order polling failed: %s", exc)

    async def _refresh_order(self, local_order: OrderState) -> None:
        if self._config.dry_run or local_order.status in {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}:
            return

        try:
            payload = await self._client.get_order(self._config.symbol, local_order.client_order_id)
        except RuntimeError as exc:
            self._track_runtime_error(exc, context="get_order")
            LOGGER.warning("Order refresh failed for %s: %s", local_order.client_order_id, exc)
            return
        previous_executed = local_order.executed_quantity
        local_order.exchange_order_id = payload.get("orderId", local_order.exchange_order_id)
        local_order.status = payload.get("status", local_order.status)
        local_order.executed_quantity = float(payload.get("executedQty", local_order.executed_quantity))
        local_order.price = self._execution_price(payload, local_order.price)
        local_order.updated_at = self._now()
        local_order.raw = payload
        await self._datastore.log_order(self._config.symbol, local_order)

        delta_qty = max(0.0, local_order.executed_quantity - previous_executed)
        if delta_qty > 0:
            self._metrics.record_fill(local_order.client_order_id)
            await self._datastore.log_fill(
                self._config.symbol,
                local_order.client_order_id,
                local_order.side.value,
                local_order.price,
                delta_qty,
                None,
                payload,
            )
            if local_order.purpose in {OrderPurpose.EXIT, OrderPurpose.STOP}:
                await self._apply_exit_fill(local_order, delta_qty)

        if local_order.purpose is OrderPurpose.ENTRY:
            if local_order.status == "FILLED":
                await self._activate_position_from_entry()
            elif local_order.status in {"CANCELED", "EXPIRED"} and local_order.executed_quantity > 0:
                await self._activate_position_from_entry()
            elif local_order.status in {"CANCELED", "EXPIRED", "REJECTED"}:
                self._entry_order = None
                self._reset_entry_attempt(now=self._now(), backoff=False)
        elif local_order.purpose in {OrderPurpose.EXIT, OrderPurpose.STOP}:
            if local_order.status == "FILLED":
                await self._close_position(local_order)
            elif local_order.status in {"CANCELED", "REJECTED", "EXPIRED"}:
                self._exit_order = None

    async def _activate_position_from_entry(self) -> None:
        if not self._entry_order or self._entry_order.executed_quantity <= 0:
            self._entry_order = None
            return
        self._position = PositionState(
            quantity=self._normalize_quantity(self._entry_order.executed_quantity),
            entry_price=self._entry_order.price,
            opened_at=self._now(),
            entry_order_id=self._entry_order.client_order_id,
            initial_quantity=self._normalize_quantity(self._entry_order.executed_quantity),
        )
        if self._position.quantity <= 0:
            LOGGER.warning("Ignoring dust-sized entry fill for %s", self._entry_order.client_order_id)
            self._position = None
            self._entry_order = None
            self._reset_entry_attempt(now=self._now(), backoff=False)
            return
        self._entry_fills += 1
        LOGGER.info("Entry filled qty=%.8f price=%.8f", self._position.quantity, self._position.entry_price)
        say("Entry filled. Now placing an exit SELL to take profit.")
        self._entry_order = None
        self._reset_entry_attempt(now=self._now(), backoff=False)
        self._entry_requote_count = 0
        self._exit_attempt_started_at = self._now()
        self._exit_attempts = 0
        self._exit_requote_count = 0
        self._exit_requote_escalated = False
        if self._current_market:
            await self._place_exit_order(self._current_market, self._position.quantity, stop_mode=False)

    async def _place_exit_order(self, market: MarketSnapshot, quantity: float, *, stop_mode: bool) -> None:
        if self._shutdown_requested:
            return
        quantity = self._normalize_quantity(quantity)
        if quantity <= 0:
            return
        if not self._market_is_sane_for_order(reason_context="exit"):
            return
        if stop_mode:
            best_bid, _ = await self._latest_best_bid_ask()
            if best_bid is None:
                return
            await self._submit_order(
                purpose=OrderPurpose.STOP,
                side=Side.SELL,
                quantity=quantity,
                price=self._quantize_price(best_bid),
                maker_only=False,
            )
            return
        if self._position is None:
            return
        if self._exit_attempt_started_at is None:
            self._exit_attempt_started_at = self._position.opened_at
        elapsed_seconds = max(0.0, (self._now() - self._exit_attempt_started_at).total_seconds())
        vol_bps = self._last_decision.volatility_bps if self._last_decision else 0.0
        offset_ticks = self.choose_offset_ticks(
            purpose=OrderPurpose.EXIT,
            spread_bps=market.spread_bps,
            vol_bps=vol_bps,
            default_offset=self._config.maker_offset_ticks,
        )
        if elapsed_seconds >= 6.0:
            offset_ticks = 0
            say(
                "Exit attempt has been open for {elapsed:.1f}s, tightening to offset_ticks=0 for faster fills.",
                elapsed=elapsed_seconds,
            )
        if elapsed_seconds >= 10.0:
            if self._config.allow_taker_exit:
                best_bid, best_ask = await self._latest_cached_best_bid_ask()
                if best_bid is None or best_ask is None:
                    self._human_skip("quote_unavailable")
                    return
                self._exit_attempts += 1
                say(
                    "Exit has waited {elapsed:.1f}s. Escalating to LIMIT exit at best_bid for faster flattening.",
                    elapsed=elapsed_seconds,
                )
                await self._submit_order(
                    purpose=OrderPurpose.EXIT,
                    side=Side.SELL,
                    quantity=quantity,
                    price=self._quantize_price(best_bid),
                    maker_only=False,
                )
                return
            say(
                "Exit not filling; staying maker-only (ALLOW_TAKER_EXIT=false). This may reduce frequency."
            )
        self._exit_attempts += 1
        await self._submit_exit_limit_maker(
            quantity=quantity,
            target_price=self._maker_exit_price(market),
            spread_bps=market.spread_bps,
            vol_bps=vol_bps,
            offset_ticks=offset_ticks,
        )

    async def _close_position(self, order: OrderState) -> None:
        LOGGER.info("Exit order completed status=%s id=%s", order.status, order.client_order_id)
        self._exit_order = None
        if self._position and self._position.quantity <= 0 and not self._position.summary_logged:
            self._position.summary_logged = True
            self._metrics.record_trade_closed(self._position.realized_pnl)
            self._update_loss_streak(self._position.realized_pnl)
            quantity = self._position.closed_quantity or self._position.initial_quantity or order.executed_quantity
            exit_price = order.price
            if self._position.closed_quantity > 0:
                exit_price = self._position.closed_notional / self._position.closed_quantity
            duration_seconds = max(0.0, (self._now() - self._position.opened_at).total_seconds())
            pnl_label = f"{self._position.realized_pnl:+.5f}"
            message = (
                "TRADE CLOSED | pnl=%s USDT | entry=%.2f | exit=%.2f | qty=%.8f | duration=%.1fs | daily_pnl=%.4f"
            )
            if self._position.realized_pnl < 0:
                message += " | LOSS"
            LOGGER.info(
                message,
                pnl_label,
                self._position.entry_price,
                exit_price,
                quantity,
                duration_seconds,
                self.daily_realized_pnl,
            )
            say(
                "Trade closed. PnL = {pnl:+.5f} USDT. Daily total = {daily_pnl:.5f} USDT.",
                pnl=self._position.realized_pnl,
                daily_pnl=self.daily_realized_pnl,
            )
            say(
                "Trade recap: entered at {entry:.2f}, exited at {exit:.2f}, held for {duration:.1f}s.",
                entry=self._position.entry_price,
                exit=exit_price,
                duration=duration_seconds,
            )
        if not self._position or self._position.quantity <= 0:
            self._position = None
            self._stop_mode = False
            self._exit_attempt_started_at = None
            self._exit_attempts = 0
            self._exit_requote_count = 0
            self._exit_requote_escalated = False
            await self._capture_balances(force=True)

    async def _submit_order(
        self,
        *,
        purpose: OrderPurpose,
        side: Side,
        quantity: float,
        price: float,
        maker_only: bool,
        maker_offset_ticks: int | None = None,
        maker_join_mode: bool = False,
    ) -> OrderState | None:
        if self._shutdown_requested:
            return None
        quantity_str = self._format_quantity(quantity, self._symbol_rules.step_size)
        client_order_id = self._new_client_order_id(purpose)
        created_at = self._now()
        submit_price = price
        price_str = self._format_price(price)
        offset_ticks = max(0, self._config.maker_offset_ticks if maker_offset_ticks is None else maker_offset_ticks)

        if not self._market_is_sane_for_order(reason_context=f"{purpose.value.lower()}_{side.value.lower()}"):
            return None

        if maker_only:
            maker_context = await self._prepare_maker_price(
                side=side,
                desired_price=price,
                base_offset_ticks=offset_ticks,
                allow_join_mode=maker_join_mode,
            )
            if maker_context is None:
                return None
            submit_price = maker_context.adjusted_price
            price_str = self._format_price(submit_price)
            LOGGER.info(
                "LIMIT_MAKER guard purpose=%s side=%s desired_price=%.8f adjusted_price=%.8f best_bid=%.8f best_ask=%.8f tick_size=%.8f offset_ticks=%d one_tick_rule=pass",
                purpose.value,
                side.value,
                maker_context.desired_price,
                maker_context.adjusted_price,
                maker_context.best_bid,
                maker_context.best_ask,
                self._symbol_rules.tick_size,
                maker_context.offset_ticks,
            )
            LOGGER.info(
                "LIMIT_MAKER guard delta purpose=%s side=%s desired_price=%.8f adjusted_price=%.8f delta_ticks=%.3f best_bid=%.8f best_ask=%.8f tick_size=%.8f offset_ticks=%d",
                purpose.value,
                side.value,
                maker_context.desired_price,
                maker_context.adjusted_price,
                maker_context.delta_ticks,
                maker_context.best_bid,
                maker_context.best_ask,
                self._symbol_rules.tick_size,
                maker_context.offset_ticks,
            )
        try:
            payload = await self._client.place_limit_order(
                self._config.symbol,
                side.value,
                quantity_str,
                price_str,
                client_order_id=client_order_id,
                maker_only=maker_only,
            )
        except RuntimeError as exc:
            self._track_runtime_error(exc, context=f"submit_{purpose.value.lower()}")
            if maker_only and self._is_binance_code(exc, -2010):
                self._count_2010 += 1
                await self._apply_rejection_controls(reason="maker_rejection_-2010", code=-2010)
                LOGGER.warning(
                    "LIMIT_MAKER rejected code=-2010 purpose=%s side=%s desired_price=%.8f adjusted_price=%.8f error=%s",
                    purpose.value,
                    side.value,
                    price,
                    submit_price,
                    exc,
                )
                say(
                    "Maker order rejected (-2010). This usually means our price would have executed immediately. We will NOT force it; skipping/requoting safely."
                )
                await self._record_rejected_order(
                    purpose=purpose,
                    side=side,
                    client_order_id=client_order_id,
                    quantity=quantity,
                    price=submit_price,
                    created_at=created_at,
                    order_type="LIMIT_MAKER",
                    reason=str(exc),
                )
                return None
            if self._is_binance_code(exc, -1021):
                await self._apply_rejection_controls(reason="timestamp_error_-1021", code=-1021)
            LOGGER.warning("Order submission rejected for %s %s: %s", purpose.value, side.value, exc)
            await self._record_rejected_order(
                purpose=purpose,
                side=side,
                client_order_id=client_order_id,
                quantity=quantity,
                price=submit_price,
                created_at=created_at,
                order_type="LIMIT_MAKER" if maker_only else "LIMIT",
                reason=str(exc),
            )
            return None
        self._consecutive_rejections = 0
        return await self._handle_submitted_order(
            purpose=purpose,
            side=side,
            quantity=quantity,
            price=submit_price,
            maker_only=maker_only,
            client_order_id=client_order_id,
            created_at=created_at,
            payload=payload,
            quantity_str=quantity_str,
            price_str=price_str,
        )

    async def _submit_exit_limit_maker(
        self,
        *,
        quantity: float,
        target_price: float,
        spread_bps: float,
        vol_bps: float,
        offset_ticks: int,
    ) -> None:
        best_bid, best_ask = await self._latest_cached_best_bid_ask()
        if best_bid is None or best_ask is None:
            self._human_skip("quote_unavailable")
            return
        if best_ask <= best_bid:
            LOGGER.info(
                "Exit LIMIT_MAKER skipped because cached quote is invalid bid=%.8f ask=%.8f",
                best_bid,
                best_ask,
            )
            self._human_skip("invalid_cached_quote")
            return
        say(
            "Exit attempt {attempt}: placing maker SELL with offset_ticks={offset}.",
            attempt=self._exit_attempts,
            offset=offset_ticks,
        )
        say(
            "Using offset_ticks={offset} because spread={spread_bps:.3f} bps, vol={vol_bps:.3f} bps.",
            offset=offset_ticks,
            spread_bps=spread_bps,
            vol_bps=vol_bps,
        )
        await self._submit_order(
            purpose=OrderPurpose.EXIT,
            side=Side.SELL,
            quantity=quantity,
            price=target_price,
            maker_only=True,
            maker_offset_ticks=offset_ticks,
        )

    async def _maybe_requote_exit_order(self, market: MarketSnapshot) -> None:
        if self._shutdown_requested:
            return
        if self._exit_requote_escalated:
            return
        if not self._exit_order or self._exit_order.status not in {"NEW", "PARTIALLY_FILLED"}:
            return
        now = self._now()
        if now < self._next_exit_reprice_at:
            return
        self._next_exit_reprice_at = now + timedelta(seconds=self._EXIT_REPRICE_INTERVAL_SECONDS)

        exit_order = self._exit_order
        remaining_qty = self._normalize_quantity(exit_order.quantity - exit_order.executed_quantity)
        if remaining_qty <= 0:
            return

        if self._exit_requote_count >= self._config.exit_max_requotes:
            LOGGER.warning(
                "Exit requote cap reached count=%d max=%d; escalating exit behavior",
                self._exit_requote_count,
                self._config.exit_max_requotes,
            )
            self._exit_requote_escalated = True
            await self._cancel_order(exit_order, "exit_requote_cap_reached")
            if self._config.allow_taker_exit:
                best_bid, best_ask = await self._latest_cached_best_bid_ask()
                if best_bid is None or best_ask is None:
                    self._human_skip("quote_unavailable")
                    return
                taker_price = floor_to_tick(best_bid, self._symbol_rules.tick_size)
                say(
                    "Exit requote cap reached ({count}); escalating to non-maker LIMIT at best_bid for immediate exit.",
                    count=self._exit_requote_count,
                )
                await self._submit_order(
                    purpose=OrderPurpose.EXIT,
                    side=Side.SELL,
                    quantity=remaining_qty,
                    price=taker_price,
                    maker_only=False,
                )
                return
            vol_bps = self._last_decision.volatility_bps if self._last_decision else 0.0
            say(
                "Exit requote cap reached ({count}) with ALLOW_TAKER_EXIT=false; escalating to maker offset_ticks=0.",
                count=self._exit_requote_count,
            )
            await self._submit_exit_limit_maker(
                quantity=remaining_qty,
                target_price=self._maker_exit_price(market),
                spread_bps=market.spread_bps,
                vol_bps=vol_bps,
                offset_ticks=0,
            )
            return

        tick = self._symbol_rules.tick_size
        target_price = self._maker_exit_price(market)
        if tick > 0 and abs(target_price - exit_order.price) < tick:
            return

        self._exit_requote_count += 1
        self._exit_requotes_total += 1
        old_price = exit_order.price
        await self._cancel_order(exit_order, "best_ask_moved")
        await self._place_exit_order(market, remaining_qty, stop_mode=False)
        if self._exit_order is not None:
            LOGGER.info(
                "Repriced EXIT SELL from %.8f to %.8f reason=best_ask_moved",
                old_price,
                self._exit_order.price,
            )

    async def _handle_submitted_order(
        self,
        *,
        purpose: OrderPurpose,
        side: Side,
        quantity: float,
        price: float,
        maker_only: bool,
        client_order_id: str,
        created_at: datetime,
        payload: dict,
        quantity_str: str,
        price_str: str,
    ) -> OrderState:
        order_state = OrderState(
            purpose=purpose,
            side=side,
            client_order_id=payload.get("clientOrderId", client_order_id),
            exchange_order_id=payload.get("orderId"),
            price=self._execution_price(payload, price),
            quantity=float(payload.get("origQty", quantity)),
            executed_quantity=float(payload.get("executedQty", 0.0)),
            status=payload.get("status", "NEW"),
            order_type=payload.get("type", "LIMIT_MAKER" if maker_only else "LIMIT"),
            created_at=created_at,
            updated_at=created_at,
            raw=payload,
        )
        self._metrics.record_order()
        await self._datastore.log_order(self._config.symbol, order_state)
        if order_state.executed_quantity > 0:
            self._metrics.record_fill(order_state.client_order_id)
            await self._datastore.log_fill(
                self._config.symbol,
                order_state.client_order_id,
                order_state.side.value,
                order_state.price,
                order_state.executed_quantity,
                None,
                payload,
            )
            if purpose in {OrderPurpose.EXIT, OrderPurpose.STOP}:
                await self._apply_exit_fill(order_state, order_state.executed_quantity)
        LOGGER.info(
            "Submitted %s %s qty=%s price=%s type=%s",
            purpose.value,
            side.value,
            quantity_str,
            price_str,
            order_state.order_type,
        )
        if purpose is OrderPurpose.ENTRY:
            say(
                "Entry order placed. Waiting up to {ttl} seconds for a fill.",
                ttl=self._config.entry_ttl_seconds,
            )
        elif purpose in {OrderPurpose.EXIT, OrderPurpose.STOP}:
            say(
                "Exit order placed. Waiting up to {ttl} seconds for a fill.",
                ttl=self._config.exit_ttl_seconds,
            )
        if purpose is OrderPurpose.ENTRY:
            self._entry_order = order_state
            if self._entry_attempt_started_at is None:
                self._entry_attempt_started_at = created_at
                self._entry_requote_count = 0
            self._next_entry_reprice_at = created_at + timedelta(seconds=self._ENTRY_REPRICE_INTERVAL_SECONDS)
        else:
            self._exit_order = order_state
            self._next_exit_reprice_at = created_at + timedelta(seconds=self._EXIT_REPRICE_INTERVAL_SECONDS)
        if purpose is OrderPurpose.ENTRY and order_state.status == "FILLED":
            await self._activate_position_from_entry()
        if purpose in {OrderPurpose.EXIT, OrderPurpose.STOP} and order_state.status == "FILLED":
            await self._close_position(order_state)
        return order_state

    async def _cancel_order(self, order: OrderState, reason: str) -> None:
        if self._config.dry_run or order.status in {"CANCELED", "FILLED", "EXPIRED", "REJECTED"}:
            return
        try:
            payload = await self._client.cancel_order(self._config.symbol, order.client_order_id)
        except RuntimeError as exc:
            self._track_runtime_error(exc, context="cancel_order")
            if self._is_binance_code(exc, -2011) or "Unknown order sent" in str(exc):
                LOGGER.info("Cancel ignored for %s; exchange says order is unknown/already closed", order.client_order_id)
                say("Cancel update: order was already closed on exchange, continuing safely.")
                order.status = "CANCELED"
                order.updated_at = self._now()
                if order.purpose is OrderPurpose.ENTRY:
                    self._entry_order = None
                else:
                    self._exit_order = None
                return
            LOGGER.warning("Cancel failed for %s: %s", order.client_order_id, exc)
            await self._refresh_order(order)
            return
        order.status = payload.get("status", "CANCELED")
        order.updated_at = self._now()
        order.raw = payload
        await self._datastore.log_cancel(self._config.symbol, order.client_order_id, reason, payload)
        await self._datastore.log_order(self._config.symbol, order)
        self._metrics.record_cancel()
        LOGGER.info("Canceled order %s reason=%s", order.client_order_id, reason)
        if order.purpose is OrderPurpose.ENTRY:
            self._entry_order = None
        else:
            self._exit_order = None

    async def _capture_balances(self, *, force: bool) -> None:
        if self._config.dry_run:
            return
        now = self._now()
        if not force and (now - self._last_balance_snapshot) < timedelta(seconds=self._config.balance_snapshot_interval):
            return
        balances = await self._client.get_balances()
        if balances:
            await self._datastore.log_balances(balances)
        self._last_balance_snapshot = now

    def _maker_exit_price(self, market: MarketSnapshot) -> float:
        tick = self._symbol_rules.tick_size
        if market.ask_price - market.bid_price > tick:
            return floor_to_tick(market.ask_price - tick, tick)
        return floor_to_tick(market.ask_price, tick)

    def _quantize_price(self, value: float) -> float:
        return floor_to_tick(value, self._symbol_rules.tick_size)

    def _quantize_price_for_side(self, value: float, side: Side) -> float:
        if side is Side.BUY:
            return floor_to_tick(value, self._symbol_rules.tick_size)
        return ceil_to_tick(value, self._symbol_rules.tick_size)

    def _quantize_quantity(self, value: float) -> float:
        return float(self._quantize(value, self._symbol_rules.step_size))

    @staticmethod
    def _quantize(value: float, step: float) -> Decimal:
        if step <= 0:
            return Decimal(str(value))
        step_dec = Decimal(str(step))
        value_dec = Decimal(str(value))
        return value_dec.quantize(step_dec, rounding=ROUND_DOWN)

    @staticmethod
    def _format_quantity(value: float, step: float) -> str:
        normalized = ExecutionEngine._quantize(value, step)
        return format(normalized.normalize(), "f")

    @staticmethod
    def _format_price(value: float) -> str:
        return format(Decimal(str(value)).normalize(), "f")

    @staticmethod
    def _new_client_order_id(purpose: OrderPurpose) -> str:
        return f"{purpose.value.lower()}_{uuid.uuid4().hex[:24]}"

    async def _apply_exit_fill(self, order: OrderState, delta_qty: float) -> None:
        if not self._position or delta_qty <= 0:
            return
        realized = (order.price - self._position.entry_price) * delta_qty
        fill_time = self._now()
        self._roll_daily_realized_pnl(fill_time.date())
        remaining_qty = self._normalize_quantity(max(0.0, self._position.quantity - delta_qty))
        await self._datastore.log_pnl(
            self._config.symbol,
            realized_pnl_usd=realized,
            cumulative_pnl_usd=self._risk.realized_pnl_today + realized,
            payload={
                "entry_order_id": self._position.entry_order_id,
                "exit_order_id": order.client_order_id,
                "entry_price": self._position.entry_price,
                "exit_price": order.price,
                "quantity": delta_qty,
            },
        )
        self._risk.record_realized_pnl(realized)
        self._metrics.record_realized_pnl(realized)
        self.daily_realized_pnl += realized
        self._position.closed_quantity += delta_qty
        self._position.closed_notional += order.price * delta_qty
        self._position.realized_pnl += realized
        self._position.quantity = remaining_qty
        LOGGER.info("Exit fill qty=%.8f realized_pnl=%.8f remaining=%.8f", delta_qty, realized, remaining_qty)

    @staticmethod
    def _execution_price(payload: dict, fallback: float) -> float:
        executed_qty = float(payload.get("executedQty", 0.0) or 0.0)
        cumulative_quote_qty = float(payload.get("cummulativeQuoteQty", 0.0) or 0.0)
        if executed_qty > 0 and cumulative_quote_qty > 0:
            return cumulative_quote_qty / executed_qty
        return float(payload.get("price", fallback))

    async def _record_rejected_order(
        self,
        *,
        purpose: OrderPurpose,
        side: Side,
        client_order_id: str,
        quantity: float,
        price: float,
        created_at: datetime,
        order_type: str,
        reason: str,
    ) -> None:
        order_state = OrderState(
            purpose=purpose,
            side=side,
            client_order_id=client_order_id,
            exchange_order_id=None,
            price=price,
            quantity=quantity,
            executed_quantity=0.0,
            status="REJECTED",
            order_type=order_type,
            created_at=created_at,
            updated_at=created_at,
            raw={"error": reason},
        )
        await self._datastore.log_order(self._config.symbol, order_state)

    def _normalize_quantity(self, quantity: float) -> float:
        normalized = self._quantize_quantity(quantity)
        if normalized < self._symbol_rules.step_size:
            return 0.0
        return normalized

    async def _refresh_open_orders(self) -> None:
        if self._entry_order:
            await self._refresh_order(self._entry_order)
        if self._exit_order:
            await self._refresh_order(self._exit_order)

    async def _submit_entry_order(
        self,
        *,
        quantity: float,
        best_bid: float,
        best_ask: float,
        spread_bps: float,
        vol_bps: float,
    ) -> OrderState | None:
        entry_price = self._quantize_price(best_bid)
        offset_ticks = self._dynamic_entry_offset_ticks(spread_bps)
        self._last_entry_offset_ticks = offset_ticks
        join_mode = spread_bps <= self._config.spread_tight_bps
        LOGGER.info(
            "Submitting ENTRY BUY best_bid=%.8f best_ask=%.8f spread_bps=%.3f tick_size=%.8f entry_price=%.8f",
            best_bid,
            best_ask,
            spread_bps,
            self._symbol_rules.tick_size,
            entry_price,
        )
        say(
            "Entry check: spread={spread_bps:.3f} bps, vol={vol_bps:.3f} bps, offset_ticks={offset}.",
            spread_bps=spread_bps,
            vol_bps=vol_bps,
            offset=offset_ticks,
        )
        say(
            "Quoting ENTRY BUY at {entry_price:.8f} (best_bid={best_bid:.8f}, spread={spread_bps:.3f} bps, offset_ticks={offset}, join_mode={join_mode}).",
            entry_price=entry_price,
            best_bid=best_bid,
            spread_bps=spread_bps,
            offset=offset_ticks,
            join_mode=join_mode,
        )
        return await self._submit_order(
            purpose=OrderPurpose.ENTRY,
            side=Side.BUY,
            quantity=quantity,
            price=entry_price,
            maker_only=True,
            maker_offset_ticks=offset_ticks,
            maker_join_mode=join_mode,
        )

    async def _prepare_maker_price(
        self,
        *,
        side: Side,
        desired_price: float,
        base_offset_ticks: int,
        allow_join_mode: bool = False,
    ) -> "_MakerPriceContext | None":
        best_bid, best_ask = await self._latest_cached_best_bid_ask()
        if best_bid is None or best_ask is None:
            LOGGER.warning(
                "LIMIT_MAKER price guard skipped because cached quote is unavailable side=%s desired_price=%.8f",
                side.value,
                desired_price,
            )
            self._human_skip("quote_unavailable")
            return None
        if best_ask <= best_bid:
            LOGGER.warning(
                "LIMIT_MAKER price guard skipped because cached quote is invalid side=%s bid=%.8f ask=%.8f",
                side.value,
                best_bid,
                best_ask,
            )
            self._human_skip("invalid_cached_quote")
            return None
        adjusted_price = self.maker_safe_price(
            side=side,
            desired_price=desired_price,
            best_bid=best_bid,
            best_ask=best_ask,
            tick_size=self._symbol_rules.tick_size,
            offset_ticks=base_offset_ticks,
            allow_join_mode=allow_join_mode,
        )
        if adjusted_price is None:
            LOGGER.warning(
                "LIMIT_MAKER price refused by hard safety side=%s desired_price=%.8f best_bid=%.8f best_ask=%.8f tick_size=%.8f offset_ticks=%d",
                side.value,
                desired_price,
                best_bid,
                best_ask,
                self._symbol_rules.tick_size,
                max(0, base_offset_ticks),
            )
            say(
                "Refusing maker order: rounded price would risk immediate execution, so this cycle is skipped."
            )
            return None
        if side is Side.BUY:
            delta_ticks = (desired_price - adjusted_price) / self._symbol_rules.tick_size if self._symbol_rules.tick_size > 0 else 0.0
        else:
            delta_ticks = (adjusted_price - desired_price) / self._symbol_rules.tick_size if self._symbol_rules.tick_size > 0 else 0.0
        if abs(adjusted_price - desired_price) >= self._symbol_rules.tick_size:
            say(
                "Adjusted maker price away from market to stay post-only (desired={desired:.8f} -> safe={safe:.8f}).",
                desired=desired_price,
                safe=adjusted_price,
            )
        return _MakerPriceContext(
            desired_price=desired_price,
            adjusted_price=adjusted_price,
            delta_ticks=delta_ticks,
            best_bid=best_bid,
            best_ask=best_ask,
            offset_ticks=max(0, base_offset_ticks),
        )

    def maker_safe_price(
        self,
        *,
        side: Side,
        desired_price: float,
        best_bid: float,
        best_ask: float,
        tick_size: float | None = None,
        offset_ticks: int = 0,
        allow_join_mode: bool = False,
    ) -> float | None:
        tick = float(self._symbol_rules.tick_size if tick_size is None else tick_size)
        if tick <= 0:
            return desired_price
        safe_offset = tick * max(0, int(offset_ticks))

        if side is Side.BUY:
            raw = min(desired_price, best_bid - safe_offset)
            adjusted = floor_to_tick(raw, tick)
            if allow_join_mode and adjusted < (best_bid - tick):
                join_candidate = floor_to_tick(best_bid, tick)
                if join_candidate >= best_ask:
                    join_candidate = floor_to_tick(best_ask - tick, tick)
                join_safe = join_candidate > 0 and join_candidate < best_ask
                if join_safe:
                    LOGGER.info(
                        "maker_safe_price side=BUY join_mode=on switching to best_bid quote adjusted=%.8f join_candidate=%.8f best_bid=%.8f best_ask=%.8f",
                        adjusted,
                        join_candidate,
                        best_bid,
                        best_ask,
                    )
                    adjusted = join_candidate
            if allow_join_mode:
                hard_safe = adjusted <= best_bid and adjusted < best_ask
            else:
                hard_safe = adjusted <= (best_bid - tick) and adjusted < best_ask
            LOGGER.info(
                "maker_safe_price side=BUY raw=%.8f adjusted=%.8f rounding=floor_to_tick hard_safe=%s best_bid=%.8f best_ask=%.8f tick_size=%.8f offset_ticks=%d join_mode=%s",
                raw,
                adjusted,
                hard_safe,
                best_bid,
                best_ask,
                tick,
                max(0, int(offset_ticks)),
                allow_join_mode,
            )
            return adjusted if hard_safe else None

        raw = max(desired_price, best_ask + safe_offset)
        adjusted = ceil_to_tick(raw, tick)
        hard_safe = adjusted >= (best_ask + tick) and adjusted > best_bid
        LOGGER.info(
            "maker_safe_price side=SELL raw=%.8f adjusted=%.8f rounding=ceil_to_tick hard_safe=%s best_bid=%.8f best_ask=%.8f tick_size=%.8f offset_ticks=%d",
            raw,
            adjusted,
            hard_safe,
            best_bid,
            best_ask,
            tick,
            max(0, int(offset_ticks)),
        )
        return adjusted if hard_safe else None

    def _ingest_market(self, market: MarketSnapshot) -> None:
        self._last_best_bid = market.bid_price
        self._last_best_ask = market.ask_price
        self._last_bid_update_ts = market.bid_update_time or market.event_time
        self._last_ask_update_ts = market.ask_update_time or market.event_time
        self._last_quote_update_ts = max(self._last_bid_update_ts, self._last_ask_update_ts)

    def _market_is_sane_for_order(self, *, reason_context: str) -> bool:
        best_bid = self._last_best_bid
        best_ask = self._last_best_ask
        quote_update_ts = self._last_quote_update_ts
        now = self._now()
        if best_bid is None or best_ask is None:
            self._warn_market_skip(reason_context, "quote_unavailable")
            return False
        if best_bid <= 0:
            self._warn_market_skip(reason_context, "best_bid_non_positive")
            return False
        if best_ask <= 0:
            self._warn_market_skip(reason_context, "best_ask_non_positive")
            return False
        if best_bid >= best_ask:
            self._warn_market_skip(reason_context, "crossed_market")
            return False
        spread_bps = self._spread_bps(best_bid, best_ask)
        if self._config.max_spread_bps > 0 and spread_bps > self._config.max_spread_bps:
            self._warn_market_skip(reason_context, f"spread_bps={spread_bps:.3f}")
            return False
        if quote_update_ts is None or (now - quote_update_ts) > self._market_stale_after:
            self._warn_market_skip(reason_context, "quote_stale")
            return False
        return True

    def _warn_market_skip(self, reason_context: str, reason: str) -> None:
        LOGGER.warning(
            "Market sanity check failed  skipping trade context=%s reason=%s best_bid=%.8f best_ask=%.8f",
            reason_context,
            reason,
            self._last_best_bid or 0.0,
            self._last_best_ask or 0.0,
        )
        if reason == "quote_stale":
            self._record_skip_bucket("stale data")
            say("Skipping order: market data looks stale (protecting from accidental taker order).")
            return
        self._human_skip(reason)

    def _human_skip(self, reason: str) -> None:
        say("Skipping: market conditions not suitable right now (reason={reason}).", reason=reason)

    def _record_skip_bucket(self, bucket: str) -> None:
        if bucket in self._skip_reason_counts:
            self._skip_reason_counts[bucket] += 1

    def _most_common_skip_bucket(self) -> str:
        order = ("stale data", "spread too small", "rejection", "ttl expired")
        return max(order, key=lambda key: self._skip_reason_counts.get(key, 0))

    def _maybe_emit_human_status(self, now: datetime) -> None:
        if self._metrics.stats.total_trades > 0:
            return
        if self._next_human_status_at is None:
            self._next_human_status_at = now + self._HUMAN_STATUS_INTERVAL
            return
        if now < self._next_human_status_at:
            return
        spread_bps = 0.0
        if self._last_best_bid is not None and self._last_best_ask is not None and self._last_best_ask > self._last_best_bid:
            spread_bps = self._spread_bps(self._last_best_bid, self._last_best_ask)
        current_offset = self._dynamic_entry_offset_ticks(spread_bps)
        self._last_entry_offset_ticks = current_offset
        say(
            "Status: no closed trades yet | attempts={attempts} fills={fills} top_skip={top_skip} spread={spread_bps:.3f} bps offset_ticks={offset}.",
            attempts=self._entry_submit_attempts,
            fills=self._entry_fills,
            top_skip=self._most_common_skip_bucket(),
            spread_bps=spread_bps,
            offset=current_offset,
        )
        self._next_human_status_at = now + self._HUMAN_STATUS_INTERVAL

    def _set_rejection_cooldown(self, *, reason: str, code: int | None) -> None:
        cooldown_ms = max(0, int(self._config.rejection_cooldown_ms))
        now = self._now()
        if cooldown_ms > 0:
            self._rejection_cooldown_until = max(
                self._rejection_cooldown_until,
                now + timedelta(milliseconds=cooldown_ms),
            )
        self._record_skip_bucket("rejection")
        say(
            "Rejection cooldown active for {cooldown_ms}ms after {reason} (code={code}).",
            cooldown_ms=cooldown_ms,
            reason=reason,
            code=code if code is not None else "n/a",
        )

    async def _apply_rejection_controls(self, *, reason: str, code: int | None) -> None:
        self._set_rejection_cooldown(reason=reason, code=code)
        self._consecutive_rejections += 1
        threshold = max(1, int(self._config.max_consecutive_rejections))
        if self._consecutive_rejections < threshold:
            return
        self._consecutive_rejections = 0
        self._rejection_pause_until = self._now() + timedelta(seconds=self._REJECTION_PAUSE_SECONDS)
        say(
            "Too many consecutive rejections ({threshold}). Pausing new entry submits for 10 seconds.",
            threshold=threshold,
        )
        await asyncio.sleep(self._REJECTION_PAUSE_SECONDS)

    def _update_loss_streak(self, pnl: float) -> None:
        if pnl < 0:
            self._consecutive_losses += 1
            if self._consecutive_losses >= self._LOSS_STREAK_THRESHOLD:
                self._cooldown_until = self._now() + self._LOSS_STREAK_COOLDOWN
                LOGGER.warning("Loss streak detected  entering cooldown")
                say("Loss streak detected. Pausing new entries for 10 minutes.")
                return
        else:
            self._consecutive_losses = 0

    def _maybe_log_metrics_summary(self, now: datetime) -> None:
        if self._next_metrics_log_at is None:
            self._next_metrics_log_at = now + self._METRICS_LOG_INTERVAL
            return
        if now < self._next_metrics_log_at:
            return
        summary = self._metrics.summary()
        LOGGER.info(
            "Metrics summary total_trades=%d winning_trades=%d losing_trades=%d breakeven_trades=%d avg_pnl=%.8f fill_rate=%.4f max_drawdown=%.8f",
            summary["total_trades"],
            summary["winning_trades"],
            summary["losing_trades"],
            summary["breakeven_trades"],
            summary["avg_pnl_usd"],
            summary["fill_rate"],
            summary["max_drawdown_usd"],
        )
        self._next_metrics_log_at = now + self._METRICS_LOG_INTERVAL

    async def _latest_cached_best_bid_ask(self) -> tuple[float | None, float | None]:
        getter = getattr(self._client, "get_cached_best_bid_ask", None)
        if callable(getter):
            try:
                return await getter(self._config.symbol)
            except RuntimeError as exc:
                self._track_runtime_error(exc, context="get_cached_best_bid_ask")
                LOGGER.warning("Best bid/ask cache refresh failed for entry repricing: %s", exc)
                return None, None
        if self._current_market is None:
            return None, None
        return self._current_market.bid_price, self._current_market.ask_price

    async def _latest_best_bid_ask(self) -> tuple[float | None, float | None]:
        try:
            return await self._client.get_best_bid_ask(self._config.symbol)
        except RuntimeError as exc:
            self._track_runtime_error(exc, context="get_best_bid_ask")
            LOGGER.warning("Best bid/ask refresh failed for exit pricing: %s", exc)
            return None, None

    def _entry_attempt_expired(self, now: datetime) -> bool:
        started_at = self._entry_attempt_started_at
        if started_at is None and self._entry_order is not None:
            started_at = self._entry_order.created_at
        if started_at is None:
            return False
        return (now - started_at).total_seconds() >= self._config.entry_ttl_seconds

    def _entry_reprice_rate_limit_allows(self, now: datetime) -> bool:
        return (now - self._last_entry_reprice_action_at).total_seconds() >= self._ENTRY_REPRICE_RATE_LIMIT_SECONDS

    def _reset_entry_attempt(self, *, now: datetime, backoff: bool) -> None:
        self._entry_attempt_started_at = None
        self._entry_requote_count = 0
        self._next_entry_reprice_at = datetime.min.replace(tzinfo=timezone.utc)
        self._next_entry_attempt_at = now
        if backoff:
            self._entry_backoff_until = now + timedelta(seconds=self._ENTRY_RETRY_BACKOFF_SECONDS)
        else:
            self._entry_backoff_until = datetime.min.replace(tzinfo=timezone.utc)

    def choose_offset_ticks(
        self,
        *,
        purpose: OrderPurpose,
        spread_bps: float,
        vol_bps: float,
        default_offset: int,
    ) -> int:
        _ = default_offset
        if purpose is OrderPurpose.ENTRY:
            offset_ticks = self._dynamic_entry_offset_ticks(spread_bps)
        else:
            offset_ticks = 1
        say(
            "Using offset_ticks={offset} because spread={spread_bps:.3f} bps, vol={vol_bps:.3f} bps.",
            offset=offset_ticks,
            spread_bps=spread_bps,
            vol_bps=vol_bps,
        )
        return offset_ticks

    def _dynamic_entry_offset_ticks(self, spread_bps: float) -> int:
        min_ticks = max(0, int(self._config.offset_ticks_min))
        max_ticks = max(min_ticks, int(self._config.offset_ticks_max))
        tight = float(self._config.spread_tight_bps)
        wide = float(self._config.spread_wide_bps)
        if wide <= tight:
            return max_ticks if spread_bps >= wide else min_ticks
        if spread_bps <= tight:
            return min_ticks
        if spread_bps >= wide:
            return max_ticks
        ratio = (spread_bps - tight) / (wide - tight)
        interpolated = min_ticks + ratio * (max_ticks - min_ticks)
        return int(round(interpolated))

    def _roll_daily_realized_pnl(self, current_date: date) -> None:
        if current_date != self._daily_realized_pnl_date:
            self._daily_realized_pnl_date = current_date
            self.daily_realized_pnl = 0.0

    @staticmethod
    def _spread_bps(best_bid: float, best_ask: float) -> float:
        mid_price = (best_bid + best_ask) / 2
        if mid_price <= 0:
            return 0.0
        return ((best_ask - best_bid) / mid_price) * 10_000

    def _now(self) -> datetime:
        if self._current_market is not None:
            return self._current_market.event_time
        return utc_now()

    @staticmethod
    def _is_binance_code(exc: RuntimeError, code: int) -> bool:
        return f'"code":{code}' in str(exc) or f'"code": {code}' in str(exc)

    def _track_runtime_error(self, exc: RuntimeError, *, context: str) -> None:
        if self._is_binance_code(exc, -1021):
            self._count_1021 += 1
            LOGGER.warning("Binance timestamp error code=-1021 context=%s error=%s", context, exc)
            if not context.startswith("submit_"):
                self._set_rejection_cooldown(reason=f"timestamp_error_{context}", code=-1021)


@dataclass(slots=True)
class _MakerPriceContext:
    desired_price: float
    adjusted_price: float
    delta_ticks: float
    best_bid: float
    best_ask: float
    offset_ticks: int
