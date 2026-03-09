from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .models import BalanceSnapshot, MarketSnapshot, OrderState, StrategyDecision


def _ts(value: datetime | None = None) -> str:
    return (value or datetime.now(timezone.utc)).isoformat()


class DataStore:
    def __init__(self, db_path: str) -> None:
        path = Path(db_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        await self._run(self._initialize_sync)

    async def close(self) -> None:
        await self._run(self._conn.close)

    async def log_signal(self, symbol: str, market: MarketSnapshot, decision: StrategyDecision) -> None:
        payload = {
            "market": market.raw,
            "reasons": decision.reasons,
        }
        await self._run(
            self._conn.execute,
            """
            INSERT INTO signals (
                ts, symbol, should_enter, spread_bps, imbalance_ratio, volatility_bps, reasons, payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _ts(market.event_time),
                symbol,
                int(decision.should_enter),
                decision.spread_bps,
                decision.imbalance_ratio,
                decision.volatility_bps,
                json.dumps(decision.reasons),
                json.dumps(payload),
            ),
        )
        await self._run(self._conn.commit)

    async def log_market_snapshot(self, symbol: str, market: MarketSnapshot, source: str) -> None:
        payload = {
            "market": market.raw,
            "source": source,
            "bid_update_time": _ts(market.bid_update_time) if market.bid_update_time else None,
            "ask_update_time": _ts(market.ask_update_time) if market.ask_update_time else None,
        }
        await self._run(
            self._conn.execute,
            """
            INSERT INTO market_data (
                ts, symbol, source, bid_price, ask_price, bid_volume, ask_volume,
                mid_price, spread, spread_bps, imbalance_ratio, payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _ts(market.event_time),
                symbol,
                source,
                market.bid_price,
                market.ask_price,
                market.bid_volume,
                market.ask_volume,
                market.mid_price,
                market.spread,
                market.spread_bps,
                market.imbalance_ratio,
                json.dumps(payload, default=str),
            ),
        )
        await self._run(self._conn.commit)

    async def log_order(self, symbol: str, order: OrderState) -> None:
        await self._run(
            self._conn.execute,
            """
            INSERT INTO orders (
                ts, symbol, client_order_id, exchange_order_id, purpose, side, order_type,
                price, quantity, executed_quantity, status, is_dry_run, payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _ts(order.updated_at),
                symbol,
                order.client_order_id,
                order.exchange_order_id,
                order.purpose.value,
                order.side.value,
                order.order_type,
                order.price,
                order.quantity,
                order.executed_quantity,
                order.status,
                int(order.is_dry_run),
                json.dumps(order.raw, default=str),
            ),
        )
        await self._run(self._conn.commit)

    async def log_cancel(self, symbol: str, client_order_id: str, reason: str, payload: dict[str, Any]) -> None:
        await self._run(
            self._conn.execute,
            """
            INSERT INTO cancels (ts, symbol, client_order_id, reason, payload)
            VALUES (?, ?, ?, ?, ?)
            """,
            (_ts(), symbol, client_order_id, reason, json.dumps(payload, default=str)),
        )
        await self._run(self._conn.commit)

    async def log_fill(
        self,
        symbol: str,
        client_order_id: str,
        side: str,
        price: float,
        quantity: float,
        realized_pnl_usd: float | None,
        payload: dict[str, Any],
    ) -> None:
        await self._run(
            self._conn.execute,
            """
            INSERT INTO fills (ts, symbol, client_order_id, side, price, quantity, realized_pnl_usd, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _ts(),
                symbol,
                client_order_id,
                side,
                price,
                quantity,
                realized_pnl_usd,
                json.dumps(payload, default=str),
            ),
        )
        await self._run(self._conn.commit)

    async def log_pnl(
        self,
        symbol: str,
        realized_pnl_usd: float,
        cumulative_pnl_usd: float,
        payload: dict[str, Any],
    ) -> None:
        await self._run(
            self._conn.execute,
            """
            INSERT INTO pnl (ts, symbol, realized_pnl_usd, cumulative_pnl_usd, payload)
            VALUES (?, ?, ?, ?, ?)
            """,
            (_ts(), symbol, realized_pnl_usd, cumulative_pnl_usd, json.dumps(payload, default=str)),
        )
        await self._run(self._conn.commit)

    async def log_balances(self, balances: list[BalanceSnapshot]) -> None:
        rows = [
            (
                _ts(balance.captured_at),
                balance.asset,
                balance.free,
                balance.locked,
                json.dumps({"asset": balance.asset, "free": balance.free, "locked": balance.locked}),
            )
            for balance in balances
        ]
        await self._run(
            self._conn.executemany,
            """
            INSERT INTO balances (ts, asset, free, locked, payload)
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )
        await self._run(self._conn.commit)

    async def load_market_snapshots(
        self,
        symbol: str,
        *,
        source: str | None = None,
        limit: int | None = None,
    ) -> list[MarketSnapshot]:
        return await self._run(self._load_market_snapshots_sync, symbol, source, limit)

    async def _run(self, func, *args):
        async with self._lock:
            return await asyncio.to_thread(func, *args)

    def _initialize_sync(self) -> None:
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                should_enter INTEGER NOT NULL,
                spread_bps REAL NOT NULL,
                imbalance_ratio REAL NOT NULL,
                volatility_bps REAL NOT NULL,
                reasons TEXT NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS market_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                source TEXT NOT NULL,
                bid_price REAL NOT NULL,
                ask_price REAL NOT NULL,
                bid_volume REAL NOT NULL,
                ask_volume REAL NOT NULL,
                mid_price REAL NOT NULL,
                spread REAL NOT NULL,
                spread_bps REAL NOT NULL,
                imbalance_ratio REAL NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                client_order_id TEXT NOT NULL,
                exchange_order_id INTEGER,
                purpose TEXT NOT NULL,
                side TEXT NOT NULL,
                order_type TEXT NOT NULL,
                price REAL NOT NULL,
                quantity REAL NOT NULL,
                executed_quantity REAL NOT NULL,
                status TEXT NOT NULL,
                is_dry_run INTEGER NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cancels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                client_order_id TEXT NOT NULL,
                reason TEXT NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                client_order_id TEXT NOT NULL,
                side TEXT NOT NULL,
                price REAL NOT NULL,
                quantity REAL NOT NULL,
                realized_pnl_usd REAL,
                payload TEXT NOT NULL
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pnl (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                realized_pnl_usd REAL NOT NULL,
                cumulative_pnl_usd REAL NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS balances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                asset TEXT NOT NULL,
                free REAL NOT NULL,
                locked REAL NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        self._conn.commit()

    def _load_market_snapshots_sync(
        self,
        symbol: str,
        source: str | None,
        limit: int | None,
    ) -> list[MarketSnapshot]:
        query = """
            SELECT ts, bid_price, ask_price, bid_volume, ask_volume, mid_price, spread, spread_bps, imbalance_ratio, payload
            FROM market_data
            WHERE symbol = ?
        """
        params: list[Any] = [symbol]
        if source:
            query += " AND source = ?"
            params.append(source)
        query += " ORDER BY ts ASC"
        rows = self._conn.execute(query, params).fetchall()
        if limit and limit > 0:
            rows = rows[-limit:]

        snapshots: list[MarketSnapshot] = []
        for row in rows:
            payload = json.loads(row[9]) if row[9] else {}
            market_payload = payload.get("market", {})
            bid_update_time = payload.get("bid_update_time")
            ask_update_time = payload.get("ask_update_time")
            snapshots.append(
                MarketSnapshot(
                    symbol=symbol,
                    bid_price=float(row[1]),
                    ask_price=float(row[2]),
                    bid_volume=float(row[3]),
                    ask_volume=float(row[4]),
                    mid_price=float(row[5]),
                    spread=float(row[6]),
                    spread_bps=float(row[7]),
                    imbalance_ratio=float(row[8]),
                    bid_update_time=datetime.fromisoformat(bid_update_time) if bid_update_time else None,
                    ask_update_time=datetime.fromisoformat(ask_update_time) if ask_update_time else None,
                    event_time=datetime.fromisoformat(row[0]),
                    raw=market_payload if isinstance(market_payload, dict) else {},
                )
            )
        return snapshots
