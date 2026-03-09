from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderPurpose(str, Enum):
    ENTRY = "ENTRY"
    EXIT = "EXIT"
    STOP = "STOP"


@dataclass(slots=True)
class MarketSnapshot:
    symbol: str
    bid_price: float
    ask_price: float
    bid_volume: float
    ask_volume: float
    mid_price: float
    spread: float
    spread_bps: float
    imbalance_ratio: float
    bid_update_time: datetime | None = None
    ask_update_time: datetime | None = None
    event_time: datetime = field(default_factory=utc_now)
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class StrategyDecision:
    should_enter: bool
    spread_bps: float
    imbalance_ratio: float
    volatility_bps: float
    reasons: list[str]


@dataclass(slots=True)
class SymbolRules:
    tick_size: float
    step_size: float
    min_qty: float
    min_notional: float
    base_asset: str
    quote_asset: str


@dataclass(slots=True)
class OrderState:
    purpose: OrderPurpose
    side: Side
    client_order_id: str
    exchange_order_id: int | None
    price: float
    quantity: float
    executed_quantity: float
    status: str
    order_type: str
    created_at: datetime
    updated_at: datetime
    is_dry_run: bool = False
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PositionState:
    quantity: float
    entry_price: float
    opened_at: datetime
    entry_order_id: str
    initial_quantity: float | None = None
    closed_quantity: float = 0.0
    closed_notional: float = 0.0
    realized_pnl: float = 0.0
    summary_logged: bool = False


@dataclass(slots=True)
class BalanceSnapshot:
    asset: str
    free: float
    locked: float
    captured_at: datetime = field(default_factory=utc_now)


@dataclass(slots=True)
class RuntimeStats:
    ws_reconnects: int = 0
    signals_evaluated: int = 0
    orders_submitted: int = 0
    filled_orders: int = 0
    fills_seen: int = 0
    cancels_sent: int = 0
    realized_pnl_usd: float = 0.0
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    breakeven_trades: int = 0
    avg_pnl_usd: float = 0.0
    fill_rate: float = 0.0
    max_drawdown_usd: float = 0.0
