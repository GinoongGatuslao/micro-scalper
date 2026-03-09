from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone


@dataclass(slots=True)
class RiskConfig:
    max_position_usd: float
    max_open_orders: int
    daily_max_loss_usd: float
    per_trade_risk_usd: float
    sl_bps: float


@dataclass(slots=True)
class RiskDecision:
    allowed: bool
    reasons: list[str]


class RiskManager:
    def __init__(self, config: RiskConfig) -> None:
        self._config = config
        self._realized_pnl_today = 0.0
        self._current_day = self._today()
        self._kill_switch = False

    @property
    def realized_pnl_today(self) -> float:
        self._roll_day()
        return self._realized_pnl_today

    @property
    def kill_switch(self) -> bool:
        self._roll_day()
        return self._kill_switch

    def record_realized_pnl(self, pnl_usd: float) -> None:
        self._roll_day()
        self._realized_pnl_today += pnl_usd
        if self._realized_pnl_today <= -abs(self._config.daily_max_loss_usd):
            self._kill_switch = True

    def can_open_entry(self, position_notional_usd: float, open_orders: int, proposed_notional_usd: float) -> RiskDecision:
        self._roll_day()
        reasons: list[str] = []
        if self._kill_switch:
            reasons.append("kill_switch_active")
        if open_orders >= self._config.max_open_orders:
            reasons.append("max_open_orders_reached")
        if position_notional_usd + proposed_notional_usd > self._config.max_position_usd:
            reasons.append("max_position_usd_exceeded")
        if proposed_notional_usd <= 0:
            reasons.append("proposed_notional_invalid")
        return RiskDecision(allowed=not reasons, reasons=reasons or ["risk_ok"])

    def stop_loss_triggered(self, entry_price: float, mid_price: float) -> bool:
        if entry_price <= 0 or self._config.sl_bps <= 0:
            return False
        stop_price = entry_price * (1 - (self._config.sl_bps / 10_000))
        return mid_price <= stop_price

    def position_size_from_risk(self, price: float) -> float:
        if price <= 0:
            return 0.0
        if self._config.sl_bps <= 0:
            return self._config.max_position_usd / price
        risk_fraction = self._config.sl_bps / 10_000
        size_by_risk = self._config.per_trade_risk_usd / (price * risk_fraction)
        size_by_cap = self._config.max_position_usd / price
        return max(0.0, min(size_by_risk, size_by_cap))

    def _roll_day(self) -> None:
        today = self._today()
        if today != self._current_day:
            self._current_day = today
            self._realized_pnl_today = 0.0
            self._kill_switch = False

    @staticmethod
    def _today() -> date:
        return datetime.now(timezone.utc).date()
