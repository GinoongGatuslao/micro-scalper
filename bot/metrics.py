from __future__ import annotations

from dataclasses import asdict

from .models import RuntimeStats


class Metrics:
    def __init__(self, *, breakeven_epsilon: float = 1e-9) -> None:
        self._stats = RuntimeStats()
        self._filled_order_ids: set[str] = set()
        self._trade_pnls: list[float] = []
        self._equity_curve = 0.0
        self._equity_peak = 0.0
        self._breakeven_epsilon = abs(float(breakeven_epsilon))

    @property
    def stats(self) -> RuntimeStats:
        return self._stats

    def record_ws_reconnect(self) -> None:
        self._stats.ws_reconnects += 1

    def record_signal(self) -> None:
        self._stats.signals_evaluated += 1

    def record_order(self) -> None:
        self._stats.orders_submitted += 1
        self._refresh_fill_rate()

    def record_fill(self, order_id: str | None = None) -> None:
        self._stats.fills_seen += 1
        if order_id and order_id not in self._filled_order_ids:
            self._filled_order_ids.add(order_id)
            self._stats.filled_orders += 1
        self._refresh_fill_rate()

    def record_cancel(self) -> None:
        self._stats.cancels_sent += 1

    def record_realized_pnl(self, pnl_usd: float) -> None:
        self._stats.realized_pnl_usd += pnl_usd

    def record_trade_closed(self, pnl_usd: float) -> None:
        self._trade_pnls.append(pnl_usd)
        self._stats.total_trades += 1
        if pnl_usd > self._breakeven_epsilon:
            self._stats.winning_trades += 1
        elif pnl_usd < -self._breakeven_epsilon:
            self._stats.losing_trades += 1
        else:
            self._stats.breakeven_trades += 1
        self._stats.avg_pnl_usd = sum(self._trade_pnls) / len(self._trade_pnls)
        self._equity_curve += pnl_usd
        self._equity_peak = max(self._equity_peak, self._equity_curve)
        self._stats.max_drawdown_usd = max(self._stats.max_drawdown_usd, self._equity_peak - self._equity_curve)

    def summary(self) -> dict[str, float | int]:
        return asdict(self._stats)

    def _refresh_fill_rate(self) -> None:
        if self._stats.orders_submitted <= 0:
            self._stats.fill_rate = 0.0
            return
        self._stats.fill_rate = self._stats.filled_orders / self._stats.orders_submitted
