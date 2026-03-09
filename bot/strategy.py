from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from statistics import pstdev

from .models import MarketSnapshot, StrategyDecision


@dataclass(slots=True)
class StrategyConfig:
    spread_min_bps: float
    max_spread_bps: float
    imbalance_min: float
    volatility_max_bps: float
    volatility_window: int


class SpreadCaptureStrategy:
    def __init__(self, config: StrategyConfig) -> None:
        self._config = config
        self._mids: deque[float] = deque(maxlen=config.volatility_window)

    def evaluate(self, market: MarketSnapshot) -> StrategyDecision:
        self._mids.append(market.mid_price)
        volatility_bps = self._volatility_bps()
        reasons: list[str] = []

        if market.spread_bps < self._config.spread_min_bps:
            reasons.append("spread_below_threshold")
        if self._config.max_spread_bps > 0 and market.spread_bps > self._config.max_spread_bps:
            reasons.append("spread_above_max")
        if market.imbalance_ratio < self._config.imbalance_min:
            reasons.append("imbalance_below_threshold")
        if len(self._mids) < self._config.volatility_window:
            reasons.append("volatility_window_not_ready")
        elif volatility_bps > self._config.volatility_max_bps:
            reasons.append("volatility_above_threshold")

        return StrategyDecision(
            should_enter=not reasons,
            spread_bps=market.spread_bps,
            imbalance_ratio=market.imbalance_ratio,
            volatility_bps=volatility_bps,
            reasons=reasons or ["entry_ok"],
        )

    def _volatility_bps(self) -> float:
        if len(self._mids) < 2:
            return 0.0
        mean_mid = sum(self._mids) / len(self._mids)
        if mean_mid <= 0:
            return 0.0
        return (pstdev(self._mids) / mean_mid) * 10_000
