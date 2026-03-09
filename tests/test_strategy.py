import unittest
from datetime import datetime, timezone

from bot.models import MarketSnapshot
from bot.strategy import SpreadCaptureStrategy, StrategyConfig


class StrategyTests(unittest.TestCase):
    def test_entry_waits_for_volatility_window(self) -> None:
        strategy = SpreadCaptureStrategy(
            StrategyConfig(
                spread_min_bps=1.0,
                max_spread_bps=20.0,
                imbalance_min=1.2,
                volatility_max_bps=2.0,
                volatility_window=3,
            )
        )
        decision = strategy.evaluate(self._market(100.00, 100.02, 5.0, 2.0))
        self.assertFalse(decision.should_enter)
        self.assertIn("volatility_window_not_ready", decision.reasons)

    def test_entry_requires_spread_imbalance_and_low_volatility(self) -> None:
        strategy = SpreadCaptureStrategy(
            StrategyConfig(
                spread_min_bps=1.0,
                max_spread_bps=20.0,
                imbalance_min=1.2,
                volatility_max_bps=2.0,
                volatility_window=3,
            )
        )
        snapshots = [
            self._market(100.00, 100.02, 5.0, 2.0),
            self._market(100.01, 100.03, 6.0, 2.5),
            self._market(100.01, 100.03, 7.0, 3.0),
        ]
        decision = None
        for snapshot in snapshots:
            decision = strategy.evaluate(snapshot)
        assert decision is not None
        self.assertTrue(decision.should_enter)
        self.assertEqual(decision.reasons, ["entry_ok"])

    def test_entry_blocked_when_volatility_above_limit(self) -> None:
        strategy = SpreadCaptureStrategy(
            StrategyConfig(
                spread_min_bps=1.0,
                max_spread_bps=20.0,
                imbalance_min=1.1,
                volatility_max_bps=0.5,
                volatility_window=3,
            )
        )
        decision = None
        for bid, ask in [(100.0, 100.02), (100.3, 100.32), (99.6, 99.62)]:
            decision = strategy.evaluate(self._market(bid, ask, 5.0, 3.0))
        assert decision is not None
        self.assertFalse(decision.should_enter)
        self.assertIn("volatility_above_threshold", decision.reasons)

    def test_entry_blocked_when_spread_above_maximum(self) -> None:
        strategy = SpreadCaptureStrategy(
            StrategyConfig(
                spread_min_bps=1.0,
                max_spread_bps=20.0,
                imbalance_min=1.1,
                volatility_max_bps=10.0,
                volatility_window=1,
            )
        )

        decision = strategy.evaluate(self._market(100.0, 100.5, 5.0, 2.0))

        self.assertFalse(decision.should_enter)
        self.assertIn("spread_above_max", decision.reasons)

    @staticmethod
    def _market(bid: float, ask: float, bid_qty: float, ask_qty: float) -> MarketSnapshot:
        mid = (bid + ask) / 2
        return MarketSnapshot(
            symbol="BTCUSDT",
            bid_price=bid,
            ask_price=ask,
            bid_volume=bid_qty,
            ask_volume=ask_qty,
            mid_price=mid,
            spread=ask - bid,
            spread_bps=((ask - bid) / mid) * 10_000,
            imbalance_ratio=bid_qty / ask_qty,
            event_time=datetime.now(timezone.utc),
            raw={},
        )


if __name__ == "__main__":
    unittest.main()
