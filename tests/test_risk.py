import unittest

from bot.risk import RiskConfig, RiskManager


class RiskTests(unittest.TestCase):
    def test_max_open_orders_blocks_new_entry(self) -> None:
        manager = RiskManager(
            RiskConfig(
                max_position_usd=100.0,
                max_open_orders=1,
                daily_max_loss_usd=20.0,
                per_trade_risk_usd=5.0,
                sl_bps=50.0,
            )
        )
        decision = manager.can_open_entry(0.0, 1, 10.0)
        self.assertFalse(decision.allowed)
        self.assertIn("max_open_orders_reached", decision.reasons)

    def test_position_size_uses_stop_distance_and_caps_by_max_position(self) -> None:
        manager = RiskManager(
            RiskConfig(
                max_position_usd=100.0,
                max_open_orders=2,
                daily_max_loss_usd=20.0,
                per_trade_risk_usd=5.0,
                sl_bps=50.0,
            )
        )
        qty = manager.position_size_from_risk(100.0)
        self.assertAlmostEqual(qty, 1.0)

    def test_kill_switch_blocks_new_entries_after_daily_loss(self) -> None:
        manager = RiskManager(
            RiskConfig(
                max_position_usd=100.0,
                max_open_orders=1,
                daily_max_loss_usd=10.0,
                per_trade_risk_usd=2.0,
                sl_bps=25.0,
            )
        )
        manager.record_realized_pnl(-11.0)
        decision = manager.can_open_entry(0.0, 0, 20.0)
        self.assertFalse(decision.allowed)
        self.assertIn("kill_switch_active", decision.reasons)

    def test_stop_loss_trigger(self) -> None:
        manager = RiskManager(
            RiskConfig(
                max_position_usd=50.0,
                max_open_orders=2,
                daily_max_loss_usd=10.0,
                per_trade_risk_usd=2.0,
                sl_bps=10.0,
            )
        )
        self.assertTrue(manager.stop_loss_triggered(100.0, 99.89))
        self.assertFalse(manager.stop_loss_triggered(100.0, 99.95))


if __name__ == "__main__":
    unittest.main()
