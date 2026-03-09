import asyncio
import csv
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from bot.datastore import DataStore
from bot.models import OrderPurpose, OrderState, Side
from bot.report import build_report, export_trades_csv


class ReportTests(unittest.TestCase):
    def test_report_summary_and_csv_export(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = f"{temp_dir}/bot.sqlite3"
            csv_path = f"{temp_dir}/trades.csv"
            asyncio.run(self._seed_data(db_path))

            summary = build_report(db_path, fee_rate_bps=10.0, symbol="BTCUSDT")
            export_trades_csv(csv_path, summary.trades)

            self.assertEqual(len(summary.trades), 2)
            self.assertAlmostEqual(summary.total_realized_pnl_usd, 2.0)
            self.assertAlmostEqual(summary.win_rate, 0.5)
            self.assertAlmostEqual(summary.avg_profit_per_trade_usd, 1.0)
            self.assertAlmostEqual(summary.avg_hold_seconds or 0.0, 450.0)
            self.assertAlmostEqual(summary.estimated_fees_usd, 0.402, places=6)
            self.assertAlmostEqual(summary.max_drawdown_usd, 3.0)
            self.assertEqual([item.day for item in summary.daily], ["2026-01-01", "2026-01-02"])
            self.assertAlmostEqual(summary.daily[0].realized_pnl_usd, 5.0)
            self.assertAlmostEqual(summary.daily[1].realized_pnl_usd, -3.0)

            with open(csv_path, newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["entry_order_id"], "entry_1")

    async def _seed_data(self, db_path: str) -> None:
        store = DataStore(db_path)
        await store.initialize()
        try:
            base = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
            await store.log_order("BTCUSDT", self._order("entry_1", OrderPurpose.ENTRY, Side.BUY, 100.0, 1.0, base))
            await store.log_order(
                "BTCUSDT",
                self._order("exit_1", OrderPurpose.EXIT, Side.SELL, 105.0, 1.0, base + timedelta(minutes=5)),
            )
            await store.log_pnl(
                "BTCUSDT",
                realized_pnl_usd=5.0,
                cumulative_pnl_usd=5.0,
                payload={
                    "entry_order_id": "entry_1",
                    "exit_order_id": "exit_1",
                    "entry_price": 100.0,
                    "exit_price": 105.0,
                    "quantity": 1.0,
                },
            )

            second_open = base + timedelta(days=1)
            await store.log_order("BTCUSDT", self._order("entry_2", OrderPurpose.ENTRY, Side.BUY, 100.0, 1.0, second_open))
            await store.log_order(
                "BTCUSDT",
                self._order("exit_2", OrderPurpose.EXIT, Side.SELL, 97.0, 1.0, second_open + timedelta(minutes=10)),
            )
            await store.log_pnl(
                "BTCUSDT",
                realized_pnl_usd=-3.0,
                cumulative_pnl_usd=2.0,
                payload={
                    "entry_order_id": "entry_2",
                    "exit_order_id": "exit_2",
                    "entry_price": 100.0,
                    "exit_price": 97.0,
                    "quantity": 1.0,
                },
            )
        finally:
            await store.close()

    @staticmethod
    def _order(client_order_id: str, purpose: OrderPurpose, side: Side, price: float, qty: float, ts: datetime) -> OrderState:
        return OrderState(
            purpose=purpose,
            side=side,
            client_order_id=client_order_id,
            exchange_order_id=None,
            price=price,
            quantity=qty,
            executed_quantity=qty,
            status="FILLED",
            order_type="LIMIT_MAKER",
            created_at=ts,
            updated_at=ts,
            raw={},
        )


if __name__ == "__main__":
    unittest.main()
