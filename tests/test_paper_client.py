import asyncio
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from bot.binance_client import PaperTradingClient, PaperTradingConfig
from bot.datastore import DataStore
from bot.models import MarketSnapshot, SymbolRules


def _market(ts: datetime, bid: float, ask: float, bid_qty: float = 5.0, ask_qty: float = 3.0) -> MarketSnapshot:
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
        event_time=ts,
        raw={"b": str(bid), "a": str(ask)},
    )


class PaperTradingClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_buy_fills_when_best_ask_crosses_after_latency(self) -> None:
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rules = SymbolRules(
            tick_size=0.01,
            step_size=0.001,
            min_qty=0.001,
            min_notional=10.0,
            base_asset="BTC",
            quote_asset="USDT",
        )
        client = PaperTradingClient(
            symbol="BTCUSDT",
            symbol_rules=rules,
            config=PaperTradingConfig(
                latency_min_ms=50,
                latency_max_ms=50,
                starting_quote_balance=1000.0,
                starting_base_balance=0.0,
                replay_speed=0.0,
            ),
            replay_snapshots=[
                _market(start, 100.00, 100.10),
                _market(start + timedelta(milliseconds=100), 100.00, 99.99),
            ],
        )

        stop_event = asyncio.Event()
        stream = client.stream_market_data("BTCUSDT", stop_event)
        first_market = await anext(stream)
        self.assertEqual(first_market.ask_price, 100.10)

        placed = await client.place_limit_order(
            "BTCUSDT",
            "BUY",
            "0.100",
            "100.00",
            client_order_id="entry_1",
            maker_only=True,
        )
        self.assertEqual(placed["status"], "NEW")

        await anext(stream)
        order = await client.get_order("BTCUSDT", "entry_1")
        self.assertEqual(order["status"], "FILLED")
        self.assertEqual(order["executedQty"], "0.1")

        balances = {item.asset: item for item in await client.get_balances()}
        self.assertAlmostEqual(balances["BTC"].free, 0.1)
        self.assertLess(balances["USDT"].free, 1000.0)

    async def test_market_data_can_be_loaded_for_replay(self) -> None:
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        market = _market(start, 100.0, 100.1)
        with tempfile.TemporaryDirectory() as temp_dir:
            store = DataStore(f"{temp_dir}/bot.sqlite3")
            await store.initialize()
            try:
                await store.log_market_snapshot("BTCUSDT", market, "live_ws")
                loaded = await store.load_market_snapshots("BTCUSDT", source="live_ws")
            finally:
                await store.close()

        self.assertEqual(len(loaded), 1)
        self.assertEqual(loaded[0].bid_price, market.bid_price)
        self.assertEqual(loaded[0].ask_price, market.ask_price)


if __name__ == "__main__":
    unittest.main()
