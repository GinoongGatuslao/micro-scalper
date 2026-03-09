import unittest
from datetime import datetime, timedelta, timezone

from bot.execution import ExecutionConfig, ExecutionEngine, ceil_to_tick, floor_to_tick
from bot.metrics import Metrics
from bot.models import MarketSnapshot, OrderPurpose, OrderState, PositionState, Side, StrategyDecision, SymbolRules
from bot.risk import RiskConfig, RiskManager
from bot.strategy import SpreadCaptureStrategy, StrategyConfig


def _market(bid: float, ask: float, *, event_time: datetime | None = None) -> MarketSnapshot:
    mid = (bid + ask) / 2
    return MarketSnapshot(
        symbol="BTCUSDT",
        bid_price=bid,
        ask_price=ask,
        bid_volume=5.0,
        ask_volume=3.0,
        mid_price=mid,
        spread=ask - bid,
        spread_bps=((ask - bid) / mid) * 10_000,
        imbalance_ratio=5.0 / 3.0,
        event_time=event_time or datetime(2026, 1, 1, tzinfo=timezone.utc),
        raw={},
    )


class _StubClient:
    def __init__(
        self,
        *,
        quotes: list[tuple[float, float]],
        responses: list[object],
        cached_quotes: list[tuple[float, float]] | None = None,
        cancel_responses: list[object] | None = None,
    ) -> None:
        self._quotes = list(quotes)
        self._responses = list(responses)
        self._cached_quotes = list(cached_quotes or [])
        self._cancel_responses = list(cancel_responses or [])
        self.place_calls: list[dict[str, object]] = []
        self.cancel_calls: list[dict[str, object]] = []

    async def get_best_bid_ask(self, symbol: str) -> tuple[float, float]:
        if not self._quotes:
            raise RuntimeError("No quote configured")
        if len(self._quotes) == 1:
            return self._quotes[0]
        return self._quotes.pop(0)

    async def get_cached_best_bid_ask(self, symbol: str) -> tuple[float | None, float | None]:
        if not self._cached_quotes:
            return None, None
        if len(self._cached_quotes) == 1:
            return self._cached_quotes[0]
        return self._cached_quotes.pop(0)

    async def place_limit_order(
        self,
        symbol: str,
        side: str,
        quantity: str,
        price: str,
        *,
        client_order_id: str,
        maker_only: bool,
        time_in_force: str = "GTC",
    ) -> dict[str, object]:
        self.place_calls.append(
            {
                "symbol": symbol,
                "side": side,
                "quantity": quantity,
                "price": price,
                "client_order_id": client_order_id,
                "maker_only": maker_only,
                "time_in_force": time_in_force,
            }
        )
        if not self._responses:
            raise RuntimeError("No response configured")
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    async def cancel_order(self, symbol: str, client_order_id: str) -> dict[str, object]:
        self.cancel_calls.append({"symbol": symbol, "client_order_id": client_order_id})
        if self._cancel_responses:
            response = self._cancel_responses.pop(0)
            if isinstance(response, Exception):
                raise response
            return response
        return {
            "symbol": symbol,
            "origClientOrderId": client_order_id,
            "clientOrderId": client_order_id,
            "status": "CANCELED",
        }

    async def get_balances(self) -> list[object]:
        return []


class _NullDataStore:
    async def log_market_snapshot(self, *args, **kwargs) -> None:
        return None

    async def log_signal(self, *args, **kwargs) -> None:
        return None

    async def log_order(self, *args, **kwargs) -> None:
        return None

    async def log_fill(self, *args, **kwargs) -> None:
        return None

    async def log_cancel(self, *args, **kwargs) -> None:
        return None

    async def log_pnl(self, *args, **kwargs) -> None:
        return None

    async def log_balances(self, *args, **kwargs) -> None:
        return None


def _order_payload(*, price: str, quantity: str, order_type: str, side: str = "SELL") -> dict[str, object]:
    return {
        "symbol": "BTCUSDT",
        "orderId": 1,
        "clientOrderId": "test_order",
        "price": price,
        "origQty": quantity,
        "executedQty": "0",
        "cummulativeQuoteQty": "0",
        "status": "NEW",
        "type": order_type,
        "side": side,
        "timeInForce": "GTC",
    }


class ExecutionEngineExitTests(unittest.IsolatedAsyncioTestCase):
    def _engine(self, client: _StubClient, market: MarketSnapshot) -> ExecutionEngine:
        engine = ExecutionEngine(
            client=client,
            strategy=SpreadCaptureStrategy(
                StrategyConfig(
                    spread_min_bps=1.0,
                    max_spread_bps=20.0,
                    imbalance_min=1.0,
                    volatility_max_bps=100.0,
                    volatility_window=2,
                )
            ),
            risk=RiskManager(
                RiskConfig(
                    max_position_usd=1_000.0,
                    max_open_orders=2,
                    daily_max_loss_usd=100.0,
                    per_trade_risk_usd=10.0,
                    sl_bps=10.0,
                )
            ),
            datastore=_NullDataStore(),
            metrics=Metrics(),
            config=ExecutionConfig(
                symbol="BTCUSDT",
                dry_run=False,
                entry_ttl_seconds=5,
                exit_ttl_seconds=5,
                maker_offset_ticks=2,
                offset_ticks_min=0,
                offset_ticks_max=3,
                spread_tight_bps=3.0,
                spread_wide_bps=10.0,
                entry_attempt_interval_ms=500,
                rejection_cooldown_ms=1500,
                max_consecutive_rejections=5,
                allow_taker_exit=False,
                entry_max_requotes=5,
                exit_max_requotes=6,
                market_stale_after_ms=500,
                max_spread_bps=20.0,
                order_poll_interval=1.0,
                balance_snapshot_interval=60.0,
                market_data_source="test",
                sync_orders_on_market_data=False,
            ),
            symbol_rules=SymbolRules(
                tick_size=0.01,
                step_size=0.001,
                min_qty=0.001,
                min_notional=10.0,
                base_asset="BTC",
                quote_asset="USDT",
            ),
        )
        engine._current_market = market
        engine._ingest_market(market)
        engine._position = PositionState(
            quantity=0.1,
            entry_price=100.0,
            opened_at=market.event_time,
            entry_order_id="entry_1",
        )
        return engine

    async def test_exit_limit_maker_on_2010_rejects_without_forcing_retry(self) -> None:
        market = _market(100.00, 100.01)
        client = _StubClient(
            quotes=[],
            cached_quotes=[(100.00, 100.01)],
            responses=[
                RuntimeError('Binance HTTP error 400: {"code":-2010,"msg":"Order would immediately match and take."}'),
            ],
        )
        engine = self._engine(client, market)

        with self.assertLogs("bot.execution", level="WARNING") as logs:
            await engine._place_exit_order(market, 0.1, stop_mode=False)

        self.assertEqual(len(client.place_calls), 1)
        self.assertIn(client.place_calls[0]["price"], {"100.02", "100.03"})
        self.assertTrue(any("code=-2010" in line for line in logs.output))
        self.assertFalse(any("retrying_once=true" in line for line in logs.output))
        self.assertIsNone(engine._exit_order)

    async def test_exit_limit_maker_skips_submit_when_cached_quote_missing(self) -> None:
        market = _market(100.00, 100.01)
        client = _StubClient(
            quotes=[],
            responses=[],
        )
        engine = self._engine(client, market)

        await engine._place_exit_order(market, 0.1, stop_mode=False)

        self.assertEqual(client.place_calls, [])
        self.assertIsNone(engine._exit_order)

    async def test_floor_and_ceil_to_tick_are_directional(self) -> None:
        self.assertEqual(floor_to_tick(100.019, 0.01), 100.01)
        self.assertEqual(ceil_to_tick(100.011, 0.01), 100.02)
        self.assertEqual(floor_to_tick(100.02, 0.01), 100.02)
        self.assertEqual(ceil_to_tick(100.02, 0.01), 100.02)

    async def test_maker_safe_price_buy_is_below_best_ask(self) -> None:
        market = _market(100.00, 100.01)
        client = _StubClient(quotes=[], responses=[])
        engine = self._engine(client, market)

        price = engine.maker_safe_price(
            side=Side.BUY,
            desired_price=100.05,
            best_bid=100.00,
            best_ask=100.02,
            offset_ticks=1,
        )

        self.assertLess(price, 100.02)
        self.assertAlmostEqual(price, 99.99)

    async def test_maker_safe_price_buy_applies_offset_ticks(self) -> None:
        market = _market(100.00, 100.05)
        client = _StubClient(quotes=[], responses=[])
        engine = self._engine(client, market)

        price = engine.maker_safe_price(
            side=Side.BUY,
            desired_price=100.03,
            best_bid=100.00,
            best_ask=100.05,
            offset_ticks=2,
        )

        self.assertAlmostEqual(price, 99.98)

    async def test_maker_safe_price_sell_is_above_best_bid(self) -> None:
        market = _market(100.00, 100.01)
        client = _StubClient(quotes=[], responses=[])
        engine = self._engine(client, market)

        price = engine.maker_safe_price(
            side=Side.SELL,
            desired_price=100.00,
            best_bid=100.02,
            best_ask=100.03,
            offset_ticks=1,
        )

        self.assertGreater(price, 100.02)
        self.assertAlmostEqual(price, 100.04)

    async def test_maker_safe_price_sell_applies_offset_ticks(self) -> None:
        market = _market(100.00, 100.05)
        client = _StubClient(quotes=[], responses=[])
        engine = self._engine(client, market)

        price = engine.maker_safe_price(
            side=Side.SELL,
            desired_price=100.03,
            best_bid=100.00,
            best_ask=100.05,
            offset_ticks=2,
        )

        self.assertAlmostEqual(price, 100.07)

    async def test_maker_safe_price_returns_none_when_one_tick_rule_fails(self) -> None:
        market = _market(100.00, 100.05)
        client = _StubClient(quotes=[], responses=[])
        engine = self._engine(client, market)

        price = engine.maker_safe_price(
            side=Side.BUY,
            desired_price=100.00,
            best_bid=100.00,
            best_ask=100.05,
            offset_ticks=0,
        )

        self.assertIsNone(price)

    async def test_quantize_price_for_side_respects_tick_size(self) -> None:
        market = _market(100.00, 100.01)
        client = _StubClient(quotes=[], responses=[])
        engine = self._engine(client, market)

        self.assertAlmostEqual(engine._quantize_price_for_side(100.019, Side.BUY), 100.01)
        self.assertAlmostEqual(engine._quantize_price_for_side(100.011, Side.SELL), 100.02)

    async def test_exit_requote_cap_escalates_to_taker_limit_when_allowed(self) -> None:
        market = _market(100.00, 100.03)
        client = _StubClient(
            quotes=[],
            cached_quotes=[(100.00, 100.03)],
            responses=[_order_payload(price="100.00", quantity="0.1", order_type="LIMIT", side="SELL")],
        )
        engine = self._engine(client, market)
        engine._config.allow_taker_exit = True
        engine._config.exit_max_requotes = 1
        opened_at = market.event_time
        engine._exit_order = OrderState(
            purpose=OrderPurpose.EXIT,
            side=Side.SELL,
            client_order_id="exit_1",
            exchange_order_id=10,
            price=100.05,
            quantity=0.1,
            executed_quantity=0.0,
            status="NEW",
            order_type="LIMIT_MAKER",
            created_at=opened_at,
            updated_at=opened_at,
        )
        engine._exit_requote_count = 1
        engine._next_exit_reprice_at = opened_at

        await engine._maybe_requote_exit_order(market)

        self.assertEqual(len(client.cancel_calls), 1)
        self.assertEqual(len(client.place_calls), 1)
        self.assertFalse(client.place_calls[0]["maker_only"])
        self.assertEqual(client.place_calls[0]["price"], "100")
        self.assertTrue(engine._exit_requote_escalated)

    async def test_exit_requote_cap_escalates_to_offset_zero_when_maker_only(self) -> None:
        market = _market(100.00, 100.03)
        client = _StubClient(
            quotes=[],
            cached_quotes=[(100.00, 100.03)],
            responses=[],
        )
        engine = self._engine(client, market)
        engine._config.allow_taker_exit = False
        engine._config.exit_max_requotes = 1
        opened_at = market.event_time
        engine._exit_order = OrderState(
            purpose=OrderPurpose.EXIT,
            side=Side.SELL,
            client_order_id="exit_1",
            exchange_order_id=10,
            price=100.05,
            quantity=0.1,
            executed_quantity=0.0,
            status="NEW",
            order_type="LIMIT_MAKER",
            created_at=opened_at,
            updated_at=opened_at,
        )
        engine._exit_requote_count = 1
        engine._next_exit_reprice_at = opened_at

        await engine._maybe_requote_exit_order(market)

        self.assertEqual(len(client.cancel_calls), 1)
        self.assertEqual(len(client.place_calls), 0)
        self.assertTrue(engine._exit_requote_escalated)


class ExecutionEngineEntryAndTradeLoggingTests(unittest.IsolatedAsyncioTestCase):
    def _engine(self, client: _StubClient, market: MarketSnapshot) -> ExecutionEngine:
        engine = ExecutionEngine(
            client=client,
            strategy=SpreadCaptureStrategy(
                StrategyConfig(
                    spread_min_bps=1.0,
                    max_spread_bps=20.0,
                    imbalance_min=1.0,
                    volatility_max_bps=100.0,
                    volatility_window=2,
                )
            ),
            risk=RiskManager(
                RiskConfig(
                    max_position_usd=1_000.0,
                    max_open_orders=2,
                    daily_max_loss_usd=100.0,
                    per_trade_risk_usd=10.0,
                    sl_bps=10.0,
                )
            ),
            datastore=_NullDataStore(),
            metrics=Metrics(),
            config=ExecutionConfig(
                symbol="BTCUSDT",
                dry_run=False,
                entry_ttl_seconds=5,
                exit_ttl_seconds=5,
                maker_offset_ticks=2,
                offset_ticks_min=0,
                offset_ticks_max=3,
                spread_tight_bps=3.0,
                spread_wide_bps=10.0,
                entry_attempt_interval_ms=500,
                rejection_cooldown_ms=1500,
                max_consecutive_rejections=5,
                allow_taker_exit=False,
                entry_max_requotes=5,
                exit_max_requotes=6,
                market_stale_after_ms=500,
                max_spread_bps=20.0,
                order_poll_interval=1.0,
                balance_snapshot_interval=60.0,
                market_data_source="test",
                sync_orders_on_market_data=False,
            ),
            symbol_rules=SymbolRules(
                tick_size=0.01,
                step_size=0.001,
                min_qty=0.001,
                min_notional=10.0,
                base_asset="BTC",
                quote_asset="USDT",
            ),
        )
        engine._current_market = market
        engine._ingest_market(market)
        return engine

    async def test_entry_reprices_from_cached_best_bid_and_logs_quote_context(self) -> None:
        opened_at = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        market = _market(100.02, 100.04, event_time=opened_at + timedelta(seconds=2))
        client = _StubClient(
            quotes=[],
            cached_quotes=[(100.03, 100.05)],
            responses=[_order_payload(price="100.03", quantity="0.1", order_type="LIMIT_MAKER", side="BUY")],
        )
        engine = self._engine(client, market)
        engine._last_decision = StrategyDecision(
            should_enter=True,
            spread_bps=2.999,
            imbalance_ratio=1.5,
            volatility_bps=0.5,
            reasons=["entry_ok"],
        )
        engine._entry_order = OrderState(
            purpose=OrderPurpose.ENTRY,
            side=Side.BUY,
            client_order_id="entry_1",
            exchange_order_id=1,
            price=100.00,
            quantity=0.1,
            executed_quantity=0.0,
            status="NEW",
            order_type="LIMIT_MAKER",
            created_at=opened_at,
            updated_at=opened_at,
        )
        engine._entry_attempt_started_at = opened_at
        engine._next_entry_reprice_at = opened_at + timedelta(seconds=1.5)
        engine._last_entry_reprice_action_at = opened_at

        with self.assertLogs("bot.execution", level="INFO") as logs:
            await engine._manage_entry_order(market)

        self.assertEqual(len(client.cancel_calls), 1)
        self.assertEqual(client.cancel_calls[0]["client_order_id"], "entry_1")
        self.assertEqual(len(client.place_calls), 1)
        self.assertEqual(client.place_calls[0]["price"], "100.03")
        self.assertTrue(any("Submitting ENTRY BUY best_bid=100.03000000" in line for line in logs.output))
        self.assertTrue(any("entry_price=100.03000000" in line for line in logs.output))
        self.assertTrue(any("delta_ticks=0.000" in line for line in logs.output))
        self.assertTrue(any("Repriced ENTRY BUY from 100.00000000 to 100.03000000 reason=best_bid_moved" in line for line in logs.output))

    async def test_entry_ttl_uses_total_attempt_time_and_starts_backoff(self) -> None:
        opened_at = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        market = _market(100.00, 100.02, event_time=opened_at + timedelta(seconds=6))
        client = _StubClient(quotes=[], responses=[])
        engine = self._engine(client, market)
        engine._ingest_market(market)
        engine._last_decision = StrategyDecision(
            should_enter=True,
            spread_bps=2.0,
            imbalance_ratio=1.5,
            volatility_bps=0.5,
            reasons=["entry_ok"],
        )
        engine._entry_order = OrderState(
            purpose=OrderPurpose.ENTRY,
            side=Side.BUY,
            client_order_id="entry_1",
            exchange_order_id=1,
            price=100.00,
            quantity=0.1,
            executed_quantity=0.0,
            status="NEW",
            order_type="LIMIT_MAKER",
            created_at=opened_at + timedelta(seconds=4),
            updated_at=opened_at + timedelta(seconds=4),
        )
        engine._entry_attempt_started_at = opened_at

        await engine._manage_entry_order(market)

        self.assertEqual(len(client.cancel_calls), 1)
        self.assertIsNone(engine._entry_order)
        self.assertEqual(engine._entry_backoff_until, market.event_time + timedelta(seconds=5))

        await engine._maybe_enter(market)

        self.assertEqual(client.place_calls, [])

    async def test_market_sanity_filter_skips_when_spread_exceeds_max(self) -> None:
        market = _market(100.00, 100.50)
        client = _StubClient(quotes=[], responses=[])
        engine = self._engine(client, market)
        engine._ingest_market(market)
        engine._last_decision = StrategyDecision(
            should_enter=True,
            spread_bps=market.spread_bps,
            imbalance_ratio=1.5,
            volatility_bps=0.5,
            reasons=["entry_ok"],
        )

        with self.assertLogs("bot.execution", level="WARNING") as logs:
            await engine._maybe_enter(market)

        self.assertEqual(client.place_calls, [])
        self.assertTrue(any("Market sanity check failed  skipping trade" in line for line in logs.output))

    async def test_market_sanity_filter_skips_when_quote_is_stale(self) -> None:
        stale_time = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        market = _market(
            100.00,
            100.02,
            event_time=stale_time + timedelta(seconds=1),
        )
        market.bid_update_time = stale_time
        market.ask_update_time = stale_time
        client = _StubClient(quotes=[], responses=[])
        engine = self._engine(client, market)
        engine._ingest_market(market)
        engine._last_decision = StrategyDecision(
            should_enter=True,
            spread_bps=market.spread_bps,
            imbalance_ratio=1.5,
            volatility_bps=0.5,
            reasons=["entry_ok"],
        )

        with self.assertLogs("bot.execution", level="WARNING") as logs:
            await engine._maybe_enter(market)

        self.assertEqual(client.place_calls, [])
        self.assertTrue(any("reason=quote_stale" in line for line in logs.output))

    async def test_trade_closed_summary_logs_once_and_resets_daily_realized_pnl(self) -> None:
        opened_at = datetime(2026, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
        closed_at = opened_at + timedelta(seconds=1.8)
        market = _market(101.00, 101.02, event_time=closed_at)
        client = _StubClient(quotes=[], responses=[])
        engine = self._engine(client, market)
        engine._position = PositionState(
            quantity=0.1,
            entry_price=100.0,
            opened_at=opened_at,
            entry_order_id="entry_1",
            initial_quantity=0.1,
        )
        engine.daily_realized_pnl = 5.0
        engine._daily_realized_pnl_date = opened_at.date() - timedelta(days=1)

        order = OrderState(
            purpose=OrderPurpose.EXIT,
            side=Side.SELL,
            client_order_id="exit_1",
            exchange_order_id=2,
            price=101.0,
            quantity=0.1,
            executed_quantity=0.1,
            status="FILLED",
            order_type="LIMIT_MAKER",
            created_at=closed_at,
            updated_at=closed_at,
        )

        with self.assertLogs("bot.execution", level="INFO") as logs:
            await engine._apply_exit_fill(order, 0.1)
            await engine._close_position(order)
            await engine._close_position(order)

        trade_logs = [line for line in logs.output if "TRADE CLOSED" in line]
        self.assertEqual(len(trade_logs), 1)
        self.assertIn("pnl=+0.10000 USDT", trade_logs[0])
        self.assertIn("duration=1.8s", trade_logs[0])
        self.assertIn("daily_pnl=0.1000", trade_logs[0])

    async def test_trade_closed_loss_summary_marks_loss(self) -> None:
        opened_at = datetime(2026, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
        closed_at = opened_at + timedelta(seconds=2)
        market = _market(99.50, 99.52, event_time=closed_at)
        client = _StubClient(quotes=[], responses=[])
        engine = self._engine(client, market)
        engine._position = PositionState(
            quantity=0.1,
            entry_price=100.0,
            opened_at=opened_at,
            entry_order_id="entry_1",
            initial_quantity=0.1,
        )

        order = OrderState(
            purpose=OrderPurpose.STOP,
            side=Side.SELL,
            client_order_id="stop_1",
            exchange_order_id=2,
            price=99.5,
            quantity=0.1,
            executed_quantity=0.1,
            status="FILLED",
            order_type="LIMIT",
            created_at=closed_at,
            updated_at=closed_at,
        )

        with self.assertLogs("bot.execution", level="INFO") as logs:
            await engine._apply_exit_fill(order, 0.1)
            await engine._close_position(order)

        trade_logs = [line for line in logs.output if "TRADE CLOSED" in line]
        self.assertEqual(len(trade_logs), 1)
        self.assertIn("LOSS", trade_logs[0])

    async def test_loss_streak_triggers_cooldown_after_three_losses(self) -> None:
        market_time = datetime(2026, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
        market = _market(99.50, 99.52, event_time=market_time)
        client = _StubClient(quotes=[], responses=[])
        engine = self._engine(client, market)
        engine._current_market = market

        with self.assertLogs("bot.execution", level="WARNING") as logs:
            engine._update_loss_streak(-0.1)
            engine._update_loss_streak(-0.2)
            engine._update_loss_streak(-0.3)

        self.assertEqual(engine._consecutive_losses, 3)
        self.assertEqual(engine._cooldown_until, market_time + timedelta(minutes=10))
        self.assertTrue(any("Loss streak detected  entering cooldown" in line for line in logs.output))

    async def test_session_summary_counts_breakeven_and_uses_wins_over_total(self) -> None:
        market = _market(100.00, 100.02)
        client = _StubClient(quotes=[], responses=[])
        engine = self._engine(client, market)
        engine._metrics.record_trade_closed(1.0)
        engine._metrics.record_trade_closed(-0.5)
        engine._metrics.record_trade_closed(0.0)

        summary = engine.session_summary()

        self.assertEqual(summary["winning_trades"], 1)
        self.assertEqual(summary["losing_trades"], 1)
        self.assertEqual(summary["breakeven_trades"], 1)
        self.assertEqual(summary["total_trades"], 3)
        self.assertAlmostEqual(summary["win_rate"], 1 / 3)


if __name__ == "__main__":
    unittest.main()
