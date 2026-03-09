from __future__ import annotations

import asyncio
import signal
import sys
from datetime import datetime, timezone

from .binance_client import BinanceClient, BinanceClientConfig, PaperTradingClient, PaperTradingConfig
from .console import say
from .config import AppConfig, load_app_config
from .datastore import DataStore
from .execution import ExecutionConfig, ExecutionEngine
from .logging import configure_logging
from .metrics import Metrics
from .models import SymbolRules
from .risk import RiskConfig, RiskManager
from .strategy import SpreadCaptureStrategy, StrategyConfig


async def main() -> int:
    config = load_app_config()
    logger = configure_logging(config.log_level)

    if config.dry_run and config.paper_trading:
        logger.error("DRY_RUN and PAPER_TRADING cannot both be true")
        return 2
    if config.market_data_mode not in {"live", "replay"}:
        logger.error("MARKET_DATA_MODE must be 'live' or 'replay'")
        return 2
    if config.market_data_mode == "replay" and not (config.dry_run or config.paper_trading):
        logger.error("Replay market data is only supported in DRY_RUN or PAPER_TRADING mode")
        return 2
    if not config.dry_run and not config.paper_trading and (not config.api_key or not config.api_secret):
        logger.error("BINANCE_API_KEY and BINANCE_API_SECRET are required unless DRY_RUN=true or PAPER_TRADING=true")
        return 2

    datastore = DataStore(config.db_path)
    replay_store: DataStore | None = None
    try:
        await datastore.initialize()
        metrics = Metrics(breakeven_epsilon=config.breakeven_epsilon)
        live_client = BinanceClient(
            BinanceClientConfig(
                api_key=config.api_key,
                api_secret=config.api_secret,
                rest_base_url=config.rest_base_url,
                ws_base_url=config.ws_base_url,
            ),
            metrics=metrics,
        )

        exchange_rules = await _load_exchange_rules_or_exit(config, live_client, logger)
        _print_startup_diagnostics(config, exchange_rules)
        symbol_rules = _resolve_symbol_rules(config, exchange_rules)
        replay_snapshots = None
        if config.market_data_mode == "replay":
            replay_store = datastore if config.replay_db_path == config.db_path else DataStore(config.replay_db_path)
            if replay_store is not datastore:
                await replay_store.initialize()
            replay_snapshots = await replay_store.load_market_snapshots(
                config.symbol,
                source=config.replay_source,
                limit=config.replay_limit,
            )
            if not replay_snapshots:
                logger.error("No recorded market data found for symbol=%s source=%s", config.symbol, config.replay_source)
                return 2

        client = _build_client(config, live_client, symbol_rules, replay_snapshots)
        strategy = SpreadCaptureStrategy(
            StrategyConfig(
                spread_min_bps=config.spread_min_bps,
                max_spread_bps=config.max_spread_bps,
                imbalance_min=config.imbalance_min,
                volatility_max_bps=config.volatility_max_bps,
                volatility_window=config.volatility_window,
            )
        )
        risk = RiskManager(
            RiskConfig(
                max_position_usd=config.max_position_usd,
                max_open_orders=config.max_open_orders,
                daily_max_loss_usd=config.daily_max_loss_usd,
                per_trade_risk_usd=config.per_trade_risk_usd,
                sl_bps=config.sl_bps,
            )
        )
        engine = ExecutionEngine(
            client=client,
            strategy=strategy,
            risk=risk,
            datastore=datastore,
            metrics=metrics,
            config=ExecutionConfig(
                symbol=config.symbol,
                dry_run=config.dry_run,
                entry_ttl_seconds=config.entry_ttl_seconds,
                exit_ttl_seconds=config.exit_ttl_seconds,
                maker_offset_ticks=config.maker_offset_ticks,
                offset_ticks_min=config.offset_ticks_min,
                offset_ticks_max=config.offset_ticks_max,
                spread_tight_bps=config.spread_tight_bps,
                spread_wide_bps=config.spread_wide_bps,
                entry_attempt_interval_ms=config.entry_attempt_interval_ms,
                rejection_cooldown_ms=config.rejection_cooldown_ms,
                max_consecutive_rejections=config.max_consecutive_rejections,
                allow_taker_exit=config.allow_taker_exit,
                entry_max_requotes=config.entry_max_requotes,
                exit_max_requotes=config.exit_max_requotes,
                market_stale_after_ms=config.market_stale_after_ms,
                max_spread_bps=config.max_spread_bps,
                order_poll_interval=config.order_poll_interval,
                balance_snapshot_interval=config.balance_snapshot_interval,
                market_data_source=_market_data_source_label(config),
                sync_orders_on_market_data=config.paper_trading or config.market_data_mode == "replay",
            ),
            symbol_rules=symbol_rules,
        )

        stop_event = asyncio.Event()
        _install_signal_handlers(stop_event, logger)
        await engine.run(stop_event)
        if stop_event.is_set():
            _print_session_summary(engine.session_summary())
        return 0
    finally:
        if replay_store is not None and replay_store is not datastore:
            await replay_store.close()
        await datastore.close()


def _build_client(
    config: AppConfig,
    live_client: BinanceClient,
    symbol_rules: SymbolRules,
    replay_snapshots: list | None,
):
    if config.paper_trading or config.market_data_mode == "replay":
        return PaperTradingClient(
            symbol=config.symbol,
            symbol_rules=symbol_rules,
            config=PaperTradingConfig(
                latency_min_ms=config.sim_latency_min_ms,
                latency_max_ms=config.sim_latency_max_ms,
                starting_quote_balance=config.sim_quote_balance,
                starting_base_balance=config.sim_base_balance,
                replay_speed=config.replay_speed,
            ),
            live_client=live_client if config.market_data_mode == "live" else None,
            replay_snapshots=replay_snapshots,
        )
    return live_client


async def _load_exchange_rules_or_exit(
    config: AppConfig,
    live_client: BinanceClient,
    logger,
) -> SymbolRules:
    try:
        return await live_client.get_symbol_rules(config.symbol)
    except RuntimeError as exc:
        message = f"Unable to load Binance exchangeInfo for {config.symbol}: {exc}"
        logger.error(message)
        print(f"ERROR: {message}", file=sys.stderr)
        raise SystemExit("Bot startup aborted: market rules could not be loaded.")


def _resolve_symbol_rules(config: AppConfig, exchange_rules: SymbolRules) -> SymbolRules:
    overrides = (
        config.tick_size,
        config.step_size,
        config.min_qty,
        config.min_notional,
        config.base_asset,
        config.quote_asset,
    )
    if all(value is not None for value in overrides):
        return SymbolRules(
            tick_size=config.tick_size or 0.0,
            step_size=config.step_size or 0.0,
            min_qty=config.min_qty or 0.0,
            min_notional=config.min_notional or 0.0,
            base_asset=config.base_asset or "",
            quote_asset=config.quote_asset or "",
        )
    return exchange_rules


def _print_startup_diagnostics(config: AppConfig, exchange_rules: SymbolRules) -> None:
    lines = [
        "-" * 50,
        "MARKET CONFIGURATION",
        f"Symbol: {config.symbol}",
        f"Base Asset: {exchange_rules.base_asset}",
        f"Quote Asset: {exchange_rules.quote_asset}",
        "",
        "Exchange Filters:",
        f"Tick Size: {_format_decimal(exchange_rules.tick_size)}",
        f"Step Size: {_format_decimal(exchange_rules.step_size)}",
        f"Minimum Quantity: {_format_decimal(exchange_rules.min_qty)}",
        (
            f"Minimum Order Value ({exchange_rules.quote_asset}): "
            f"{_format_decimal(exchange_rules.min_notional)}"
        ),
        "",
        "Derived Limits:",
        f"Max Position USD: {_format_decimal(config.max_position_usd)}",
        f"Per Trade Risk USD: {_format_decimal(config.per_trade_risk_usd)}",
        "",
        "Status:",
        "Market rules loaded successfully.",
        "-" * 50,
    ]
    print("\n".join(lines))
    say(
        "Market configuration loaded and exchangeInfo confirmed for {symbol}. Tick size={tick} step size={step}.",
        symbol=config.symbol,
        tick=_format_decimal(exchange_rules.tick_size),
        step=_format_decimal(exchange_rules.step_size),
    )


def _format_decimal(value: float) -> str:
    formatted = f"{value:.16f}".rstrip("0").rstrip(".")
    return formatted or "0"


def _market_data_source_label(config: AppConfig) -> str:
    if config.market_data_mode == "replay":
        source = config.replay_source or "any"
        return f"replay:{source}"
    return "live_ws"


def _install_signal_handlers(stop_event: asyncio.Event, logger) -> None:
    def stop_handler(*_args):
        logger.info("Shutdown signal received")
        stop_event.set()

    if sys.platform == "win32":
        signal.signal(signal.SIGINT, stop_handler)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, stop_handler)
        return

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_handler)


def _print_session_summary(summary: dict[str, object]) -> None:
    start_time = _format_datetime(summary.get("start_time"))
    end_time = _format_datetime(summary.get("end_time"))
    lines = [
        "-" * 50,
        "SESSION SUMMARY (Beginner-Friendly)",
        f"Symbol: {summary.get('symbol', '')}",
        f"Start time: {start_time}",
        f"End time: {end_time}",
        f"Runtime: {summary.get('runtime', '0s')}",
        "",
        "Trades:",
        f"- Total trades: {int(summary.get('total_trades', 0))}",
        f"- Wins: {int(summary.get('winning_trades', 0))}",
        f"- Losses: {int(summary.get('losing_trades', 0))}",
        f"- Breakeven: {int(summary.get('breakeven_trades', 0))}",
        f"- Win rate (wins/total): {float(summary.get('win_rate', 0.0)):.1%}",
        f"- Fill rate: {float(summary.get('fill_rate', 0.0)):.2f}",
        "",
        "PnL:",
        f"- Gross PnL (USDT): {float(summary.get('daily_pnl_usd', 0.0)):+.4f}",
        f"- Max drawdown (USDT): {float(summary.get('max_drawdown_usd', 0.0)):.4f}",
        f"- Avg PnL per trade: {float(summary.get('avg_pnl_usd', 0.0)):+.4f}",
        "",
        "Execution quality:",
        f"- Maker rejections (-2010): {int(summary.get('count_2010', 0))}",
        f"- Timestamp errors (-1021): {int(summary.get('count_1021', 0))}",
        (
            "- Requotes (entry/exit): "
            f"{int(summary.get('entry_requotes', 0))}/{int(summary.get('exit_requotes', 0))}"
        ),
        "-" * 50,
    ]
    print("\n".join(lines))


def _format_datetime(value: object) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return ""


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
