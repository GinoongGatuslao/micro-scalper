from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, replace
from pathlib import Path


@dataclass(slots=True)
class AppConfig:
    profile: str
    symbol: str
    dry_run: bool
    paper_trading: bool
    market_data_mode: str
    db_path: str
    replay_db_path: str
    replay_source: str | None
    replay_limit: int | None
    replay_speed: float
    log_level: str
    rest_base_url: str
    ws_base_url: str
    api_key: str
    api_secret: str
    spread_min_bps: float
    imbalance_min: float
    volatility_max_bps: float
    volatility_window: int
    entry_ttl_seconds: int
    exit_ttl_seconds: int
    maker_offset_ticks: int
    offset_ticks_min: int
    offset_ticks_max: int
    spread_tight_bps: float
    spread_wide_bps: float
    entry_attempt_interval_ms: int
    rejection_cooldown_ms: int
    max_consecutive_rejections: int
    allow_taker_exit: bool
    entry_max_requotes: int
    exit_max_requotes: int
    market_stale_after_ms: int
    breakeven_epsilon: float
    max_spread_bps: float
    max_position_usd: float
    max_open_orders: int
    daily_max_loss_usd: float
    per_trade_risk_usd: float
    starting_capital_usd: float
    max_drawdown_pct: float
    sl_bps: float
    order_poll_interval: float
    balance_snapshot_interval: float
    sim_latency_min_ms: int
    sim_latency_max_ms: int
    sim_quote_balance: float
    sim_base_balance: float
    tick_size: float | None
    step_size: float | None
    min_qty: float | None
    min_notional: float | None
    base_asset: str | None
    quote_asset: str | None


def load_dotenv(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def load_app_config() -> AppConfig:
    load_dotenv()
    file_overrides = {}
    config_path = os.getenv("BOT_CONFIG_TOML")
    if config_path:
        with open(config_path, "rb") as handle:
            file_overrides = tomllib.load(handle)

    def pick(name: str, default: str) -> str:
        return str(os.getenv(name, file_overrides.get(name.lower(), default)))

    def pick_bool(name: str, default: bool) -> bool:
        raw = os.getenv(name)
        if raw is None:
            raw = file_overrides.get(name.lower(), default)
        if isinstance(raw, bool):
            return raw
        return str(raw).strip().lower() in {"1", "true", "yes", "on"}

    def pick_optional_float(name: str) -> float | None:
        raw = os.getenv(name)
        if raw is None:
            raw = file_overrides.get(name.lower())
        if raw in {None, ""}:
            return None
        return float(raw)

    def pick_optional_int(name: str) -> int | None:
        raw = os.getenv(name)
        if raw is None:
            raw = file_overrides.get(name.lower())
        if raw in {None, ""}:
            return None
        return int(raw)

    def pick_optional_str(name: str) -> str | None:
        raw = os.getenv(name)
        if raw is None:
            raw = file_overrides.get(name.lower())
        if raw in {None, ""}:
            return None
        return str(raw)

    config = AppConfig(
        profile=pick("BOT_PROFILE", "balanced").lower(),
        symbol=pick("SYMBOL", "BTCUSDT").upper(),
        dry_run=pick_bool("DRY_RUN", True),
        paper_trading=pick_bool("PAPER_TRADING", False),
        market_data_mode=pick("MARKET_DATA_MODE", "live").lower(),
        db_path=pick("DB_PATH", "data/bot.sqlite3"),
        replay_db_path=pick("REPLAY_DB_PATH", pick("DB_PATH", "data/bot.sqlite3")),
        replay_source=pick_optional_str("REPLAY_SOURCE"),
        replay_limit=pick_optional_int("REPLAY_LIMIT"),
        replay_speed=float(pick("REPLAY_SPEED", "0")),
        log_level=pick("LOG_LEVEL", "INFO"),
        rest_base_url=pick("BINANCE_REST_BASE_URL", "https://testnet.binance.vision"),
        ws_base_url=pick("BINANCE_WS_BASE_URL", "wss://stream.testnet.binance.vision"),
        api_key=pick("BINANCE_API_KEY", ""),
        api_secret=pick("BINANCE_API_SECRET", ""),
        spread_min_bps=float(pick("SPREAD_MIN_BPS", pick("SPREAD_MIN", "1.5"))),
        imbalance_min=float(pick("IMB_MIN", "1.3")),
        volatility_max_bps=float(pick("VOL_MAX_BPS", pick("VOL_MAX", "4"))),
        volatility_window=int(pick("VOL_WINDOW", "20")),
        entry_ttl_seconds=int(pick("ENTRY_TTL", "15")),
        exit_ttl_seconds=int(pick("EXIT_TTL", "12")),
        maker_offset_ticks=int(pick("BOT_MAKER_OFFSET_TICKS", pick("MAKER_OFFSET_TICKS", "2"))),
        offset_ticks_min=int(pick("OFFSET_TICKS_MIN", "0")),
        offset_ticks_max=int(pick("OFFSET_TICKS_MAX", "3")),
        spread_tight_bps=float(pick("SPREAD_TIGHT_BPS", "3.0")),
        spread_wide_bps=float(pick("SPREAD_WIDE_BPS", "10.0")),
        entry_attempt_interval_ms=int(pick("ENTRY_ATTEMPT_INTERVAL_MS", "500")),
        rejection_cooldown_ms=int(pick("REJECTION_COOLDOWN_MS", "1500")),
        max_consecutive_rejections=int(pick("MAX_CONSECUTIVE_REJECTIONS", "5")),
        allow_taker_exit=pick_bool("ALLOW_TAKER_EXIT", False),
        entry_max_requotes=int(pick("ENTRY_MAX_REQUOTES", "5")),
        exit_max_requotes=int(pick("EXIT_MAX_REQUOTES", "6")),
        market_stale_after_ms=int(pick("MARKET_STALE_AFTER_MS", "500")),
        breakeven_epsilon=float(pick("BREAKEVEN_EPSILON", "1e-9")),
        max_spread_bps=float(pick("MAX_SPREAD_BPS", "20")),
        max_position_usd=float(pick("MAX_POSITION_USD", "70")),
        max_open_orders=int(pick("MAX_OPEN_ORDERS", "2")),
        daily_max_loss_usd=float(pick("DAILY_MAX_LOSS_USD", "8")),
        per_trade_risk_usd=float(pick("PER_TRADE_RISK_USD", "0.75")),
        starting_capital_usd=float(pick("STARTING_CAPITAL_USD", "1000")),
        max_drawdown_pct=float(pick("MAX_DRAWDOWN_PCT", "20")),
        sl_bps=float(pick("SL_BPS", "10")),
        order_poll_interval=float(pick("ORDER_POLL_INTERVAL", "1.0")),
        balance_snapshot_interval=float(pick("BALANCE_SNAPSHOT_INTERVAL", "300")),
        sim_latency_min_ms=int(pick("SIM_LATENCY_MIN_MS", "50")),
        sim_latency_max_ms=int(pick("SIM_LATENCY_MAX_MS", "250")),
        sim_quote_balance=float(pick("SIM_QUOTE_BALANCE", "1000")),
        sim_base_balance=float(pick("SIM_BASE_BALANCE", "0")),
        tick_size=pick_optional_float("TICK_SIZE"),
        step_size=pick_optional_float("STEP_SIZE"),
        min_qty=pick_optional_float("MIN_QTY"),
        min_notional=pick_optional_float("MIN_NOTIONAL"),
        base_asset=pick_optional_str("BASE_ASSET"),
        quote_asset=pick_optional_str("QUOTE_ASSET"),
    )

    return _apply_profile_overrides(config)


def _apply_profile_overrides(config: AppConfig) -> AppConfig:
    """
    Profile-aware overrides for preset tuning.
    hf_paper: aggressive high-frequency paper profile sized for ~1000 USDT virtual capital.
    agg_live: live-leaning profile that prioritizes higher win-rate and faster exits while capping drawdown.
    """
    if config.profile == "hf_paper":
        return replace(
            config,
            dry_run=False,  # rely on paper trading fills for realism
            paper_trading=True,
            spread_min_bps=min(config.spread_min_bps, 0.8),
            imbalance_min=max(config.imbalance_min, 1.05),
            volatility_max_bps=max(config.volatility_max_bps, 12.0),
            volatility_window=min(config.volatility_window, 10),
            maker_offset_ticks=min(config.maker_offset_ticks, 1),
            offset_ticks_min=0,
            offset_ticks_max=max(config.offset_ticks_max, 2),
            entry_attempt_interval_ms=min(config.entry_attempt_interval_ms, 250),
            rejection_cooldown_ms=min(config.rejection_cooldown_ms, 800),
            max_consecutive_rejections=max(config.max_consecutive_rejections, 8),
            allow_taker_exit=True,
            entry_max_requotes=max(config.entry_max_requotes, 8),
            exit_max_requotes=max(config.exit_max_requotes, 8),
            max_spread_bps=max(config.max_spread_bps, 30.0),
            max_position_usd=max(config.max_position_usd, 250.0),
            max_open_orders=max(config.max_open_orders, 3),
            daily_max_loss_usd=max(config.daily_max_loss_usd, 40.0),
            per_trade_risk_usd=max(config.per_trade_risk_usd, 10.0),
            starting_capital_usd=max(config.starting_capital_usd, 1000.0),
            max_drawdown_pct=min(config.max_drawdown_pct, 20.0),
            sim_quote_balance=max(config.sim_quote_balance, 1000.0),
            sim_base_balance=0.0,
        )

    if config.profile == "agg_live":
        return replace(
            config,
            dry_run=False,
            paper_trading=False,
            spread_min_bps=max(config.spread_min_bps, 1.2),
            imbalance_min=max(config.imbalance_min, 1.10),
            volatility_max_bps=min(config.volatility_max_bps, 6.0),
            volatility_window=max(8, min(config.volatility_window, 20)),
            maker_offset_ticks=min(config.maker_offset_ticks, 1),
            offset_ticks_min=0,
            offset_ticks_max=max(config.offset_ticks_max, 2),
            entry_attempt_interval_ms=min(config.entry_attempt_interval_ms, 350),
            rejection_cooldown_ms=min(config.rejection_cooldown_ms, 1000),
            max_consecutive_rejections=max(config.max_consecutive_rejections, 10),
            allow_taker_exit=True,
            entry_max_requotes=max(config.entry_max_requotes, 8),
            exit_max_requotes=max(config.exit_max_requotes, 8),
            max_spread_bps=min(max(config.max_spread_bps, 25.0), 35.0),
            max_position_usd=max(config.max_position_usd, 400.0),
            max_open_orders=max(config.max_open_orders, 3),
            daily_max_loss_usd=max(config.daily_max_loss_usd, 80.0),
            per_trade_risk_usd=max(config.per_trade_risk_usd, 20.0),
            starting_capital_usd=max(config.starting_capital_usd, 1000.0),
            max_drawdown_pct=min(config.max_drawdown_pct, 20.0),
        )

    return config
