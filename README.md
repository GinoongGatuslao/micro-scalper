# Micro Scalper MVP

Production-minded personal-use MVP for Binance Spot, starting on testnet. The bot is intentionally narrow:

- long-only spread-capture entry
- maker-first entry and take-profit exits
- maker-price guard with tick offsets and retry-once handling for `LIMIT_MAKER`
- deterministic imbalance and volatility filters
- market sanity checks for bad quotes, stale quotes, and wide spreads
- loss-streak cooldown and daily loss protection
- SQLite audit trail for signals, orders, cancels, fills, PnL, and balances
- dry-run mode for decision testing without order placement
- paper-trading mode with simulated fills and latency
- replay mode from recorded market snapshots
- optional human-readable console stream alongside structured logs

> No live strategy can be guaranteed “100% winning.” The aggressive profile below aims for higher win rate and faster exits while capping downside so you don’t blow up the bankroll.

## Aggressive Live Profile (≈$1k capital target)

Set `BOT_PROFILE=agg_live` (default in `.env.example`) for a live-leaning preset tuned for more favorable setups and a controlled drawdown cap:

- Entry selectivity for win rate: `SPREAD_MIN_BPS=1.2`, `IMB_MIN=1.10`, `VOL_MAX_BPS=6`, `VOL_WINDOW=12`.
- Faster cadence but not spammy: `ENTRY_ATTEMPT_INTERVAL_MS=350`, `MAX_CONSECUTIVE_REJECTIONS=10`, maker offset = 1 tick.
- Exit speed-ups: `ALLOW_TAKER_EXIT=true`, `EXIT_MAX_REQUOTES=8`, tight maker offsets to avoid languishing orders.
- Risk sizing around $1k: `MAX_POSITION_USD=400`, `PER_TRADE_RISK_USD=20` (≈2% of capital at $1k), `MAX_OPEN_ORDERS=3`.
- Loss caps: `DAILY_MAX_LOSS_USD=80` and `MAX_DRAWDOWN_PCT=20` (kill switch if cumulative loss hits 20% of starting capital).

Run (testnet endpoints by default):

```bash
BOT_PROFILE=agg_live python -m bot.run
```

Other profiles:

- `balanced` (default if unset): original parameters.
- `hf_paper`: high-frequency paper-only profile for fast iteration without live risk.

## Repo Layout

```text
bot/
  run.py
  binance_client.py
  strategy.py
  risk.py
  execution.py
  datastore.py
  models.py
  logging.py
  metrics.py
tests/
```

## Setup

1. Create and activate a Python 3.11+ virtual environment.
2. Install dependencies:

```bash
pip install websockets
```

3. Copy `.env.example` to `.env` and fill in testnet credentials.

## Environment

```dotenv
BINANCE_API_KEY=
BINANCE_API_SECRET=
BINANCE_REST_BASE_URL=https://testnet.binance.vision
BINANCE_WS_BASE_URL=wss://stream.testnet.binance.vision
SYMBOL=BTCUSDT
DRY_RUN=true
PAPER_TRADING=false
MARKET_DATA_MODE=live
DB_PATH=data/bot.sqlite3
REPLAY_DB_PATH=data/bot.sqlite3
REPLAY_SOURCE=live_ws
REPLAY_LIMIT=
REPLAY_SPEED=0
LOG_LEVEL=INFO
ESTIMATED_FEE_BPS=10.0
SPREAD_MIN_BPS=1.0
SPREAD_MIN_BPS=1.5
IMB_MIN=1.3
VOL_MAX_BPS=4
VOL_WINDOW=20
ENTRY_TTL=15
EXIT_TTL=12
BOT_MAKER_OFFSET_TICKS=2
MAX_SPREAD_BPS=20
MAX_POSITION_USD=70
MAX_OPEN_ORDERS=2
DAILY_MAX_LOSS_USD=8
PER_TRADE_RISK_USD=0.75
SL_BPS=10
ORDER_POLL_INTERVAL=1.0
BALANCE_SNAPSHOT_INTERVAL=300
HUMAN_CONSOLE=true
SIM_LATENCY_MIN_MS=50
SIM_LATENCY_MAX_MS=250
SIM_QUOTE_BALANCE=1000
SIM_BASE_BALANCE=0
TICK_SIZE=
STEP_SIZE=
MIN_QTY=
MIN_NOTIONAL=
BASE_ASSET=
QUOTE_ASSET=
```

## Key Env Vars

- `MAX_SPREAD_BPS`: hard order-entry guard. The bot skips new orders when spread exceeds this threshold.
- `BOT_MAKER_OFFSET_TICKS`: extra safety distance applied to maker prices before quantization.
- `ENTRY_TTL` / `EXIT_TTL`: how long entry and exit orders may rest before cancel/reprice handling.
- `HUMAN_CONSOLE`: `true` by default. Set `false` to disable the extra plain-English stdout messages.
- `TICK_SIZE`, `STEP_SIZE`, `MIN_QTY`, `MIN_NOTIONAL`, `BASE_ASSET`, `QUOTE_ASSET`: optional symbol-rule overrides for replay or offline operation.

## Run

Dry-run:

```bash
python -m bot.run
```

Paper trading with live WS:

1. Set `DRY_RUN=false`.
2. Set `PAPER_TRADING=true`.
3. Keep `MARKET_DATA_MODE=live`.
4. Run `python -m bot.run`.

Paper trading replay from recorded market data:

1. First record live market data by running either dry-run or paper mode with `MARKET_DATA_MODE=live`.
2. Set `PAPER_TRADING=true`.
3. Set `MARKET_DATA_MODE=replay`.
4. Optionally set `REPLAY_DB_PATH`, `REPLAY_SOURCE`, `REPLAY_LIMIT`, and `REPLAY_SPEED`.
5. If you want replay without Binance REST access, provide `TICK_SIZE`, `STEP_SIZE`, `MIN_QTY`, `MIN_NOTIONAL`, `BASE_ASSET`, and `QUOTE_ASSET`.
6. Run `python -m bot.run`.

Live on testnet:

1. Set `DRY_RUN=false`.
2. Set `PAPER_TRADING=false`.
3. Keep `MARKET_DATA_MODE=live`.
4. Fill `BINANCE_API_KEY` and `BINANCE_API_SECRET`.
5. Run `python -m bot.run`.

Report:

```bash
python -m bot.report
python -m bot.report --symbol BTCUSDT --fee-rate-bps 10 --export-csv data/reports/btcusdt_trades.csv
```

## Notes

- BUY `LIMIT_MAKER` prices are forced strictly below best ask. SELL `LIMIT_MAKER` prices are forced strictly above best bid.
- The maker guard applies `BOT_MAKER_OFFSET_TICKS`, quantizes to tick size, and logs `desired_price`, `adjusted_price`, `delta_ticks`, `best_bid`, `best_ask`, `tick_size`, and `offset_ticks`.
- Entry uses `LIMIT_MAKER` near best bid with final maker-safety validation against cached book prices.
- Normal take-profit exit uses `LIMIT_MAKER` near best ask with the same maker-safety guard and a single `-2010` retry.
- Hard stop-loss does not use market orders. It cancels the existing exit and sends a more aggressive plain `LIMIT` sell at the current best bid to prioritize flattening.
- The bot skips order placement when quotes are invalid, crossed, stale by more than 500ms, or when spread exceeds `MAX_SPREAD_BPS`.
- Order state is polled over REST. This keeps the MVP simpler than adding a user data stream.
- Paper mode simulates order activation latency and fills only when best bid/ask crosses the order price after the order becomes active.
- Recorded market snapshots are stored in SQLite `market_data` and can be replayed later.
- Daily loss kill switch blocks new entries after the configured realized loss threshold is breached.
- Three consecutive losing trades trigger a 10-minute cooldown before new entries are allowed again.
- Internal metrics track total trades, win/loss counts, average PnL, fill rate, and max drawdown, with a summary log every 10 minutes.

## Human Console

When `HUMAN_CONSOLE=true`, the bot prints short stdout lines prefixed like:

```text
[HUMAN] 2026-03-04T12:34:56.000000+00:00 Connected to market data stream for BTCUSDT.
```

These messages are additive only. Existing Python logger output and log levels are unchanged.

The human console reports:

- stream connection
- entry attempts, placement, and TTL cancels
- maker rejections and retry outcome
- entry fills and exit placement
- trade close PnL summaries
- stop-loss activity
- skip reasons for sanity and safety gates

Identical human skip messages are rate-limited to once every 5 seconds to avoid loop spam.

## Tests

```bash
python -m unittest discover -s tests -v
```

## Operational Guardrails

- No secrets are hardcoded.
- SIGINT and SIGTERM trigger graceful shutdown.
- Open orders are canceled on shutdown when not in dry-run mode.
- WebSocket reconnects use exponential backoff.
