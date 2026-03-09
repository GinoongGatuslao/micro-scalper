from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .config import load_dotenv


@dataclass(slots=True)
class TradeReport:
    symbol: str
    entry_order_id: str
    open_ts: datetime | None
    close_ts: datetime | None
    realized_pnl_usd: float
    estimated_fees_usd: float
    hold_seconds: float | None
    fill_count: int


@dataclass(slots=True)
class DailyReport:
    day: str
    realized_pnl_usd: float
    estimated_fees_usd: float
    trade_count: int
    win_count: int


@dataclass(slots=True)
class SummaryReport:
    trades: list[TradeReport]
    daily: list[DailyReport]
    total_realized_pnl_usd: float
    win_rate: float
    avg_profit_per_trade_usd: float
    avg_hold_seconds: float | None
    estimated_fees_usd: float
    max_drawdown_usd: float


def build_report(db_path: str, *, fee_rate_bps: float = 10.0, symbol: str | None = None) -> SummaryReport:
    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(path)
    try:
        conn.row_factory = sqlite3.Row
        trades = _build_trades(conn, fee_rate_bps=fee_rate_bps, symbol=symbol)
        daily = _build_daily(trades)
        total_realized = sum(item.realized_pnl_usd for item in trades)
        estimated_fees = sum(item.estimated_fees_usd for item in trades)
        trade_count = len(trades)
        wins = sum(1 for item in trades if item.realized_pnl_usd > 0)
        avg_profit = total_realized / trade_count if trade_count else 0.0
        hold_values = [item.hold_seconds for item in trades if item.hold_seconds is not None]
        avg_hold = sum(hold_values) / len(hold_values) if hold_values else None
        max_drawdown = _max_drawdown([item.realized_pnl_usd for item in trades])
        return SummaryReport(
            trades=trades,
            daily=daily,
            total_realized_pnl_usd=total_realized,
            win_rate=(wins / trade_count) if trade_count else 0.0,
            avg_profit_per_trade_usd=avg_profit,
            avg_hold_seconds=avg_hold,
            estimated_fees_usd=estimated_fees,
            max_drawdown_usd=max_drawdown,
        )
    finally:
        conn.close()


def export_trades_csv(path: str, trades: list[TradeReport]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "symbol",
                "entry_order_id",
                "open_ts",
                "close_ts",
                "realized_pnl_usd",
                "estimated_fees_usd",
                "hold_seconds",
                "fill_count",
            ],
        )
        writer.writeheader()
        for trade in trades:
            writer.writerow(
                {
                    "symbol": trade.symbol,
                    "entry_order_id": trade.entry_order_id,
                    "open_ts": trade.open_ts.isoformat() if trade.open_ts else "",
                    "close_ts": trade.close_ts.isoformat() if trade.close_ts else "",
                    "realized_pnl_usd": f"{trade.realized_pnl_usd:.8f}",
                    "estimated_fees_usd": f"{trade.estimated_fees_usd:.8f}",
                    "hold_seconds": f"{trade.hold_seconds:.3f}" if trade.hold_seconds is not None else "",
                    "fill_count": trade.fill_count,
                }
            )


def _build_trades(conn: sqlite3.Connection, *, fee_rate_bps: float, symbol: str | None) -> list[TradeReport]:
    order_times = _load_order_times(conn, symbol=symbol)
    pnl_rows = _load_pnl_rows(conn, symbol=symbol)
    grouped: dict[str, dict[str, Any]] = {}

    for row in pnl_rows:
        payload = row["payload"]
        entry_order_id = str(payload.get("entry_order_id") or "")
        if not entry_order_id:
            continue
        trade = grouped.setdefault(
            entry_order_id,
            {
                "symbol": row["symbol"],
                "entry_order_id": entry_order_id,
                "realized_pnl_usd": 0.0,
                "estimated_fees_usd": 0.0,
                "close_ts": [],
                "fill_count": 0,
            },
        )
        quantity = float(payload.get("quantity", 0.0) or 0.0)
        entry_price = float(payload.get("entry_price", 0.0) or 0.0)
        exit_price = float(payload.get("exit_price", 0.0) or 0.0)
        notional = (entry_price * quantity) + (exit_price * quantity)
        trade["realized_pnl_usd"] += float(row["realized_pnl_usd"])
        trade["estimated_fees_usd"] += notional * (fee_rate_bps / 10_000)
        trade["fill_count"] += 1
        exit_order_id = payload.get("exit_order_id")
        close_ts = order_times[exit_order_id]["last_ts"] if exit_order_id and exit_order_id in order_times else row["ts"]
        trade["close_ts"].append(close_ts)

    trade_reports: list[TradeReport] = []
    for entry_order_id, trade in grouped.items():
        open_ts = order_times.get(entry_order_id, {}).get("first_ts")
        close_ts_values = trade["close_ts"]
        close_ts = max(close_ts_values) if close_ts_values else None
        hold_seconds = None
        if open_ts and close_ts:
            hold_seconds = max(0.0, (close_ts - open_ts).total_seconds())
        trade_reports.append(
            TradeReport(
                symbol=trade["symbol"],
                entry_order_id=entry_order_id,
                open_ts=open_ts,
                close_ts=close_ts,
                realized_pnl_usd=trade["realized_pnl_usd"],
                estimated_fees_usd=trade["estimated_fees_usd"],
                hold_seconds=hold_seconds,
                fill_count=trade["fill_count"],
            )
        )

    trade_reports.sort(key=lambda item: item.close_ts or item.open_ts or datetime.min)
    return trade_reports


def _build_daily(trades: list[TradeReport]) -> list[DailyReport]:
    daily: dict[str, DailyReport] = {}
    for trade in trades:
        day = (trade.close_ts or trade.open_ts)
        if day is None:
            continue
        day_key = day.date().isoformat()
        entry = daily.setdefault(
            day_key,
            DailyReport(
                day=day_key,
                realized_pnl_usd=0.0,
                estimated_fees_usd=0.0,
                trade_count=0,
                win_count=0,
            ),
        )
        entry.realized_pnl_usd += trade.realized_pnl_usd
        entry.estimated_fees_usd += trade.estimated_fees_usd
        entry.trade_count += 1
        if trade.realized_pnl_usd > 0:
            entry.win_count += 1
    return [daily[key] for key in sorted(daily)]


def _max_drawdown(realized_pnls: list[float]) -> float:
    peak = 0.0
    equity = 0.0
    max_drawdown = 0.0
    for pnl in realized_pnls:
        equity += pnl
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return max_drawdown


def _load_order_times(conn: sqlite3.Connection, *, symbol: str | None) -> dict[str, dict[str, datetime]]:
    query = "SELECT client_order_id, ts FROM orders"
    params: list[Any] = []
    if symbol:
        query += " WHERE symbol = ?"
        params.append(symbol)
    query += " ORDER BY ts ASC"

    order_times: dict[str, dict[str, datetime]] = {}
    for row in conn.execute(query, params):
        ts = _parse_ts(row["ts"])
        client_order_id = row["client_order_id"]
        entry = order_times.setdefault(client_order_id, {"first_ts": ts, "last_ts": ts})
        if ts < entry["first_ts"]:
            entry["first_ts"] = ts
        if ts > entry["last_ts"]:
            entry["last_ts"] = ts
    return order_times


def _load_pnl_rows(conn: sqlite3.Connection, *, symbol: str | None) -> list[dict[str, Any]]:
    query = "SELECT ts, symbol, realized_pnl_usd, payload FROM pnl"
    params: list[Any] = []
    if symbol:
        query += " WHERE symbol = ?"
        params.append(symbol)
    query += " ORDER BY ts ASC"

    rows: list[dict[str, Any]] = []
    for row in conn.execute(query, params):
        payload = json.loads(row["payload"]) if row["payload"] else {}
        rows.append(
            {
                "ts": _parse_ts(row["ts"]),
                "symbol": row["symbol"],
                "realized_pnl_usd": float(row["realized_pnl_usd"]),
                "payload": payload if isinstance(payload, dict) else {},
            }
        )
    return rows


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "n/a"
    total_seconds = int(round(seconds))
    minutes, secs = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m {secs}s"


def _print_report(summary: SummaryReport) -> None:
    print(f"Trades: {len(summary.trades)}")
    print(f"Realized PnL: {summary.total_realized_pnl_usd:.8f} USD")
    print(f"Win rate: {summary.win_rate:.2%}")
    print(f"Avg profit/trade: {summary.avg_profit_per_trade_usd:.8f} USD")
    print(f"Avg hold time: {_format_duration(summary.avg_hold_seconds)}")
    print(f"Fees estimate: {summary.estimated_fees_usd:.8f} USD")
    print(f"Max drawdown: {summary.max_drawdown_usd:.8f} USD")
    print("")
    print("Daily PnL")
    if not summary.daily:
        print("  no completed trades")
        return
    for item in summary.daily:
        win_rate = (item.win_count / item.trade_count) if item.trade_count else 0.0
        print(
            f"  {item.day} | pnl={item.realized_pnl_usd:.8f} | trades={item.trade_count} "
            f"| win_rate={win_rate:.2%} | fees={item.estimated_fees_usd:.8f}"
        )


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Generate a trading report from the SQLite audit log.")
    parser.add_argument("--db-path", default=os.getenv("DB_PATH", "data/bot.sqlite3"), help="Path to the SQLite database.")
    parser.add_argument("--symbol", default=None, help="Optional symbol filter, e.g. BTCUSDT.")
    parser.add_argument(
        "--fee-rate-bps",
        type=float,
        default=float(os.getenv("ESTIMATED_FEE_BPS", "10.0")),
        help="Estimated fee rate in basis points applied on both entry and exit notional.",
    )
    parser.add_argument("--export-csv", default=None, help="Optional path to export per-trade CSV.")
    args = parser.parse_args()

    summary = build_report(args.db_path, fee_rate_bps=args.fee_rate_bps, symbol=args.symbol)
    _print_report(summary)
    if args.export_csv:
        export_trades_csv(args.export_csv, summary.trades)
        print("")
        print(f"CSV exported to {args.export_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
