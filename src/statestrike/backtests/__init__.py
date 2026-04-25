from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
from pydantic import BaseModel, ConfigDict, Field

from statestrike.paths import build_normalized_path
from statestrike.readiness import (
    BacktestReadinessReport,
    ReadinessProfileName,
    run_backtest_readiness,
)
from statestrike.storage import _parquet_source

# Diagnostics backtests use DuckDB only to query normalized parquet inputs.
# They are not a storage or online write path.


class BacktestRunResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    status: str
    symbol: str | None = None
    trade_count: int = Field(default=0, ge=0)
    order_count: int = Field(default=0, ge=0)
    pnl: float = 0.0
    readiness_report: BacktestReadinessReport


def run_sanity_noop(
    *,
    root: Path,
    trading_date: date,
    symbols: tuple[str, ...],
) -> BacktestRunResult:
    readiness_report = _require_readiness(
        root=root,
        trading_date=trading_date,
        symbols=symbols,
    )
    return BacktestRunResult(
        name="sanity_noop",
        status="completed",
        trade_count=0,
        order_count=0,
        pnl=0.0,
        readiness_report=readiness_report,
    )


def run_sanity_single_trade_roundtrip(
    *,
    root: Path,
    trading_date: date,
    symbol: str,
) -> BacktestRunResult:
    symbol = symbol.upper()
    readiness_report = _require_readiness(
        root=root,
        trading_date=trading_date,
        symbols=(symbol,),
        profile="nautilus_baseline_ready",
        allow_warning=True,
    )
    trades = _read_trade_frame(
        root=root,
        trading_date=trading_date,
        symbol=symbol,
    )
    if len(trades) < 2:
        raise ValueError("sanity_single_trade_roundtrip requires at least 2 trades")
    entry = float(trades[0]["price"])
    exit_ = float(trades[1]["price"])
    pnl = exit_ - entry
    return BacktestRunResult(
        name="sanity_single_trade_roundtrip",
        status="completed",
        symbol=symbol,
        trade_count=2,
        order_count=2,
        pnl=pnl,
        readiness_report=readiness_report,
    )


def run_baseline_simple_momentum(
    *,
    root: Path,
    trading_date: date,
    symbol: str,
) -> BacktestRunResult:
    symbol = symbol.upper()
    readiness_report = _require_readiness(
        root=root,
        trading_date=trading_date,
        symbols=(symbol,),
        profile="nautilus_baseline_ready",
        allow_warning=True,
    )
    trades = _read_trade_frame(
        root=root,
        trading_date=trading_date,
        symbol=symbol,
    )
    if len(trades) < 3:
        raise ValueError("baseline_simple_momentum requires at least 3 trades")

    pnl = 0.0
    order_count = 0
    for current, nxt in zip(trades, trades[1:], strict=False):
        current_price = float(current["price"])
        next_price = float(nxt["price"])
        if next_price > current_price:
            pnl += next_price - current_price
            order_count += 1
        elif next_price < current_price:
            pnl += current_price - next_price
            order_count += 1

    return BacktestRunResult(
        name="baseline_simple_momentum",
        status="completed",
        symbol=symbol,
        trade_count=len(trades),
        order_count=order_count,
        pnl=pnl,
        readiness_report=readiness_report,
    )


def _require_readiness(
    *,
    root: Path,
    trading_date: date,
    symbols: tuple[str, ...],
    profile: ReadinessProfileName = "funding_aware_ready",
    allow_warning: bool = False,
) -> BacktestReadinessReport:
    readiness_report = run_backtest_readiness(
        root=root,
        trading_date=trading_date,
        symbols=symbols,
        profile=profile,
    )
    allowed_statuses = {"ready", "warning"} if allow_warning else {"ready"}
    if readiness_report.status not in allowed_statuses:
        raise ValueError(
            f"backtest dataset not ready: {', '.join(readiness_report.blocking_reasons or readiness_report.warning_reasons)}"
        )
    return readiness_report


def _read_trade_frame(
    *,
    root: Path,
    trading_date: date,
    symbol: str,
) -> list[dict[str, object]]:
    partition_root = build_normalized_path(
        root=root,
        channel="trades",
        trading_date=trading_date,
        symbol=symbol,
    )
    files = sorted(partition_root.glob("*.parquet"))
    if not files:
        raise ValueError(f"no normalized trades found for {symbol}")
    connection = duckdb.connect()
    try:
        source = _parquet_source(files)
        frame = connection.execute(
            f"""
            SELECT *
            FROM {source}
            ORDER BY exchange_ts, recv_ts_ns, recv_seq, trade_event_id
            """
        ).fetchdf()
    finally:
        connection.close()
    return frame.to_dict(orient="records")
