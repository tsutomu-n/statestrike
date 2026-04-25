from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Literal

from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.identifiers import InstrumentId, TradeId
from nautilus_trader.model.objects import Price, Quantity
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from statestrike.paths import build_export_path
from statestrike.readiness import BacktestReadinessReport, run_backtest_readiness


class NautilusBaselineResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    strategy_name: str
    symbol: str
    date_range: tuple[str, str]
    trade_count: int = Field(ge=0)
    order_count: int = Field(ge=0)
    fee: float
    realized_pnl: float
    max_drawdown: float
    dataset_profile: str
    readiness_status: str
    funding_treatment: Literal["used", "ignored", "missing_blocked"]
    ts_init_min_ns: int | None = None
    ts_init_max_ns: int | None = None
    readiness_report: BacktestReadinessReport


def run_nautilus_simple_momentum(
    *,
    root: Path,
    trading_date: date,
    symbol: str,
) -> NautilusBaselineResult:
    symbol = symbol.upper()
    readiness_report = run_backtest_readiness(
        root=root,
        trading_date=trading_date,
        symbols=(symbol,),
        profile="nautilus_baseline_ready",
    )
    if readiness_report.status == "blocked":
        raise ValueError(
            f"Nautilus baseline dataset not ready: {', '.join(readiness_report.blocking_reasons)}"
        )

    ticks = _load_truth_trade_ticks(
        root=root,
        trading_date=trading_date,
        symbol=symbol,
    )
    if len(ticks) < 3:
        raise ValueError("nautilus_simple_momentum requires at least 3 trade ticks")

    realized_pnl, order_count, max_drawdown = _run_simple_momentum(ticks)
    ts_init_values = [tick.ts_init for tick in ticks]
    return NautilusBaselineResult(
        strategy_name="nautilus_simple_momentum",
        symbol=symbol,
        date_range=(trading_date.isoformat(), trading_date.isoformat()),
        trade_count=len(ticks),
        order_count=order_count,
        fee=0.0,
        realized_pnl=realized_pnl,
        max_drawdown=max_drawdown,
        dataset_profile=readiness_report.profile.name,
        readiness_status=readiness_report.status,
        funding_treatment="ignored",
        ts_init_min_ns=min(ts_init_values),
        ts_init_max_ns=max(ts_init_values),
        readiness_report=readiness_report,
    )


def _load_truth_trade_ticks(
    *,
    root: Path,
    trading_date: date,
    symbol: str,
) -> list[TradeTick]:
    path = (
        build_export_path(
            root=root,
            category="truth",
            target="nautilus",
            trading_date=trading_date,
            symbol=symbol,
        )
        / "trade_ticks.parquet"
    )
    frame = pd.read_parquet(path)
    if frame.empty:
        return []
    frame = frame.sort_values(
        by=["exchange_ts", "ts_init_ns", "recv_seq", "trade_event_id"]
    ).reset_index(drop=True)
    instrument_id = InstrumentId.from_str(_instrument_id(symbol))
    return [
        TradeTick(
            instrument_id=instrument_id,
            price=Price(float(row["price"]), 8),
            size=Quantity(float(row["size"]), 8),
            aggressor_side=_aggressor_side(str(row["side"])),
            trade_id=TradeId(str(row["native_tid"] or row["trade_event_id"])[:36]),
            ts_event=_to_ns(int(row["exchange_ts"])),
            ts_init=int(row["ts_init_ns"]),
        )
        for row in frame.to_dict(orient="records")
    ]


def _run_simple_momentum(ticks: list[TradeTick]) -> tuple[float, int, float]:
    pnl = 0.0
    order_count = 0
    peak = 0.0
    max_drawdown = 0.0
    for current, nxt in zip(ticks, ticks[1:], strict=False):
        current_price = float(current.price)
        next_price = float(nxt.price)
        if next_price > current_price:
            pnl += next_price - current_price
            order_count += 1
        elif next_price < current_price:
            pnl += current_price - next_price
            order_count += 1
        peak = max(peak, pnl)
        max_drawdown = max(max_drawdown, peak - pnl)
    return pnl, order_count, max_drawdown


def _aggressor_side(side: str) -> AggressorSide:
    return AggressorSide.BUYER if side == "buy" else AggressorSide.SELLER


def _to_ns(timestamp: int) -> int:
    return timestamp * 1_000_000 if timestamp < 1_000_000_000_000_000 else timestamp


def _instrument_id(symbol: str) -> str:
    return f"{symbol.upper()}-USD-PERP.HYPERLIQUID"
