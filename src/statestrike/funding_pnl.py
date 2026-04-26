from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from statestrike.funding import (
    funding_history_manifest_path,
    funding_history_sidecar_path,
)
from statestrike.paths import build_export_path
from statestrike.storage import _read_parquet_frame


class FundingPositionInterval(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    opened_ts_ns: int = Field(ge=0)
    closed_ts_ns: int | None = Field(default=None, ge=0)
    signed_position_size: float
    source: str


class FundingLedgerEntry(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    funding_time_ms: int = Field(ge=0)
    position_size: float
    oracle_price: float = Field(gt=0.0)
    funding_rate: float
    funding_payment: float
    funding_pnl: float
    oracle_price_source: Literal["asset_ctx_asof_or_before"]
    funding_rate_source: Literal["fundingHistory"]
    mark_price_observed: float | None = Field(default=None, gt=0.0)
    formula: str = "funding_pnl = -(position_size * oracle_price * funding_rate)"


class FundingPnlLedger(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    trading_date: date
    source_type: Literal["fundingHistory"]
    endpoint_type: Literal["official_info"]
    funding_history_path: str
    funding_history_manifest_path: str
    position_source: str
    entry_count: int = Field(ge=0)
    funding_pnl: float
    entries: tuple[FundingLedgerEntry, ...]


class FundingAugmentedPnlSummary(BaseModel):
    model_config = ConfigDict(frozen=True)

    gross_pnl_ex_funding: float
    fee_cost: float = Field(ge=0.0)
    funding_pnl: float
    net_pnl_ex_funding: float
    net_pnl_after_funding: float
    net_pnl_ex_funding_source: str = (
        'Nautilus BacktestResult.stats_pnls["USD"]["PnL (total)"]'
    )
    funding_pnl_source: str = "ex_post_funding_ledger"


def build_funding_pnl_ledger(
    *,
    root: Path,
    trading_date: date,
    symbol: str,
    position_intervals: tuple[FundingPositionInterval, ...],
) -> FundingPnlLedger:
    symbol = symbol.upper()
    history_path = funding_history_sidecar_path(
        root=root,
        trading_date=trading_date,
    )
    manifest_path = funding_history_manifest_path(
        root=root,
        trading_date=trading_date,
    )
    manifest = _load_official_funding_history_manifest(
        manifest_path=manifest_path,
        root=root,
        trading_date=trading_date,
    )
    history = _read_parquet_frame(history_path)
    if history.empty:
        entries: tuple[FundingLedgerEntry, ...] = ()
    else:
        symbol_history = history[history["symbol"].astype(str).str.upper() == symbol]
        intervals = tuple(
            interval for interval in position_intervals if interval.symbol.upper() == symbol
        )
        asset_ctx = _load_asset_ctx(root=root, trading_date=trading_date, symbol=symbol)
        entries = tuple(
            _entry_for_funding_row(
                funding_row=row,
                interval=interval,
                asset_ctx=asset_ctx,
            )
            for _, row in symbol_history.sort_values("funding_time_ms").iterrows()
            if (interval := _position_at_funding_time(row, intervals)) is not None
        )

    return FundingPnlLedger(
        symbol=symbol,
        trading_date=trading_date,
        source_type=manifest["source_type"],
        endpoint_type=manifest["endpoint_type"],
        funding_history_path=history_path.as_posix(),
        funding_history_manifest_path=manifest_path.as_posix(),
        position_source=_position_source(position_intervals),
        entry_count=len(entries),
        funding_pnl=sum(entry.funding_pnl for entry in entries),
        entries=entries,
    )


def build_augmented_pnl_summary(
    *,
    gross_pnl_ex_funding: float,
    fee_cost: float,
    net_pnl_ex_funding: float,
    funding_pnl: float,
) -> FundingAugmentedPnlSummary:
    return FundingAugmentedPnlSummary(
        gross_pnl_ex_funding=gross_pnl_ex_funding,
        fee_cost=fee_cost,
        funding_pnl=funding_pnl,
        net_pnl_ex_funding=net_pnl_ex_funding,
        net_pnl_after_funding=net_pnl_ex_funding + funding_pnl,
    )


def _load_official_funding_history_manifest(
    *,
    manifest_path: Path,
    root: Path,
    trading_date: date,
) -> dict[str, object]:
    if not manifest_path.exists():
        predicted_path = (
            root
            / "enriched"
            / "funding_schedule"
            / f"date={trading_date.isoformat()}"
            / "predicted_funding.parquet"
        )
        if predicted_path.exists():
            raise ValueError(
                "historical funding PnL requires official fundingHistory; "
                "predictedFundings is not accepted"
            )
        raise ValueError("historical funding PnL requires official fundingHistory")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if (
        payload.get("source_type") != "fundingHistory"
        or payload.get("endpoint_type") != "official_info"
    ):
        raise ValueError("historical funding PnL requires official fundingHistory")
    return payload


def _load_asset_ctx(*, root: Path, trading_date: date, symbol: str) -> pd.DataFrame:
    path = (
        build_export_path(
            root=root,
            category="truth",
            target="nautilus",
            trading_date=trading_date,
            symbol=symbol,
        )
        / "asset_ctx.parquet"
    )
    frame = _read_parquet_frame(path)
    if frame.empty:
        return frame
    frame = frame.copy()
    fallback_ts_ms = (
        frame["recv_ts"]
        if "recv_ts" in frame
        else frame["recv_ts_ns"].astype("int64") // 1_000_000
    )
    frame["asof_ts_ms"] = frame["exchange_ts"].where(
        frame["exchange_ts"].notna(),
        fallback_ts_ms,
    )
    return frame.sort_values(["asof_ts_ms", "recv_ts_ns", "recv_seq"]).reset_index(
        drop=True
    )


def _position_at_funding_time(
    funding_row: pd.Series,
    intervals: tuple[FundingPositionInterval, ...],
) -> FundingPositionInterval | None:
    funding_time_ns = int(funding_row["funding_time_ms"]) * 1_000_000
    for interval in intervals:
        if interval.opened_ts_ns >= funding_time_ns:
            continue
        if interval.closed_ts_ns is not None and interval.closed_ts_ns < funding_time_ns:
            continue
        if interval.signed_position_size == 0.0:
            continue
        return interval
    return None


def _entry_for_funding_row(
    *,
    funding_row: pd.Series,
    interval: FundingPositionInterval,
    asset_ctx: pd.DataFrame,
) -> FundingLedgerEntry:
    funding_time_ms = int(funding_row["funding_time_ms"])
    oracle_row = _oracle_row_asof(asset_ctx=asset_ctx, funding_time_ms=funding_time_ms)
    position_size = float(interval.signed_position_size)
    oracle_price = float(oracle_row["oracle_px"])
    funding_rate = float(funding_row["funding_rate"])
    funding_payment = position_size * oracle_price * funding_rate
    return FundingLedgerEntry(
        symbol=str(funding_row["symbol"]).upper(),
        funding_time_ms=funding_time_ms,
        position_size=position_size,
        oracle_price=oracle_price,
        funding_rate=funding_rate,
        funding_payment=funding_payment,
        funding_pnl=-funding_payment,
        oracle_price_source="asset_ctx_asof_or_before",
        funding_rate_source="fundingHistory",
        mark_price_observed=(
            float(oracle_row["mark_px"]) if "mark_px" in oracle_row.index else None
        ),
    )


def _oracle_row_asof(*, asset_ctx: pd.DataFrame, funding_time_ms: int) -> pd.Series:
    if asset_ctx.empty:
        raise ValueError("oracle price is required for funding PnL")
    candidates = asset_ctx[asset_ctx["asof_ts_ms"].astype("int64") <= funding_time_ms]
    if candidates.empty:
        raise ValueError("oracle price as-of row is required for funding PnL")
    return candidates.iloc[-1]


def _position_source(position_intervals: tuple[FundingPositionInterval, ...]) -> str:
    sources = sorted({interval.source for interval in position_intervals})
    return ",".join(sources) if sources else "none"
