from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pandas as pd
import pytest

from statestrike.funding import build_funding_history_sidecar
from statestrike.funding_pnl import (
    FundingPositionInterval,
    build_augmented_pnl_summary,
    build_funding_pnl_ledger,
)
from statestrike.storage import _write_parquet_frame


def funding_payload(symbol: str, trading_date: date) -> list[dict[str, str | int]]:
    start = datetime.combine(trading_date, datetime.min.time(), tzinfo=UTC)
    return [
        {
            "coin": symbol,
            "fundingRate": "0.001",
            "premium": "0.0001",
            "time": int((start + timedelta(hours=hour)).timestamp() * 1000),
        }
        for hour in range(24)
    ]


def write_asset_ctx(
    root,
    *,
    trading_date: date,
    symbol: str,
    oracle_px: float,
    mark_px: float,
    exchange_ts: int,
) -> None:
    path = (
        root
        / "exports"
        / "truth"
        / "nautilus"
        / f"date={trading_date.isoformat()}"
        / f"symbol={symbol}"
        / "asset_ctx.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    _write_parquet_frame(
        path=path,
        frame=pd.DataFrame(
            [
                {
                    "symbol": symbol,
                    "exchange_ts": exchange_ts,
                    "mark_px": mark_px,
                    "oracle_px": oracle_px,
                    "funding_rate": 0.0,
                    "recv_ts_ns": exchange_ts * 1_000_000,
                    "recv_seq": 1,
                }
            ]
        ),
    )


def test_funding_pnl_uses_official_history_and_oracle_price(tmp_path) -> None:
    trading_date = date(2026, 4, 25)
    start = datetime.combine(trading_date, datetime.min.time(), tzinfo=UTC)
    funding_ms = int((start + timedelta(hours=1)).timestamp() * 1000)
    build_funding_history_sidecar(
        root=tmp_path,
        trading_date=trading_date,
        symbols=("BTC",),
        funding_history_payloads={"BTC": funding_payload("BTC", trading_date)},
    )
    write_asset_ctx(
        tmp_path,
        trading_date=trading_date,
        symbol="BTC",
        oracle_px=100.0,
        mark_px=125.0,
        exchange_ts=funding_ms - 1,
    )

    ledger = build_funding_pnl_ledger(
        root=tmp_path,
        trading_date=trading_date,
        symbol="BTC",
        position_intervals=(
            FundingPositionInterval(
                symbol="BTC",
                opened_ts_ns=(funding_ms - 10_000) * 1_000_000,
                closed_ts_ns=(funding_ms + 10_000) * 1_000_000,
                signed_position_size=2.0,
                source="test_position_fixture",
            ),
        ),
    )

    assert ledger.source_type == "fundingHistory"
    assert ledger.entry_count == 1
    assert ledger.entries[0].oracle_price == pytest.approx(100.0)
    assert ledger.entries[0].mark_price_observed == pytest.approx(125.0)
    assert ledger.entries[0].funding_payment == pytest.approx(0.2)
    assert ledger.entries[0].funding_pnl == pytest.approx(-0.2)
    assert ledger.funding_pnl == pytest.approx(-0.2)


def test_funding_pnl_is_positive_for_short_when_rate_is_positive(tmp_path) -> None:
    trading_date = date(2026, 4, 25)
    start = datetime.combine(trading_date, datetime.min.time(), tzinfo=UTC)
    funding_ms = int((start + timedelta(hours=1)).timestamp() * 1000)
    build_funding_history_sidecar(
        root=tmp_path,
        trading_date=trading_date,
        symbols=("ETH",),
        funding_history_payloads={"ETH": funding_payload("ETH", trading_date)},
    )
    write_asset_ctx(
        tmp_path,
        trading_date=trading_date,
        symbol="ETH",
        oracle_px=2000.0,
        mark_px=2100.0,
        exchange_ts=funding_ms - 1,
    )

    ledger = build_funding_pnl_ledger(
        root=tmp_path,
        trading_date=trading_date,
        symbol="ETH",
        position_intervals=(
            FundingPositionInterval(
                symbol="ETH",
                opened_ts_ns=(funding_ms - 1) * 1_000_000,
                closed_ts_ns=(funding_ms + 10_000) * 1_000_000,
                signed_position_size=-0.5,
                source="test_position_fixture",
            ),
        ),
    )

    assert ledger.entries[0].funding_payment == pytest.approx(-1.0)
    assert ledger.entries[0].funding_pnl == pytest.approx(1.0)
    assert ledger.funding_pnl == pytest.approx(1.0)


def test_augmented_pnl_keeps_nautilus_net_pnl_separate_from_funding() -> None:
    summary = build_augmented_pnl_summary(
        gross_pnl_ex_funding=10.0,
        fee_cost=1.5,
        net_pnl_ex_funding=8.5,
        funding_pnl=-0.25,
    )

    assert summary.gross_pnl_ex_funding == pytest.approx(10.0)
    assert summary.fee_cost == pytest.approx(1.5)
    assert summary.funding_pnl == pytest.approx(-0.25)
    assert summary.net_pnl_ex_funding == pytest.approx(8.5)
    assert summary.net_pnl_after_funding == pytest.approx(8.25)


def test_predicted_funding_sidecar_is_not_accepted_for_historical_pnl(tmp_path) -> None:
    trading_date = date(2026, 4, 25)
    predicted_dir = (
        tmp_path
        / "enriched"
        / "funding_schedule"
        / f"date={trading_date.isoformat()}"
    )
    predicted_dir.mkdir(parents=True)
    _write_parquet_frame(
        path=predicted_dir / "predicted_funding.parquet",
        frame=pd.DataFrame(
            [
                {
                    "symbol": "BTC",
                    "next_funding_ts": 1,
                    "funding_interval_hours": 1,
                    "enrichment_kind": "predicted_funding",
                }
            ]
        ),
    )

    with pytest.raises(ValueError, match="fundingHistory"):
        build_funding_pnl_ledger(
            root=tmp_path,
            trading_date=trading_date,
            symbol="BTC",
            position_intervals=(),
        )
