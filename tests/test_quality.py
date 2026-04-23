from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from statestrike.collector import CollectorConfig, collect_market_batch
from statestrike.quality import run_quality_audit
from statestrike.schemas import validate_records
from statestrike.storage import NormalizedWriter, QuarantineWriter


FIXTURES = Path(__file__).parent / "fixtures" / "hyperliquid"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_quality_audit_summarizes_normalized_tables(tmp_path) -> None:
    config = CollectorConfig(
        allowed_symbols=("BTC",),
        source_priority=("ws", "info", "s3", "tardis"),
        market_data_network="mainnet",
        flush_interval_ms=1000,
        snapshot_recovery_enabled=True,
        channels=("l2Book", "trades", "activeAssetCtx"),
        candle_interval=None,
    )
    batch = collect_market_batch(
        messages=[
            load_fixture("l2_book.json"),
            load_fixture("trades.json"),
            load_fixture("active_asset_ctx.json"),
        ],
        config=config,
        capture_session_id="session-1",
        reconnect_epoch=0,
        book_epoch=1,
        recv_ts_start=1713818880100,
    )
    normalized = NormalizedWriter(root=tmp_path)
    quarantine = QuarantineWriter(root=tmp_path)
    trading_date = date(2026, 4, 22)

    for table, rows in batch.normalized_rows.items():
        candidate_rows = rows
        if table == "trades":
            candidate_rows = [
                *rows,
                {
                    **rows[0],
                    "size": 0.0,
                },
            ]
        result = validate_records(table, candidate_rows)
        if result.valid_rows:
            normalized.write_rows(
                table=table,
                trading_date=trading_date,
                symbol="BTC",
                rows=result.valid_rows,
            )
        if result.quarantined_rows:
            quarantine.write_rows(
                table=table,
                trading_date=trading_date,
                symbol="BTC",
                rows=result.quarantined_rows,
            )

    report = run_quality_audit(
        normalized_root=tmp_path,
        quarantine_root=tmp_path,
        trading_date=trading_date,
        symbols=("BTC",),
        skew_warning_ms=45,
        skew_severe_ms=90,
        asset_ctx_stale_threshold_ms=123456,
    )

    assert report.thresholds.skew_warning_ms == 45
    assert report.thresholds.skew_severe_ms == 90
    assert report.thresholds.asset_ctx_stale_threshold_ms == 123456
    assert report.row_counts["book_events"] == 1
    assert report.row_counts["book_levels"] == 4
    assert report.row_counts["trades"] == 2
    assert report.row_counts["asset_ctx"] == 1
    assert report.crossed_book_count == 0
    assert report.skew_alerts["trades"] == "warning"
    assert report.quarantine_row_counts["trades"] == 1
    assert report.quarantine_rates["trades"] == pytest.approx(1 / 3)
    assert report.quarantine_reason_counts["trades"] == {
        "size:greater_than(0)": 1,
    }
