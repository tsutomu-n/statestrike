from __future__ import annotations

import json
from datetime import date
from pathlib import Path

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
        result = validate_records(table, rows)
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
        trading_date=trading_date,
        symbols=("BTC",),
    )

    assert report.row_counts["book_events"] == 1
    assert report.row_counts["book_levels"] == 4
    assert report.row_counts["trades"] == 2
    assert report.row_counts["asset_ctx"] == 1
    assert report.crossed_book_count == 0
