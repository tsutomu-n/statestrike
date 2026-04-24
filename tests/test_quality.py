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


def test_quality_audit_counts_gap_and_duplicate_metrics(tmp_path) -> None:
    normalized = NormalizedWriter(root=tmp_path)
    trading_date = date(2026, 4, 22)

    normalized.write_rows(
        table="book_events",
        trading_date=trading_date,
        symbol="BTC",
        rows=[
            {
                "book_event_id": "be-1",
                "capture_session_id": "session-1",
                "reconnect_epoch": 0,
                "book_epoch": 1,
                "symbol": "BTC",
                "exchange_ts": 1000,
                "recv_ts": 1010,
                "event_kind": "snapshot",
                "source": "ws",
                "raw_msg_hash": "raw-1",
                "dedup_hash": "be-1",
                "n_bids": 1,
                "n_asks": 1,
            },
            {
                "book_event_id": "be-2",
                "capture_session_id": "session-1",
                "reconnect_epoch": 1,
                "book_epoch": 2,
                "symbol": "BTC",
                "exchange_ts": 900,
                "recv_ts": 1020,
                "event_kind": "recovery_snapshot",
                "source": "ws",
                "raw_msg_hash": "raw-2",
                "dedup_hash": "be-2",
                "n_bids": 0,
                "n_asks": 1,
            },
        ],
    )
    normalized.write_rows(
        table="book_levels",
        trading_date=trading_date,
        symbol="BTC",
        rows=[
            {
                "book_event_id": "be-1",
                "capture_session_id": "session-1",
                "reconnect_epoch": 0,
                "book_epoch": 1,
                "symbol": "BTC",
                "exchange_ts": 1000,
                "recv_ts": 1010,
                "source": "ws",
                "raw_msg_hash": "raw-1",
                "dedup_hash": "bl-1",
                "side": "bid",
                "level_idx": 0,
                "price": 100.0,
                "size": 1.0,
            },
            {
                "book_event_id": "be-1",
                "capture_session_id": "session-1",
                "reconnect_epoch": 0,
                "book_epoch": 1,
                "symbol": "BTC",
                "exchange_ts": 1000,
                "recv_ts": 1010,
                "source": "ws",
                "raw_msg_hash": "raw-1",
                "dedup_hash": "bl-2",
                "side": "ask",
                "level_idx": 0,
                "price": 101.0,
                "size": 1.0,
            },
        ],
    )
    normalized.write_rows(
        table="trades",
        trading_date=trading_date,
        symbol="BTC",
        rows=[
            {
                "trade_event_id": "t-1",
                "symbol": "BTC",
                "exchange_ts": 1000,
                "recv_ts": 1010,
                "price": 100.0,
                "size": 1.0,
                "side": "buy",
                "capture_session_id": "session-1",
                "reconnect_epoch": 0,
                "source": "ws",
                "raw_msg_hash": "raw-t1",
                "dedup_hash": "dup-1",
            },
            {
                "trade_event_id": "t-2",
                "symbol": "BTC",
                "exchange_ts": 1000,
                "recv_ts": 1011,
                "price": 100.0,
                "size": 1.0,
                "side": "buy",
                "capture_session_id": "session-1",
                "reconnect_epoch": 0,
                "source": "ws",
                "raw_msg_hash": "raw-t2",
                "dedup_hash": "dup-1",
            },
            {
                "trade_event_id": "t-3",
                "symbol": "BTC",
                "exchange_ts": 200000,
                "recv_ts": 900,
                "price": 101.0,
                "size": 1.5,
                "side": "sell",
                "capture_session_id": "session-1",
                "reconnect_epoch": 1,
                "source": "ws",
                "raw_msg_hash": "raw-t3",
                "dedup_hash": "dup-3",
            },
        ],
    )
    normalized.write_rows(
        table="asset_ctx",
        trading_date=trading_date,
        symbol="BTC",
        rows=[
            {
                "asset_ctx_event_id": "ctx-1",
                "symbol": "BTC",
                "exchange_ts": 1000,
                "recv_ts": 1000,
                "mark_px": 100.0,
                "oracle_px": 99.0,
                "funding_rate": 0.0001,
                "open_interest": 10.0,
                "mid_px": 99.5,
                "basis": 0.01,
                "next_funding_ts": 3600000,
                "capture_session_id": "session-1",
                "reconnect_epoch": 0,
                "source": "ws",
                "raw_msg_hash": "raw-c1",
                "dedup_hash": "ctx-1",
            },
            {
                "asset_ctx_event_id": "ctx-2",
                "symbol": "BTC",
                "exchange_ts": 500000,
                "recv_ts": 500001,
                "mark_px": 101.0,
                "oracle_px": 100.0,
                "funding_rate": 0.0002,
                "open_interest": 11.0,
                "mid_px": 100.5,
                "basis": 0.01,
                "next_funding_ts": 7200000,
                "capture_session_id": "session-1",
                "reconnect_epoch": 1,
                "source": "ws",
                "raw_msg_hash": "raw-c2",
                "dedup_hash": "ctx-2",
            },
        ],
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

    assert report.gap_count >= 2
    assert report.book_epoch_switch_count == 1
    assert report.duplicate_trade_count == 1
    assert report.non_monotonic_exchange_ts_count >= 1
    assert report.non_monotonic_recv_ts_count >= 1
    assert report.empty_snapshot_count == 1
