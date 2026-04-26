from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from hftbacktest import DEPTH_EVENT, EXCH_EVENT, LOCAL_EVENT, TRADE_EVENT
from hftbacktest.data import validate_event_order
import numpy as np
import pandas as pd

from statestrike.collector import CollectorConfig, collect_market_batch
from statestrike.exports import (
    export_hftbacktest_npz,
    export_nautilus_catalog,
    validate_export_bundle,
)
from statestrike.schemas import validate_records
from statestrike.storage import NormalizedWriter


FIXTURES = Path(__file__).parent / "fixtures" / "hyperliquid"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _write_valid_normalized_rows(tmp_path: Path) -> tuple[Path, date]:
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
    trading_date = date(2026, 4, 22)
    normalized = NormalizedWriter(root=tmp_path)
    for table, rows in batch.normalized_rows.items():
        result = validate_records(table, rows)
        if result.valid_rows:
            normalized.write_rows(
                table=table,
                trading_date=trading_date,
                symbol="BTC",
                rows=result.valid_rows,
            )
    return tmp_path, trading_date


def test_export_nautilus_catalog_writes_compare_ready_bundle(tmp_path) -> None:
    root, trading_date = _write_valid_normalized_rows(tmp_path)

    export_dir = export_nautilus_catalog(
        normalized_root=root,
        export_root=root,
        trading_date=trading_date,
        symbol="BTC",
    )

    assert export_dir.as_posix().endswith(
        "/exports/truth/nautilus/date=2026-04-22/symbol=BTC"
    )
    assert (export_dir / "instrument.parquet").exists()
    assert (export_dir / "trade_ticks.parquet").exists()
    assert (export_dir / "book_levels.parquet").exists()
    assert (export_dir / "asset_ctx.parquet").exists()

    instrument_df = pd.read_parquet(export_dir / "instrument.parquet")
    assert instrument_df.loc[0, "instrument_id"] == "BTC-USD-PERP.HYPERLIQUID"

    trade_ticks = pd.read_parquet(export_dir / "trade_ticks.parquet")
    assert "ts_init_ns" in trade_ticks.columns
    assert trade_ticks["ts_init_ns"].tolist() == trade_ticks["recv_ts_ns"].tolist()


def test_export_nautilus_catalog_sorts_same_ms_rows_by_ns_and_sequence(tmp_path) -> None:
    trading_date = date(2026, 4, 22)
    writer = NormalizedWriter(root=tmp_path)
    rows = [
        {
            "trade_event_id": "trade-late",
            "native_tid": "2",
            "symbol": "BTC",
            "exchange_ts": 1713818880000,
            "recv_ts": 1713818880100,
            "recv_ts_ns": 1713818880100000200,
            "recv_seq": 2,
            "price": 100.0,
            "size": 1.0,
            "side": "buy",
            "capture_session_id": "session-1",
            "reconnect_epoch": 0,
            "source": "ws",
            "raw_msg_hash": "raw-late",
            "dedup_hash": "dedup-late",
        },
        {
            "trade_event_id": "trade-early",
            "native_tid": "1",
            "symbol": "BTC",
            "exchange_ts": 1713818880000,
            "recv_ts": 1713818880100,
            "recv_ts_ns": 1713818880100000100,
            "recv_seq": 1,
            "price": 99.0,
            "size": 1.0,
            "side": "sell",
            "capture_session_id": "session-1",
            "reconnect_epoch": 0,
            "source": "ws",
            "raw_msg_hash": "raw-early",
            "dedup_hash": "dedup-early",
        },
    ]
    writer.write_rows(
        table="trades",
        trading_date=trading_date,
        symbol="BTC",
        rows=rows,
    )

    export_dir = export_nautilus_catalog(
        normalized_root=tmp_path,
        export_root=tmp_path,
        trading_date=trading_date,
        symbol="BTC",
    )

    trade_ticks = pd.read_parquet(export_dir / "trade_ticks.parquet")
    assert trade_ticks["trade_event_id"].tolist() == ["trade-early", "trade-late"]
    assert trade_ticks["ts_init_ns"].tolist() == [
        1713818880100000100,
        1713818880100000200,
    ]


def test_export_hftbacktest_npz_writes_current_structured_array(tmp_path) -> None:
    root, trading_date = _write_valid_normalized_rows(tmp_path)

    npz_path = export_hftbacktest_npz(
        normalized_root=root,
        export_root=root,
        trading_date=trading_date,
        symbol="BTC",
    )

    assert npz_path.as_posix().endswith(
        "/exports/corrected/hftbacktest/date=2026-04-22/symbol=BTC/btc_market_data.npz"
    )
    data = np.load(npz_path)["data"]

    assert data.dtype.names == (
        "ev",
        "exch_ts",
        "local_ts",
        "px",
        "qty",
        "order_id",
        "ival",
        "fval",
    )
    validate_event_order(data)
    assert np.all(data["local_ts"] >= data["exch_ts"])
    assert data["exch_ts"].min() == 1713818880000000000
    assert (data["local_ts"] - data["exch_ts"]).min() == 41000000
    assert (data["local_ts"] - data["exch_ts"]).max() == 100000000
    assert {int(code & 0xFF) for code in data["ev"]} == {
        DEPTH_EVENT,
        TRADE_EVENT,
    }
    assert np.any((data["ev"] & EXCH_EVENT) == EXCH_EVENT)
    assert np.any((data["ev"] & LOCAL_EVENT) == LOCAL_EVENT)


def test_validate_export_bundle_reports_integrity_stats(tmp_path) -> None:
    root, trading_date = _write_valid_normalized_rows(tmp_path)
    export_nautilus_catalog(
        normalized_root=root,
        export_root=root,
        trading_date=trading_date,
        symbol="BTC",
    )
    export_hftbacktest_npz(
        normalized_root=root,
        export_root=root,
        trading_date=trading_date,
        symbol="BTC",
    )

    report = validate_export_bundle(
        export_root=root,
        trading_date=trading_date,
        symbol="BTC",
    )

    assert report.nautilus_tables["trade_ticks"].row_count == 2
    assert report.nautilus_tables["trade_ticks"].symbol_count == 1
    assert report.nautilus_tables["trade_ticks"].null_count == 0
    assert report.nautilus_tables["trade_ticks"].exchange_ts_monotonic is True
    assert report.hftbacktest.row_count == 6
    assert report.hftbacktest.null_count == 0
    assert report.hftbacktest.exchange_ts_monotonic is True
    assert report.hftbacktest.local_ts_monotonic is True
    assert report.hftbacktest.event_order_valid is True
    assert report.hftbacktest.feed_latency_ns_nonnegative is True
    assert report.hftbacktest.feed_latency_ns_min == 41000000
    assert report.hftbacktest.feed_latency_ns_max == 100000000
    assert report.hftbacktest.exchange_ts_min_ns == 1713818880000000000
    assert report.hftbacktest.exchange_ts_max_ns == 1713818880060000000
    assert report.hftbacktest.local_ts_min_ns == 1713818880100000000
    assert report.hftbacktest.local_ts_max_ns == 1713818880101000000
    assert report.hftbacktest.event_codes == (1, 2)
    assert report.truth_exports["nautilus"].category == "truth"
    assert report.truth_exports["nautilus"].truth_preserving is True
    assert report.corrected_exports["hftbacktest"].category == "corrected"
    assert report.corrected_exports["hftbacktest"].truth_preserving is False
    assert report.corrected_exports["hftbacktest"].correction_applied == (
        "correct_local_timestamp",
        "correct_event_order",
    )
    assert report.correction_applied == ("hftbacktest",)


def test_validate_export_bundle_detects_invalid_hftbacktest_event_order(
    tmp_path,
) -> None:
    root, trading_date = _write_valid_normalized_rows(tmp_path)
    export_nautilus_catalog(
        normalized_root=root,
        export_root=root,
        trading_date=trading_date,
        symbol="BTC",
    )
    npz_path = export_hftbacktest_npz(
        normalized_root=root,
        export_root=root,
        trading_date=trading_date,
        symbol="BTC",
    )
    with np.load(npz_path) as archive:
        data = archive["data"][::-1].copy()
    np.savez_compressed(npz_path, data=data)

    report = validate_export_bundle(
        export_root=root,
        trading_date=trading_date,
        symbol="BTC",
    )

    assert report.hftbacktest.event_order_valid is False
    assert report.hftbacktest.feed_latency_ns_nonnegative is True
