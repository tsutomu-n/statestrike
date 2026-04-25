from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb
from hftbacktest import DEPTH_EVENT, EXCH_EVENT, LOCAL_EVENT, TRADE_EVENT
from hftbacktest.data import validate_event_order
import numpy as np

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

    instrument_df = duckdb.connect().read_parquet(str(export_dir / "instrument.parquet")).df()
    assert instrument_df.loc[0, "instrument_id"] == "BTC-USD-PERP.HYPERLIQUID"


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
    assert report.hftbacktest.event_codes == (1, 2)
    assert report.correction_applied == ("hftbacktest",)
