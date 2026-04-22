from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb
import numpy as np

from statestrike.collector import CollectorConfig, collect_market_batch
from statestrike.exports import export_hftbacktest_npz, export_nautilus_catalog
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

    assert (export_dir / "instrument.parquet").exists()
    assert (export_dir / "trade_ticks.parquet").exists()
    assert (export_dir / "book_levels.parquet").exists()
    assert (export_dir / "asset_ctx.parquet").exists()

    instrument_df = duckdb.connect().read_parquet(str(export_dir / "instrument.parquet")).df()
    assert instrument_df.loc[0, "instrument_id"] == "BTC-USD-PERP.HYPERLIQUID"


def test_export_hftbacktest_npz_writes_six_column_array(tmp_path) -> None:
    root, trading_date = _write_valid_normalized_rows(tmp_path)

    npz_path = export_hftbacktest_npz(
        normalized_root=root,
        export_root=root,
        trading_date=trading_date,
        symbol="BTC",
    )

    data = np.load(npz_path)["data"]

    assert data.shape[1] == 6
    assert set(data[:, 0].astype(int)) == {2, 4}
