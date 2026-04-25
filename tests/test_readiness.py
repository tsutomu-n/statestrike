from __future__ import annotations

import copy
import json
from datetime import date
from pathlib import Path

import duckdb

from statestrike.collector import CollectorConfig
from statestrike.readiness import (
    BacktestReadinessThresholds,
    _export_contract_is_valid,
    run_backtest_readiness,
)
from statestrike.smoke import run_smoke_batch
from statestrike.storage import _write_parquet_frame


FIXTURES = Path(__file__).parent / "fixtures" / "hyperliquid"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def readiness_config() -> CollectorConfig:
    return CollectorConfig(
        allowed_symbols=("BTC",),
        source_priority=("ws", "info", "s3", "tardis"),
        market_data_network="mainnet",
        flush_interval_ms=1000,
        snapshot_recovery_enabled=True,
        channels=("l2Book", "trades", "activeAssetCtx"),
        candle_interval=None,
    )


def enrich_next_funding_ts(path: Path, *, value: int) -> None:
    connection = duckdb.connect()
    try:
        frame = connection.read_parquet(str(path)).df()
    finally:
        connection.close()
    frame["next_funding_ts"] = value
    _write_parquet_frame(path=path, frame=frame)


def test_backtest_readiness_blocks_when_funding_enrichment_is_missing(tmp_path) -> None:
    run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        messages=[
            load_fixture("l2_book.json"),
            load_fixture("trades.json"),
            load_fixture("active_asset_ctx.json"),
        ],
        config=readiness_config(),
        capture_session_id="session-ready-blocked",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )

    report = run_backtest_readiness(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        symbols=("BTC",),
    )

    assert report.status == "blocked"
    assert "funding_enrichment_incomplete" in report.blocking_reasons
    assert report.capture_log_file_count == 1
    assert report.missing_recv_timestamp_count == 0


def test_backtest_readiness_ready_after_enrichment_for_clean_dataset(tmp_path) -> None:
    result = run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        messages=[
            load_fixture("l2_book.json"),
            load_fixture("trades.json"),
            load_fixture("active_asset_ctx.json"),
        ],
        config=readiness_config(),
        capture_session_id="session-ready-clean",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )
    enrich_next_funding_ts(result.normalized_paths["asset_ctx:BTC"], value=1713819600000)

    report = run_backtest_readiness(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        symbols=("BTC",),
    )

    assert report.status == "ready"
    assert report.blocking_reasons == ()
    assert report.warning_reasons == ()
    assert report.export_validations["BTC"].correction_applied == ("hftbacktest",)
    assert report.export_contract_invalid_symbols == ()


def test_backtest_readiness_warns_on_quarantine_rate_without_blocking(tmp_path) -> None:
    invalid_trades = copy.deepcopy(load_fixture("trades.json"))
    invalid_trades["data"][0]["sz"] = "0.0"
    result = run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        messages=[
            load_fixture("l2_book.json"),
            invalid_trades,
            load_fixture("active_asset_ctx.json"),
        ],
        config=readiness_config(),
        capture_session_id="session-ready-warning",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )
    enrich_next_funding_ts(result.normalized_paths["asset_ctx:BTC"], value=1713819600000)

    report = run_backtest_readiness(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        symbols=("BTC",),
        thresholds=BacktestReadinessThresholds(
            quarantine_warning_rate=0.2,
            quarantine_blocking_rate=0.5,
        ),
    )

    assert report.status == "warning"
    assert report.blocking_reasons == ()
    assert report.warning_reasons == ("quarantine_rate_over_warning_threshold",)


def test_backtest_readiness_blocks_when_export_contract_is_invalid(tmp_path) -> None:
    result = run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        messages=[
            load_fixture("l2_book.json"),
            load_fixture("trades.json"),
            load_fixture("active_asset_ctx.json"),
        ],
        config=readiness_config(),
        capture_session_id="session-ready-invalid-export-contract",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )
    enrich_next_funding_ts(result.normalized_paths["asset_ctx:BTC"], value=1713819600000)

    report = run_backtest_readiness(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        symbols=("BTC",),
    )

    invalid_report = report.export_validations["BTC"].model_copy(
        update={
            "corrected_exports": {
                **report.export_validations["BTC"].corrected_exports,
                "hftbacktest": report.export_validations["BTC"].corrected_exports[
                    "hftbacktest"
                ].model_copy(update={"truth_preserving": True}),
            }
        }
    )

    assert report.status == "ready"
    assert report.export_contract_invalid_symbols == ()
    assert _export_contract_is_valid(report.export_validations["BTC"]) is True
    assert _export_contract_is_valid(invalid_report) is False
