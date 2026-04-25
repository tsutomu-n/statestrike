from __future__ import annotations

import copy
import json
from datetime import date
from pathlib import Path

import duckdb
import pytest

from statestrike.backtests import (
    run_baseline_simple_momentum,
    run_sanity_noop,
    run_sanity_single_trade_roundtrip,
)
from statestrike.collector import CollectorConfig
from statestrike.smoke import run_smoke_batch
from statestrike.storage import _write_parquet_frame


FIXTURES = Path(__file__).parent / "fixtures" / "hyperliquid"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def backtest_config() -> CollectorConfig:
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


def prepare_ready_dataset(tmp_path: Path) -> None:
    result = run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        messages=[
            load_fixture("l2_book.json"),
            load_fixture("trades.json"),
            load_fixture("active_asset_ctx.json"),
        ],
        config=backtest_config(),
        capture_session_id="session-backtest",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )
    enrich_next_funding_ts(result.normalized_paths["asset_ctx:BTC"], value=1713819600000)


def test_sanity_noop_requires_ready_dataset_and_returns_zero_pnl(tmp_path) -> None:
    prepare_ready_dataset(tmp_path)

    result = run_sanity_noop(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        symbols=("BTC",),
    )

    assert result.name == "sanity_noop"
    assert result.status == "completed"
    assert result.trade_count == 0
    assert result.order_count == 0
    assert result.pnl == 0.0
    assert result.readiness_report.status == "ready"


def test_sanity_single_trade_roundtrip_uses_first_two_trades(tmp_path) -> None:
    prepare_ready_dataset(tmp_path)

    result = run_sanity_single_trade_roundtrip(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        symbol="BTC",
    )

    assert result.name == "sanity_single_trade_roundtrip"
    assert result.trade_count == 2
    assert result.order_count == 2
    assert result.pnl == pytest.approx(0.25)


def test_baseline_simple_momentum_computes_positive_pnl_from_fixture_trend(tmp_path) -> None:
    prepare_ready_dataset(tmp_path)
    rising_trades = copy.deepcopy(load_fixture("trades.json"))
    rising_trades["data"].append(
        {
            "coin": "BTC",
            "side": "B",
            "px": "100.75",
            "sz": "0.2",
            "time": 1713818880070,
            "tid": 3,
        }
    )
    result = run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 24),
        messages=[
            load_fixture("l2_book.json"),
            rising_trades,
            load_fixture("active_asset_ctx.json"),
        ],
        config=backtest_config(),
        capture_session_id="session-backtest-momentum",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )
    enrich_next_funding_ts(result.normalized_paths["asset_ctx:BTC"], value=1713819600000)

    backtest = run_baseline_simple_momentum(
        root=tmp_path,
        trading_date=date(2026, 4, 24),
        symbol="BTC",
    )

    assert backtest.name == "baseline_simple_momentum"
    assert backtest.trade_count == 3
    assert backtest.order_count == 2
    assert backtest.pnl == pytest.approx(0.5)


def test_backtests_block_when_readiness_gate_is_not_ready(tmp_path) -> None:
    run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        messages=[
            load_fixture("l2_book.json"),
            load_fixture("trades.json"),
            load_fixture("active_asset_ctx.json"),
        ],
        config=backtest_config(),
        capture_session_id="session-backtest-blocked",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )

    with pytest.raises(ValueError, match="backtest dataset not ready"):
        run_sanity_noop(
            root=tmp_path,
            trading_date=date(2026, 4, 23),
            symbols=("BTC",),
        )
