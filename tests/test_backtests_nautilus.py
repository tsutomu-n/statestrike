from __future__ import annotations

import copy
import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from statestrike.backtests import run_baseline_simple_momentum
from statestrike.backtests.nautilus import run_nautilus_simple_momentum
from statestrike.collector import CollectorConfig
from statestrike.enrichment import enrich_funding_schedule_from_predicted_fundings
from statestrike.smoke import run_smoke_batch


FIXTURES = Path(__file__).parent / "fixtures" / "hyperliquid"
PREDICTED_FUNDINGS = [
    [
        "BTC",
        [
            [
                "HlPerp",
                {
                    "fundingRate": "0.0000125",
                    "nextFundingTime": 1713819600000,
                    "fundingIntervalHours": 1,
                },
            ]
        ],
    ]
]


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def nautilus_config() -> CollectorConfig:
    return CollectorConfig(
        allowed_symbols=("BTC",),
        source_priority=("ws", "info", "s3", "tardis"),
        market_data_network="mainnet",
        flush_interval_ms=1000,
        snapshot_recovery_enabled=True,
        channels=("l2Book", "trades", "activeAssetCtx"),
        candle_interval=None,
    )


def prepare_nautilus_dataset(tmp_path: Path, *, enrich: bool = True) -> None:
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
    run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 24),
        messages=[
            load_fixture("l2_book.json"),
            rising_trades,
            load_fixture("active_asset_ctx.json"),
        ],
        config=nautilus_config(),
        capture_session_id="session-nautilus",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )
    if enrich:
        enrich_funding_schedule_from_predicted_fundings(
            root=tmp_path,
            trading_date=date(2026, 4, 24),
            symbols=("BTC",),
            predicted_fundings=PREDICTED_FUNDINGS,
            enrichment_asof_ts=1713819000000,
        )


def test_nautilus_simple_momentum_runs_from_truth_export(tmp_path) -> None:
    prepare_nautilus_dataset(tmp_path)

    result = run_nautilus_simple_momentum(
        root=tmp_path,
        trading_date=date(2026, 4, 24),
        symbol="BTC",
    )

    assert result.strategy_name == "nautilus_simple_momentum"
    assert result.symbol == "BTC"
    assert result.dataset_profile == "nautilus_baseline_candidate"
    assert result.readiness_status == "ready"
    assert result.funding_treatment == "ignored"
    assert result.trade_count == 3
    assert result.order_count == 2
    assert result.realized_pnl == pytest.approx(0.5)
    assert result.fee == 0.0
    assert result.max_drawdown == 0.0


def test_nautilus_baseline_uses_ts_init_ns_from_truth_export(tmp_path) -> None:
    prepare_nautilus_dataset(tmp_path)
    trade_ticks_path = (
        tmp_path
        / "exports"
        / "truth"
        / "nautilus"
        / "date=2026-04-24"
        / "symbol=BTC"
        / "trade_ticks.parquet"
    )
    trade_ticks = pd.read_parquet(trade_ticks_path)

    result = run_nautilus_simple_momentum(
        root=tmp_path,
        trading_date=date(2026, 4, 24),
        symbol="BTC",
    )

    assert result.ts_init_min_ns == int(trade_ticks["ts_init_ns"].min())
    assert result.ts_init_min_ns != int(trade_ticks["recv_ts"].min())


def test_nautilus_baseline_rejects_unready_dataset_before_running(tmp_path) -> None:
    prepare_nautilus_dataset(tmp_path)
    (
        tmp_path
        / "exports"
        / "truth"
        / "nautilus"
        / "date=2026-04-24"
        / "symbol=BTC"
        / "trade_ticks.parquet"
    ).unlink()

    with pytest.raises(ValueError, match="Nautilus baseline dataset not ready"):
        run_nautilus_simple_momentum(
            root=tmp_path,
            trading_date=date(2026, 4, 24),
            symbol="BTC",
        )


def test_nautilus_baseline_is_separate_from_diagnostics_backtests(tmp_path) -> None:
    prepare_nautilus_dataset(tmp_path)

    diagnostics = run_baseline_simple_momentum(
        root=tmp_path,
        trading_date=date(2026, 4, 24),
        symbol="BTC",
    )
    nautilus = run_nautilus_simple_momentum(
        root=tmp_path,
        trading_date=date(2026, 4, 24),
        symbol="BTC",
    )

    assert diagnostics.name == "baseline_simple_momentum"
    assert nautilus.strategy_name == "nautilus_simple_momentum"
