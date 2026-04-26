from __future__ import annotations

import copy
import json
from datetime import date
from pathlib import Path

import pandas as pd

from statestrike.baseline_input import build_nautilus_baseline_input
from statestrike.collector import CollectorConfig
from statestrike.enrichment import enrich_funding_schedule_from_predicted_fundings
from statestrike.readiness import run_backtest_readiness
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


def baseline_config() -> CollectorConfig:
    return CollectorConfig(
        allowed_symbols=("BTC",),
        source_priority=("ws", "info", "s3", "tardis"),
        market_data_network="mainnet",
        flush_interval_ms=1000,
        snapshot_recovery_enabled=True,
        channels=("l2Book", "trades", "activeAssetCtx"),
        candle_interval=None,
    )


def test_baseline_input_dedups_session_replay_without_mutating_source(
    tmp_path,
) -> None:
    source_root = tmp_path / "source"
    output_root = tmp_path / "candidate"
    trading_date = date(2026, 4, 23)
    messages = [
        load_fixture("l2_book.json"),
        load_fixture("trades.json"),
        load_fixture("active_asset_ctx.json"),
    ]
    run_smoke_batch(
        root=source_root,
        trading_date=trading_date,
        messages=copy.deepcopy(messages),
        config=baseline_config(),
        capture_session_id="session-source-0001",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )
    run_smoke_batch(
        root=source_root,
        trading_date=trading_date,
        messages=copy.deepcopy(messages),
        config=baseline_config(),
        capture_session_id="session-source-0002",
        batch_id="0002",
        recv_ts_start=1713819780100,
    )
    enrich_funding_schedule_from_predicted_fundings(
        root=source_root,
        trading_date=trading_date,
        symbols=("BTC",),
        predicted_fundings=PREDICTED_FUNDINGS,
        enrichment_asof_ts=1713819000000,
    )

    source_report = run_backtest_readiness(
        root=source_root,
        trading_date=trading_date,
        symbols=("BTC",),
        profile="nautilus_baseline_candidate",
    )

    assert source_report.status == "blocked"
    assert "baseline_input_session_replay_dedup_missing" in source_report.blocking_reasons

    result = build_nautilus_baseline_input(
        source_root=source_root,
        output_root=output_root,
        trading_date=trading_date,
        symbols=("BTC",),
    )

    assert result.manifest.session_replay_dedup_applied is True
    assert result.manifest.removed_duplicate_classification == "session_replay"
    assert result.manifest.removed_duplicate_count == 2
    assert result.manifest.unexplained_duplicate_count == 0
    assert result.manifest.input_sessions == ("session-source-0001", "session-source-0002")
    assert result.manifest_path.exists()

    source_trades = pd.concat(
        pd.read_parquet(path)
        for path in sorted(
            (
                source_root
                / "normalized"
                / "trades"
                / "date=2026-04-23"
                / "symbol=BTC"
            ).glob("*.parquet")
        )
    )
    output_trades = pd.read_parquet(
        output_root
        / "normalized"
        / "trades"
        / "date=2026-04-23"
        / "symbol=BTC"
        / "baseline-input.parquet"
    )

    assert len(source_trades) == 4
    assert len(output_trades) == 2
    assert output_trades["dedup_hash"].is_unique
    assert (output_root / "capture_log" / "date=2026-04-23").exists()
    assert (
        output_root
        / "exports"
        / "truth"
        / "nautilus"
        / "date=2026-04-23"
        / "symbol=BTC"
        / "trade_ticks.parquet"
    ).exists()

    candidate_report = run_backtest_readiness(
        root=output_root,
        trading_date=trading_date,
        symbols=("BTC",),
        profile="nautilus_baseline_candidate",
    )

    assert candidate_report.status == "ready"
    assert candidate_report.blocking_reasons == ()
    assert candidate_report.profile.name == "nautilus_baseline_candidate"
