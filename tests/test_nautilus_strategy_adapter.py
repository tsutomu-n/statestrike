from __future__ import annotations

import copy
import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from statestrike.baseline_input import build_nautilus_baseline_input
from statestrike.backtests.nautilus_strategy_adapter import run_nautilus_strategy_adapter
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


def adapter_config() -> CollectorConfig:
    return CollectorConfig(
        allowed_symbols=("BTC",),
        source_priority=("ws", "info", "s3", "tardis"),
        market_data_network="mainnet",
        flush_interval_ms=1000,
        snapshot_recovery_enabled=True,
        channels=("l2Book", "trades", "activeAssetCtx"),
        candle_interval=None,
    )


def prepare_candidate_dataset(tmp_path: Path) -> Path:
    source_root = tmp_path / "source"
    output_root = tmp_path / "candidate"
    trading_date = date(2026, 4, 24)
    trades = copy.deepcopy(load_fixture("trades.json"))
    trades["data"].append(
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
        root=source_root,
        trading_date=trading_date,
        messages=[
            load_fixture("l2_book.json"),
            trades,
            load_fixture("active_asset_ctx.json"),
        ],
        config=adapter_config(),
        capture_session_id="session-adapter",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )
    enrich_funding_schedule_from_predicted_fundings(
        root=source_root,
        trading_date=trading_date,
        symbols=("BTC",),
        predicted_fundings=PREDICTED_FUNDINGS,
        enrichment_asof_ts=1713819000000,
    )
    build_nautilus_baseline_input(
        source_root=source_root,
        output_root=output_root,
        trading_date=trading_date,
        symbols=("BTC",),
    )
    return output_root


def test_nautilus_strategy_adapter_emits_orders_inside_backtest_node(tmp_path) -> None:
    root = prepare_candidate_dataset(tmp_path)

    result = run_nautilus_strategy_adapter(
        root=root,
        trading_date=date(2026, 4, 24),
        symbol="BTC",
        trade_size=Decimal("10"),
    )

    assert result.strategy_name == "nautilus_single_trade_roundtrip_strategy"
    assert result.readiness_status == "ready"
    assert result.nautilus_result.trade_count == 3
    assert result.nautilus_result.order_count > 0
    assert result.nautilus_result.position_count > 0
    assert result.nautilus_result.fee_cost > 0
    assert result.nautilus_result.net_pnl == pytest.approx(
        result.nautilus_result.gross_pnl - result.nautilus_result.fee_cost
    )
    assert result.nautilus_result.max_drawdown >= 0
    assert result.nautilus_result.source == "nautilus_backtest_result_and_engine_cache"
    assert result.control_result.strategy_name == "nautilus_baseline_harness_v1"
    assert result.parity.same_symbol is True
    assert result.parity.same_derived_input is True
    assert result.parity.exact_pnl_match_required is False
    assert result.parity.exact_fill_count_match_required is False
