from __future__ import annotations

import copy
import json
from datetime import date
from pathlib import Path

import pytest

from statestrike.baseline_input import build_nautilus_baseline_input
from statestrike.backtests.nautilus_harness import (
    compute_max_drawdown,
    run_nautilus_baseline_harness_v1,
)
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


def harness_config() -> CollectorConfig:
    return CollectorConfig(
        allowed_symbols=("BTC",),
        source_priority=("ws", "info", "s3", "tardis"),
        market_data_network="mainnet",
        flush_interval_ms=1000,
        snapshot_recovery_enabled=True,
        channels=("l2Book", "trades", "activeAssetCtx"),
        candle_interval=None,
    )


def prepare_candidate_dataset(tmp_path: Path, *, prices: tuple[float, ...]) -> Path:
    source_root = tmp_path / "source"
    output_root = tmp_path / "candidate"
    trading_date = date(2026, 4, 24)
    trades = copy.deepcopy(load_fixture("trades.json"))
    trades["data"] = [
        {
            "coin": "BTC",
            "side": "B" if index % 2 == 0 else "A",
            "px": str(price),
            "sz": "0.2",
            "time": 1713818880050 + index * 10,
            "tid": index + 1,
        }
        for index, price in enumerate(prices)
    ]
    run_smoke_batch(
        root=source_root,
        trading_date=trading_date,
        messages=[
            load_fixture("l2_book.json"),
            trades,
            load_fixture("active_asset_ctx.json"),
        ],
        config=harness_config(),
        capture_session_id="session-harness",
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


def test_nautilus_harness_records_assumptions_and_charges_fees(tmp_path) -> None:
    root = prepare_candidate_dataset(tmp_path, prices=(100.25, 100.50, 100.75))

    result = run_nautilus_baseline_harness_v1(
        root=root,
        trading_date=date(2026, 4, 24),
        symbol="BTC",
    )

    assert result.strategy_name == "nautilus_baseline_harness_v1"
    assert result.config.assumed_taker_fee_rate == pytest.approx(0.0004)
    assert result.config.assumed_maker_fee_rate == pytest.approx(0.0004)
    assert result.config.funding_treatment == "ignored"
    assert result.metrics.fee_cost > 0
    assert result.metrics.order_count > 0
    assert result.metrics.turnover > 0
    assert result.metrics.net_pnl == pytest.approx(
        result.metrics.gross_pnl
        - result.metrics.fee_cost
        + result.metrics.funding_pnl
    )
    assert "ParquetDataCatalog" in result.high_level_path.components
    assert "BacktestNode" in result.high_level_path.components
    assert result.high_level_path.status == "configured"
    assert result.high_level_path.backtest_node_configured is True


def test_nautilus_harness_max_drawdown_is_computed_from_equity_curve(tmp_path) -> None:
    root = prepare_candidate_dataset(tmp_path, prices=(100.25, 100.26, 100.27))

    result = run_nautilus_baseline_harness_v1(
        root=root,
        trading_date=date(2026, 4, 24),
        symbol="BTC",
    )

    assert result.metrics.max_drawdown > 0
    assert result.metrics.max_drawdown == pytest.approx(
        compute_max_drawdown(result.metrics.equity_curve)
    )
    assert result.metrics.equity_curve_source == "closed_trade_net_pnl"


def test_nautilus_harness_control_target_parity_contract(tmp_path) -> None:
    root = prepare_candidate_dataset(tmp_path, prices=(100.25, 100.50, 100.75))

    result = run_nautilus_baseline_harness_v1(
        root=root,
        trading_date=date(2026, 4, 24),
        symbol="BTC",
    )

    assert result.parity.same_symbol is True
    assert result.parity.same_input_trade_universe is True
    assert result.parity.exact_pnl_match_required is False
    assert result.parity.exact_fill_count_match_required is False
    assert result.control.trade_count == result.metrics.trade_count
    assert result.control.order_count > 0
    assert result.metrics.order_count > 0
    assert result.metrics.fee_cost > 0
    assert result.metrics.max_drawdown >= 0
