from __future__ import annotations

import copy
import json
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from statestrike.baseline_input import build_nautilus_baseline_input
from statestrike.collector import CollectorConfig
from statestrike.enrichment import enrich_funding_schedule_from_predicted_fundings
from statestrike.funding import build_funding_history_sidecar
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
PREDICTED_FUNDINGS_ETH = [
    [
        "ETH",
        [
            [
                "HlPerp",
                {
                    "fundingRate": "0.000008",
                    "nextFundingTime": 1713819600000,
                    "fundingIntervalHours": 1,
                },
            ]
        ],
    ]
]


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def with_symbol(message: dict, symbol: str) -> dict:
    patched = copy.deepcopy(message)
    data = patched["data"]
    if isinstance(data, list):
        for row in data:
            row["coin"] = symbol
    else:
        data["coin"] = symbol
    return patched


def adapter_config(*, symbol: str = "BTC") -> CollectorConfig:
    return CollectorConfig(
        allowed_symbols=(symbol,),
        source_priority=("ws", "info", "s3", "tardis"),
        market_data_network="mainnet",
        flush_interval_ms=1000,
        snapshot_recovery_enabled=True,
        channels=("l2Book", "trades", "activeAssetCtx"),
        candle_interval=None,
    )


def prepare_candidate_dataset(
    tmp_path: Path,
    *,
    prices: tuple[float, ...] = (100.25, 100.50, 100.75),
    symbol: str = "BTC",
) -> Path:
    source_root = tmp_path / "source"
    output_root = tmp_path / "candidate"
    trading_date = date(2026, 4, 24)
    trades = copy.deepcopy(load_fixture("trades.json"))
    trades["data"] = [
        {
            "coin": symbol,
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
            with_symbol(load_fixture("l2_book.json"), symbol),
            trades,
            with_symbol(load_fixture("active_asset_ctx.json"), symbol),
        ],
        config=adapter_config(symbol=symbol),
        capture_session_id="session-adapter",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )
    enrich_funding_schedule_from_predicted_fundings(
        root=source_root,
        trading_date=trading_date,
        symbols=(symbol,),
        predicted_fundings=(
            PREDICTED_FUNDINGS if symbol == "BTC" else PREDICTED_FUNDINGS_ETH
        ),
        enrichment_asof_ts=1713819000000,
    )
    build_nautilus_baseline_input(
        source_root=source_root,
        output_root=output_root,
        trading_date=trading_date,
        symbols=(symbol,),
    )
    return output_root


def funding_history_payload(symbol: str, trading_date: date) -> list[dict[str, str | int]]:
    start = datetime.combine(trading_date, datetime.min.time(), tzinfo=UTC)
    return [
        {
            "coin": symbol,
            "fundingRate": "0.0000125",
            "premium": "0.0000010",
            "time": int((start + timedelta(hours=hour)).timestamp() * 1000),
        }
        for hour in range(24)
    ]


def run_strategy_adapter_in_child(
    *,
    root: Path,
    trading_date: date,
    symbol: str,
    tmp_path: Path,
    runner_name: str,
    extra_kwargs: dict[str, object] | None = None,
) -> dict:
    output_path = tmp_path / f"{runner_name}.json"
    kwargs = extra_kwargs or {}
    code = f"""
from datetime import date
from decimal import Decimal
import json
from pathlib import Path
import sys
from statestrike.backtests.nautilus_strategy_adapter import {runner_name}

root = Path(sys.argv[1])
output_path = Path(sys.argv[2])
kwargs = json.loads(sys.argv[3])
if "trade_size" in kwargs:
    kwargs["trade_size"] = Decimal(str(kwargs["trade_size"]))
result = {runner_name}(
    root=root,
    trading_date=date.fromisoformat(sys.argv[4]),
    symbol=sys.argv[5],
    **kwargs,
)
output_path.write_text(result.model_dump_json(), encoding="utf-8")
"""
    subprocess.run(
        [
            sys.executable,
            "-c",
            code,
            root.as_posix(),
            output_path.as_posix(),
            json.dumps(kwargs),
            trading_date.isoformat(),
            symbol,
        ],
        check=True,
        cwd=Path(__file__).resolve().parents[1],
    )
    return json.loads(output_path.read_text(encoding="utf-8"))


def test_nautilus_strategy_adapter_emits_orders_inside_backtest_node(tmp_path) -> None:
    root = prepare_candidate_dataset(tmp_path)

    result = run_strategy_adapter_in_child(
        root=root,
        trading_date=date(2026, 4, 24),
        symbol="BTC",
        tmp_path=tmp_path,
        runner_name="run_nautilus_strategy_adapter",
        extra_kwargs={"trade_size": "10"},
    )

    assert result["strategy_name"] == "nautilus_single_trade_roundtrip_strategy"
    assert result["readiness_status"] == "ready"
    nautilus = result["nautilus_result"]
    assert nautilus["input_trade_tick_count"] == 3
    assert nautilus["executed_order_count"] > 0
    assert nautilus["fill_event_count"] > 0
    assert nautilus["closed_position_count"] > 0
    assert nautilus["fee_cost"] > 0
    assert nautilus["fee_cost_source"] == "sum(OrderFilled.commission)"
    assert (
        nautilus["net_pnl_source"]
        == 'BacktestResult.stats_pnls["USD"]["PnL (total)"]'
    )
    assert nautilus["net_pnl"] == pytest.approx(
        nautilus["gross_pnl"] - nautilus["fee_cost"]
    )
    assert nautilus["max_drawdown"] >= 0
    assert len(nautilus["equity_curve"]) >= 2
    assert nautilus["equity_curve"][-1] == pytest.approx(nautilus["net_pnl"])
    assert nautilus["equity_curve_source"] == "PositionClosed.realized_pnl"
    assert nautilus["source"] == "nautilus_backtest_result_and_engine_cache"
    assert result["control_result"]["strategy_name"] == "nautilus_baseline_harness_v1"
    assert result["parity"]["same_symbol"] is True
    assert result["parity"]["same_derived_input"] is True
    assert result["parity"]["exact_pnl_match_required"] is False
    assert result["parity"]["exact_fill_count_match_required"] is False


def test_nautilus_repeated_order_strategy_emits_multiple_orders(tmp_path) -> None:
    root = prepare_candidate_dataset(
        tmp_path,
        prices=(100.25, 100.50, 100.75, 101.00, 101.25, 101.50),
    )

    result = run_strategy_adapter_in_child(
        root=root,
        trading_date=date(2026, 4, 24),
        symbol="BTC",
        tmp_path=tmp_path,
        runner_name="run_nautilus_repeated_order_strategy_harness",
        extra_kwargs={"trade_size": "10", "max_roundtrips": 2},
    )

    assert result["strategy_name"] == "nautilus_simple_momentum_strategy"
    nautilus = result["nautilus_result"]
    assert nautilus["input_trade_tick_count"] == 6
    assert nautilus["executed_order_count"] > 2
    assert nautilus["fill_event_count"] > 2
    assert nautilus["closed_position_count"] >= 1
    assert nautilus["fee_cost"] > 0
    assert nautilus["position_interval_count"] > 0
    assert nautilus["position_intervals"][0]["source"] == "nautilus_order_fills"
    assert nautilus["net_pnl"] == pytest.approx(
        nautilus["gross_pnl"] - nautilus["fee_cost"]
    )
    assert nautilus["max_drawdown"] >= 0
    assert len(nautilus["equity_curve"]) >= 2
    assert nautilus["equity_curve"][-1] == pytest.approx(nautilus["net_pnl"])
    assert nautilus["equity_curve_source"] == "PositionClosed.realized_pnl"
    assert result["control_result"]["strategy_name"] == "nautilus_baseline_harness_v1"
    assert result["parity"]["same_symbol"] is True
    assert result["parity"]["same_derived_input"] is True


def test_repeated_order_strategy_with_funding_pnl_keeps_net_fields_separate(
    tmp_path,
) -> None:
    trading_date = date(2026, 4, 24)
    root = prepare_candidate_dataset(
        tmp_path,
        prices=(100.25, 100.50, 100.75, 101.00, 101.25, 101.50),
    )
    build_funding_history_sidecar(
        root=root,
        trading_date=trading_date,
        symbols=("BTC",),
        funding_history_payloads={"BTC": funding_history_payload("BTC", trading_date)},
    )

    result = run_strategy_adapter_in_child(
        root=root,
        trading_date=trading_date,
        symbol="BTC",
        tmp_path=tmp_path,
        runner_name="run_nautilus_repeated_order_strategy_with_funding_pnl",
        extra_kwargs={"trade_size": "10", "max_roundtrips": 2},
    )

    assert result["strategy_name"] == (
        "nautilus_simple_momentum_strategy_with_funding_pnl"
    )
    assert result["dataset_profile"] == "nautilus_funding_candidate"
    assert result["funding_treatment"] == "ex_post_ledger"
    assert result["funding_ledger"]["source_type"] == "fundingHistory"
    assert result["funding_ledger"]["entry_count"] == 0
    assert result["baseline_result"]["nautilus_result"]["funding_treatment"] == "ignored"
    augmented = result["augmented_pnl"]
    nautilus = result["baseline_result"]["nautilus_result"]
    assert augmented["gross_pnl_ex_funding"] == pytest.approx(nautilus["gross_pnl"])
    assert augmented["fee_cost"] == pytest.approx(nautilus["fee_cost"])
    assert augmented["net_pnl_ex_funding"] == pytest.approx(nautilus["net_pnl"])
    assert augmented["funding_pnl"] == pytest.approx(0.0)
    assert augmented["net_pnl_after_funding"] == pytest.approx(
        augmented["net_pnl_ex_funding"] + augmented["funding_pnl"]
    )


def test_nautilus_repeated_order_strategy_accepts_eth_added_symbol(tmp_path) -> None:
    root = prepare_candidate_dataset(
        tmp_path,
        prices=(2000.25, 2000.50, 2000.75, 2001.00, 2001.25, 2001.50),
        symbol="ETH",
    )

    result = run_strategy_adapter_in_child(
        root=root,
        trading_date=date(2026, 4, 24),
        symbol="ETH",
        tmp_path=tmp_path,
        runner_name="run_nautilus_repeated_order_strategy_harness",
        extra_kwargs={"trade_size": "1", "max_roundtrips": 2},
    )

    assert result["strategy_name"] == "nautilus_simple_momentum_strategy"
    assert result["symbol"] == "ETH"
    nautilus = result["nautilus_result"]
    assert nautilus["input_trade_tick_count"] == 6
    assert nautilus["executed_order_count"] > 2
    assert nautilus["fill_event_count"] > 2
    assert nautilus["fee_cost"] > 0
    assert result["parity"]["same_symbol"] is True
    assert result["parity"]["same_derived_input"] is True
