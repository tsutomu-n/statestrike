from __future__ import annotations

from statestrike.exports import export_hftbacktest_npz
from statestrike.hftbacktest_acceptance import (
    HftbacktestAcceptanceConfig,
    run_hftbacktest_replay_acceptance,
)
from test_exports import _write_valid_normalized_rows


def test_hftbacktest_replay_acceptance_reads_feed_to_end(tmp_path) -> None:
    root, trading_date = _write_valid_normalized_rows(tmp_path)
    npz_path = export_hftbacktest_npz(
        normalized_root=root,
        export_root=root,
        trading_date=trading_date,
        symbol="BTC",
    )

    report = run_hftbacktest_replay_acceptance(
        npz_path=npz_path,
        symbol="BTC",
        config=HftbacktestAcceptanceConfig(
            tick_size=1.0,
            lot_size=0.001,
            order_qty=0.001,
            passive_order_distance_ticks=1000,
        ),
    )

    assert report.accepted is True
    assert report.reached_end is True
    assert report.feed_event_count > 0
    assert report.timeout_count == 0
    assert report.terminal_position == 0.0
    assert report.order_probe_submitted is True
    assert report.order_probe_response_received is True
    assert report.order_probe_filled is False
    assert report.order_latency_request_ns == report.order_latency_exchange_ns
    assert report.order_latency_exchange_ns == report.order_latency_response_observed_ns


def test_hftbacktest_replay_acceptance_reports_model_contract(tmp_path) -> None:
    root, trading_date = _write_valid_normalized_rows(tmp_path)
    npz_path = export_hftbacktest_npz(
        normalized_root=root,
        export_root=root,
        trading_date=trading_date,
        symbol="BTC",
    )

    report = run_hftbacktest_replay_acceptance(
        npz_path=npz_path,
        symbol="BTC",
        config=HftbacktestAcceptanceConfig(),
    )

    assert report.asset_type == "linear"
    assert report.market_depth_backend == "hashmap"
    assert report.queue_model == "risk_adverse"
    assert report.order_latency_model == "constant"
    assert report.exchange_model == "no_partial_fill"
    assert report.order_latency_entry_ns == 0
    assert report.order_latency_response_ns == 0
