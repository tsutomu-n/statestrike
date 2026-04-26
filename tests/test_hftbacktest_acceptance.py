from __future__ import annotations

import math

from statestrike.exports import export_hftbacktest_npz
from statestrike.hftbacktest_acceptance import (
    HftbacktestAcceptanceConfig,
    HftbacktestMechanicalProbeConfig,
    run_hftbacktest_mechanical_fill_probe_report,
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


def test_hftbacktest_mechanical_fill_probe_report_separates_probe_families(
    tmp_path,
) -> None:
    root, trading_date = _write_valid_normalized_rows(tmp_path)
    npz_path = export_hftbacktest_npz(
        normalized_root=root,
        export_root=root,
        trading_date=trading_date,
        symbol="BTC",
    )

    report = run_hftbacktest_mechanical_fill_probe_report(
        npz_path=npz_path,
        symbol="BTC",
        config=HftbacktestMechanicalProbeConfig(
            tick_size=1.0,
            lot_size=0.001,
            order_qty=0.001,
            far_passive_distance_ticks=1000,
        ),
    )

    assert report.accepted is True
    assert report.acceptance_level == "P4-18_mechanical_reporting"
    assert report.latency_contract.model == "synthetic_constant_latency"
    assert report.latency_contract.calibrated is False
    assert report.queue_contract.model == "risk_adverse"
    assert report.queue_contract.calibrated is False
    assert report.calibration_claimed is False
    assert report.fill_realism_claimed is False
    assert report.strategy_edge_claimed is False
    assert {(probe.family, probe.side) for probe in report.probes} == {
        ("no_fill_passive", "buy"),
        ("passive_maker", "buy"),
        ("crossing_taker", "buy"),
        ("no_fill_passive", "sell"),
        ("passive_maker", "sell"),
        ("crossing_taker", "sell"),
    }
    for probe in report.probes:
        assert probe.submit_after_feed_events == 1
        assert probe.submit_feed_event_count is not None
        assert probe.submit_feed_event_count >= probe.submit_after_feed_events

    no_fill = next(
        probe
        for probe in report.probes
        if probe.family == "no_fill_passive" and probe.side == "buy"
    )
    assert no_fill.submitted is True
    assert no_fill.response_received is True
    assert no_fill.filled is False
    assert no_fill.fill_count_delta == 0
    assert no_fill.terminal_position == 0.0

    sell_no_fill = next(
        probe
        for probe in report.probes
        if probe.family == "no_fill_passive" and probe.side == "sell"
    )
    assert sell_no_fill.submitted is True
    assert sell_no_fill.response_received is True
    assert sell_no_fill.filled is False
    assert sell_no_fill.fill_count_delta == 0
    assert sell_no_fill.terminal_position == 0.0

    passive = next(
        probe
        for probe in report.probes
        if probe.family == "passive_maker" and probe.side == "buy"
    )
    assert passive.submitted is True
    assert passive.response_received is True
    assert passive.fill_count_delta >= 0
    assert math.isfinite(passive.fee_delta)

    crossing = next(
        probe
        for probe in report.probes
        if probe.family == "crossing_taker" and probe.side == "buy"
    )
    assert crossing.submitted is True
    assert crossing.response_received is True
    assert crossing.filled is True
    assert crossing.fill_count_delta >= 1
    assert crossing.trading_volume_delta > 0
    assert crossing.trading_value_delta > 0
    assert crossing.implied_avg_fill_price is not None
    assert crossing.implied_avg_fill_price > 0
    assert crossing.terminal_position == crossing.signed_fill_qty
    assert len(report.fill_ledger) >= 1

    sell_crossing = next(
        probe
        for probe in report.probes
        if probe.family == "crossing_taker" and probe.side == "sell"
    )
    assert sell_crossing.submitted is True
    assert sell_crossing.response_received is True
    assert sell_crossing.filled is True
    assert sell_crossing.fill_count_delta >= 1
    assert sell_crossing.trading_volume_delta > 0
    assert sell_crossing.trading_value_delta > 0
    assert sell_crossing.implied_avg_fill_price is not None
    assert sell_crossing.implied_avg_fill_price > 0
    assert sell_crossing.signed_fill_qty < 0
    assert sell_crossing.terminal_position == sell_crossing.signed_fill_qty


def test_hftbacktest_mechanical_fill_probe_report_reports_schedule_contract(
    tmp_path,
) -> None:
    root, trading_date = _write_valid_normalized_rows(tmp_path)
    npz_path = export_hftbacktest_npz(
        normalized_root=root,
        export_root=root,
        trading_date=trading_date,
        symbol="BTC",
    )

    report = run_hftbacktest_mechanical_fill_probe_report(
        npz_path=npz_path,
        symbol="BTC",
        config=HftbacktestMechanicalProbeConfig(
            tick_size=1.0,
            lot_size=0.001,
            order_qty=0.001,
            no_fill_submit_after_feed_events=1,
            passive_maker_submit_after_feed_events=1,
            crossing_taker_submit_after_feed_events=1,
        ),
    )

    assert report.accepted is True
    schedules = {
        (probe.family, probe.side): (
            probe.submit_after_feed_events,
            probe.submit_feed_event_count,
        )
        for probe in report.probes
    }
    assert schedules == {
        ("no_fill_passive", "buy"): (1, 1),
        ("no_fill_passive", "sell"): (1, 1),
        ("passive_maker", "buy"): (1, 1),
        ("passive_maker", "sell"): (1, 1),
        ("crossing_taker", "buy"): (1, 1),
        ("crossing_taker", "sell"): (1, 1),
    }


def test_hftbacktest_mechanical_fill_probe_report_is_deterministic(tmp_path) -> None:
    root, trading_date = _write_valid_normalized_rows(tmp_path)
    npz_path = export_hftbacktest_npz(
        normalized_root=root,
        export_root=root,
        trading_date=trading_date,
        symbol="BTC",
    )

    first = run_hftbacktest_mechanical_fill_probe_report(
        npz_path=npz_path,
        symbol="BTC",
        config=HftbacktestMechanicalProbeConfig(
            tick_size=1.0,
            lot_size=0.001,
            order_qty=0.001,
        ),
    )
    second = run_hftbacktest_mechanical_fill_probe_report(
        npz_path=npz_path,
        symbol="BTC",
        config=HftbacktestMechanicalProbeConfig(
            tick_size=1.0,
            lot_size=0.001,
            order_qty=0.001,
        ),
    )

    assert first.model_dump(mode="json") == second.model_dump(mode="json")
