from __future__ import annotations

from pathlib import Path
from typing import Literal

import hftbacktest as hftbacktest_pkg
from hftbacktest import BacktestAsset, HashMapMarketDepthBacktest
from hftbacktest.order import GTC, GTX, IOC, LIMIT
import numpy as np
from pydantic import BaseModel, ConfigDict, Field


class HftbacktestAcceptanceConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    asset_type: Literal["linear"] = "linear"
    market_depth_backend: Literal["hashmap"] = "hashmap"
    queue_model: Literal["risk_adverse"] = "risk_adverse"
    order_latency_model: Literal["constant"] = "constant"
    exchange_model: Literal["no_partial_fill"] = "no_partial_fill"
    contract_size: float = Field(default=1.0, gt=0)
    tick_size: float = Field(default=0.1, gt=0)
    lot_size: float = Field(default=0.001, gt=0)
    maker_fee: float = -0.0001
    taker_fee: float = 0.0007
    order_latency_entry_ns: int = Field(default=0, ge=0)
    order_latency_response_ns: int = Field(default=0, ge=0)
    replay_timeout_ns: int = Field(default=10**18, gt=0)
    order_qty: float = Field(default=0.001, gt=0)
    passive_order_distance_ticks: int = Field(default=1_000_000, gt=0)
    submit_order_probe: bool = True


class HftbacktestReplayAcceptanceReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    accepted: bool
    symbol: str
    row_count: int = Field(ge=0)
    feed_event_count: int = Field(ge=0)
    timeout_count: int = Field(ge=0)
    reached_end: bool
    terminal_position: float
    final_best_bid: float | None = None
    final_best_ask: float | None = None
    feed_latency_ns_min: int | None = None
    feed_latency_ns_max: int | None = None
    asset_type: Literal["linear"]
    market_depth_backend: Literal["hashmap"]
    queue_model: Literal["risk_adverse"]
    order_latency_model: Literal["constant"]
    exchange_model: Literal["no_partial_fill"]
    order_latency_entry_ns: int = Field(ge=0)
    order_latency_response_ns: int = Field(ge=0)
    order_probe_submitted: bool
    order_probe_response_received: bool
    order_probe_filled: bool
    order_latency_request_ns: int | None = None
    order_latency_exchange_ns: int | None = None
    order_latency_response_observed_ns: int | None = None


class HftbacktestMechanicalProbeConfig(HftbacktestAcceptanceConfig):
    latency_label: Literal["synthetic_constant_latency"] = "synthetic_constant_latency"
    far_passive_distance_ticks: int = Field(default=1000, gt=0)
    replay_row_limit: int | None = Field(default=None, gt=0)
    run_no_fill_passive: bool = True
    run_passive_maker: bool = True
    run_crossing_taker: bool = True


class HftbacktestLatencyContract(BaseModel):
    model_config = ConfigDict(frozen=True)

    model: Literal["synthetic_constant_latency"]
    calibrated: bool
    entry_ns: int = Field(ge=0)
    response_ns: int = Field(ge=0)


class HftbacktestQueueContract(BaseModel):
    model_config = ConfigDict(frozen=True)

    model: Literal["risk_adverse"]
    calibrated: bool


class HftbacktestFillLedgerEntry(BaseModel):
    model_config = ConfigDict(frozen=True)

    probe_name: str
    family: Literal["passive_maker", "crossing_taker"]
    order_id: int
    side: Literal["buy"]
    fill_count: int = Field(ge=1)
    signed_fill_qty: float
    trading_volume: float = Field(gt=0)
    trading_value: float = Field(gt=0)
    implied_avg_fill_price: float = Field(gt=0)
    fee: float
    observed_ts_ns: int = Field(ge=0)


class HftbacktestMechanicalProbeResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    probe_name: str
    family: Literal["no_fill_passive", "passive_maker", "crossing_taker"]
    side: Literal["buy"]
    order_id: int
    price_rule: str
    time_in_force: Literal["GTX", "GTC", "IOC"]
    order_type: Literal["LIMIT"]
    submitted: bool
    response_received: bool
    filled: bool
    reached_end: bool
    timeout_count: int = Field(ge=0)
    feed_event_count: int = Field(ge=0)
    submit_ts_ns: int | None = Field(default=None, ge=0)
    response_ts_ns: int | None = Field(default=None, ge=0)
    best_bid_at_submit: float | None = None
    best_ask_at_submit: float | None = None
    submit_price: float | None = None
    order_qty: float = Field(gt=0)
    fill_count_delta: int = Field(ge=0)
    signed_fill_qty: float
    trading_volume_delta: float = Field(ge=0)
    trading_value_delta: float = Field(ge=0)
    fee_delta: float
    implied_avg_fill_price: float | None = None
    terminal_position: float


class HftbacktestMechanicalProbeReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    accepted: bool
    acceptance_level: Literal["P4-18_mechanical_reporting"]
    symbol: str
    row_count: int = Field(ge=0)
    hftbacktest_version: str
    latency_contract: HftbacktestLatencyContract
    queue_contract: HftbacktestQueueContract
    asset_type: Literal["linear"]
    market_depth_backend: Literal["hashmap"]
    exchange_model: Literal["no_partial_fill"]
    calibration_claimed: bool
    fill_realism_claimed: bool
    strategy_edge_claimed: bool
    probes: tuple[HftbacktestMechanicalProbeResult, ...]
    fill_ledger: tuple[HftbacktestFillLedgerEntry, ...]
    blocking_reasons: tuple[str, ...] = ()
    non_claims: tuple[str, ...] = (
        "No order-latency calibration is claimed.",
        "No queue-model calibration is claimed.",
        "No fill-realism acceptance is claimed.",
        "No strategy edge is claimed.",
        "No tiny-live approval or live readiness is claimed.",
    )


def run_hftbacktest_replay_acceptance(
    *,
    npz_path: Path,
    symbol: str,
    config: HftbacktestAcceptanceConfig | None = None,
) -> HftbacktestReplayAcceptanceReport:
    config = config or HftbacktestAcceptanceConfig()
    with np.load(npz_path) as archive:
        data = archive["data"].copy()

    hbt = _build_hashmap_backtest(data=data, config=config)
    try:
        return _run_replay(
            hbt=hbt,
            row_count=int(len(data)),
            symbol=symbol.upper(),
            config=config,
        )
    finally:
        hbt.close()


def run_hftbacktest_mechanical_fill_probe_report(
    *,
    npz_path: Path,
    symbol: str,
    config: HftbacktestMechanicalProbeConfig | None = None,
) -> HftbacktestMechanicalProbeReport:
    config = config or HftbacktestMechanicalProbeConfig()
    with np.load(npz_path) as archive:
        data = archive["data"].copy()
    if config.replay_row_limit is not None:
        data = data[: config.replay_row_limit].copy()

    probes = tuple(
        _run_single_mechanical_probe(
            data=data,
            symbol=symbol.upper(),
            config=config,
            probe_name=probe_name,
            family=family,
            order_id=order_id,
            price_rule=price_rule,
            time_in_force=time_in_force,
        )
        for probe_name, family, order_id, price_rule, time_in_force in _mechanical_probe_specs(
            config
        )
    )
    fill_ledger = tuple(
        entry
        for probe in probes
        for entry in _probe_fill_ledger_entries(probe)
    )
    blocking_reasons = _mechanical_probe_blocking_reasons(probes)
    return HftbacktestMechanicalProbeReport(
        accepted=not blocking_reasons,
        acceptance_level="P4-18_mechanical_reporting",
        symbol=symbol.upper(),
        row_count=int(len(data)),
        hftbacktest_version=getattr(hftbacktest_pkg, "__version__", "unknown"),
        latency_contract=HftbacktestLatencyContract(
            model=config.latency_label,
            calibrated=False,
            entry_ns=config.order_latency_entry_ns,
            response_ns=config.order_latency_response_ns,
        ),
        queue_contract=HftbacktestQueueContract(
            model=config.queue_model,
            calibrated=False,
        ),
        asset_type=config.asset_type,
        market_depth_backend=config.market_depth_backend,
        exchange_model=config.exchange_model,
        calibration_claimed=False,
        fill_realism_claimed=False,
        strategy_edge_claimed=False,
        probes=probes,
        fill_ledger=fill_ledger,
        blocking_reasons=blocking_reasons,
    )


def _run_replay(
    *,
    hbt,
    row_count: int,
    symbol: str,
    config: HftbacktestAcceptanceConfig,
) -> HftbacktestReplayAcceptanceReport:
    feed_event_count = 0
    timeout_count = 0
    reached_end = False
    feed_latencies: list[int] = []
    order_probe_submitted = False
    order_probe_response_received = False
    order_latency: tuple[int, int, int] | None = None

    for _ in range(row_count + 1000):
        result = hbt.wait_next_feed(False, config.replay_timeout_ns)
        if result == 0:
            timeout_count += 1
            continue
        if result == 1:
            reached_end = True
            break
        if result != 2:
            continue

        feed_event_count += 1
        feed_latency = hbt.feed_latency(0)
        if feed_latency is not None:
            feed_latencies.append(int(feed_latency[1] - feed_latency[0]))

        if config.submit_order_probe and not order_probe_submitted:
            depth = hbt.depth(0)
            if depth.best_bid > 0 and depth.best_ask > 0:
                price = max(
                    config.tick_size,
                    depth.best_bid
                    - config.passive_order_distance_ticks * config.tick_size,
                )
                submit_result = hbt.submit_buy_order(
                    0,
                    1,
                    price,
                    config.order_qty,
                    GTX,
                    LIMIT,
                    True,
                )
                order_probe_submitted = submit_result == 0
                order_latency = hbt.order_latency(0)
                order_probe_response_received = order_latency is not None

    final_depth = hbt.depth(0)
    state_values = hbt.state_values(0)
    terminal_position = float(state_values.position)
    accepted = (
        reached_end
        and feed_event_count > 0
        and timeout_count == 0
        and terminal_position == 0.0
        and (
            not config.submit_order_probe
            or (order_probe_submitted and order_probe_response_received)
        )
    )
    return HftbacktestReplayAcceptanceReport(
        accepted=accepted,
        symbol=symbol,
        row_count=row_count,
        feed_event_count=feed_event_count,
        timeout_count=timeout_count,
        reached_end=reached_end,
        terminal_position=terminal_position,
        final_best_bid=float(final_depth.best_bid) if final_depth.best_bid > 0 else None,
        final_best_ask=float(final_depth.best_ask) if final_depth.best_ask > 0 else None,
        feed_latency_ns_min=min(feed_latencies) if feed_latencies else None,
        feed_latency_ns_max=max(feed_latencies) if feed_latencies else None,
        asset_type=config.asset_type,
        market_depth_backend=config.market_depth_backend,
        queue_model=config.queue_model,
        order_latency_model=config.order_latency_model,
        exchange_model=config.exchange_model,
        order_latency_entry_ns=config.order_latency_entry_ns,
        order_latency_response_ns=config.order_latency_response_ns,
        order_probe_submitted=order_probe_submitted,
        order_probe_response_received=order_probe_response_received,
        order_probe_filled=float(state_values.num_trades) > 0,
        order_latency_request_ns=int(order_latency[0]) if order_latency is not None else None,
        order_latency_exchange_ns=int(order_latency[1]) if order_latency is not None else None,
        order_latency_response_observed_ns=(
            int(order_latency[2]) if order_latency is not None else None
        ),
    )


def _mechanical_probe_specs(
    config: HftbacktestMechanicalProbeConfig,
) -> tuple[tuple[str, str, int, str, int], ...]:
    specs: list[tuple[str, str, int, str, int]] = []
    if config.run_no_fill_passive:
        specs.append(
            (
                "far_passive_buy_no_fill",
                "no_fill_passive",
                10_001,
                "far_below_best_bid",
                GTX,
            )
        )
    if config.run_passive_maker:
        specs.append(
            (
                "best_bid_post_only_buy",
                "passive_maker",
                10_002,
                "at_best_bid",
                GTX,
            )
        )
    if config.run_crossing_taker:
        specs.append(
            (
                "crossing_ioc_buy",
                "crossing_taker",
                10_003,
                "at_best_ask_ioc",
                IOC,
            )
        )
    return tuple(specs)


def _run_single_mechanical_probe(
    *,
    data: np.ndarray,
    symbol: str,
    config: HftbacktestMechanicalProbeConfig,
    probe_name: str,
    family: str,
    order_id: int,
    price_rule: str,
    time_in_force: int,
) -> HftbacktestMechanicalProbeResult:
    hbt = _build_hashmap_backtest(data=data, config=config)
    try:
        return _execute_single_mechanical_probe(
            hbt=hbt,
            row_count=int(len(data)),
            symbol=symbol,
            config=config,
            probe_name=probe_name,
            family=family,
            order_id=order_id,
            price_rule=price_rule,
            time_in_force=time_in_force,
        )
    finally:
        hbt.close()


def _execute_single_mechanical_probe(
    *,
    hbt,
    row_count: int,
    symbol: str,
    config: HftbacktestMechanicalProbeConfig,
    probe_name: str,
    family: str,
    order_id: int,
    price_rule: str,
    time_in_force: int,
) -> HftbacktestMechanicalProbeResult:
    del symbol
    feed_event_count = 0
    timeout_count = 0
    reached_end = False
    submitted = False
    response_received = False
    submit_ts_ns: int | None = None
    response_ts_ns: int | None = None
    best_bid_at_submit: float | None = None
    best_ask_at_submit: float | None = None
    submit_price: float | None = None
    before_position = 0.0
    before_trades = 0
    before_volume = 0.0
    before_value = 0.0
    before_fee = 0.0

    for _ in range(row_count + 1000):
        result = hbt.wait_next_feed(False, config.replay_timeout_ns)
        if result == 0:
            timeout_count += 1
            continue
        if result == 1:
            reached_end = True
            break
        if result != 2:
            continue

        feed_event_count += 1
        if submitted:
            continue

        depth = hbt.depth(0)
        if not _has_finite_positive_bbo(depth):
            continue

        best_bid_at_submit = float(depth.best_bid)
        best_ask_at_submit = float(depth.best_ask)
        submit_price = _probe_submit_price(
            best_bid=best_bid_at_submit,
            best_ask=best_ask_at_submit,
            price_rule=price_rule,
            config=config,
        )
        state_values = hbt.state_values(0)
        before_position = float(state_values.position)
        before_trades = int(state_values.num_trades)
        before_volume = float(state_values.trading_volume)
        before_value = float(state_values.trading_value)
        before_fee = float(state_values.fee)
        submit_ts_ns = _optional_nonnegative_timestamp(hbt.current_timestamp)
        submit_result = hbt.submit_buy_order(
            0,
            order_id,
            submit_price,
            config.order_qty,
            time_in_force,
            LIMIT,
            True,
        )
        submitted = submit_result == 0
        order_latency = hbt.order_latency(0)
        response_received = order_latency is not None
        response_ts_ns = (
            _optional_nonnegative_timestamp(order_latency[2])
            if order_latency is not None
            else None
        )
        if family == "no_fill_passive":
            break

    final_state = hbt.state_values(0)
    fill_count_delta = int(final_state.num_trades) - before_trades
    trading_volume_delta = float(final_state.trading_volume) - before_volume
    trading_value_delta = float(final_state.trading_value) - before_value
    fee_delta = float(final_state.fee) - before_fee
    signed_fill_qty = float(final_state.position) - before_position
    implied_avg_fill_price = (
        trading_value_delta / trading_volume_delta
        if trading_volume_delta > 0
        else None
    )
    return HftbacktestMechanicalProbeResult(
        probe_name=probe_name,
        family=family,
        side="buy",
        order_id=order_id,
        price_rule=price_rule,
        time_in_force=_time_in_force_label(time_in_force),
        order_type="LIMIT",
        submitted=submitted,
        response_received=response_received,
        filled=fill_count_delta > 0,
        reached_end=reached_end,
        timeout_count=timeout_count,
        feed_event_count=feed_event_count,
        submit_ts_ns=submit_ts_ns,
        response_ts_ns=response_ts_ns,
        best_bid_at_submit=best_bid_at_submit,
        best_ask_at_submit=best_ask_at_submit,
        submit_price=submit_price,
        order_qty=config.order_qty,
        fill_count_delta=fill_count_delta,
        signed_fill_qty=signed_fill_qty,
        trading_volume_delta=trading_volume_delta,
        trading_value_delta=trading_value_delta,
        fee_delta=fee_delta,
        implied_avg_fill_price=implied_avg_fill_price,
        terminal_position=float(final_state.position),
    )


def _probe_submit_price(
    *,
    best_bid: float,
    best_ask: float,
    price_rule: str,
    config: HftbacktestMechanicalProbeConfig,
) -> float:
    if price_rule == "far_below_best_bid":
        return max(
            config.tick_size,
            best_bid - config.far_passive_distance_ticks * config.tick_size,
        )
    if price_rule == "at_best_bid":
        return best_bid
    if price_rule == "at_best_ask_ioc":
        return best_ask
    raise ValueError(f"unsupported mechanical probe price rule: {price_rule}")


def _has_finite_positive_bbo(depth) -> bool:
    return (
        bool(np.isfinite(depth.best_bid))
        and bool(np.isfinite(depth.best_ask))
        and depth.best_bid > 0
        and depth.best_ask > 0
    )


def _optional_nonnegative_timestamp(value: int) -> int | None:
    timestamp = int(value)
    return timestamp if timestamp >= 0 else None


def _time_in_force_label(time_in_force: int) -> Literal["GTX", "GTC", "IOC"]:
    if time_in_force == GTX:
        return "GTX"
    if time_in_force == GTC:
        return "GTC"
    if time_in_force == IOC:
        return "IOC"
    raise ValueError(f"unsupported time in force: {time_in_force}")


def _probe_fill_ledger_entries(
    probe: HftbacktestMechanicalProbeResult,
) -> tuple[HftbacktestFillLedgerEntry, ...]:
    if (
        not probe.filled
        or probe.family == "no_fill_passive"
        or probe.implied_avg_fill_price is None
        or probe.response_ts_ns is None
    ):
        return ()
    return (
        HftbacktestFillLedgerEntry(
            probe_name=probe.probe_name,
            family=probe.family,
            order_id=probe.order_id,
            side=probe.side,
            fill_count=probe.fill_count_delta,
            signed_fill_qty=probe.signed_fill_qty,
            trading_volume=probe.trading_volume_delta,
            trading_value=probe.trading_value_delta,
            implied_avg_fill_price=probe.implied_avg_fill_price,
            fee=probe.fee_delta,
            observed_ts_ns=probe.response_ts_ns,
        ),
    )


def _mechanical_probe_blocking_reasons(
    probes: tuple[HftbacktestMechanicalProbeResult, ...],
) -> tuple[str, ...]:
    reasons: list[str] = []
    by_family = {probe.family: probe for probe in probes}
    required = {"no_fill_passive", "passive_maker", "crossing_taker"}
    missing = sorted(required - set(by_family))
    if missing:
        reasons.append(f"missing_probe_families:{','.join(missing)}")
    for probe in probes:
        if not probe.submitted:
            reasons.append(f"{probe.probe_name}:not_submitted")
        if not probe.response_received:
            reasons.append(f"{probe.probe_name}:response_missing")
        if probe.trading_volume_delta < 0 or probe.trading_value_delta < 0:
            reasons.append(f"{probe.probe_name}:negative_fill_metric")
    no_fill = by_family.get("no_fill_passive")
    if no_fill is not None and no_fill.filled:
        reasons.append("no_fill_passive:unexpected_fill")
    crossing = by_family.get("crossing_taker")
    if crossing is not None and not crossing.filled:
        reasons.append("crossing_taker:not_filled")
    return tuple(reasons)


def _build_hashmap_backtest(
    *,
    data: np.ndarray,
    config: HftbacktestAcceptanceConfig,
):
    asset = (
        BacktestAsset()
        .add_data(data.copy())
        .linear_asset(config.contract_size)
        .tick_size(config.tick_size)
        .lot_size(config.lot_size)
        .trading_value_fee_model(config.maker_fee, config.taker_fee)
        .constant_order_latency(
            config.order_latency_entry_ns,
            config.order_latency_response_ns,
        )
        .risk_adverse_queue_model()
        .no_partial_fill_exchange()
    )
    return HashMapMarketDepthBacktest([asset])
