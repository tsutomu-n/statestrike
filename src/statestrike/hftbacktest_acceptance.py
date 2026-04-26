from __future__ import annotations

from pathlib import Path
from typing import Literal

from hftbacktest import BacktestAsset, HashMapMarketDepthBacktest
from hftbacktest.order import GTX, LIMIT
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


def run_hftbacktest_replay_acceptance(
    *,
    npz_path: Path,
    symbol: str,
    config: HftbacktestAcceptanceConfig | None = None,
) -> HftbacktestReplayAcceptanceReport:
    config = config or HftbacktestAcceptanceConfig()
    with np.load(npz_path) as archive:
        data = archive["data"]
    data = data.copy()

    asset = (
        BacktestAsset()
        .add_data(data)
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
    hbt = HashMapMarketDepthBacktest([asset])
    try:
        return _run_replay(
            hbt=hbt,
            row_count=int(len(data)),
            symbol=symbol.upper(),
            config=config,
        )
    finally:
        hbt.close()


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
