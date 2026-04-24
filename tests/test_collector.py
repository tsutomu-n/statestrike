from __future__ import annotations

import json
from pathlib import Path

import pytest

from statestrike.collector import (
    CollectorConfig,
    build_subscription_requests,
    collect_market_batch,
)


FIXTURES = Path(__file__).parent / "fixtures" / "hyperliquid"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_collect_market_batch_replays_hyperliquid_fixtures() -> None:
    config = CollectorConfig(
        allowed_symbols=("BTC",),
        source_priority=("ws", "info", "s3", "tardis"),
        market_data_network="mainnet",
        flush_interval_ms=1000,
        snapshot_recovery_enabled=True,
        channels=("l2Book", "trades", "activeAssetCtx"),
        candle_interval=None,
    )

    result = collect_market_batch(
        messages=[
            load_fixture("l2_book.json"),
            load_fixture("trades.json"),
            load_fixture("active_asset_ctx.json"),
        ],
        config=config,
        capture_session_id="session-1",
        reconnect_epoch=0,
        book_epoch=1,
        recv_ts_start=1713818880100,
    )

    assert result.raw_count == 3
    assert result.normalized_counts["book_events"] == 1
    assert result.normalized_counts["book_levels"] == 4
    assert result.normalized_counts["trades"] == 2
    assert result.normalized_counts["asset_ctx"] == 1


def test_build_subscription_requests_include_phase15_candle_feed() -> None:
    config = CollectorConfig(
        allowed_symbols=("BTC", "ETH"),
        source_priority=("ws", "info", "s3", "tardis"),
        market_data_network="mainnet",
        flush_interval_ms=1000,
        snapshot_recovery_enabled=True,
        channels=("l2Book", "trades", "activeAssetCtx", "candle"),
        candle_interval="1m",
    )

    requests = build_subscription_requests(config)

    assert requests[0] == {
        "method": "subscribe",
        "subscription": {"type": "l2Book", "coin": "BTC"},
    }
    assert requests[-1] == {
        "method": "subscribe",
        "subscription": {"type": "candle", "coin": "ETH", "interval": "1m"},
    }


def test_build_subscription_requests_reject_subscription_budget_overflow() -> None:
    config = CollectorConfig(
        run_mode="network",
        allowed_symbols=("BTC", "ETH"),
        source_priority=("ws", "info", "s3", "tardis"),
        market_data_network="mainnet",
        flush_interval_ms=1000,
        snapshot_recovery_enabled=True,
        channels=("l2Book", "trades", "activeAssetCtx", "candle"),
        candle_interval="1m",
        max_subscriptions_per_connection=7,
    )

    with pytest.raises(
        ValueError,
        match="subscription count exceeds max_subscriptions_per_connection",
    ):
        build_subscription_requests(config)


def test_collect_market_batch_keeps_candle_out_of_canonical_tables() -> None:
    config = CollectorConfig(
        allowed_symbols=("BTC",),
        source_priority=("ws", "info", "s3", "tardis"),
        market_data_network="mainnet",
        flush_interval_ms=1000,
        snapshot_recovery_enabled=True,
        channels=("l2Book", "trades", "activeAssetCtx", "candle"),
        candle_interval="1m",
    )

    result = collect_market_batch(
        messages=[
            load_fixture("l2_book.json"),
            load_fixture("trades.json"),
            load_fixture("active_asset_ctx.json"),
            load_fixture("candle.json"),
        ],
        config=config,
        capture_session_id="session-1",
        reconnect_epoch=0,
        book_epoch=1,
        recv_ts_start=1713818880100,
    )

    assert result.raw_count == 4
    assert result.normalized_counts == {
        "book_events": 1,
        "book_levels": 4,
        "trades": 2,
        "asset_ctx": 1,
    }


def test_collector_config_validates_runtime_backoff_bounds() -> None:
    with pytest.raises(
        ValueError,
        match="reconnect_backoff_max_ms must be greater than or equal to reconnect_backoff_initial_ms",
    ):
        CollectorConfig(
            allowed_symbols=("BTC",),
            source_priority=("ws", "info", "s3", "tardis"),
            market_data_network="mainnet",
            flush_interval_ms=1000,
            reconnect_backoff_initial_ms=5_000,
            reconnect_backoff_max_ms=1_000,
            snapshot_recovery_enabled=True,
        )
