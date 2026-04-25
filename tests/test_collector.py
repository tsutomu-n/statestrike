from __future__ import annotations

import json
from pathlib import Path

import pytest

from statestrike.collector import (
    CollectorConfig,
    build_subscription_requests,
    collect_market_batch,
)
from statestrike.recovery import MessageCaptureContext, MessageIngressMeta


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


def test_collect_market_batch_applies_message_recovery_contexts() -> None:
    config = CollectorConfig(
        allowed_symbols=("BTC",),
        source_priority=("ws", "info", "s3", "tardis"),
        market_data_network="mainnet",
        flush_interval_ms=1000,
        snapshot_recovery_enabled=True,
        channels=("l2Book",),
        candle_interval=None,
    )
    initial = load_fixture("l2_book.json")
    recovered = load_fixture("l2_book.json")
    recovered["data"]["time"] = recovered["data"]["time"] + 100

    result = collect_market_batch(
        messages=[initial, recovered],
        message_contexts=[
            MessageCaptureContext(
                reconnect_epoch=0,
                book_epoch=1,
                book_event_kind="snapshot",
                continuity_status="continuous",
            ),
            MessageCaptureContext(
                reconnect_epoch=1,
                book_epoch=2,
                book_event_kind="recovery_snapshot",
                continuity_status="recovered",
                recovery_classification="recoverable",
                recovery_succeeded=True,
            ),
        ],
        config=config,
        capture_session_id="session-ctx",
        reconnect_epoch=0,
        book_epoch=1,
        recv_ts_start=1713818880100,
    )

    assert [row["event_kind"] for row in result.normalized_rows["book_events"]] == [
        "snapshot",
        "recovery_snapshot",
    ]
    assert result.normalized_rows["book_events"][1]["book_epoch"] == 2
    assert result.normalized_rows["book_events"][1]["continuity_status"] == "recovered"
    assert (
        result.normalized_rows["book_events"][1]["recovery_classification"]
        == "recoverable"
    )


def test_collect_market_batch_uses_ingress_metadata_recv_timestamps_in_order() -> None:
    config = CollectorConfig(
        allowed_symbols=("BTC",),
        source_priority=("ws", "info", "s3", "tardis"),
        market_data_network="mainnet",
        flush_interval_ms=1000,
        snapshot_recovery_enabled=True,
        channels=("l2Book", "trades", "activeAssetCtx"),
        candle_interval=None,
    )

    ingress = [
        MessageIngressMeta(
            recv_wall_ns=1713818880102000000,
            recv_mono_ns=100,
            recv_seq=9,
            connection_id="conn-1",
        ),
        MessageIngressMeta(
            recv_wall_ns=1713818880100000000,
            recv_mono_ns=90,
            recv_seq=7,
            connection_id="conn-1",
        ),
        MessageIngressMeta(
            recv_wall_ns=1713818880101000000,
            recv_mono_ns=95,
            recv_seq=8,
            connection_id="conn-1",
        ),
    ]

    result = collect_market_batch(
        messages=[
            load_fixture("l2_book.json"),
            load_fixture("trades.json"),
            load_fixture("active_asset_ctx.json"),
        ],
        ingress_metadata=ingress,
        config=config,
        capture_session_id="session-1",
        reconnect_epoch=0,
        book_epoch=1,
        recv_ts_start=1713818880999,
    )

    assert result.normalized_rows["book_events"][0]["recv_ts"] == 1713818880102
    assert result.normalized_rows["book_events"][0]["recv_ts_ns"] == 1713818880102000000
    assert result.normalized_rows["book_events"][0]["recv_seq"] == 9
    assert [row["recv_ts"] for row in result.normalized_rows["trades"]] == [
        1713818880100,
        1713818880100,
    ]
    assert [row["recv_ts_ns"] for row in result.normalized_rows["trades"]] == [
        1713818880100000000,
        1713818880100000000,
    ]
    assert [row["recv_seq"] for row in result.normalized_rows["trades"]] == [7, 7]
    assert result.normalized_rows["asset_ctx"][0]["recv_ts"] == 1713818880101
    assert result.normalized_rows["asset_ctx"][0]["recv_ts_ns"] == 1713818880101000000
    assert result.normalized_rows["asset_ctx"][0]["recv_seq"] == 8


def test_collect_market_batch_rejects_synthetic_recv_ts_fallback_in_network_mode() -> None:
    config = CollectorConfig(
        run_mode="network",
        allowed_symbols=("BTC",),
        source_priority=("ws", "info", "s3", "tardis"),
        market_data_network="mainnet",
        flush_interval_ms=1000,
        snapshot_recovery_enabled=True,
        channels=("trades",),
        candle_interval=None,
    )

    with pytest.raises(
        ValueError,
        match="ingress_metadata is required for run_mode='network'",
    ):
        collect_market_batch(
            messages=[load_fixture("trades.json")],
            config=config,
            capture_session_id="session-network",
            reconnect_epoch=0,
            book_epoch=1,
            recv_ts_start=1713818880100,
        )


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


def test_collector_config_rejects_ws_override_in_fixture_mode() -> None:
    with pytest.raises(ValueError, match="ws_url_override requires run_mode='network'"):
        CollectorConfig(
            run_mode="fixture",
            ws_url_override="wss://example.invalid/ws",
            allowed_symbols=("BTC",),
            source_priority=("ws", "info", "s3", "tardis"),
            market_data_network="mainnet",
            flush_interval_ms=1000,
            snapshot_recovery_enabled=True,
        )


def test_collector_config_rejects_unimplemented_message_rate_guard() -> None:
    with pytest.raises(
        ValueError,
        match="max_messages_per_minute_guard is reserved but not implemented yet",
    ):
        CollectorConfig(
            run_mode="network",
            allowed_symbols=("BTC",),
            source_priority=("ws", "info", "s3", "tardis"),
            market_data_network="mainnet",
            flush_interval_ms=1000,
            snapshot_recovery_enabled=True,
            max_messages_per_minute_guard=120,
        )
