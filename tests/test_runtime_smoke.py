from __future__ import annotations

import asyncio

import pytest

from statestrike.collector import CollectorConfig
from statestrike.recovery import MessageCaptureContext, MessageIngressMeta
from statestrike.runtime import RuntimeCapture, collect_public_runtime_capture


def runtime_config() -> CollectorConfig:
    return CollectorConfig(
        run_mode="network",
        allowed_symbols=("BTC",),
        source_priority=("ws", "info", "s3", "tardis"),
        market_data_network="mainnet",
        flush_interval_ms=1000,
        heartbeat_timeout_ms=30_000,
        reconnect_backoff_initial_ms=1_000,
        reconnect_backoff_max_ms=30_000,
        snapshot_recovery_enabled=True,
        channels=("l2Book", "trades", "activeAssetCtx"),
    )


def test_collect_public_runtime_capture_retries_failed_attempt_and_tracks_epoch() -> None:
    calls = 0
    sleep_delays: list[float] = []

    async def fake_transport(
        *,
        config: CollectorConfig,
        max_messages: int,
        max_runtime_seconds: int,
        ping_interval_seconds: int,
        reconnect_limit: int,
    ) -> RuntimeCapture:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("synthetic disconnect")
        return RuntimeCapture(
            messages=[{"channel": "trades", "data": {"coin": "BTC"}}],
            recv_ts_start=1713818880100,
            started_at="2026-04-24T00:00:00Z",
            ended_at="2026-04-24T00:00:01Z",
        )

    async def fake_sleep(delay: float) -> None:
        sleep_delays.append(delay)

    result = asyncio.run(
        collect_public_runtime_capture(
            config=runtime_config(),
            max_messages=1,
            max_runtime_seconds=30,
            ping_interval_seconds=5,
            reconnect_limit=2,
            transport=fake_transport,
            sleep=fake_sleep,
            monotonic_fn=lambda: 0.0,
        )
    )

    assert calls == 2
    assert len(sleep_delays) == 1
    assert 0.9 <= sleep_delays[0] <= 1.1
    assert result.reconnect_count == 1
    assert result.reconnect_epoch == 1
    assert result.ws_disconnect_count == 1
    assert result.messages == [{"channel": "trades", "data": {"coin": "BTC"}}]


def test_collect_public_runtime_capture_raises_after_reconnect_limit() -> None:
    calls = 0
    sleep_delays: list[float] = []

    async def failing_transport(
        *,
        config: CollectorConfig,
        max_messages: int,
        max_runtime_seconds: int,
        ping_interval_seconds: int,
        reconnect_limit: int,
    ) -> RuntimeCapture:
        nonlocal calls
        calls += 1
        raise RuntimeError("synthetic disconnect")

    async def fake_sleep(delay: float) -> None:
        sleep_delays.append(delay)

    with pytest.raises(RuntimeError, match="network collector failed after 2 attempts"):
        asyncio.run(
            collect_public_runtime_capture(
                config=runtime_config(),
                max_messages=1,
                max_runtime_seconds=30,
                ping_interval_seconds=5,
                reconnect_limit=1,
                transport=failing_transport,
                sleep=fake_sleep,
                monotonic_fn=lambda: 0.0,
            )
        )

    assert calls == 2
    assert len(sleep_delays) == 1


def test_collect_public_runtime_capture_preserves_recovery_message_contexts() -> None:
    async def fake_transport(
        *,
        config: CollectorConfig,
        max_messages: int,
        max_runtime_seconds: int,
        ping_interval_seconds: int,
        reconnect_limit: int,
    ) -> RuntimeCapture:
        return RuntimeCapture(
            messages=[{"channel": "l2Book", "data": {"coin": "BTC"}}],
            message_contexts=[
                MessageCaptureContext(
                    reconnect_epoch=1,
                    book_epoch=2,
                    book_event_kind="recovery_snapshot",
                    continuity_status="recovered",
                    recovery_classification="recoverable",
                    recovery_succeeded=True,
                )
            ],
            recv_ts_start=1713818880100,
            started_at="2026-04-24T00:00:00Z",
            ended_at="2026-04-24T00:00:01Z",
            reconnect_count=1,
            reconnect_epoch=1,
            book_epoch=2,
            gap_flags=("ws_reconnect",),
        )

    result = asyncio.run(
        collect_public_runtime_capture(
            config=runtime_config(),
            max_messages=1,
            max_runtime_seconds=30,
            ping_interval_seconds=5,
            reconnect_limit=2,
            transport=fake_transport,
            monotonic_fn=lambda: 0.0,
        )
    )

    assert result.book_epoch == 2
    assert result.message_contexts[0].book_event_kind == "recovery_snapshot"
    assert result.message_contexts[0].continuity_status == "recovered"


def test_collect_public_runtime_capture_preserves_ingress_metadata() -> None:
    async def fake_transport(
        *,
        config: CollectorConfig,
        max_messages: int,
        max_runtime_seconds: int,
        ping_interval_seconds: int,
        reconnect_limit: int,
    ) -> RuntimeCapture:
        return RuntimeCapture(
            messages=[{"channel": "trades", "data": {"coin": "BTC"}}],
            ingress_metadata=[
                MessageIngressMeta(
                    recv_wall_ns=1713818880100000000,
                    recv_mono_ns=123,
                    recv_seq=1,
                    connection_id="conn-1",
                )
            ],
            recv_ts_start=1713818880100,
            started_at="2026-04-24T00:00:00Z",
            ended_at="2026-04-24T00:00:01Z",
        )

    result = asyncio.run(
        collect_public_runtime_capture(
            config=runtime_config(),
            max_messages=1,
            max_runtime_seconds=30,
            ping_interval_seconds=5,
            reconnect_limit=2,
            transport=fake_transport,
            monotonic_fn=lambda: 0.0,
        )
    )

    assert len(result.ingress_metadata) == 1
    assert result.ingress_metadata[0].recv_seq == 1
    assert result.ingress_metadata[0].connection_id == "conn-1"
