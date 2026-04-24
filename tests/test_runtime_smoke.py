from __future__ import annotations

import asyncio

import pytest

from statestrike.collector import CollectorConfig
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
