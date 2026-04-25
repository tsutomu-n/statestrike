from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import time
from typing import Any, Awaitable, Callable

import pybotters
from pydantic import BaseModel, ConfigDict, Field

from statestrike.collector import CollectorConfig, build_subscription_requests
from statestrike.reconnect import ReconnectBackoffPolicy
from statestrike.recovery import (
    BookRecoveryTracker,
    MessageCaptureContext,
    MessageIngressMeta,
)

RuntimeSleep = Callable[[float], Awaitable[None]]
RuntimeTransport = Callable[..., Awaitable["RuntimeCapture"]]


class RuntimeCapture(BaseModel):
    model_config = ConfigDict(frozen=True)

    messages: list[dict[str, Any]]
    message_contexts: tuple[MessageCaptureContext, ...] = ()
    ingress_metadata: tuple[MessageIngressMeta, ...] = ()
    recv_ts_start: int
    started_at: str
    ended_at: str
    ws_disconnect_count: int = 0
    reconnect_count: int = 0
    reconnect_epoch: int = 0
    book_epoch: int = Field(default=1, ge=1)
    gap_flags: tuple[str, ...] = ()


async def collect_public_runtime_capture(
    *,
    config: CollectorConfig,
    max_messages: int,
    max_runtime_seconds: int,
    ping_interval_seconds: int,
    reconnect_limit: int,
    transport: RuntimeTransport | None = None,
    sleep: RuntimeSleep = asyncio.sleep,
    monotonic_fn: Callable[[], float] = time.monotonic,
) -> RuntimeCapture:
    transport = transport or collect_public_messages_once
    policy = ReconnectBackoffPolicy(
        initial_ms=config.reconnect_backoff_initial_ms,
        max_ms=config.reconnect_backoff_max_ms,
    )
    started_at = _utc_now_isoformat()
    recv_ts_start = int(time.time_ns() // 1_000_000)
    deadline = monotonic_fn() + max_runtime_seconds
    retry_count = 0

    while True:
        remaining_runtime_seconds = max(1, int(deadline - monotonic_fn()))
        try:
            attempt_capture = await transport(
                config=config,
                max_messages=max_messages,
                max_runtime_seconds=remaining_runtime_seconds,
                ping_interval_seconds=ping_interval_seconds,
                reconnect_limit=reconnect_limit,
            )
            if "reconnect_limit_exceeded" in attempt_capture.gap_flags:
                raise RuntimeError("reconnect limit exceeded during live capture")
            total_reconnects = retry_count + attempt_capture.reconnect_count
            gap_flags = list(attempt_capture.gap_flags)
            if total_reconnects and "ws_reconnect" not in gap_flags:
                gap_flags.append("ws_reconnect")
            book_epoch = (
                max(1, total_reconnects + 1)
                if config.book_snapshot_refresh_on_reconnect and total_reconnects > 0
                else attempt_capture.book_epoch
            )
            return RuntimeCapture(
                messages=attempt_capture.messages,
                message_contexts=attempt_capture.message_contexts,
                ingress_metadata=attempt_capture.ingress_metadata,
                recv_ts_start=recv_ts_start,
                started_at=started_at,
                ended_at=attempt_capture.ended_at,
                ws_disconnect_count=retry_count + attempt_capture.ws_disconnect_count,
                reconnect_count=total_reconnects,
                reconnect_epoch=total_reconnects,
                book_epoch=book_epoch,
                gap_flags=tuple(gap_flags),
            )
        except Exception as exc:
            if retry_count >= reconnect_limit or monotonic_fn() >= deadline:
                raise RuntimeError(
                    f"network collector failed after {retry_count + 1} attempts: {exc}"
                ) from exc
            delay_ms = policy.delay_ms(retry_count=retry_count)
            await sleep(delay_ms / 1000.0)
            retry_count += 1


async def collect_public_messages_once(
    *,
    config: CollectorConfig,
    max_messages: int,
    max_runtime_seconds: int,
    ping_interval_seconds: int,
    reconnect_limit: int,
) -> RuntimeCapture:
    requests = build_subscription_requests(config)
    queue: asyncio.Queue[
        tuple[dict[str, Any], MessageCaptureContext, MessageIngressMeta]
    ] = asyncio.Queue()
    stop_event = asyncio.Event()
    expected_acks = max(1, len(requests))
    subscription_response_count = 0
    reconnect_count = 0
    message_seq = 0
    connection_id = "conn-0"
    recovery_tracker = BookRecoveryTracker(
        symbols=(
            config.allowed_symbols
            if "l2Book" in config.channels and config.book_snapshot_refresh_on_reconnect
            else ()
        )
    )

    def on_message(message: dict[str, Any], _ws: Any) -> None:
        nonlocal reconnect_count, subscription_response_count, message_seq, connection_id
        channel = message.get("channel")
        if channel == "subscriptionResponse":
            subscription_response_count += 1
            next_reconnect_count = max(0, (subscription_response_count // expected_acks) - 1)
            while reconnect_count < next_reconnect_count:
                reconnect_count += 1
                connection_id = f"conn-{reconnect_count}"
                recovery_tracker.mark_reconnect()
            if reconnect_count > reconnect_limit:
                stop_event.set()
            return
        if channel == "pong":
            return
        if channel in config.channels:
            ingress_meta = MessageIngressMeta(
                recv_wall_ns=time.time_ns(),
                recv_mono_ns=time.monotonic_ns(),
                recv_seq=message_seq,
                connection_id=connection_id,
            )
            message_seq += 1
            queue.put_nowait(
                (message, recovery_tracker.classify_message(message), ingress_meta)
            )
            if queue.qsize() >= max_messages:
                stop_event.set()

    started_at = _utc_now_isoformat()
    recv_ts_start = int(time.time_ns() // 1_000_000)
    messages: list[dict[str, Any]] = []
    message_contexts: list[MessageCaptureContext] = []
    ingress_metadata: list[MessageIngressMeta] = []

    async with pybotters.Client() as client:
        app = client.ws_connect(
            _hyperliquid_ws_url(config),
            send_json=requests,
            hdlr_json=on_message,
            heartbeat=float(config.heartbeat_timeout_ms) / 1000.0,
            auth=None,
        )
        deadline = time.monotonic() + max_runtime_seconds
        while (
            len(messages) < max_messages
            and time.monotonic() < deadline
            and not stop_event.is_set()
        ):
            timeout = min(float(ping_interval_seconds), deadline - time.monotonic())
            if timeout <= 0:
                break
            try:
                message, message_context, ingress_meta = await asyncio.wait_for(
                    queue.get(), timeout=timeout
                )
            except asyncio.TimeoutError:
                current_ws = app.current_ws
                if current_ws is not None:
                    try:
                        await current_ws.send_json({"method": "ping"})
                    except Exception:
                        pass
                continue
            messages.append(message)
            message_contexts.append(message_context)
            ingress_metadata.append(ingress_meta)
        if app.current_ws is not None:
            await app.current_ws.close()

    gap_flags: list[str] = []
    if reconnect_count:
        gap_flags.append("ws_reconnect")
    if reconnect_count > reconnect_limit:
        gap_flags.append("reconnect_limit_exceeded")
    gap_flags.extend(recovery_tracker.finalize_gap_flags())
    return RuntimeCapture(
        messages=messages,
        message_contexts=tuple(message_contexts),
        ingress_metadata=tuple(ingress_metadata),
        recv_ts_start=recv_ts_start,
        started_at=started_at,
        ended_at=_utc_now_isoformat(),
        ws_disconnect_count=reconnect_count,
        reconnect_count=reconnect_count,
        reconnect_epoch=reconnect_count,
        book_epoch=max(
            [context.book_epoch for context in message_contexts],
            default=(
                reconnect_count + 1
                if config.book_snapshot_refresh_on_reconnect and reconnect_count > 0
                else 1
            ),
        ),
        gap_flags=tuple(gap_flags),
    )


def _hyperliquid_ws_url(config: CollectorConfig) -> str:
    if config.ws_url_override is not None:
        return config.ws_url_override
    if config.market_data_network == "testnet":
        return "wss://api.hyperliquid-testnet.xyz/ws"
    return "wss://api.hyperliquid.xyz/ws"


def _utc_now_isoformat() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )
