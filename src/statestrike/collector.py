from __future__ import annotations

from typing import Any, Literal

import pybotters
from pydantic import BaseModel, ConfigDict, model_validator

from statestrike.normalize import (
    normalize_active_asset_ctx,
    normalize_l2_book,
    normalize_trades,
)
from statestrike.recovery import (
    MessageCaptureContext,
    MessageIngressMeta,
    resolve_ingress_metadata,
    resolve_message_contexts,
)

CollectorChannel = Literal["l2Book", "trades", "activeAssetCtx", "candle"]
CandleInterval = Literal[
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "8h",
    "12h",
    "1d",
    "3d",
    "1w",
    "1M",
]


class CollectorConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    run_mode: Literal["fixture", "network"] = "fixture"
    ws_url_override: str | None = None
    allowed_symbols: tuple[str, ...]
    source_priority: tuple[Literal["ws", "info", "s3", "tardis"], ...]
    market_data_network: Literal["mainnet", "testnet"]
    flush_interval_ms: int
    heartbeat_timeout_ms: int = 30_000
    reconnect_backoff_initial_ms: int = 1_000
    reconnect_backoff_max_ms: int = 30_000
    snapshot_recovery_enabled: bool
    book_snapshot_refresh_on_reconnect: bool = True
    max_subscriptions_per_connection: int = 64
    max_messages_per_minute_guard: int | None = None
    channels: tuple[CollectorChannel, ...] = (
        "l2Book",
        "trades",
        "activeAssetCtx",
    )
    candle_interval: CandleInterval | None = None

    @model_validator(mode="after")
    def validate_channel_options(self) -> "CollectorConfig":
        if self.candle_interval and "candle" not in self.channels:
            raise ValueError("candle_interval requires the candle channel")
        if "candle" in self.channels and self.candle_interval is None:
            raise ValueError("candle channel requires candle_interval")
        if self.heartbeat_timeout_ms <= 0:
            raise ValueError("heartbeat_timeout_ms must be positive")
        if self.reconnect_backoff_initial_ms <= 0:
            raise ValueError("reconnect_backoff_initial_ms must be positive")
        if self.reconnect_backoff_max_ms < self.reconnect_backoff_initial_ms:
            raise ValueError(
                "reconnect_backoff_max_ms must be greater than or equal to reconnect_backoff_initial_ms"
            )
        if self.max_subscriptions_per_connection <= 0:
            raise ValueError("max_subscriptions_per_connection must be positive")
        if (
            self.max_messages_per_minute_guard is not None
            and self.max_messages_per_minute_guard <= 0
        ):
            raise ValueError("max_messages_per_minute_guard must be positive")
        return self


class CollectorBatchResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    raw_count: int
    normalized_counts: dict[str, int]
    normalized_rows: dict[str, list[dict[str, Any]]]


def build_subscription_requests(config: CollectorConfig) -> list[dict[str, Any]]:
    requests: list[dict[str, Any]] = []
    for symbol in config.allowed_symbols:
        for channel in config.channels:
            subscription: dict[str, Any] = {
                "type": channel,
                "coin": symbol,
            }
            if channel == "candle":
                subscription["interval"] = config.candle_interval
            requests.append(
                {
                    "method": "subscribe",
                    "subscription": subscription,
                }
            )
    if len(requests) > config.max_subscriptions_per_connection:
        raise ValueError("subscription count exceeds max_subscriptions_per_connection")
    return requests


def collect_market_batch(
    *,
    messages: list[dict[str, Any]],
    message_contexts: list[MessageCaptureContext] | tuple[MessageCaptureContext, ...] | None = None,
    ingress_metadata: list[MessageIngressMeta] | tuple[MessageIngressMeta, ...] | None = None,
    config: CollectorConfig,
    capture_session_id: str,
    reconnect_epoch: int,
    book_epoch: int,
    recv_ts_start: int,
) -> CollectorBatchResult:
    store = pybotters.HyperliquidDataStore()
    resolved_contexts = resolve_message_contexts(
        messages=messages,
        message_contexts=message_contexts,
        reconnect_epoch=reconnect_epoch,
        book_epoch=book_epoch,
    )
    resolved_ingress = resolve_ingress_metadata(
        messages=messages,
        ingress_metadata=ingress_metadata,
        recv_ts_start=recv_ts_start,
    )
    normalized_rows: dict[str, list[dict[str, Any]]] = {
        "book_events": [],
        "book_levels": [],
        "trades": [],
        "asset_ctx": [],
    }
    for message, message_context, ingress_meta in zip(
        messages, resolved_contexts, resolved_ingress, strict=True
    ):
        store.onmessage(message, None)
        channel = message["channel"]
        recv_ts = ingress_meta.recv_wall_ns // 1_000_000
        if channel not in config.channels:
            continue
        symbol = _extract_symbol(message)
        if config.allowed_symbols and symbol not in config.allowed_symbols:
            continue
        if channel == "l2Book":
            book_event, book_levels = normalize_l2_book(
                message=message,
                capture_session_id=capture_session_id,
                reconnect_epoch=message_context.reconnect_epoch,
                book_epoch=message_context.book_epoch,
                recv_ts=recv_ts,
                source="ws",
                event_kind=message_context.book_event_kind,
                continuity_status=message_context.continuity_status,
                recovery_classification=message_context.recovery_classification,
                recovery_succeeded=message_context.recovery_succeeded,
            )
            normalized_rows["book_events"].append(book_event)
            normalized_rows["book_levels"].extend(book_levels)
        elif channel == "trades":
            normalized_rows["trades"].extend(
                normalize_trades(
                    message=message,
                    capture_session_id=capture_session_id,
                    reconnect_epoch=message_context.reconnect_epoch,
                    recv_ts=recv_ts,
                    source="ws",
                )
            )
        elif channel == "activeAssetCtx":
            normalized_rows["asset_ctx"].append(
                normalize_active_asset_ctx(
                    message=message,
                    capture_session_id=capture_session_id,
                    reconnect_epoch=message_context.reconnect_epoch,
                    recv_ts=recv_ts,
                    source="ws",
                )
            )
    return CollectorBatchResult(
        raw_count=len(messages),
        normalized_counts={
            table: len(rows)
            for table, rows in normalized_rows.items()
        },
        normalized_rows=normalized_rows,
    )


def _extract_symbol(message: dict[str, Any]) -> str:
    data = message.get("data", {})
    if isinstance(data, dict) and "coin" in data:
        return str(data["coin"]).upper()
    if isinstance(data, dict) and "s" in data:
        return str(data["s"]).upper()
    if isinstance(data, list) and data:
        return str(data[0]["coin"]).upper()
    return ""
