from __future__ import annotations

from typing import Any, Literal

import pybotters
from pydantic import BaseModel, ConfigDict, model_validator

from statestrike.normalize import (
    normalize_active_asset_ctx,
    normalize_l2_book,
    normalize_trades,
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

    allowed_symbols: tuple[str, ...]
    source_priority: tuple[Literal["ws", "info", "s3", "tardis"], ...]
    market_data_network: Literal["mainnet", "testnet"]
    flush_interval_ms: int
    snapshot_recovery_enabled: bool
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
    return requests


def collect_market_batch(
    *,
    messages: list[dict[str, Any]],
    config: CollectorConfig,
    capture_session_id: str,
    reconnect_epoch: int,
    book_epoch: int,
    recv_ts_start: int,
) -> CollectorBatchResult:
    store = pybotters.HyperliquidDataStore()
    normalized_rows: dict[str, list[dict[str, Any]]] = {
        "book_events": [],
        "book_levels": [],
        "trades": [],
        "asset_ctx": [],
    }
    recv_ts = recv_ts_start
    for message in messages:
        store.onmessage(message, None)
        channel = message["channel"]
        if channel not in config.channels:
            recv_ts += 1
            continue
        symbol = _extract_symbol(message)
        if config.allowed_symbols and symbol not in config.allowed_symbols:
            recv_ts += 1
            continue
        if channel == "l2Book":
            book_event, book_levels = normalize_l2_book(
                message=message,
                capture_session_id=capture_session_id,
                reconnect_epoch=reconnect_epoch,
                book_epoch=book_epoch,
                recv_ts=recv_ts,
                source="ws",
            )
            normalized_rows["book_events"].append(book_event)
            normalized_rows["book_levels"].extend(book_levels)
        elif channel == "trades":
            normalized_rows["trades"].extend(
                normalize_trades(
                    message=message,
                    capture_session_id=capture_session_id,
                    recv_ts=recv_ts,
                    source="ws",
                )
            )
        elif channel == "activeAssetCtx":
            normalized_rows["asset_ctx"].append(
                normalize_active_asset_ctx(
                    message=message,
                    capture_session_id=capture_session_id,
                    recv_ts=recv_ts,
                    source="ws",
                )
            )
        recv_ts += 1
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
