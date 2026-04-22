from __future__ import annotations

from typing import Any, Literal

import pybotters
from pydantic import BaseModel, ConfigDict

from statestrike.normalize import (
    normalize_active_asset_ctx,
    normalize_l2_book,
    normalize_trades,
)


class CollectorConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    allowed_symbols: tuple[str, ...]
    source_priority: tuple[Literal["ws", "info", "s3", "tardis"], ...]
    market_data_network: Literal["mainnet", "testnet"]
    flush_interval_ms: int
    snapshot_recovery_enabled: bool


class CollectorBatchResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    raw_count: int
    normalized_counts: dict[str, int]
    normalized_rows: dict[str, list[dict[str, Any]]]


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
    if isinstance(data, list) and data:
        return str(data[0]["coin"]).upper()
    return ""
