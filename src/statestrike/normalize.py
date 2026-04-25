from __future__ import annotations

from typing import Any

from statestrike.identifiers import canonical_hash
from statestrike.models import AssetContextEvent


def normalize_l2_book(
    *,
    message: dict[str, Any],
    capture_session_id: str,
    reconnect_epoch: int,
    book_epoch: int,
    recv_ts: int,
    source: str,
    event_kind: str | None = None,
    continuity_status: str = "continuous",
    recovery_classification: str | None = None,
    recovery_succeeded: bool | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    raw_msg_hash = canonical_hash(message)
    data = message["data"]
    coin = data["coin"].upper()
    levels = data["levels"]
    book_event_id = canonical_hash(
        {
            "channel": "l2Book",
            "capture_session_id": capture_session_id,
            "book_epoch": book_epoch,
            "raw_msg_hash": raw_msg_hash,
        }
    )
    book_event = {
        "book_event_id": book_event_id,
        "capture_session_id": capture_session_id,
        "reconnect_epoch": reconnect_epoch,
        "book_epoch": book_epoch,
        "symbol": coin,
        "exchange_ts": int(data["time"]),
        "recv_ts": recv_ts,
        "event_kind": event_kind
        or ("recovery_snapshot" if reconnect_epoch > 0 or book_epoch > 1 else "snapshot"),
        "continuity_status": continuity_status,
        "recovery_classification": recovery_classification,
        "recovery_succeeded": recovery_succeeded,
        "source": source,
        "raw_msg_hash": raw_msg_hash,
        "dedup_hash": book_event_id,
        "n_bids": len(levels[0]),
        "n_asks": len(levels[1]),
    }
    book_levels: list[dict[str, Any]] = []
    for side_name, side_levels in (("bid", levels[0]), ("ask", levels[1])):
        for level_idx, level in enumerate(side_levels):
            level_dedup_hash = canonical_hash(
                {
                    "book_event_id": book_event_id,
                    "side": side_name,
                    "level_idx": level_idx,
                    "price": str(level["px"]),
                    "size": str(level["sz"]),
                }
            )
            book_levels.append(
                {
                    "book_event_id": book_event_id,
                    "capture_session_id": capture_session_id,
                    "reconnect_epoch": reconnect_epoch,
                    "book_epoch": book_epoch,
                    "symbol": coin,
                    "exchange_ts": int(data["time"]),
                    "recv_ts": recv_ts,
                    "source": source,
                    "raw_msg_hash": raw_msg_hash,
                    "dedup_hash": level_dedup_hash,
                    "side": side_name,
                    "level_idx": level_idx,
                    "price": float(level["px"]),
                    "size": float(level["sz"]),
                }
            )
    return book_event, book_levels


def normalize_trades(
    *,
    message: dict[str, Any],
    capture_session_id: str,
    reconnect_epoch: int = 0,
    recv_ts: int,
    source: str,
) -> list[dict[str, Any]]:
    raw_msg_hash = canonical_hash(message)
    rows: list[dict[str, Any]] = []
    for trade in message["data"]:
        symbol = str(trade["coin"]).upper()
        native_trade_id = trade.get("tid")
        native_tid = str(native_trade_id) if native_trade_id is not None else None
        dedup_hash = canonical_hash(
            {
                "symbol": symbol,
                "exchange_ts": int(trade["time"]),
                "price": str(trade["px"]),
                "size": str(trade["sz"]),
                "side": trade["side"],
                "tid": native_trade_id,
            }
        )
        trade_event_id = canonical_hash(
            {
                "symbol": symbol,
                "exchange_ts": int(trade["time"]),
                "native_tid": native_tid,
                "price": str(trade["px"]),
                "size": str(trade["sz"]),
                "side": trade["side"],
            }
        )
        rows.append(
            {
                "trade_event_id": trade_event_id,
                "native_tid": native_tid,
                "symbol": symbol,
                "exchange_ts": int(trade["time"]),
                "recv_ts": recv_ts,
                "price": float(trade["px"]),
                "size": float(trade["sz"]),
                "side": "buy" if trade["side"] == "B" else "sell",
                "capture_session_id": capture_session_id,
                "reconnect_epoch": reconnect_epoch,
                "source": source,
                "raw_msg_hash": raw_msg_hash,
                "dedup_hash": dedup_hash,
            }
        )
    return rows


def normalize_active_asset_ctx(
    *,
    message: dict[str, Any],
    capture_session_id: str,
    reconnect_epoch: int = 0,
    recv_ts: int,
    source: str,
) -> dict[str, Any]:
    raw_msg_hash = canonical_hash(message)
    data = message["data"]
    ctx = data["ctx"]
    exchange_ts = ctx.get("time")
    exchange_ts_quality = "exact" if exchange_ts is not None else "missing"
    base = AssetContextEvent(
        asset_ctx_event_id=canonical_hash(
            {
                "channel": "activeAssetCtx",
                "capture_session_id": capture_session_id,
                "raw_msg_hash": raw_msg_hash,
            }
        ),
        symbol=data["coin"],
        exchange_ts=int(exchange_ts) if exchange_ts is not None else None,
        exchange_ts_quality=exchange_ts_quality,
        recv_ts=recv_ts,
        mark_px=float(ctx["markPx"]),
        oracle_px=float(ctx["oraclePx"]),
        funding_rate=float(ctx["funding"]),
        open_interest=float(ctx["openInterest"]),
        mid_px=float(ctx.get("midPx", ctx["markPx"])),
    ).model_dump()
    base.update(
        {
            "capture_session_id": capture_session_id,
            "reconnect_epoch": reconnect_epoch,
            "source": source,
            "raw_msg_hash": raw_msg_hash,
            "dedup_hash": canonical_hash(
                {
                    "symbol": base["symbol"],
                    "exchange_ts": base["exchange_ts"],
                    "exchange_ts_quality": base["exchange_ts_quality"],
                    "mark_px": base["mark_px"],
                    "oracle_px": base["oracle_px"],
                    "funding_rate": base["funding_rate"],
                    "open_interest": base["open_interest"],
                    "mid_px": base["mid_px"],
                }
            ),
        }
    )
    return base
