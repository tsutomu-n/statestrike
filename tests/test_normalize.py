from __future__ import annotations

import json
from pathlib import Path

from statestrike.normalize import (
    normalize_active_asset_ctx,
    normalize_l2_book,
    normalize_trades,
)


FIXTURES = Path(__file__).parent / "fixtures" / "hyperliquid"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_normalize_l2_book_fixture_into_event_and_levels() -> None:
    message = load_fixture("l2_book.json")

    book_event, book_levels = normalize_l2_book(
        message=message,
        capture_session_id="session-1",
        reconnect_epoch=0,
        book_epoch=1,
        recv_ts=1713818880100,
        source="ws",
    )

    assert book_event["symbol"] == "BTC"
    assert book_event["event_kind"] == "snapshot"
    assert book_event["continuity_status"] == "continuous"
    assert book_event["recovery_classification"] is None
    assert book_event["recovery_succeeded"] is None
    assert book_event["n_bids"] == 2
    assert book_event["n_asks"] == 2
    assert len(book_levels) == 4
    assert {level["side"] for level in book_levels} == {"bid", "ask"}


def test_normalize_l2_book_marks_recovery_snapshot_after_reconnect() -> None:
    message = load_fixture("l2_book.json")

    book_event, book_levels = normalize_l2_book(
        message=message,
        capture_session_id="session-1",
        reconnect_epoch=2,
        book_epoch=3,
        recv_ts=1713818880100,
        source="ws",
        event_kind="recovery_snapshot",
        continuity_status="recovered",
        recovery_classification="recoverable",
        recovery_succeeded=True,
    )

    assert book_event["event_kind"] == "recovery_snapshot"
    assert book_event["continuity_status"] == "recovered"
    assert book_event["recovery_classification"] == "recoverable"
    assert book_event["recovery_succeeded"] is True
    assert book_event["dedup_hash"] == book_event["book_event_id"]
    assert book_levels[0]["capture_session_id"] == "session-1"
    assert book_levels[0]["reconnect_epoch"] == 2
    assert book_levels[0]["book_epoch"] == 3
    assert book_levels[0]["symbol"] == "BTC"
    assert book_levels[0]["dedup_hash"]


def test_normalize_trades_fixture_into_trade_rows() -> None:
    message = load_fixture("trades.json")

    rows = normalize_trades(
        message=message,
        capture_session_id="session-1",
        reconnect_epoch=1,
        recv_ts=1713818880101,
        source="ws",
    )

    assert len(rows) == 2
    assert rows[0]["symbol"] == "BTC"
    assert rows[0]["side"] == "buy"
    assert rows[0]["reconnect_epoch"] == 1
    assert rows[0]["native_tid"] == "1"
    assert rows[0]["trade_event_id"] != rows[0]["native_tid"]
    assert rows[1]["side"] == "sell"


def test_normalize_asset_context_fixture_into_row() -> None:
    message = load_fixture("active_asset_ctx.json")

    row = normalize_active_asset_ctx(
        message=message,
        capture_session_id="session-1",
        reconnect_epoch=4,
        recv_ts=1713818880102,
        source="ws",
    )

    assert row["symbol"] == "BTC"
    assert row["mark_px"] == 100.3
    assert row["reconnect_epoch"] == 4
    assert round(row["basis"], 6) == 0.003
    assert row["exchange_ts"] is None
    assert row["exchange_ts_quality"] == "missing"
    assert row["next_funding_ts"] is None
