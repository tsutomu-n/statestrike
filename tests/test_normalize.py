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
    assert book_event["n_bids"] == 2
    assert book_event["n_asks"] == 2
    assert len(book_levels) == 4
    assert {level["side"] for level in book_levels} == {"bid", "ask"}


def test_normalize_trades_fixture_into_trade_rows() -> None:
    message = load_fixture("trades.json")

    rows = normalize_trades(
        message=message,
        capture_session_id="session-1",
        recv_ts=1713818880101,
        source="ws",
    )

    assert len(rows) == 2
    assert rows[0]["symbol"] == "BTC"
    assert rows[0]["side"] == "buy"
    assert rows[1]["side"] == "sell"


def test_normalize_asset_context_fixture_into_row() -> None:
    message = load_fixture("active_asset_ctx.json")

    row = normalize_active_asset_ctx(
        message=message,
        capture_session_id="session-1",
        recv_ts=1713818880102,
        source="ws",
    )

    assert row["symbol"] == "BTC"
    assert row["mark_px"] == 100.3
    assert round(row["basis"], 6) == 0.003
