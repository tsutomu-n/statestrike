from __future__ import annotations

import copy
import json
from pathlib import Path

from statestrike.recovery import BookRecoveryTracker, resolve_ingress_metadata


FIXTURES = Path(__file__).parent / "fixtures" / "hyperliquid"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_book_recovery_tracker_marks_recovery_snapshot_and_missing_symbols() -> None:
    tracker = BookRecoveryTracker(symbols=("BTC", "ETH"))

    initial_context = tracker.classify_message(load_fixture("l2_book.json"))
    assert initial_context.reconnect_epoch == 0
    assert initial_context.book_epoch == 1
    assert initial_context.book_event_kind == "snapshot"
    assert initial_context.continuity_status == "continuous"

    tracker.mark_reconnect()

    trade_context = tracker.classify_message(load_fixture("trades.json"))
    assert trade_context.reconnect_epoch == 1
    assert trade_context.book_epoch == 1
    assert trade_context.continuity_status == "recovery_pending"

    recovered_message = copy.deepcopy(load_fixture("l2_book.json"))
    recovered_message["data"]["time"] = recovered_message["data"]["time"] + 100
    recovered_context = tracker.classify_message(recovered_message)
    assert recovered_context.reconnect_epoch == 1
    assert recovered_context.book_epoch == 2
    assert recovered_context.book_event_kind == "recovery_snapshot"
    assert recovered_context.continuity_status == "recovered"
    assert recovered_context.recovery_classification == "recoverable"
    assert recovered_context.recovery_succeeded is True

    assert tracker.finalize_gap_flags() == ("l2_book_non_recoverable:ETH",)


def test_ingress_metadata_docstring_freezes_fixture_only_fallback_contract() -> None:
    docstring = resolve_ingress_metadata.__doc__ or ""

    assert "fixture-only" in docstring
    assert "recv_ts_start" in docstring
    assert "network" in docstring
    assert "ingress metadata" in docstring
