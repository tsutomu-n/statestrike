from __future__ import annotations

import json
from pathlib import Path

from statestrike.normalize import normalize_trades
from statestrike.schemas import validate_records


FIXTURES = Path(__file__).parent / "fixtures" / "hyperliquid"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_schema_gate_accepts_valid_trade_rows() -> None:
    rows = normalize_trades(
        message=load_fixture("trades.json"),
        capture_session_id="session-1",
        recv_ts=1713818880101,
        source="ws",
    )

    result = validate_records("trades", rows)

    assert result.valid_count == 2
    assert result.quarantined_count == 0


def test_schema_gate_splits_invalid_trade_rows_to_quarantine() -> None:
    rows = normalize_trades(
        message=load_fixture("trades.json"),
        capture_session_id="session-1",
        recv_ts=1713818880101,
        source="ws",
    )
    invalid = dict(rows[0])
    invalid["price"] = 0.0

    result = validate_records("trades", [rows[1], invalid])

    assert result.valid_count == 1
    assert result.quarantined_count == 1
    assert result.quarantined_rows[0]["price"] == 0.0
    assert result.quarantined_rows[0]["quarantine_reason"] == "price:greater_than(0)"
    assert result.quarantined_rows[0]["quarantine_reason_count"] == 1
