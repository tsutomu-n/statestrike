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
        reconnect_epoch=0,
        recv_ts=1713818880101,
        recv_ts_ns=1713818880101000000,
        recv_seq=1,
        source="ws",
    )

    result = validate_records("trades", rows)

    assert result.valid_count == 2
    assert result.quarantined_count == 0


def test_schema_gate_splits_invalid_trade_rows_to_quarantine() -> None:
    rows = normalize_trades(
        message=load_fixture("trades.json"),
        capture_session_id="session-1",
        reconnect_epoch=0,
        recv_ts=1713818880101,
        recv_ts_ns=1713818880101000000,
        recv_seq=1,
        source="ws",
    )
    invalid = dict(rows[0])
    invalid["price"] = 0.0

    result = validate_records("trades", [rows[1], invalid])

    assert result.valid_count == 1
    assert result.quarantined_count == 1
    assert result.quarantined_rows[0]["price"] == 0.0
    assert result.quarantined_rows[0]["quarantine_category"] == "schema"
    assert result.quarantined_rows[0]["quarantine_reason"] == "price:greater_than(0)"
    assert result.quarantined_rows[0]["quarantine_reason_count"] == 1


def test_schema_gate_vectorizes_and_splits_multiple_invalid_rows_by_index() -> None:
    rows = normalize_trades(
        message=load_fixture("trades.json"),
        capture_session_id="session-1",
        reconnect_epoch=0,
        recv_ts=1713818880101,
        recv_ts_ns=1713818880101000000,
        recv_seq=1,
        source="ws",
    )
    invalid_price_and_side = dict(rows[0])
    invalid_price_and_side["price"] = 0.0
    invalid_price_and_side["side"] = "hold"
    invalid_size = dict(rows[1])
    invalid_size["size"] = 0.0

    result = validate_records(
        "trades",
        [rows[0], invalid_price_and_side, invalid_size],
    )

    assert result.valid_count == 1
    assert result.quarantined_count == 2
    assert result.quarantined_rows[0]["quarantine_reason_count"] == 2
    assert result.quarantined_rows[0]["quarantine_reason"] == (
        "price:greater_than(0); side:isin(['buy', 'sell'])"
    )
    assert result.quarantined_rows[1]["quarantine_reason"] == "size:greater_than(0)"


def test_schema_gate_accepts_asset_context_rows_with_missing_exchange_timestamp() -> None:
    row = {
        "asset_ctx_event_id": "ctx-1",
        "symbol": "BTC",
        "exchange_ts": None,
        "exchange_ts_quality": "missing",
        "recv_ts": 1713818880102,
        "recv_ts_ns": 1713818880102000000,
        "recv_seq": 2,
        "mark_px": 100.3,
        "oracle_px": 100.0,
        "funding_rate": 0.0001,
        "open_interest": 1234.5,
        "mid_px": 100.2,
        "basis": 0.003,
        "next_funding_ts": None,
        "capture_session_id": "session-1",
        "reconnect_epoch": 0,
        "source": "ws",
        "raw_msg_hash": "raw-ctx",
        "dedup_hash": "dedup-ctx",
    }

    result = validate_records("asset_ctx", [row])

    assert result.valid_count == 1
    assert result.quarantined_count == 0
