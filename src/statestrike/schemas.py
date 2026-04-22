from __future__ import annotations

from typing import Any

import pandas as pd
import pandera.pandas as pa
from pydantic import BaseModel, ConfigDict


BOOK_EVENTS_SCHEMA = pa.DataFrameSchema(
    {
        "book_event_id": pa.Column(str),
        "capture_session_id": pa.Column(str),
        "reconnect_epoch": pa.Column(int, pa.Check.ge(0)),
        "book_epoch": pa.Column(int, pa.Check.ge(0)),
        "symbol": pa.Column(str),
        "exchange_ts": pa.Column(int, pa.Check.ge(0)),
        "recv_ts": pa.Column(int, pa.Check.ge(0)),
        "event_kind": pa.Column(str, pa.Check.isin(["snapshot", "delta", "recovery_snapshot"])),
        "source": pa.Column(str, pa.Check.isin(["ws", "info", "s3", "tardis"])),
        "raw_msg_hash": pa.Column(str),
        "n_bids": pa.Column(int, pa.Check.ge(0)),
        "n_asks": pa.Column(int, pa.Check.ge(0)),
    },
    strict=True,
    coerce=True,
)

BOOK_LEVELS_SCHEMA = pa.DataFrameSchema(
    {
        "book_event_id": pa.Column(str),
        "side": pa.Column(str, pa.Check.isin(["bid", "ask"])),
        "level_idx": pa.Column(int, pa.Check.ge(0)),
        "price": pa.Column(float, pa.Check.gt(0)),
        "size": pa.Column(float, pa.Check.gt(0)),
    },
    strict=True,
    coerce=True,
)

TRADES_SCHEMA = pa.DataFrameSchema(
    {
        "trade_event_id": pa.Column(object),
        "symbol": pa.Column(str),
        "exchange_ts": pa.Column(int, pa.Check.ge(0)),
        "recv_ts": pa.Column(int, pa.Check.ge(0)),
        "price": pa.Column(float, pa.Check.gt(0)),
        "size": pa.Column(float, pa.Check.gt(0)),
        "side": pa.Column(str, pa.Check.isin(["buy", "sell"])),
        "capture_session_id": pa.Column(str),
        "source": pa.Column(str, pa.Check.isin(["ws", "info", "s3", "tardis"])),
        "raw_msg_hash": pa.Column(str),
        "dedup_hash": pa.Column(str),
    },
    strict=True,
    coerce=True,
)

ASSET_CTX_SCHEMA = pa.DataFrameSchema(
    {
        "asset_ctx_event_id": pa.Column(str),
        "symbol": pa.Column(str),
        "exchange_ts": pa.Column(int, pa.Check.ge(0)),
        "recv_ts": pa.Column(int, pa.Check.ge(0)),
        "mark_px": pa.Column(float, pa.Check.gt(0)),
        "oracle_px": pa.Column(float, pa.Check.gt(0)),
        "funding_rate": pa.Column(float),
        "open_interest": pa.Column(float, pa.Check.ge(0)),
        "mid_px": pa.Column(float, pa.Check.gt(0)),
        "basis": pa.Column(float),
        "next_funding_ts": pa.Column(int, pa.Check.ge(0)),
        "capture_session_id": pa.Column(str),
        "source": pa.Column(str, pa.Check.isin(["ws", "info", "s3", "tardis"])),
        "raw_msg_hash": pa.Column(str),
        "dedup_hash": pa.Column(str),
    },
    strict=True,
    coerce=True,
)


SCHEMAS = {
    "book_events": BOOK_EVENTS_SCHEMA,
    "book_levels": BOOK_LEVELS_SCHEMA,
    "trades": TRADES_SCHEMA,
    "asset_ctx": ASSET_CTX_SCHEMA,
}


class ValidationResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    valid_rows: list[dict[str, Any]]
    quarantined_rows: list[dict[str, Any]]

    @property
    def valid_count(self) -> int:
        return len(self.valid_rows)

    @property
    def quarantined_count(self) -> int:
        return len(self.quarantined_rows)


def validate_records(table: str, records: list[dict[str, Any]]) -> ValidationResult:
    if not records:
        return ValidationResult(valid_rows=[], quarantined_rows=[])

    schema = SCHEMAS[table]
    valid_rows: list[dict[str, Any]] = []
    quarantined_rows: list[dict[str, Any]] = []
    for record in records:
        frame = pd.DataFrame([record])
        try:
            validated = schema.validate(frame, lazy=True)
        except Exception:
            quarantined_rows.append(record)
            continue
        valid_rows.extend(validated.to_dict(orient="records"))
    return ValidationResult(valid_rows=valid_rows, quarantined_rows=quarantined_rows)
