from __future__ import annotations

from typing import Any

import pandas as pd
import pandera
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
        "recv_ts_ns": pa.Column(int, pa.Check.ge(0)),
        "recv_seq": pa.Column(int, pa.Check.ge(0)),
        "event_kind": pa.Column(str, pa.Check.isin(["snapshot", "delta", "recovery_snapshot"])),
        "continuity_status": pa.Column(
            str,
            pa.Check.isin(
                ["continuous", "recovery_pending", "recovered", "non_recoverable"]
            ),
        ),
        "recovery_classification": pa.Column(
            object,
            pa.Check.isin(["recoverable", "non_recoverable"]),
            nullable=True,
        ),
        "recovery_succeeded": pa.Column(object, nullable=True),
        "source": pa.Column(str, pa.Check.isin(["ws", "info", "s3", "tardis"])),
        "raw_msg_hash": pa.Column(str),
        "dedup_hash": pa.Column(str),
        "n_bids": pa.Column(int, pa.Check.ge(0)),
        "n_asks": pa.Column(int, pa.Check.ge(0)),
    },
    strict=True,
    coerce=True,
    ordered=True,
)

BOOK_LEVELS_SCHEMA = pa.DataFrameSchema(
    {
        "book_event_id": pa.Column(str),
        "capture_session_id": pa.Column(str),
        "reconnect_epoch": pa.Column(int, pa.Check.ge(0)),
        "book_epoch": pa.Column(int, pa.Check.ge(0)),
        "symbol": pa.Column(str),
        "exchange_ts": pa.Column(int, pa.Check.ge(0)),
        "recv_ts": pa.Column(int, pa.Check.ge(0)),
        "recv_ts_ns": pa.Column(int, pa.Check.ge(0)),
        "recv_seq": pa.Column(int, pa.Check.ge(0)),
        "source": pa.Column(str, pa.Check.isin(["ws", "info", "s3", "tardis"])),
        "raw_msg_hash": pa.Column(str),
        "dedup_hash": pa.Column(str),
        "side": pa.Column(str, pa.Check.isin(["bid", "ask"])),
        "level_idx": pa.Column(int, pa.Check.ge(0)),
        "price": pa.Column(float, pa.Check.gt(0)),
        "size": pa.Column(float, pa.Check.gt(0)),
    },
    strict=True,
    coerce=True,
    ordered=True,
)

TRADES_SCHEMA = pa.DataFrameSchema(
    {
        "trade_event_id": pa.Column(object),
        "native_tid": pa.Column(object, nullable=True),
        "symbol": pa.Column(str),
        "exchange_ts": pa.Column(int, pa.Check.ge(0)),
        "recv_ts": pa.Column(int, pa.Check.ge(0)),
        "recv_ts_ns": pa.Column(int, pa.Check.ge(0)),
        "recv_seq": pa.Column(int, pa.Check.ge(0)),
        "price": pa.Column(float, pa.Check.gt(0)),
        "size": pa.Column(float, pa.Check.gt(0)),
        "side": pa.Column(str, pa.Check.isin(["buy", "sell"])),
        "capture_session_id": pa.Column(str),
        "reconnect_epoch": pa.Column(int, pa.Check.ge(0)),
        "source": pa.Column(str, pa.Check.isin(["ws", "info", "s3", "tardis"])),
        "raw_msg_hash": pa.Column(str),
        "dedup_hash": pa.Column(str),
    },
    strict=True,
    coerce=True,
    ordered=True,
)

ASSET_CTX_SCHEMA = pa.DataFrameSchema(
    {
        "asset_ctx_event_id": pa.Column(str),
        "symbol": pa.Column(str),
        "exchange_ts": pa.Column(object, nullable=True),
        "exchange_ts_quality": pa.Column(
            str,
            pa.Check.isin(["exact", "missing", "derived"]),
        ),
        "recv_ts": pa.Column(int, pa.Check.ge(0)),
        "recv_ts_ns": pa.Column(int, pa.Check.ge(0)),
        "recv_seq": pa.Column(int, pa.Check.ge(0)),
        "mark_px": pa.Column(float, pa.Check.gt(0)),
        "oracle_px": pa.Column(float, pa.Check.gt(0)),
        "funding_rate": pa.Column(float),
        "open_interest": pa.Column(float, pa.Check.ge(0)),
        "mid_px": pa.Column(float, pa.Check.gt(0)),
        "basis": pa.Column(float),
        "next_funding_ts": pa.Column(object, nullable=True),
        "capture_session_id": pa.Column(str),
        "reconnect_epoch": pa.Column(int, pa.Check.ge(0)),
        "source": pa.Column(str, pa.Check.isin(["ws", "info", "s3", "tardis"])),
        "raw_msg_hash": pa.Column(str),
        "dedup_hash": pa.Column(str),
    },
    strict=True,
    coerce=True,
    ordered=True,
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
    frame = pd.DataFrame(records)
    try:
        validated = schema.validate(frame, lazy=True)
    except pandera.errors.SchemaErrors as exc:
        failing_indexes = _extract_failing_indexes(exc)
        if not failing_indexes:
            return ValidationResult(
                valid_rows=[],
                quarantined_rows=[
                    {
                        **record,
                        "quarantine_category": "schema",
                        "quarantine_reason": _extract_quarantine_reason(exc),
                        "quarantine_reason_count": _count_failure_reasons(exc),
                    }
                    for record in records
                ],
            )

        quarantined_rows = [
            {
                **records[index],
                "quarantine_category": "schema",
                "quarantine_reason": _extract_quarantine_reason_for_index(exc, index=index),
                "quarantine_reason_count": _count_failure_reasons_for_index(
                    exc, index=index
                ),
            }
            for index in sorted(failing_indexes)
        ]
        valid_indexes = [index for index in range(len(records)) if index not in failing_indexes]
        valid_rows = _validate_subset_rows(
            schema=schema,
            frame=frame,
            indexes=valid_indexes,
        )
        return ValidationResult(valid_rows=valid_rows, quarantined_rows=quarantined_rows)
    except Exception as exc:
        return ValidationResult(
            valid_rows=[],
            quarantined_rows=[
                {
                    **record,
                    "quarantine_category": "runtime",
                    "quarantine_reason": type(exc).__name__,
                    "quarantine_reason_count": 1,
                }
                for record in records
            ],
        )
    return ValidationResult(
        valid_rows=validated.to_dict(orient="records"),
        quarantined_rows=[],
    )


def _validate_subset_rows(
    *,
    schema: pa.DataFrameSchema,
    frame: pd.DataFrame,
    indexes: list[int],
) -> list[dict[str, Any]]:
    if not indexes:
        return []
    subset = frame.iloc[indexes].copy()
    validated_subset = schema.validate(subset, lazy=True)
    return validated_subset.to_dict(orient="records")


def _extract_failing_indexes(exc: pandera.errors.SchemaErrors) -> set[int]:
    failure_cases = exc.failure_cases
    indexes: set[int] = set()
    for value in failure_cases.get("index", pd.Series(dtype=object)).tolist():
        if pd.isna(value):
            continue
        try:
            normalized = int(value)
        except (TypeError, ValueError):
            continue
        indexes.add(normalized)
    return indexes


def _failure_cases_for_index(
    exc: pandera.errors.SchemaErrors, *, index: int
) -> pd.DataFrame:
    failure_cases = exc.failure_cases
    if "index" not in failure_cases:
        return failure_cases.iloc[0:0]
    mask = failure_cases["index"].apply(
        lambda value: False if pd.isna(value) else int(value) == index
    )
    return failure_cases.loc[mask]


def _extract_quarantine_reason(exc: pandera.errors.SchemaErrors) -> str:
    failure_cases = exc.failure_cases
    return _extract_quarantine_reason_from_failure_cases(failure_cases)


def _extract_quarantine_reason_for_index(
    exc: pandera.errors.SchemaErrors, *, index: int
) -> str:
    return _extract_quarantine_reason_from_failure_cases(
        _failure_cases_for_index(exc, index=index)
    )


def _extract_quarantine_reason_from_failure_cases(failure_cases: pd.DataFrame) -> str:
    reasons: list[str] = []
    for _, row in failure_cases.iterrows():
        column = row.get("column")
        check = row.get("check")
        if pd.isna(column) or column is None:
            column = "row"
        if pd.isna(check) or check is None:
            check = "schema_error"
        reason = f"{column}:{check}"
        if reason not in reasons:
            reasons.append(reason)
    return "; ".join(reasons) if reasons else "row:schema_error"


def _count_failure_reasons(exc: pandera.errors.SchemaErrors) -> int:
    reason_string = _extract_quarantine_reason(exc)
    return len(reason_string.split("; "))


def _count_failure_reasons_for_index(
    exc: pandera.errors.SchemaErrors, *, index: int
) -> int:
    reason_string = _extract_quarantine_reason_for_index(exc, index=index)
    return len(reason_string.split("; "))
