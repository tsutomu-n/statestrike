from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict

from statestrike.collector import CollectorConfig, collect_market_batch
from statestrike.exports import (
    ExportValidationReport,
    export_hftbacktest_npz,
    export_nautilus_catalog,
    validate_export_bundle,
)
from statestrike.identifiers import new_capture_session_id
from statestrike.models import ManifestRecord
from statestrike.quality import QualityAuditReport, run_quality_audit
from statestrike.schemas import validate_records
from statestrike.storage import NormalizedWriter, QuarantineWriter, RawWriter


class SmokeBatchResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    capture_session_id: str
    manifest_path: Path
    raw_paths: dict[str, Path]
    normalized_paths: dict[str, Path]
    quarantine_paths: dict[str, Path]
    audit_report: QualityAuditReport
    export_validations: dict[str, ExportValidationReport]


def run_smoke_batch(
    *,
    root: Path,
    trading_date: date,
    messages: list[dict[str, Any]],
    config: CollectorConfig,
    batch_id: str = "0001",
    capture_session_id: str | None = None,
    recv_ts_start: int,
    reconnect_epoch: int = 0,
    book_epoch: int = 1,
) -> SmokeBatchResult:
    capture_session_id = capture_session_id or new_capture_session_id()
    started_at = _utc_now_isoformat()
    raw_writer = RawWriter(root=root)
    normalized_writer = NormalizedWriter(root=root)
    quarantine_writer = QuarantineWriter(root=root)

    raw_paths: dict[str, Path] = {}
    normalized_paths: dict[str, Path] = {}
    quarantine_paths: dict[str, Path] = {}
    export_validations: dict[str, ExportValidationReport] = {}

    grouped_messages = _group_messages_by_channel_and_symbol(
        messages=messages,
        allowed_symbols=config.allowed_symbols,
    )
    for (channel, symbol), grouped in grouped_messages.items():
        raw_paths[f"{channel}:{symbol}"] = raw_writer.write_batch(
            trading_date=trading_date,
            channel=channel,
            symbol=symbol,
            capture_session_id=capture_session_id,
            batch_id=batch_id,
            messages=grouped,
        )

    symbols = tuple(sorted({symbol for _, symbol in grouped_messages}))
    recv_ts = recv_ts_start
    for symbol in symbols:
        symbol_messages = [
            message
            for message in messages
            if _extract_symbol(message) == symbol
        ]
        batch = collect_market_batch(
            messages=symbol_messages,
            config=config,
            capture_session_id=capture_session_id,
            reconnect_epoch=reconnect_epoch,
            book_epoch=book_epoch,
            recv_ts_start=recv_ts,
        )
        recv_ts += len(symbol_messages)
        for table, rows in batch.normalized_rows.items():
            validation = validate_records(table, rows)
            if validation.valid_rows:
                normalized_paths[f"{table}:{symbol}"] = normalized_writer.write_rows(
                    table=table,
                    trading_date=trading_date,
                    symbol=symbol,
                    rows=validation.valid_rows,
                )
            if validation.quarantined_rows:
                quarantine_paths[f"{table}:{symbol}"] = quarantine_writer.write_rows(
                    table=table,
                    trading_date=trading_date,
                    symbol=symbol,
                    rows=validation.quarantined_rows,
                )
        export_nautilus_catalog(
            normalized_root=root,
            export_root=root,
            trading_date=trading_date,
            symbol=symbol,
        )
        export_hftbacktest_npz(
            normalized_root=root,
            export_root=root,
            trading_date=trading_date,
            symbol=symbol,
        )
        export_validations[symbol] = validate_export_bundle(
            export_root=root,
            trading_date=trading_date,
            symbol=symbol,
        )

    audit_report = run_quality_audit(
        normalized_root=root,
        quarantine_root=root,
        trading_date=trading_date,
        symbols=symbols,
    )
    manifest = ManifestRecord(
        capture_session_id=capture_session_id,
        started_at=started_at,
        ended_at=_utc_now_isoformat(),
        channels=tuple(sorted({message["channel"] for message in messages})),
        symbols=symbols,
        row_count=len(messages),
        ws_disconnect_count=0,
        reconnect_count=reconnect_epoch,
        gap_flags=(),
    )
    manifest_path = raw_writer.write_manifest(
        trading_date=trading_date,
        manifest=manifest,
    )
    return SmokeBatchResult(
        capture_session_id=capture_session_id,
        manifest_path=manifest_path,
        raw_paths=raw_paths,
        normalized_paths=normalized_paths,
        quarantine_paths=quarantine_paths,
        audit_report=audit_report,
        export_validations=export_validations,
    )


def _group_messages_by_channel_and_symbol(
    *,
    messages: list[dict[str, Any]],
    allowed_symbols: tuple[str, ...],
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for message in messages:
        symbol = _extract_symbol(message)
        if not symbol:
            continue
        if allowed_symbols and symbol not in allowed_symbols:
            continue
        key = (message["channel"], symbol)
        grouped.setdefault(key, []).append(message)
    return grouped


def _extract_symbol(message: dict[str, Any]) -> str:
    data = message.get("data", {})
    if isinstance(data, dict):
        if "coin" in data:
            return str(data["coin"]).upper()
        if "s" in data:
            return str(data["s"]).upper()
    if isinstance(data, list) and data:
        return str(data[0]["coin"]).upper()
    return ""


def _utc_now_isoformat() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )
