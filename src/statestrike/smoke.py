from __future__ import annotations

import argparse
import asyncio
import json
from datetime import date, datetime, timezone
from pathlib import Path
import sys
import time
from typing import Any, Awaitable, Callable, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from statestrike.collector import (
    CollectorConfig,
    collect_market_batch,
)
from statestrike.exports import (
    ExportValidationReport,
    export_hftbacktest_npz,
    export_nautilus_catalog,
    validate_export_bundle,
)
from statestrike.identifiers import new_capture_session_id
from statestrike.models import ManifestRecord
from statestrike.paths import (
    build_export_validation_report_path,
    build_quality_report_path,
    build_smoke_campaign_report_path,
)
from statestrike.quality import QualityAuditReport, QualityAuditThresholds, run_quality_audit
from statestrike.recovery import (
    MessageCaptureContext,
    MessageIngressMeta,
    count_non_recoverable_book_gaps,
    count_recoverable_book_gaps,
    max_book_epoch,
    resolve_ingress_metadata,
    resolve_message_contexts,
)
from statestrike.runtime import collect_public_runtime_capture
from statestrike.schemas import validate_records
from statestrike.settings import Settings
from statestrike.storage import (
    CaptureLogWriter,
    NormalizedWriter,
    QuarantineWriter,
    RawWriter,
)

SmokeTransport = Callable[..., Awaitable["SmokeTransportCapture"]]


class SmokeBatchResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    capture_session_id: str
    manifest_path: Path
    capture_log_path: Path
    derived_capture_paths: dict[str, Path]
    normalized_paths: dict[str, Path]
    quarantine_paths: dict[str, Path]
    audit_report_paths: dict[str, Path]
    export_validation_report_paths: dict[str, Path]
    audit_report: QualityAuditReport
    export_validations: dict[str, ExportValidationReport]

    @property
    def raw_paths(self) -> dict[str, Path]:
        return self.derived_capture_paths


class SmokeTransportCapture(BaseModel):
    model_config = ConfigDict(frozen=True)

    messages: list[dict[str, Any]]
    message_contexts: tuple[MessageCaptureContext, ...] = ()
    ingress_metadata: tuple[MessageIngressMeta, ...] = ()
    recv_ts_start: int
    started_at: str
    ended_at: str
    ws_disconnect_count: int = 0
    reconnect_count: int = 0
    reconnect_epoch: int = 0
    book_epoch: int = 1
    gap_flags: tuple[str, ...] = ()


class SmokeCampaignSession(BaseModel):
    model_config = ConfigDict(frozen=True)

    capture_session_id: str
    manifest_path: Path
    capture_log_path: Path | None = None
    row_count: int
    channels: tuple[str, ...] = ()
    symbols: tuple[str, ...] = ()
    derived_capture_paths: dict[str, Path] = Field(default_factory=dict)
    normalized_paths: dict[str, Path] = Field(default_factory=dict)
    quarantine_paths: dict[str, Path] = Field(default_factory=dict)
    ws_disconnect_count: int = 0
    reconnect_count: int = 0
    gap_flags: tuple[str, ...] = ()

    @model_validator(mode="before")
    @classmethod
    def apply_legacy_raw_paths(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        normalized = dict(data)
        if "derived_capture_paths" not in normalized and "raw_paths" in normalized:
            normalized["derived_capture_paths"] = normalized["raw_paths"]
        return normalized

    @property
    def raw_paths(self) -> dict[str, Path]:
        return self.derived_capture_paths


class SmokeCampaignScope(BaseModel):
    model_config = ConfigDict(frozen=True)

    trading_date: date
    allowed_symbols: tuple[str, ...]
    observed_symbols: tuple[str, ...]
    market_data_network: Literal["mainnet", "testnet"]
    smoke_channels: tuple[str, ...]
    smoke_candle_interval: str | None = None
    thresholds: QualityAuditThresholds


class SmokeCampaignResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    campaign_id: str
    started_at: str
    ended_at: str
    status: Literal["running", "completed", "failed"]
    requested_session_count: int
    session_count: int
    total_row_count: int
    sessions: tuple[SmokeCampaignSession, ...]
    scope: SmokeCampaignScope | None = None
    report_paths: dict[str, Path]
    final_audit_report_paths: dict[str, Path] = Field(default_factory=dict)
    final_export_validation_report_paths: dict[str, Path] = Field(default_factory=dict)
    final_audit_report: QualityAuditReport | None = None
    final_export_validations: dict[str, ExportValidationReport] = Field(default_factory=dict)
    error_message: str | None = None

    @model_validator(mode="before")
    @classmethod
    def apply_legacy_defaults(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        normalized = dict(data)
        session_count = int(normalized.get("session_count", 0))
        normalized.setdefault("requested_session_count", session_count)
        if "status" not in normalized:
            normalized["status"] = (
                "failed" if normalized.get("error_message") else "completed"
            )
        normalized.setdefault("report_paths", {})
        normalized.setdefault("final_audit_report_paths", {})
        normalized.setdefault("final_export_validation_report_paths", {})
        normalized.setdefault("final_export_validations", {})
        return normalized


def run_smoke_batch(
    *,
    root: Path,
    trading_date: date,
    messages: list[dict[str, Any]],
    message_contexts: list[MessageCaptureContext] | tuple[MessageCaptureContext, ...] | None = None,
    ingress_metadata: list[MessageIngressMeta] | tuple[MessageIngressMeta, ...] | None = None,
    config: CollectorConfig,
    batch_id: str = "0001",
    capture_session_id: str | None = None,
    recv_ts_start: int,
    reconnect_epoch: int = 0,
    book_epoch: int = 1,
    manifest_reconnect_count: int | None = None,
    started_at: str | None = None,
    ended_at: str | None = None,
    ws_disconnect_count: int = 0,
    gap_flags: tuple[str, ...] = (),
    skew_warning_ms: int = 250,
    skew_severe_ms: int = 1_000,
    asset_ctx_stale_threshold_ms: int = 300_000,
    trade_gap_threshold_ms: int = 60_000,
    quarantine_warning_rate: float = 0.001,
    quarantine_severe_rate: float = 0.01,
) -> SmokeBatchResult:
    capture_session_id = capture_session_id or new_capture_session_id()
    started_at = started_at or _utc_now_isoformat()
    raw_writer = RawWriter(root=root)
    capture_log_writer = CaptureLogWriter(root=root)
    normalized_writer = NormalizedWriter(root=root)
    quarantine_writer = QuarantineWriter(root=root)

    derived_capture_paths: dict[str, Path] = {}
    normalized_paths: dict[str, Path] = {}
    quarantine_paths: dict[str, Path] = {}
    audit_report_paths: dict[str, Path] = {}
    export_validation_report_paths: dict[str, Path] = {}
    export_validations: dict[str, ExportValidationReport] = {}
    resolved_contexts = resolve_message_contexts(
        messages=messages,
        message_contexts=message_contexts,
        reconnect_epoch=reconnect_epoch,
        book_epoch=book_epoch,
    )
    resolved_ingress = resolve_ingress_metadata(
        messages=messages,
        ingress_metadata=ingress_metadata,
        recv_ts_start=recv_ts_start,
        allow_fixture_fallback=(config.run_mode == "fixture"),
    )
    capture_log_path = capture_log_writer.write_batch(
        trading_date=trading_date,
        capture_session_id=capture_session_id,
        batch_id=batch_id,
        messages=messages,
        ingress_metadata=resolved_ingress,
        message_contexts=resolved_contexts,
    )

    grouped_messages = _group_messages_by_channel_and_symbol(
        messages=messages,
        allowed_symbols=config.allowed_symbols,
    )
    for (channel, symbol), grouped in grouped_messages.items():
        derived_capture_paths[f"{channel}:{symbol}"] = raw_writer.write_batch(
            trading_date=trading_date,
            channel=channel,
            symbol=symbol,
            capture_session_id=capture_session_id,
            batch_id=batch_id,
            messages=grouped,
        )

    observed_symbols = tuple(sorted({symbol for _, symbol in grouped_messages}))
    audit_symbols = config.allowed_symbols or observed_symbols
    batch = collect_market_batch(
        messages=messages,
        message_contexts=resolved_contexts,
        ingress_metadata=resolved_ingress,
        config=config,
        capture_session_id=capture_session_id,
        reconnect_epoch=reconnect_epoch,
        book_epoch=book_epoch,
        recv_ts_start=recv_ts_start,
    )
    for table, rows in batch.normalized_rows.items():
        validation = validate_records(table, rows)
        for symbol, symbol_rows in _group_rows_by_symbol(validation.valid_rows).items():
            normalized_paths[f"{table}:{symbol}"] = normalized_writer.write_rows(
                table=table,
                trading_date=trading_date,
                symbol=symbol,
                rows=symbol_rows,
            )
        for symbol, symbol_rows in _group_rows_by_symbol(validation.quarantined_rows).items():
            quarantine_paths[f"{table}:{symbol}"] = quarantine_writer.write_rows(
                table=table,
                trading_date=trading_date,
                symbol=symbol,
                rows=symbol_rows,
            )
    for symbol in observed_symbols:
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
        export_validation_report_paths[symbol] = _write_export_validation_report(
            root=root,
            trading_date=trading_date,
            symbol=symbol,
            report=export_validations[symbol],
        )

    audit_report = run_quality_audit(
        normalized_root=root,
        quarantine_root=root,
        trading_date=trading_date,
        symbols=audit_symbols,
        skew_warning_ms=skew_warning_ms,
        skew_severe_ms=skew_severe_ms,
        asset_ctx_stale_threshold_ms=asset_ctx_stale_threshold_ms,
        trade_gap_threshold_ms=trade_gap_threshold_ms,
        quarantine_warning_rate=quarantine_warning_rate,
        quarantine_severe_rate=quarantine_severe_rate,
    )
    audit_report_paths = _write_audit_reports(
        root=root,
        trading_date=trading_date,
        report=audit_report,
    )
    manifest = ManifestRecord(
        capture_session_id=capture_session_id,
        capture_semantics_version="phase1_truth_capture_v1",
        truth_capture_artifact="capture_log",
        derived_capture_artifacts=("raw_ws",),
        truth_export_targets=("nautilus",),
        corrected_export_targets=("hftbacktest",),
        started_at=started_at,
        ended_at=ended_at or _utc_now_isoformat(),
        channels=tuple(sorted({message["channel"] for message in messages})),
        symbols=audit_symbols,
        row_count=len(messages),
        ws_disconnect_count=ws_disconnect_count,
        reconnect_count=(
            reconnect_epoch if manifest_reconnect_count is None else manifest_reconnect_count
        ),
        book_epoch_count=max_book_epoch(
            resolved_contexts,
            fallback=max(1, book_epoch),
        ),
        book_continuity_gap_count=(
            count_recoverable_book_gaps(resolved_contexts)
            + count_non_recoverable_book_gaps(gap_flags)
        ),
        recoverable_book_gap_count=count_recoverable_book_gaps(resolved_contexts),
        non_recoverable_book_gap_count=count_non_recoverable_book_gaps(gap_flags),
        gap_flags=gap_flags,
    )
    manifest_path = raw_writer.write_manifest(
        trading_date=trading_date,
        manifest=manifest,
    )
    return SmokeBatchResult(
        capture_session_id=capture_session_id,
        manifest_path=manifest_path,
        capture_log_path=capture_log_path,
        derived_capture_paths=derived_capture_paths,
        normalized_paths=normalized_paths,
        quarantine_paths=quarantine_paths,
        audit_report_paths=audit_report_paths,
        export_validation_report_paths=export_validation_report_paths,
        audit_report=audit_report,
        export_validations=export_validations,
    )


async def run_smoke_session(
    *,
    settings: Settings,
    trading_date: date | None = None,
    transport: SmokeTransport | None = None,
    capture_session_id: str | None = None,
    batch_id: str = "0001",
) -> SmokeBatchResult:
    _capture, result = await _run_single_smoke_capture(
        settings=settings,
        trading_date=trading_date,
        transport=transport,
        capture_session_id=capture_session_id,
        batch_id=batch_id,
    )
    return result


async def run_smoke_campaign(
    *,
    settings: Settings,
    session_count: int,
    trading_date: date | None = None,
    transport: SmokeTransport | None = None,
    capture_session_id: str | None = None,
    resume_campaign: bool = False,
) -> SmokeCampaignResult:
    if session_count < 1:
        raise ValueError("session_count must be at least 1")
    if resume_campaign and capture_session_id is None:
        raise ValueError("resume_campaign requires explicit capture_session_id")

    campaign_id = capture_session_id or new_capture_session_id()
    report_paths = _campaign_report_paths(
        root=settings.data_root,
        campaign_id=campaign_id,
    )
    previous_result = (
        _load_campaign_result(root=settings.data_root, campaign_id=campaign_id)
        if resume_campaign
        else None
    )
    if resume_campaign and previous_result is None:
        raise ValueError(
            f"resume_campaign requested but no persisted campaign summary found for {campaign_id}"
        )

    campaign_trading_date = _resolve_campaign_trading_date(
        previous_result=previous_result,
        trading_date=trading_date,
    )

    started_at = (
        previous_result.started_at
        if previous_result is not None
        else _utc_now_isoformat()
    )
    sessions: list[SmokeCampaignSession] = (
        list(previous_result.sessions) if previous_result is not None else []
    )
    if session_count < len(sessions):
        raise ValueError(
            "session_count must be greater than or equal to persisted session_count"
        )
    current_result = previous_result
    current_scope = _build_campaign_scope(
        settings=settings,
        trading_date=campaign_trading_date,
        sessions=sessions,
    )

    if previous_result is not None:
        _validate_resume_scope(
            persisted_result=previous_result,
            current_scope=current_scope,
        )
        _validate_persisted_campaign_artifacts(previous_result)
        if len(sessions) == session_count:
            current_result = _build_campaign_result(
                campaign_id=campaign_id,
                started_at=started_at,
                requested_session_count=session_count,
                status=previous_result.status,
                sessions=sessions,
                scope=current_scope,
                report_paths=report_paths,
                error_message=previous_result.error_message,
                root=settings.data_root,
            )
            _write_campaign_reports(result=current_result)
            return current_result

    try:
        for session_index in range(len(sessions), session_count):
            session_capture_id = _campaign_session_id(
                campaign_id=campaign_id,
                session_index=session_index,
            )
            capture, result = await _run_single_smoke_capture(
                settings=settings,
                trading_date=trading_date,
                transport=transport,
                capture_session_id=session_capture_id,
                batch_id=f"{session_index + 1:04d}",
            )
            sessions.append(
                SmokeCampaignSession(
                    capture_session_id=result.capture_session_id,
                    manifest_path=result.manifest_path,
                    capture_log_path=result.capture_log_path,
                    row_count=len(capture.messages),
                    channels=_artifact_channels(result.derived_capture_paths),
                    symbols=_artifact_symbols(result.derived_capture_paths),
                    derived_capture_paths=result.derived_capture_paths,
                    normalized_paths=result.normalized_paths,
                    quarantine_paths=result.quarantine_paths,
                    ws_disconnect_count=capture.ws_disconnect_count,
                    reconnect_count=capture.reconnect_count,
                    gap_flags=capture.gap_flags,
                )
            )
            current_scope = _build_campaign_scope(
                settings=settings,
                trading_date=campaign_trading_date,
                sessions=sessions,
            )
            current_result = _build_campaign_result(
                campaign_id=campaign_id,
                started_at=started_at,
                requested_session_count=session_count,
                status=(
                    "completed"
                    if session_index + 1 == session_count
                    else "running"
                ),
                sessions=sessions,
                scope=current_scope,
                report_paths=report_paths,
                error_message=None,
                root=settings.data_root,
            )
            _write_campaign_reports(result=current_result)
    except Exception as exc:
        current_scope = _build_campaign_scope(
            settings=settings,
            trading_date=campaign_trading_date,
            sessions=sessions,
        )
        current_result = _build_campaign_result(
            campaign_id=campaign_id,
            started_at=started_at,
            requested_session_count=session_count,
            status="failed",
            sessions=sessions,
            scope=current_scope,
            report_paths=report_paths,
            error_message=str(exc),
            root=settings.data_root,
        )
        _write_campaign_reports(result=current_result)
        raise

    if current_result is not None:
        return current_result
    raise RuntimeError("smoke campaign did not run any sessions")


async def _run_single_smoke_capture(
    *,
    settings: Settings,
    trading_date: date | None,
    transport: SmokeTransport | None,
    capture_session_id: str | None,
    batch_id: str,
) -> tuple[SmokeTransportCapture, SmokeBatchResult]:
    config = settings.build_smoke_collector_config()
    transport = transport or collect_public_smoke_messages
    capture = await transport(
        config=config,
        max_messages=settings.smoke_max_messages,
        max_runtime_seconds=settings.smoke_max_runtime_seconds,
        ping_interval_seconds=settings.smoke_ping_interval_seconds,
        reconnect_limit=settings.smoke_reconnect_limit,
    )
    result = run_smoke_batch(
        root=settings.data_root,
        trading_date=trading_date or date.today(),
        messages=capture.messages,
        message_contexts=capture.message_contexts,
        ingress_metadata=capture.ingress_metadata,
        config=config,
        batch_id=batch_id,
        capture_session_id=capture_session_id,
        recv_ts_start=capture.recv_ts_start,
        reconnect_epoch=(
            capture.reconnect_epoch
            if capture.reconnect_epoch > 0
            else capture.reconnect_count
        ),
        book_epoch=capture.book_epoch,
        manifest_reconnect_count=capture.reconnect_count,
        started_at=capture.started_at,
        ended_at=capture.ended_at,
        ws_disconnect_count=capture.ws_disconnect_count,
        gap_flags=capture.gap_flags,
        skew_warning_ms=settings.smoke_skew_warning_ms,
        skew_severe_ms=settings.smoke_skew_severe_ms,
        asset_ctx_stale_threshold_ms=settings.smoke_asset_ctx_stale_threshold_ms,
        trade_gap_threshold_ms=settings.smoke_trade_gap_threshold_ms,
        quarantine_warning_rate=settings.smoke_quarantine_warning_rate,
        quarantine_severe_rate=settings.smoke_quarantine_severe_rate,
    )
    return capture, result


async def collect_public_smoke_messages(
    *,
    config: CollectorConfig,
    max_messages: int,
    max_runtime_seconds: int,
    ping_interval_seconds: int,
    reconnect_limit: int,
) -> SmokeTransportCapture:
    capture = await collect_public_runtime_capture(
        config=config,
        max_messages=max_messages,
        max_runtime_seconds=max_runtime_seconds,
        ping_interval_seconds=ping_interval_seconds,
        reconnect_limit=reconnect_limit,
    )
    return SmokeTransportCapture(
        messages=capture.messages,
        message_contexts=capture.message_contexts,
        ingress_metadata=capture.ingress_metadata,
        recv_ts_start=capture.recv_ts_start,
        started_at=capture.started_at,
        ended_at=capture.ended_at,
        ws_disconnect_count=capture.ws_disconnect_count,
        reconnect_count=capture.reconnect_count,
        reconnect_epoch=capture.reconnect_epoch,
        book_epoch=capture.book_epoch,
        gap_flags=capture.gap_flags,
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


def _group_rows_by_symbol(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        symbol = str(row.get("symbol", "")).upper()
        if not symbol:
            continue
        grouped.setdefault(symbol, []).append(row)
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


def _write_audit_reports(
    *,
    root: Path,
    trading_date: date,
    report: QualityAuditReport,
) -> dict[str, Path]:
    json_path = build_quality_report_path(
        root=root,
        trading_date=trading_date,
        suffix="json",
    )
    markdown_path = build_quality_report_path(
        root=root,
        trading_date=trading_date,
        suffix="md",
    )
    _write_text_atomic(
        path=json_path,
        content=report.model_dump_json(indent=2),
    )
    _write_text_atomic(
        path=markdown_path,
        content=_render_audit_markdown(report),
    )
    return {
        "json": json_path,
        "md": markdown_path,
    }


def _write_export_validation_report(
    *,
    root: Path,
    trading_date: date,
    symbol: str,
    report: ExportValidationReport,
) -> Path:
    path = build_export_validation_report_path(
        root=root,
        trading_date=trading_date,
        symbol=symbol,
    )
    _write_text_atomic(
        path=path,
        content=report.model_dump_json(indent=2),
    )
    return path


def _campaign_report_paths(*, root: Path, campaign_id: str) -> dict[str, Path]:
    return {
        "json": build_smoke_campaign_report_path(
            root=root,
            campaign_id=campaign_id,
            suffix="json",
        ),
        "md": build_smoke_campaign_report_path(
            root=root,
            campaign_id=campaign_id,
            suffix="md",
        ),
    }


def _write_campaign_reports(*, result: SmokeCampaignResult) -> None:
    json_path = result.report_paths["json"]
    markdown_path = result.report_paths["md"]
    _write_text_atomic(
        path=json_path,
        content=result.model_dump_json(indent=2),
    )
    _write_text_atomic(
        path=markdown_path,
        content=_render_campaign_markdown(result),
    )


def _load_campaign_result(*, root: Path, campaign_id: str) -> SmokeCampaignResult | None:
    path = build_smoke_campaign_report_path(
        root=root,
        campaign_id=campaign_id,
        suffix="json",
    )
    if not path.exists():
        return None
    try:
        return SmokeCampaignResult.model_validate_json(path.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover - defensive path
        raise ValueError(
            f"invalid persisted campaign summary for {campaign_id}: {exc}"
        ) from exc


def _build_campaign_result(
    *,
    campaign_id: str,
    started_at: str,
    requested_session_count: int,
    status: Literal["running", "completed", "failed"],
    sessions: list[SmokeCampaignSession],
    scope: SmokeCampaignScope,
    report_paths: dict[str, Path],
    error_message: str | None,
    root: Path,
) -> SmokeCampaignResult:
    (
        audit_report,
        audit_report_paths,
        export_validations,
        export_validation_report_paths,
    ) = _recompute_campaign_reports(
        root=root,
        scope=scope,
    )
    return SmokeCampaignResult(
        campaign_id=campaign_id,
        started_at=started_at,
        ended_at=_utc_now_isoformat(),
        status=status,
        requested_session_count=requested_session_count,
        session_count=len(sessions),
        total_row_count=sum(session.row_count for session in sessions),
        sessions=tuple(sessions),
        scope=scope,
        report_paths=report_paths,
        final_audit_report_paths=audit_report_paths,
        final_export_validation_report_paths=export_validation_report_paths,
        final_audit_report=audit_report,
        final_export_validations=export_validations,
        error_message=error_message,
    )


def _resolve_campaign_trading_date(
    *,
    previous_result: SmokeCampaignResult | None,
    trading_date: date | None,
) -> date:
    if trading_date is not None:
        return trading_date
    if previous_result is not None and previous_result.scope is not None:
        return previous_result.scope.trading_date
    return date.today()


def _build_campaign_scope(
    *,
    settings: Settings,
    trading_date: date,
    sessions: list[SmokeCampaignSession],
) -> SmokeCampaignScope:
    allowed_symbols = tuple(sorted(settings.require_smoke_symbols()))
    observed_symbols = tuple(
        sorted({symbol for session in sessions for symbol in session.symbols})
    )
    smoke_channels = tuple(settings.smoke_channels)
    return SmokeCampaignScope(
        trading_date=trading_date,
        allowed_symbols=allowed_symbols,
        observed_symbols=observed_symbols,
        market_data_network=settings.market_data_network,
        smoke_channels=smoke_channels,
        smoke_candle_interval=(
            settings.smoke_candle_interval if "candle" in smoke_channels else None
        ),
        thresholds=QualityAuditThresholds(
            skew_warning_ms=settings.smoke_skew_warning_ms,
            skew_severe_ms=settings.smoke_skew_severe_ms,
            asset_ctx_stale_threshold_ms=settings.smoke_asset_ctx_stale_threshold_ms,
            trade_gap_threshold_ms=settings.smoke_trade_gap_threshold_ms,
            quarantine_warning_rate=settings.smoke_quarantine_warning_rate,
            quarantine_severe_rate=settings.smoke_quarantine_severe_rate,
        ),
    )


def _validate_resume_scope(
    *,
    persisted_result: SmokeCampaignResult,
    current_scope: SmokeCampaignScope,
) -> None:
    persisted_scope = persisted_result.scope
    if persisted_scope is None:
        raise ValueError(
            "persisted campaign summary missing scope metadata; cannot safely resume"
        )
    mismatches: list[str] = []
    if persisted_scope.trading_date != current_scope.trading_date:
        mismatches.append("trading_date")
    if persisted_scope.allowed_symbols != current_scope.allowed_symbols:
        mismatches.append("allowed_symbols")
    if persisted_scope.market_data_network != current_scope.market_data_network:
        mismatches.append("market_data_network")
    if persisted_scope.smoke_channels != current_scope.smoke_channels:
        mismatches.append("smoke_channels")
    if persisted_scope.smoke_candle_interval != current_scope.smoke_candle_interval:
        mismatches.append("smoke_candle_interval")
    if persisted_scope.thresholds != current_scope.thresholds:
        mismatches.append("thresholds")
    if mismatches:
        raise ValueError(
            "resume_campaign scope mismatch: " + ", ".join(mismatches)
        )


def _validate_persisted_campaign_artifacts(result: SmokeCampaignResult) -> None:
    missing_paths: list[str] = []
    for session in result.sessions:
        required_paths = (
            [session.manifest_path]
            + ([session.capture_log_path] if session.capture_log_path is not None else [])
            + list(session.derived_capture_paths.values())
            + list(session.normalized_paths.values())
            + list(session.quarantine_paths.values())
        )
        for path in required_paths:
            if not Path(path).exists():
                missing_paths.append(str(path))
                if len(missing_paths) >= 3:
                    break
        if len(missing_paths) >= 3:
            break
    if missing_paths:
        raise ValueError(
            "missing persisted artifact: " + ", ".join(missing_paths)
        )


def _recompute_campaign_reports(
    *,
    root: Path,
    scope: SmokeCampaignScope,
) -> tuple[
    QualityAuditReport,
    dict[str, Path],
    dict[str, ExportValidationReport],
    dict[str, Path],
]:
    audit_report = run_quality_audit(
        normalized_root=root,
        quarantine_root=root,
        trading_date=scope.trading_date,
        symbols=scope.allowed_symbols,
        skew_warning_ms=scope.thresholds.skew_warning_ms,
        skew_severe_ms=scope.thresholds.skew_severe_ms,
        asset_ctx_stale_threshold_ms=scope.thresholds.asset_ctx_stale_threshold_ms,
        trade_gap_threshold_ms=scope.thresholds.trade_gap_threshold_ms,
        quarantine_warning_rate=scope.thresholds.quarantine_warning_rate,
        quarantine_severe_rate=scope.thresholds.quarantine_severe_rate,
    )
    audit_report_paths = _write_audit_reports(
        root=root,
        trading_date=scope.trading_date,
        report=audit_report,
    )
    export_validations: dict[str, ExportValidationReport] = {}
    export_validation_report_paths: dict[str, Path] = {}
    for symbol in scope.observed_symbols:
        export_validations[symbol] = validate_export_bundle(
            export_root=root,
            trading_date=scope.trading_date,
            symbol=symbol,
        )
        export_validation_report_paths[symbol] = _write_export_validation_report(
            root=root,
            trading_date=scope.trading_date,
            symbol=symbol,
            report=export_validations[symbol],
        )
    return (
        audit_report,
        audit_report_paths,
        export_validations,
        export_validation_report_paths,
    )


def _artifact_channels(paths: dict[str, Path]) -> tuple[str, ...]:
    return tuple(sorted({key.split(":", 1)[0] for key in paths}))


def _artifact_symbols(paths: dict[str, Path]) -> tuple[str, ...]:
    return tuple(sorted({key.split(":", 1)[1] for key in paths}))


def _write_text_atomic(*, path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{time.time_ns()}.tmp")
    try:
        temp_path.write_text(content, encoding="utf-8")
        temp_path.replace(path)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def _render_audit_markdown(report: QualityAuditReport) -> str:
    lines = [
        "# Quality Audit",
        "",
        "## Thresholds",
        f"- skew_warning_ms: {report.thresholds.skew_warning_ms}",
        f"- skew_severe_ms: {report.thresholds.skew_severe_ms}",
        (
            "- asset_ctx_stale_threshold_ms: "
            f"{report.thresholds.asset_ctx_stale_threshold_ms}"
        ),
        f"- trade_gap_threshold_ms: {report.thresholds.trade_gap_threshold_ms}",
        (
            "- quarantine_warning_rate: "
            f"{report.thresholds.quarantine_warning_rate}"
        ),
        (
            "- quarantine_severe_rate: "
            f"{report.thresholds.quarantine_severe_rate}"
        ),
        "",
        "## Gap Metrics",
        f"- gap_count: {report.gap_count}",
        f"- book_continuity_gap_count: {report.book_continuity_gap_count}",
        f"- recoverable_book_gap_count: {report.recoverable_book_gap_count}",
        f"- non_recoverable_book_gap_count: {report.non_recoverable_book_gap_count}",
        f"- book_epoch_switch_count: {report.book_epoch_switch_count}",
        f"- trade_gap_count: {report.trade_gap_count}",
        f"- asset_ctx_gap_count: {report.asset_ctx_gap_count}",
        f"- duplicate_trade_count: {report.duplicate_trade_count}",
        f"- raw_duplicate_trade_count: {report.raw_duplicate_trade_count}",
        (
            "- reconnect_replay_duplicate_trade_count: "
            f"{report.reconnect_replay_duplicate_trade_count}"
        ),
        (
            "- unexplained_duplicate_trade_count: "
            f"{report.unexplained_duplicate_trade_count}"
        ),
        (
            "- non_monotonic_exchange_ts_count: "
            f"{report.non_monotonic_exchange_ts_count}"
        ),
        f"- non_monotonic_recv_ts_count: {report.non_monotonic_recv_ts_count}",
        (
            "- exchange_sorted_recv_inversion_count: "
            f"{report.exchange_sorted_recv_inversion_count}"
        ),
        f"- capture_order_integrity: {report.capture_order_integrity}",
        f"- capture_log_file_count: {report.capture_log_file_count}",
        f"- capture_log_row_count: {report.capture_log_row_count}",
        (
            "- capture_order_missing_recv_timestamp_count: "
            f"{report.capture_order_missing_recv_timestamp_count}"
        ),
        (
            "- capture_order_recv_seq_non_monotonic_count: "
            f"{report.capture_order_recv_seq_non_monotonic_count}"
        ),
        f"- empty_snapshot_count: {report.empty_snapshot_count}",
        "",
    ]
    for table in ("book_events", "book_levels", "trades", "asset_ctx"):
        lines.append(f"## {table}")
        lines.append(f"- row_count: {report.row_counts.get(table, 0)}")
        lines.append(
            f"- quarantine_rate: {report.quarantine_rates.get(table, 0.0):.6f}"
        )
        lines.append(
            f"- quarantine_alert: {report.quarantine_alerts.get(table, 'n/a')}"
        )
        lines.append(
            f"- skew_alert: {report.skew_alerts.get(table, 'n/a')}"
        )
        reason_counts = report.quarantine_reason_counts.get(table, {})
        if reason_counts:
            lines.append(
                "- quarantine_reasons: "
                + ", ".join(f"{reason}={count}" for reason, count in reason_counts.items())
            )
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def _render_campaign_markdown(result: SmokeCampaignResult) -> str:
    lines = [
        "# Smoke Campaign",
        "",
        f"- campaign_id: {result.campaign_id}",
        f"- started_at: {result.started_at}",
        f"- ended_at: {result.ended_at}",
        f"- status: {result.status}",
        f"- requested_session_count: {result.requested_session_count}",
        f"- session_count: {result.session_count}",
        f"- total_row_count: {result.total_row_count}",
        "",
    ]
    if result.scope is not None:
        lines.extend(
            [
                "## Scope",
                f"- trading_date: {result.scope.trading_date.isoformat()}",
                "- allowed_symbols: " + ", ".join(result.scope.allowed_symbols),
                "- observed_symbols: " + ", ".join(result.scope.observed_symbols),
                f"- market_data_network: {result.scope.market_data_network}",
                "- smoke_channels: " + ", ".join(result.scope.smoke_channels),
                (
                    "- thresholds: "
                    f"warning={result.scope.thresholds.skew_warning_ms}, "
                    f"severe={result.scope.thresholds.skew_severe_ms}, "
                    "asset_ctx_stale="
                    f"{result.scope.thresholds.asset_ctx_stale_threshold_ms}, "
                    "trade_gap="
                    f"{result.scope.thresholds.trade_gap_threshold_ms}, "
                    "quarantine="
                    f"{result.scope.thresholds.quarantine_warning_rate}/"
                    f"{result.scope.thresholds.quarantine_severe_rate}"
                ),
                "",
                "## Sessions",
            ]
        )
    else:
        lines.append("## Sessions")
    for session in result.sessions:
        lines.append(f"- {session.capture_session_id}: rows={session.row_count}")
        lines.append(f"  manifest_path={session.manifest_path}")
    if result.error_message is not None:
        lines.extend(
            [
                "",
                "## Failure",
                f"- error_message: {result.error_message}",
            ]
        )
    if result.final_audit_report_paths:
        lines.extend(
            [
                "",
                "## Final Reports",
                f"- audit_json: {result.final_audit_report_paths['json']}",
                f"- audit_md: {result.final_audit_report_paths['md']}",
            ]
        )
    for symbol, path in sorted(result.final_export_validation_report_paths.items()):
        lines.append(f"- export_validation_{symbol}: {path}")
    return "\n".join(lines).strip() + "\n"


def _campaign_session_id(*, campaign_id: str, session_index: int) -> str:
    return f"{campaign_id}-{session_index + 1:04d}"


def main(
    argv: list[str] | None = None,
    *,
    transport: SmokeTransport | None = None,
) -> int:
    args = _parse_args(argv)
    try:
        if args.session_count < 1:
            raise ValueError("session_count must be at least 1")
        if args.resume_campaign and args.capture_session_id is None:
            raise ValueError("resume_campaign requires explicit capture_session_id")
        settings = _build_settings_from_args(args)
        trading_date = date.fromisoformat(args.date) if args.date is not None else None
        if args.session_count > 1 or args.resume_campaign or args.campaign_mode:
            result = asyncio.run(
                run_smoke_campaign(
                    settings=settings,
                    session_count=args.session_count,
                    trading_date=trading_date,
                    transport=transport,
                    capture_session_id=args.capture_session_id,
                    resume_campaign=args.resume_campaign,
                )
            )
        else:
            result = asyncio.run(
                run_smoke_session(
                    settings=settings,
                    trading_date=trading_date,
                    transport=transport,
                    capture_session_id=args.capture_session_id,
                    batch_id=args.batch_id,
                )
            )
        
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(result.model_dump(mode="json"), indent=2, ensure_ascii=True))
    return 0


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="statestrike-smoke",
        description="Run a Phase 1.5 public market-data smoke session.",
    )
    parser.add_argument("--data-root")
    parser.add_argument("--allowed-symbols")
    parser.add_argument("--channels")
    parser.add_argument("--candle-interval")
    parser.add_argument("--market-data-network", choices=("mainnet", "testnet"))
    parser.add_argument("--date")
    parser.add_argument("--capture-session-id")
    parser.add_argument("--batch-id", default="0001")
    parser.add_argument("--session-count", type=int, default=1)
    parser.add_argument("--campaign-mode", action="store_true")
    parser.add_argument("--resume-campaign", action="store_true")
    parser.add_argument("--max-runtime-seconds", type=int)
    parser.add_argument("--max-messages", type=int)
    parser.add_argument("--ping-interval-seconds", type=int)
    parser.add_argument("--reconnect-limit", type=int)
    parser.add_argument("--skew-warning-ms", type=int)
    parser.add_argument("--skew-severe-ms", type=int)
    parser.add_argument("--asset-ctx-stale-threshold-ms", type=int)
    parser.add_argument("--trade-gap-threshold-ms", type=int)
    parser.add_argument("--quarantine-warning-rate", type=float)
    parser.add_argument("--quarantine-severe-rate", type=float)
    return parser.parse_args(argv)


def _build_settings_from_args(args: argparse.Namespace) -> Settings:
    overrides: dict[str, Any] = {}
    if args.data_root is not None:
        overrides["data_root"] = Path(args.data_root)
    if args.allowed_symbols is not None:
        overrides["allowed_symbols"] = tuple(
            symbol.strip().upper()
            for symbol in args.allowed_symbols.split(",")
            if symbol.strip()
        )
    if args.channels is not None:
        overrides["smoke_channels"] = tuple(
            channel.strip()
            for channel in args.channels.split(",")
            if channel.strip()
        )
    if args.candle_interval is not None:
        overrides["smoke_candle_interval"] = args.candle_interval
    if args.market_data_network is not None:
        overrides["market_data_network"] = args.market_data_network
    if args.max_runtime_seconds is not None:
        overrides["smoke_max_runtime_seconds"] = args.max_runtime_seconds
    if args.max_messages is not None:
        overrides["smoke_max_messages"] = args.max_messages
    if args.ping_interval_seconds is not None:
        overrides["smoke_ping_interval_seconds"] = args.ping_interval_seconds
    if args.reconnect_limit is not None:
        overrides["smoke_reconnect_limit"] = args.reconnect_limit
    if args.skew_warning_ms is not None:
        overrides["smoke_skew_warning_ms"] = args.skew_warning_ms
    if args.skew_severe_ms is not None:
        overrides["smoke_skew_severe_ms"] = args.skew_severe_ms
    if args.asset_ctx_stale_threshold_ms is not None:
        overrides["smoke_asset_ctx_stale_threshold_ms"] = (
            args.asset_ctx_stale_threshold_ms
        )
    if args.trade_gap_threshold_ms is not None:
        overrides["smoke_trade_gap_threshold_ms"] = args.trade_gap_threshold_ms
    if args.quarantine_warning_rate is not None:
        overrides["smoke_quarantine_warning_rate"] = args.quarantine_warning_rate
    if args.quarantine_severe_rate is not None:
        overrides["smoke_quarantine_severe_rate"] = args.quarantine_severe_rate
    return Settings(**overrides)


if __name__ == "__main__":
    raise SystemExit(main())
