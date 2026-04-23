from __future__ import annotations

import argparse
import asyncio
import json
from datetime import date, datetime, timezone
from pathlib import Path
import sys
import time
from typing import Any, Awaitable, Callable

import pybotters
from pydantic import BaseModel, ConfigDict

from statestrike.collector import (
    CollectorConfig,
    build_subscription_requests,
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
from statestrike.quality import QualityAuditReport, run_quality_audit
from statestrike.schemas import validate_records
from statestrike.settings import Settings
from statestrike.storage import NormalizedWriter, QuarantineWriter, RawWriter

SmokeTransport = Callable[..., Awaitable["SmokeTransportCapture"]]


class SmokeBatchResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    capture_session_id: str
    manifest_path: Path
    raw_paths: dict[str, Path]
    normalized_paths: dict[str, Path]
    quarantine_paths: dict[str, Path]
    audit_report_paths: dict[str, Path]
    export_validation_report_paths: dict[str, Path]
    audit_report: QualityAuditReport
    export_validations: dict[str, ExportValidationReport]


class SmokeTransportCapture(BaseModel):
    model_config = ConfigDict(frozen=True)

    messages: list[dict[str, Any]]
    recv_ts_start: int
    started_at: str
    ended_at: str
    ws_disconnect_count: int = 0
    reconnect_count: int = 0
    gap_flags: tuple[str, ...] = ()


class SmokeCampaignSession(BaseModel):
    model_config = ConfigDict(frozen=True)

    capture_session_id: str
    manifest_path: Path
    row_count: int
    ws_disconnect_count: int = 0
    reconnect_count: int = 0
    gap_flags: tuple[str, ...] = ()


class SmokeCampaignResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    campaign_id: str
    started_at: str
    ended_at: str
    session_count: int
    total_row_count: int
    sessions: tuple[SmokeCampaignSession, ...]
    report_paths: dict[str, Path]
    final_audit_report_paths: dict[str, Path]
    final_export_validation_report_paths: dict[str, Path]
    final_audit_report: QualityAuditReport
    final_export_validations: dict[str, ExportValidationReport]


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
    manifest_reconnect_count: int | None = None,
    started_at: str | None = None,
    ended_at: str | None = None,
    ws_disconnect_count: int = 0,
    gap_flags: tuple[str, ...] = (),
    skew_warning_ms: int = 250,
    skew_severe_ms: int = 1_000,
    asset_ctx_stale_threshold_ms: int = 300_000,
) -> SmokeBatchResult:
    capture_session_id = capture_session_id or new_capture_session_id()
    started_at = started_at or _utc_now_isoformat()
    raw_writer = RawWriter(root=root)
    normalized_writer = NormalizedWriter(root=root)
    quarantine_writer = QuarantineWriter(root=root)

    raw_paths: dict[str, Path] = {}
    normalized_paths: dict[str, Path] = {}
    quarantine_paths: dict[str, Path] = {}
    audit_report_paths: dict[str, Path] = {}
    export_validation_report_paths: dict[str, Path] = {}
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
        symbols=symbols,
        skew_warning_ms=skew_warning_ms,
        skew_severe_ms=skew_severe_ms,
        asset_ctx_stale_threshold_ms=asset_ctx_stale_threshold_ms,
    )
    audit_report_paths = _write_audit_reports(
        root=root,
        trading_date=trading_date,
        report=audit_report,
    )
    manifest = ManifestRecord(
        capture_session_id=capture_session_id,
        started_at=started_at,
        ended_at=ended_at or _utc_now_isoformat(),
        channels=tuple(sorted({message["channel"] for message in messages})),
        symbols=symbols,
        row_count=len(messages),
        ws_disconnect_count=ws_disconnect_count,
        reconnect_count=(
            reconnect_epoch if manifest_reconnect_count is None else manifest_reconnect_count
        ),
        gap_flags=gap_flags,
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
) -> SmokeCampaignResult:
    if session_count < 1:
        raise ValueError("session_count must be at least 1")

    campaign_id = capture_session_id or new_capture_session_id()
    started_at = _utc_now_isoformat()
    sessions: list[SmokeCampaignSession] = []
    final_result: SmokeBatchResult | None = None

    for session_index in range(session_count):
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
                row_count=len(capture.messages),
                ws_disconnect_count=capture.ws_disconnect_count,
                reconnect_count=capture.reconnect_count,
                gap_flags=capture.gap_flags,
            )
        )
        final_result = result

    if final_result is None:
        raise RuntimeError("smoke campaign did not run any sessions")

    report_paths = _campaign_report_paths(
        root=settings.data_root,
        campaign_id=campaign_id,
    )
    result = SmokeCampaignResult(
        campaign_id=campaign_id,
        started_at=started_at,
        ended_at=_utc_now_isoformat(),
        session_count=len(sessions),
        total_row_count=sum(session.row_count for session in sessions),
        sessions=tuple(sessions),
        report_paths=report_paths,
        final_audit_report_paths=final_result.audit_report_paths,
        final_export_validation_report_paths=final_result.export_validation_report_paths,
        final_audit_report=final_result.audit_report,
        final_export_validations=final_result.export_validations,
    )
    _write_campaign_reports(result=result)
    return result


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
        config=config,
        batch_id=batch_id,
        capture_session_id=capture_session_id,
        recv_ts_start=capture.recv_ts_start,
        reconnect_epoch=0,
        manifest_reconnect_count=capture.reconnect_count,
        started_at=capture.started_at,
        ended_at=capture.ended_at,
        ws_disconnect_count=capture.ws_disconnect_count,
        gap_flags=capture.gap_flags,
        skew_warning_ms=settings.smoke_skew_warning_ms,
        skew_severe_ms=settings.smoke_skew_severe_ms,
        asset_ctx_stale_threshold_ms=settings.smoke_asset_ctx_stale_threshold_ms,
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
    requests = build_subscription_requests(config)
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    stop_event = asyncio.Event()
    expected_acks = max(1, len(requests))
    subscription_response_count = 0
    reconnect_count = 0

    def on_message(message: dict[str, Any], _ws: Any) -> None:
        nonlocal reconnect_count, subscription_response_count
        channel = message.get("channel")
        if channel == "subscriptionResponse":
            subscription_response_count += 1
            reconnect_count = max(0, (subscription_response_count // expected_acks) - 1)
            if reconnect_count > reconnect_limit:
                stop_event.set()
            return
        if channel == "pong":
            return
        if channel in config.channels:
            queue.put_nowait(message)
            if queue.qsize() >= max_messages:
                stop_event.set()

    started_at = _utc_now_isoformat()
    recv_ts_start = int(time.time_ns() // 1_000_000)
    messages: list[dict[str, Any]] = []

    async with pybotters.Client() as client:
        app = client.ws_connect(
            _hyperliquid_ws_url(config.market_data_network),
            send_json=requests,
            hdlr_json=on_message,
            heartbeat=float(ping_interval_seconds),
            auth=None,
        )
        deadline = time.monotonic() + max_runtime_seconds
        while (
            len(messages) < max_messages
            and time.monotonic() < deadline
            and not stop_event.is_set()
        ):
            timeout = min(float(ping_interval_seconds), deadline - time.monotonic())
            if timeout <= 0:
                break
            try:
                message = await asyncio.wait_for(queue.get(), timeout=timeout)
            except asyncio.TimeoutError:
                current_ws = app.current_ws
                if current_ws is not None:
                    try:
                        await current_ws.send_json({"method": "ping"})
                    except Exception:
                        pass
                continue
            messages.append(message)
        if app.current_ws is not None:
            await app.current_ws.close()

    gap_flags: list[str] = []
    if reconnect_count:
        gap_flags.append("ws_reconnect")
    if reconnect_count > reconnect_limit:
        gap_flags.append("reconnect_limit_exceeded")
    return SmokeTransportCapture(
        messages=messages,
        recv_ts_start=recv_ts_start,
        started_at=started_at,
        ended_at=_utc_now_isoformat(),
        ws_disconnect_count=reconnect_count,
        reconnect_count=reconnect_count,
        gap_flags=tuple(gap_flags),
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
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        report.model_dump_json(indent=2),
        encoding="utf-8",
    )
    markdown_path.write_text(
        _render_audit_markdown(report),
        encoding="utf-8",
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
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        report.model_dump_json(indent=2),
        encoding="utf-8",
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
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        result.model_dump_json(indent=2),
        encoding="utf-8",
    )
    markdown_path.write_text(
        _render_campaign_markdown(result),
        encoding="utf-8",
    )


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
        "",
    ]
    for table in ("book_events", "book_levels", "trades", "asset_ctx"):
        lines.append(f"## {table}")
        lines.append(f"- row_count: {report.row_counts.get(table, 0)}")
        lines.append(
            f"- quarantine_rate: {report.quarantine_rates.get(table, 0.0):.6f}"
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
        f"- session_count: {result.session_count}",
        f"- total_row_count: {result.total_row_count}",
        "",
        "## Sessions",
    ]
    for session in result.sessions:
        lines.append(f"- {session.capture_session_id}: rows={session.row_count}")
        lines.append(f"  manifest_path={session.manifest_path}")
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


def _hyperliquid_ws_url(network: str) -> str:
    if network == "testnet":
        return "wss://api.hyperliquid-testnet.xyz/ws"
    return "wss://api.hyperliquid.xyz/ws"


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
        settings = _build_settings_from_args(args)
        trading_date = date.fromisoformat(args.date) if args.date is not None else None
        if args.session_count > 1:
            result = asyncio.run(
                run_smoke_campaign(
                    settings=settings,
                    session_count=args.session_count,
                    trading_date=trading_date,
                    transport=transport,
                    capture_session_id=args.capture_session_id,
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
    parser.add_argument("--market-data-network", choices=("mainnet", "testnet"))
    parser.add_argument("--date")
    parser.add_argument("--capture-session-id")
    parser.add_argument("--batch-id", default="0001")
    parser.add_argument("--session-count", type=int, default=1)
    parser.add_argument("--max-runtime-seconds", type=int)
    parser.add_argument("--max-messages", type=int)
    parser.add_argument("--ping-interval-seconds", type=int)
    parser.add_argument("--reconnect-limit", type=int)
    parser.add_argument("--skew-warning-ms", type=int)
    parser.add_argument("--skew-severe-ms", type=int)
    parser.add_argument("--asset-ctx-stale-threshold-ms", type=int)
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
    return Settings(**overrides)


if __name__ == "__main__":
    raise SystemExit(main())
