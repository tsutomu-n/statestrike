from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Literal

import duckdb
from pydantic import BaseModel, ConfigDict, Field
import zstandard

from statestrike.exports import ExportValidationReport, validate_export_bundle
from statestrike.paths import build_normalized_path
from statestrike.quality import QualityAuditReport, run_quality_audit
from statestrike.storage import _parquet_source


class BacktestReadinessThresholds(BaseModel):
    model_config = ConfigDict(frozen=True)

    max_non_monotonic_recv_ts_count: int = Field(default=0, ge=0)
    quarantine_warning_rate: float = Field(default=0.01, ge=0.0)
    quarantine_blocking_rate: float = Field(default=0.05, ge=0.0)
    max_duplicate_trade_count: int = Field(default=0, ge=0)
    max_non_recoverable_book_gap_count: int = Field(default=0, ge=0)


class BacktestReadinessReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    status: Literal["ready", "warning", "blocked"]
    blocking_reasons: tuple[str, ...] = ()
    warning_reasons: tuple[str, ...] = ()
    thresholds: BacktestReadinessThresholds
    quality_report: QualityAuditReport
    export_validations: dict[str, ExportValidationReport]
    capture_log_file_count: int = Field(ge=0)
    capture_log_row_count: int = Field(ge=0)
    missing_recv_timestamp_count: int = Field(ge=0)
    recv_seq_non_monotonic_count: int = Field(ge=0)
    funding_enrichment_missing_count: int = Field(ge=0)
    max_quarantine_rate: float = Field(ge=0.0)
    truth_corrected_mixed: bool = False
    export_contract_invalid_symbols: tuple[str, ...] = ()


def run_backtest_readiness(
    *,
    root: Path,
    trading_date: date,
    symbols: tuple[str, ...],
    thresholds: BacktestReadinessThresholds | None = None,
) -> BacktestReadinessReport:
    thresholds = thresholds or BacktestReadinessThresholds()
    quality_report = run_quality_audit(
        normalized_root=root,
        quarantine_root=root,
        trading_date=trading_date,
        symbols=symbols,
    )
    export_validations = {
        symbol: validate_export_bundle(
            export_root=root,
            trading_date=trading_date,
            symbol=symbol,
        )
        for symbol in symbols
    }
    (
        capture_log_file_count,
        capture_log_row_count,
        missing_recv_timestamp_count,
        recv_seq_non_monotonic_count,
    ) = _inspect_capture_logs(root=root, trading_date=trading_date)
    funding_enrichment_missing_count = _count_missing_funding_enrichment(
        root=root,
        trading_date=trading_date,
        symbols=symbols,
    )
    max_quarantine_rate = max(quality_report.quarantine_rates.values(), default=0.0)
    truth_corrected_mixed = _truth_corrected_mixed(root=root)
    export_contract_invalid_symbols = tuple(
        symbol
        for symbol, report in export_validations.items()
        if not _export_contract_is_valid(report)
    )

    blocking_reasons: list[str] = []
    warning_reasons: list[str] = []

    if capture_log_file_count == 0:
        blocking_reasons.append("capture_log_missing")
    if missing_recv_timestamp_count > 0:
        blocking_reasons.append("missing_per_message_recv_timestamp")
    if recv_seq_non_monotonic_count > 0:
        blocking_reasons.append("recv_seq_not_monotonic")
    if quality_report.non_monotonic_recv_ts_count > thresholds.max_non_monotonic_recv_ts_count:
        blocking_reasons.append("non_monotonic_recv_ts_over_threshold")
    if max_quarantine_rate > thresholds.quarantine_blocking_rate:
        blocking_reasons.append("quarantine_rate_over_blocking_threshold")
    elif max_quarantine_rate > thresholds.quarantine_warning_rate:
        warning_reasons.append("quarantine_rate_over_warning_threshold")
    if quality_report.duplicate_trade_count > thresholds.max_duplicate_trade_count:
        blocking_reasons.append("duplicate_trade_count_over_threshold")
    if (
        quality_report.non_recoverable_book_gap_count
        > thresholds.max_non_recoverable_book_gap_count
    ):
        blocking_reasons.append("non_recoverable_book_gap_count_over_threshold")
    if funding_enrichment_missing_count > 0:
        blocking_reasons.append("funding_enrichment_incomplete")
    if truth_corrected_mixed:
        blocking_reasons.append("truth_corrected_mixed")
    if export_contract_invalid_symbols:
        blocking_reasons.append("export_contract_invalid")

    status: Literal["ready", "warning", "blocked"]
    if blocking_reasons:
        status = "blocked"
    elif warning_reasons:
        status = "warning"
    else:
        status = "ready"

    return BacktestReadinessReport(
        status=status,
        blocking_reasons=tuple(blocking_reasons),
        warning_reasons=tuple(warning_reasons),
        thresholds=thresholds,
        quality_report=quality_report,
        export_validations=export_validations,
        capture_log_file_count=capture_log_file_count,
        capture_log_row_count=capture_log_row_count,
        missing_recv_timestamp_count=missing_recv_timestamp_count,
        recv_seq_non_monotonic_count=recv_seq_non_monotonic_count,
        funding_enrichment_missing_count=funding_enrichment_missing_count,
        max_quarantine_rate=max_quarantine_rate,
        truth_corrected_mixed=truth_corrected_mixed,
        export_contract_invalid_symbols=export_contract_invalid_symbols,
    )


def _inspect_capture_logs(
    *,
    root: Path,
    trading_date: date,
) -> tuple[int, int, int, int]:
    capture_root = root / "capture_log" / f"date={trading_date.isoformat()}"
    files = sorted(capture_root.glob("session=*/capture-log*.jsonl.zst"))
    file_count = len(files)
    row_count = 0
    missing_recv_timestamp_count = 0
    recv_seq_non_monotonic_count = 0
    for path in files:
        previous_recv_seq: int | None = None
        with zstandard.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                row_count += 1
                payload = json.loads(line)
                ingress = payload.get("ingress", {})
                recv_wall_ns = ingress.get("recv_wall_ns")
                recv_seq = ingress.get("recv_seq")
                if recv_wall_ns is None:
                    missing_recv_timestamp_count += 1
                if recv_seq is None:
                    recv_seq_non_monotonic_count += 1
                    continue
                recv_seq_value = int(recv_seq)
                if previous_recv_seq is not None and recv_seq_value <= previous_recv_seq:
                    recv_seq_non_monotonic_count += 1
                previous_recv_seq = recv_seq_value
    return (
        file_count,
        row_count,
        missing_recv_timestamp_count,
        recv_seq_non_monotonic_count,
    )


def _count_missing_funding_enrichment(
    *,
    root: Path,
    trading_date: date,
    symbols: tuple[str, ...],
) -> int:
    files: list[Path] = []
    for symbol in symbols:
        partition_root = build_normalized_path(
            root=root,
            channel="asset_ctx",
            trading_date=trading_date,
            symbol=symbol,
        )
        files.extend(sorted(partition_root.glob("*.parquet")))
    if not files:
        return 0
    connection = duckdb.connect()
    try:
        source = _parquet_source(files)
        return int(
            connection.execute(
                f"""
                SELECT COUNT(*)
                FROM {source}
                WHERE next_funding_ts IS NULL
                """
            ).fetchone()[0]
        )
    finally:
        connection.close()


def _truth_corrected_mixed(*, root: Path) -> bool:
    legacy_truth = (root / "exports" / "nautilus").exists()
    legacy_corrected = (root / "exports" / "hftbacktest").exists()
    return legacy_truth or legacy_corrected


def _export_contract_is_valid(report: ExportValidationReport) -> bool:
    if "nautilus" not in report.truth_exports:
        return False
    if "hftbacktest" not in report.corrected_exports:
        return False

    truth_targets = set(report.truth_exports)
    corrected_targets = set(report.corrected_exports)
    if truth_targets & corrected_targets:
        return False

    for artifact in report.truth_exports.values():
        if artifact.category != "truth" or not artifact.truth_preserving:
            return False
        if artifact.correction_applied:
            return False
    for artifact in report.corrected_exports.values():
        if artifact.category != "corrected" or artifact.truth_preserving:
            return False
        if not artifact.correction_applied:
            return False
    return True
