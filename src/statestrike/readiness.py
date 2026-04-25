from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Literal

import duckdb
from pydantic import BaseModel, ConfigDict, Field
import zstandard

from statestrike.exports import ExportValidationReport, validate_export_bundle
from statestrike.paths import build_export_path, build_normalized_path
from statestrike.quality import QualityAuditReport, run_quality_audit
from statestrike.storage import _parquet_source


ReadinessProfileName = Literal[
    "substrate_ready",
    "nautilus_baseline_ready",
    "funding_aware_ready",
]


class ReadinessProfile(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: ReadinessProfileName
    required_normalized_tables: tuple[str, ...]
    required_truth_exports: tuple[str, ...] = ()
    required_corrected_exports: tuple[str, ...] = ()
    funding_required: bool = True

    @classmethod
    def substrate_ready(cls) -> "ReadinessProfile":
        return cls(
            name="substrate_ready",
            required_normalized_tables=(
                "trades",
                "book_events",
                "book_levels",
                "asset_ctx",
            ),
            funding_required=False,
        )

    @classmethod
    def nautilus_baseline_ready(cls) -> "ReadinessProfile":
        return cls(
            name="nautilus_baseline_ready",
            required_normalized_tables=(
                "trades",
                "book_events",
                "book_levels",
                "asset_ctx",
            ),
            required_truth_exports=("nautilus",),
            required_corrected_exports=("hftbacktest",),
            funding_required=False,
        )

    @classmethod
    def funding_aware_ready(cls) -> "ReadinessProfile":
        return cls(
            name="funding_aware_ready",
            required_normalized_tables=(
                "trades",
                "book_events",
                "book_levels",
                "asset_ctx",
            ),
            required_truth_exports=("nautilus",),
            required_corrected_exports=("hftbacktest",),
            funding_required=True,
        )


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
    profile: ReadinessProfile
    thresholds: BacktestReadinessThresholds
    quality_report: QualityAuditReport
    export_validations: dict[str, ExportValidationReport]
    requested_symbols: tuple[str, ...]
    observed_symbols: tuple[str, ...]
    missing_requested_symbols: tuple[str, ...] = ()
    unexpected_observed_symbols: tuple[str, ...] = ()
    capture_log_file_count: int = Field(ge=0)
    capture_log_row_count: int = Field(ge=0)
    missing_recv_timestamp_count: int = Field(ge=0)
    recv_seq_non_monotonic_count: int = Field(ge=0)
    funding_enrichment_missing_count: int = Field(ge=0)
    max_quarantine_rate: float = Field(ge=0.0)
    truth_corrected_mixed: bool = False
    export_contract_invalid_symbols: tuple[str, ...] = ()
    normalized_table_invalid_symbols: tuple[str, ...] = ()
    export_artifact_invalid_symbols: tuple[str, ...] = ()


def run_backtest_readiness(
    *,
    root: Path,
    trading_date: date,
    symbols: tuple[str, ...],
    thresholds: BacktestReadinessThresholds | None = None,
    profile: ReadinessProfile | ReadinessProfileName | None = None,
) -> BacktestReadinessReport:
    thresholds = thresholds or BacktestReadinessThresholds()
    readiness_profile = _resolve_profile(profile)
    requested_symbols = tuple(symbol.upper() for symbol in symbols)
    observed_symbols = _observed_symbols(root=root, trading_date=trading_date)
    requested_symbol_set = set(requested_symbols)
    observed_symbol_set = set(observed_symbols)
    missing_requested_symbols = tuple(
        symbol for symbol in requested_symbols if symbol not in observed_symbol_set
    )
    unexpected_observed_symbols = tuple(
        symbol for symbol in observed_symbols if symbol not in requested_symbol_set
    )
    quality_report = run_quality_audit(
        normalized_root=root,
        quarantine_root=root,
        trading_date=trading_date,
        symbols=requested_symbols,
    )
    export_validations = {
        symbol: validate_export_bundle(
            export_root=root,
            trading_date=trading_date,
            symbol=symbol,
        )
        for symbol in requested_symbols
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
        symbols=requested_symbols,
    )
    funding_source_unsupported = _funding_source_unsupported_for_dex(
        root=root,
        trading_date=trading_date,
        symbols=requested_symbols,
    )
    max_quarantine_rate = max(quality_report.quarantine_rates.values(), default=0.0)
    truth_corrected_mixed = _truth_corrected_mixed(root=root)
    normalized_table_invalid_symbols = _normalized_table_invalid_symbols(
        root=root,
        trading_date=trading_date,
        symbols=requested_symbols,
        profile=readiness_profile,
    )
    export_artifact_invalid_symbols = _export_artifact_invalid_symbols(
        root=root,
        trading_date=trading_date,
        reports=export_validations,
        profile=readiness_profile,
    )
    export_contract_invalid_symbols = tuple(
        symbol
        for symbol, report in export_validations.items()
        if not _export_contract_is_valid_for_profile(report, profile=readiness_profile)
    )

    blocking_reasons: list[str] = []
    warning_reasons: list[str] = []

    if capture_log_file_count == 0:
        blocking_reasons.append("capture_log_missing")
    if missing_recv_timestamp_count > 0:
        blocking_reasons.append("missing_per_message_recv_timestamp")
    if recv_seq_non_monotonic_count > 0:
        blocking_reasons.append("recv_seq_not_monotonic")
    if missing_requested_symbols:
        blocking_reasons.append("requested_symbol_missing")
    if normalized_table_invalid_symbols:
        blocking_reasons.append("required_normalized_table_missing")
    if export_artifact_invalid_symbols:
        blocking_reasons.append("required_export_artifact_incomplete")
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
    if funding_enrichment_missing_count > 0 and readiness_profile.funding_required:
        blocking_reasons.append("funding_enrichment_incomplete")
    elif funding_enrichment_missing_count > 0:
        warning_reasons.append("funding_enrichment_incomplete")
    if funding_source_unsupported:
        blocking_reasons.append("funding_source_unsupported_for_dex")
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
        profile=readiness_profile,
        thresholds=thresholds,
        quality_report=quality_report,
        export_validations=export_validations,
        requested_symbols=requested_symbols,
        observed_symbols=observed_symbols,
        missing_requested_symbols=missing_requested_symbols,
        unexpected_observed_symbols=unexpected_observed_symbols,
        capture_log_file_count=capture_log_file_count,
        capture_log_row_count=capture_log_row_count,
        missing_recv_timestamp_count=missing_recv_timestamp_count,
        recv_seq_non_monotonic_count=recv_seq_non_monotonic_count,
        funding_enrichment_missing_count=funding_enrichment_missing_count,
        max_quarantine_rate=max_quarantine_rate,
        truth_corrected_mixed=truth_corrected_mixed,
        export_contract_invalid_symbols=export_contract_invalid_symbols,
        normalized_table_invalid_symbols=normalized_table_invalid_symbols,
        export_artifact_invalid_symbols=export_artifact_invalid_symbols,
    )


def _resolve_profile(
    profile: ReadinessProfile | ReadinessProfileName | None,
) -> ReadinessProfile:
    if isinstance(profile, ReadinessProfile):
        return profile
    profile_name = profile or "funding_aware_ready"
    if profile_name == "substrate_ready":
        return ReadinessProfile.substrate_ready()
    if profile_name == "nautilus_baseline_ready":
        return ReadinessProfile.nautilus_baseline_ready()
    if profile_name == "funding_aware_ready":
        return ReadinessProfile.funding_aware_ready()
    raise ValueError(f"unknown readiness profile: {profile_name}")


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
    sidecar_symbols = _funding_sidecar_covered_symbols(
        root=root,
        trading_date=trading_date,
    )
    files: list[Path] = []
    for symbol in symbols:
        if symbol in sidecar_symbols:
            continue
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


def _funding_sidecar_covered_symbols(
    *,
    root: Path,
    trading_date: date,
) -> set[str]:
    path = _funding_sidecar_path(root=root, trading_date=trading_date)
    if not path.exists():
        return set()
    connection = duckdb.connect()
    try:
        source = _parquet_source([path])
        rows = connection.execute(
            f"""
            SELECT DISTINCT symbol
            FROM {source}
            WHERE enrichment_kind = 'predicted_funding'
              AND next_funding_ts IS NOT NULL
              AND funding_interval_hours IS NOT NULL
            """
        ).fetchall()
    finally:
        connection.close()
    return {str(row[0]).upper() for row in rows}


def _funding_source_unsupported_for_dex(
    *,
    root: Path,
    trading_date: date,
    symbols: tuple[str, ...],
) -> bool:
    path = _funding_sidecar_path(root=root, trading_date=trading_date)
    if not path.exists():
        return False
    connection = duckdb.connect()
    try:
        source = _parquet_source([path])
        rows = connection.execute(
            f"""
            SELECT COUNT(*)
            FROM {source}
            WHERE symbol IN ({", ".join("?" for _ in symbols)})
              AND enrichment_kind = 'unsupported'
            """,
            list(symbols),
        ).fetchone()
    finally:
        connection.close()
    return bool(rows and int(rows[0]) > 0)


def _funding_sidecar_path(*, root: Path, trading_date: date) -> Path:
    return (
        root
        / "enriched"
        / "funding_schedule"
        / f"date={trading_date.isoformat()}"
        / "predicted_funding.parquet"
    )


def _observed_symbols(*, root: Path, trading_date: date) -> tuple[str, ...]:
    normalized_root = root / "normalized"
    symbols: set[str] = set()
    for date_root in normalized_root.glob(f"*/date={trading_date.isoformat()}"):
        for symbol_root in date_root.glob("symbol=*"):
            if list(symbol_root.glob("*.parquet")):
                symbols.add(symbol_root.name.removeprefix("symbol=").upper())
    return tuple(sorted(symbols))


def _normalized_table_invalid_symbols(
    *,
    root: Path,
    trading_date: date,
    symbols: tuple[str, ...],
    profile: ReadinessProfile,
) -> tuple[str, ...]:
    invalid_symbols: list[str] = []
    for symbol in symbols:
        for table in profile.required_normalized_tables:
            partition_root = build_normalized_path(
                root=root,
                channel=table,
                trading_date=trading_date,
                symbol=symbol,
            )
            files = sorted(partition_root.glob("*.parquet"))
            if not files or _count_parquet_rows(files) == 0:
                invalid_symbols.append(symbol)
                break
    return tuple(invalid_symbols)


def _export_artifact_invalid_symbols(
    *,
    root: Path,
    trading_date: date,
    reports: dict[str, ExportValidationReport],
    profile: ReadinessProfile,
) -> tuple[str, ...]:
    invalid_symbols: list[str] = []
    for symbol, report in reports.items():
        if not _export_artifacts_are_complete(
            root=root,
            trading_date=trading_date,
            symbol=symbol,
            report=report,
            profile=profile,
        ):
            invalid_symbols.append(symbol)
    return tuple(invalid_symbols)


def _export_artifacts_are_complete(
    *,
    root: Path,
    trading_date: date,
    symbol: str,
    report: ExportValidationReport,
    profile: ReadinessProfile,
) -> bool:
    if "nautilus" in profile.required_truth_exports:
        nautilus_dir = build_export_path(
            root=root,
            category="truth",
            target="nautilus",
            trading_date=trading_date,
            symbol=symbol,
        )
        for table_name, filename in (
            ("instrument", "instrument.parquet"),
            ("trade_ticks", "trade_ticks.parquet"),
            ("book_levels", "book_levels.parquet"),
            ("asset_ctx", "asset_ctx.parquet"),
        ):
            path = nautilus_dir / filename
            validation = report.nautilus_tables.get(table_name)
            if (
                validation is None
                or not path.exists()
                or path.stat().st_size == 0
                or validation.row_count == 0
            ):
                return False
    if "hftbacktest" in profile.required_corrected_exports:
        hft_dir = build_export_path(
            root=root,
            category="corrected",
            target="hftbacktest",
            trading_date=trading_date,
            symbol=symbol,
        )
        path = hft_dir / f"{symbol.lower()}_market_data.npz"
        if (
            not path.exists()
            or path.stat().st_size == 0
            or report.hftbacktest.row_count == 0
        ):
            return False
    return True


def _count_parquet_rows(files: list[Path]) -> int:
    if not files:
        return 0
    connection = duckdb.connect()
    try:
        source = _parquet_source(files)
        return int(connection.execute(f"SELECT COUNT(*) FROM {source}").fetchone()[0])
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


def _export_contract_is_valid_for_profile(
    report: ExportValidationReport,
    *,
    profile: ReadinessProfile,
) -> bool:
    if profile.required_truth_exports or profile.required_corrected_exports:
        return _export_contract_is_valid(report)
    return True
