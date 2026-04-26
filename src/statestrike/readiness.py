from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal

import duckdb
from pydantic import BaseModel, ConfigDict, Field
import zstandard

from statestrike.exports import ExportValidationReport, validate_export_bundle
from statestrike.funding import (
    funding_history_manifest_path,
    funding_history_sidecar_path,
)
from statestrike.paths import build_export_path, build_normalized_path
from statestrike.quality import QualityAuditReport, run_quality_audit
from statestrike.storage import _parquet_source

# DuckDB is retained for readiness queries over parquet artifacts; capture and
# normalized write paths remain file/parquet-writer owned.


ReadinessProfileName = Literal[
    "substrate_ready",
    "nautilus_baseline_ready",
    "nautilus_baseline_candidate",
    "nautilus_funding_candidate",
    "funding_aware_ready",
]


class ReadinessProfile(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: ReadinessProfileName
    required_normalized_tables: tuple[str, ...]
    required_truth_exports: tuple[str, ...] = ()
    required_corrected_exports: tuple[str, ...] = ()
    funding_required: bool = True
    historical_funding_required: bool = False
    allowed_warning_reasons: tuple[str, ...] = ()
    baseline_input_manifest_required: bool = False

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
    def nautilus_baseline_candidate(cls) -> "ReadinessProfile":
        return cls(
            name="nautilus_baseline_candidate",
            required_normalized_tables=(
                "trades",
                "book_events",
                "book_levels",
                "asset_ctx",
            ),
            required_truth_exports=("nautilus",),
            funding_required=False,
            allowed_warning_reasons=(
                "exchange_sorted_recv_inversion_observed",
                "session_replay_duplicate_trade_observed",
            ),
            baseline_input_manifest_required=True,
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

    @classmethod
    def nautilus_funding_candidate(cls) -> "ReadinessProfile":
        return cls(
            name="nautilus_funding_candidate",
            required_normalized_tables=(
                "trades",
                "book_events",
                "book_levels",
                "asset_ctx",
            ),
            required_truth_exports=("nautilus",),
            funding_required=False,
            historical_funding_required=True,
            baseline_input_manifest_required=True,
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
    funding_history_manifest_path: str | None = None
    funding_history_source_type: str | None = None
    funding_history_endpoint_type: str | None = None
    funding_history_row_count: int = Field(default=0, ge=0)
    funding_history_missing_symbols: tuple[str, ...] = ()
    funding_history_unsupported_symbols: tuple[str, ...] = ()
    funding_history_interval_mismatch_symbols: tuple[str, ...] = ()
    funding_history_coverage_incomplete_symbols: tuple[str, ...] = ()
    max_quarantine_rate: float = Field(ge=0.0)
    truth_corrected_mixed: bool = False
    export_contract_invalid_symbols: tuple[str, ...] = ()
    normalized_table_invalid_symbols: tuple[str, ...] = ()
    export_artifact_invalid_symbols: tuple[str, ...] = ()


class _FundingHistoryValidation(BaseModel):
    model_config = ConfigDict(frozen=True)

    blocking_reasons: tuple[str, ...] = ()
    manifest_path: str | None = None
    source_type: str | None = None
    endpoint_type: str | None = None
    row_count: int = Field(default=0, ge=0)
    missing_symbols: tuple[str, ...] = ()
    unsupported_symbols: tuple[str, ...] = ()
    interval_mismatch_symbols: tuple[str, ...] = ()
    coverage_incomplete_symbols: tuple[str, ...] = ()


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
    capture_log_file_count = quality_report.capture_log_file_count
    capture_log_row_count = quality_report.capture_log_row_count
    missing_recv_timestamp_count = (
        quality_report.capture_order_missing_recv_timestamp_count
    )
    recv_seq_non_monotonic_count = (
        quality_report.capture_order_recv_seq_non_monotonic_count
    )
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
    funding_history_validation = _validate_funding_history_sidecar(
        root=root,
        trading_date=trading_date,
        symbols=requested_symbols,
        enabled=readiness_profile.historical_funding_required,
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
    if (
        quality_report.exchange_sorted_recv_inversion_count
        > thresholds.max_non_monotonic_recv_ts_count
    ):
        warning_reasons.append("exchange_sorted_recv_inversion_observed")
    if max_quarantine_rate > thresholds.quarantine_blocking_rate:
        blocking_reasons.append("quarantine_rate_over_blocking_threshold")
    elif max_quarantine_rate > thresholds.quarantine_warning_rate:
        warning_reasons.append("quarantine_rate_over_warning_threshold")
    if quality_report.unexplained_duplicate_trade_count > thresholds.max_duplicate_trade_count:
        blocking_reasons.append("unexplained_duplicate_trade_count_over_threshold")
    else:
        if quality_report.reconnect_replay_duplicate_trade_count > 0:
            warning_reasons.append("reconnect_replay_duplicate_trade_observed")
        if quality_report.session_replay_duplicate_trade_count > 0:
            warning_reasons.append("session_replay_duplicate_trade_observed")
    if (
        quality_report.non_recoverable_book_gap_count
        > thresholds.max_non_recoverable_book_gap_count
    ):
        blocking_reasons.append("non_recoverable_book_gap_count_over_threshold")
    if funding_enrichment_missing_count > 0 and readiness_profile.funding_required:
        blocking_reasons.append("funding_enrichment_incomplete")
    elif (
        funding_enrichment_missing_count > 0
        and not readiness_profile.historical_funding_required
    ):
        warning_reasons.append("funding_enrichment_incomplete")
    if funding_source_unsupported:
        blocking_reasons.append("funding_source_unsupported_for_dex")
    if readiness_profile.historical_funding_required:
        blocking_reasons.extend(funding_history_validation.blocking_reasons)
    if truth_corrected_mixed:
        blocking_reasons.append("truth_corrected_mixed")
    if export_contract_invalid_symbols:
        blocking_reasons.append("export_contract_invalid")
    if readiness_profile.allowed_warning_reasons:
        allowed_warning_reasons = set(readiness_profile.allowed_warning_reasons)
        disallowed_warning_reasons = tuple(
            reason for reason in warning_reasons if reason not in allowed_warning_reasons
        )
        if disallowed_warning_reasons:
            blocking_reasons.append("candidate_disallowed_warning")
    if (
        readiness_profile.baseline_input_manifest_required
        and "session_replay_duplicate_trade_observed" in warning_reasons
        and not _baseline_input_session_replay_dedup_applied(
            root=root,
            trading_date=trading_date,
            symbols=requested_symbols,
        )
    ):
        blocking_reasons.append("baseline_input_session_replay_dedup_missing")

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
        funding_history_manifest_path=funding_history_validation.manifest_path,
        funding_history_source_type=funding_history_validation.source_type,
        funding_history_endpoint_type=funding_history_validation.endpoint_type,
        funding_history_row_count=funding_history_validation.row_count,
        funding_history_missing_symbols=funding_history_validation.missing_symbols,
        funding_history_unsupported_symbols=funding_history_validation.unsupported_symbols,
        funding_history_interval_mismatch_symbols=(
            funding_history_validation.interval_mismatch_symbols
        ),
        funding_history_coverage_incomplete_symbols=(
            funding_history_validation.coverage_incomplete_symbols
        ),
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
    if profile_name == "nautilus_baseline_candidate":
        return ReadinessProfile.nautilus_baseline_candidate()
    if profile_name == "nautilus_funding_candidate":
        return ReadinessProfile.nautilus_funding_candidate()
    if profile_name == "funding_aware_ready":
        return ReadinessProfile.funding_aware_ready()
    raise ValueError(f"unknown readiness profile: {profile_name}")


def _validate_funding_history_sidecar(
    *,
    root: Path,
    trading_date: date,
    symbols: tuple[str, ...],
    enabled: bool,
) -> _FundingHistoryValidation:
    if not enabled:
        return _FundingHistoryValidation()

    manifest_path = funding_history_manifest_path(root=root, trading_date=trading_date)
    history_path = funding_history_sidecar_path(root=root, trading_date=trading_date)
    predicted_path = _funding_sidecar_path(root=root, trading_date=trading_date)
    if not manifest_path.exists() or not history_path.exists():
        reason = (
            "predicted_funding_used_for_historical_baseline"
            if predicted_path.exists()
            else "funding_sidecar_missing"
        )
        return _FundingHistoryValidation(blocking_reasons=(reason,))

    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return _FundingHistoryValidation(
            blocking_reasons=("funding_source_not_official",),
            manifest_path=manifest_path.as_posix(),
        )

    source_type = payload.get("source_type")
    endpoint_type = payload.get("endpoint_type")
    blocking_reasons: list[str] = []
    if source_type != "fundingHistory" or endpoint_type != "official_info":
        blocking_reasons.append("funding_source_not_official")

    start_time_ms = int(payload.get("start_time_ms", _trading_date_start_ms(trading_date)))
    end_time_ms = int(payload.get("end_time_ms", _trading_date_start_ms(trading_date) + 86_400_000))
    manifest_interval_hours = int(payload.get("funding_interval_hours", 0) or 0)
    row_count = int(payload.get("row_count", 0) or 0)
    unsupported_symbol_count = int(payload.get("unsupported_symbol_count", 0) or 0)
    unsupported_symbols = tuple(symbols) if unsupported_symbol_count > 0 else ()

    rows = _funding_history_rows_by_symbol(path=history_path, symbols=symbols)
    expected_bucket_count = _expected_funding_bucket_count(
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
    )
    start_bucket = start_time_ms // 3_600_000
    end_bucket = (end_time_ms - 1) // 3_600_000
    missing_symbols: list[str] = []
    interval_mismatch_symbols: list[str] = []
    coverage_incomplete_symbols: list[str] = []
    for symbol in symbols:
        row = rows.get(symbol)
        if row is None or row["row_count"] == 0:
            missing_symbols.append(symbol)
            continue
        if manifest_interval_hours != 1 or row["interval_mismatch_count"] > 0:
            interval_mismatch_symbols.append(symbol)
        if (
            row["hour_bucket_count"] < expected_bucket_count
            or row["min_hour_bucket"] > start_bucket
            or row["max_hour_bucket"] < end_bucket
        ):
            coverage_incomplete_symbols.append(symbol)

    if missing_symbols or unsupported_symbols:
        blocking_reasons.append("funding_symbol_coverage_incomplete")
    if coverage_incomplete_symbols:
        blocking_reasons.append("funding_history_incomplete")
    if interval_mismatch_symbols:
        blocking_reasons.append("funding_interval_mismatch")

    return _FundingHistoryValidation(
        blocking_reasons=tuple(dict.fromkeys(blocking_reasons)),
        manifest_path=manifest_path.as_posix(),
        source_type=str(source_type) if source_type is not None else None,
        endpoint_type=str(endpoint_type) if endpoint_type is not None else None,
        row_count=row_count,
        missing_symbols=tuple(missing_symbols),
        unsupported_symbols=unsupported_symbols,
        interval_mismatch_symbols=tuple(interval_mismatch_symbols),
        coverage_incomplete_symbols=tuple(coverage_incomplete_symbols),
    )


def _funding_history_rows_by_symbol(
    *,
    path: Path,
    symbols: tuple[str, ...],
) -> dict[str, dict[str, int]]:
    if not symbols:
        return {}
    connection = duckdb.connect()
    try:
        source = _parquet_source([path])
        rows = connection.execute(
            f"""
            SELECT
                symbol,
                COUNT(*) AS row_count,
                COUNT(DISTINCT floor(funding_time_ms / 3600000)) AS hour_bucket_count,
                MIN(funding_time_ms) AS min_timestamp_ms,
                MAX(funding_time_ms) AS max_timestamp_ms,
                MIN(floor(funding_time_ms / 3600000)) AS min_hour_bucket,
                MAX(floor(funding_time_ms / 3600000)) AS max_hour_bucket,
                SUM(CASE WHEN funding_interval_hours != 1 THEN 1 ELSE 0 END)
                    AS interval_mismatch_count
            FROM {source}
            WHERE symbol IN ({", ".join("?" for _ in symbols)})
            GROUP BY symbol
            """,
            list(symbols),
        ).fetchall()
    finally:
        connection.close()
    return {
        str(symbol).upper(): {
            "row_count": int(row_count),
            "hour_bucket_count": int(hour_bucket_count),
            "min_timestamp_ms": int(min_timestamp_ms),
            "max_timestamp_ms": int(max_timestamp_ms),
            "min_hour_bucket": int(min_hour_bucket),
            "max_hour_bucket": int(max_hour_bucket),
            "interval_mismatch_count": int(interval_mismatch_count or 0),
        }
        for (
            symbol,
            row_count,
            hour_bucket_count,
            min_timestamp_ms,
            max_timestamp_ms,
            min_hour_bucket,
            max_hour_bucket,
            interval_mismatch_count,
        ) in rows
    }


def _expected_funding_bucket_count(*, start_time_ms: int, end_time_ms: int) -> int:
    return max(1, int((end_time_ms - start_time_ms) // 3_600_000))


def _trading_date_start_ms(trading_date: date) -> int:
    return int(
        datetime.combine(
            trading_date,
            datetime.min.time(),
            tzinfo=UTC,
        ).timestamp()
        * 1000
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


def _baseline_input_session_replay_dedup_applied(
    *,
    root: Path,
    trading_date: date,
    symbols: tuple[str, ...],
) -> bool:
    path = (
        root
        / "baseline_input"
        / f"date={trading_date.isoformat()}"
        / "baseline_input_manifest.json"
    )
    if not path.exists():
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return False
    manifest_symbols = {
        str(symbol).upper() for symbol in payload.get("symbols", [])
    }
    requested_symbols = {symbol.upper() for symbol in symbols}
    return (
        bool(payload.get("session_replay_dedup_applied"))
        and requested_symbols <= manifest_symbols
        and payload.get("removed_duplicate_classification") == "session_replay"
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
    for target in profile.required_truth_exports:
        artifact = report.truth_exports.get(target)
        if artifact is None:
            return False
        if artifact.category != "truth" or not artifact.truth_preserving:
            return False
        if artifact.correction_applied:
            return False
    for target in profile.required_corrected_exports:
        artifact = report.corrected_exports.get(target)
        if artifact is None:
            return False
        if artifact.category != "corrected" or artifact.truth_preserving:
            return False
        if not artifact.correction_applied:
            return False
    if profile.required_truth_exports and profile.required_corrected_exports:
        if set(profile.required_truth_exports) & set(profile.required_corrected_exports):
            return False
    return True
