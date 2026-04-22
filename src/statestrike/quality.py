from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
from pydantic import BaseModel, ConfigDict, Field

from statestrike.paths import build_normalized_path, build_quarantine_path
from statestrike.storage import _parquet_source


class QualityAuditReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    row_counts: dict[str, int]
    quarantine_row_counts: dict[str, int]
    quarantine_rates: dict[str, float]
    gap_count: int = Field(ge=0)
    skew_summary: dict[str, dict[str, int | None]]
    skew_alerts: dict[str, str]
    crossed_book_count: int = Field(ge=0)
    zero_or_negative_qty_count: int = Field(ge=0)
    asset_ctx_stale_count: int = Field(ge=0)


def run_quality_audit(
    *,
    normalized_root: Path,
    quarantine_root: Path | None = None,
    trading_date: date,
    symbols: tuple[str, ...],
    skew_warning_ms: int = 250,
    skew_severe_ms: int = 1_000,
    asset_ctx_stale_threshold_ms: int = 300_000,
) -> QualityAuditReport:
    connection = duckdb.connect()
    try:
        tables = ("book_events", "book_levels", "trades", "asset_ctx")
        row_counts = {
            table: _count_rows(
                connection=connection,
                files=_table_files(
                    normalized_root=normalized_root,
                    table=table,
                    trading_date=trading_date,
                    symbols=symbols,
                ),
            )
            for table in tables
        }
        quarantine_row_counts = {
            table: _count_rows(
                connection=connection,
                files=_quarantine_files(
                    quarantine_root=quarantine_root,
                    table=table,
                    trading_date=trading_date,
                    symbols=symbols,
                ),
            )
            for table in tables
        }
        quarantine_rates = {
            table: _calculate_quarantine_rate(
                valid_count=row_counts[table],
                quarantined_count=quarantine_row_counts[table],
            )
            for table in tables
        }
        skew_summary = {
            table: _summarize_skew(
                connection=connection,
                files=_table_files(
                    normalized_root=normalized_root,
                    table=table,
                    trading_date=trading_date,
                    symbols=symbols,
                ),
            )
            for table in ("book_events", "trades", "asset_ctx")
        }
        skew_alerts = {
            table: _classify_skew(
                summary=skew_summary[table],
                warning_ms=skew_warning_ms,
                severe_ms=skew_severe_ms,
            )
            for table in skew_summary
        }
        crossed_book_count = _count_crossed_books(
            connection=connection,
            files=_table_files(
                normalized_root=normalized_root,
                table="book_levels",
                trading_date=trading_date,
                symbols=symbols,
            ),
        )
        non_positive_size_count = (
            _count_non_positive_values(
                connection=connection,
                files=_table_files(
                    normalized_root=normalized_root,
                    table="book_levels",
                    trading_date=trading_date,
                    symbols=symbols,
                ),
                column_name="size",
            )
            + _count_non_positive_values(
                connection=connection,
                files=_table_files(
                    normalized_root=normalized_root,
                    table="trades",
                    trading_date=trading_date,
                    symbols=symbols,
                ),
                column_name="size",
            )
        )
        asset_ctx_stale_count = _count_stale_asset_ctx(
            connection=connection,
            files=_table_files(
                normalized_root=normalized_root,
                table="asset_ctx",
                trading_date=trading_date,
                symbols=symbols,
            ),
            stale_threshold_ms=asset_ctx_stale_threshold_ms,
        )
    finally:
        connection.close()

    return QualityAuditReport(
        row_counts=row_counts,
        quarantine_row_counts=quarantine_row_counts,
        quarantine_rates=quarantine_rates,
        gap_count=0,
        skew_summary=skew_summary,
        skew_alerts=skew_alerts,
        crossed_book_count=crossed_book_count,
        zero_or_negative_qty_count=non_positive_size_count,
        asset_ctx_stale_count=asset_ctx_stale_count,
    )


def _table_files(
    *,
    normalized_root: Path,
    table: str,
    trading_date: date,
    symbols: tuple[str, ...],
) -> list[Path]:
    files: list[Path] = []
    for symbol in symbols:
        partition_root = build_normalized_path(
            root=normalized_root,
            channel=table,
            trading_date=trading_date,
            symbol=symbol,
        )
        files.extend(sorted(partition_root.glob("*.parquet")))
    return files


def _quarantine_files(
    *,
    quarantine_root: Path | None,
    table: str,
    trading_date: date,
    symbols: tuple[str, ...],
) -> list[Path]:
    if quarantine_root is None:
        return []
    files: list[Path] = []
    for symbol in symbols:
        partition_root = build_quarantine_path(
            root=quarantine_root,
            table=table,
            trading_date=trading_date,
            symbol=symbol,
        )
        files.extend(sorted(partition_root.glob("*.parquet")))
    return files


def _count_rows(*, connection: duckdb.DuckDBPyConnection, files: list[Path]) -> int:
    if not files:
        return 0
    source = _parquet_source(files)
    return int(connection.execute(f"SELECT COUNT(*) FROM {source}").fetchone()[0])


def _summarize_skew(
    *,
    connection: duckdb.DuckDBPyConnection,
    files: list[Path],
) -> dict[str, int | None]:
    if not files:
        return {"min_ms": None, "max_ms": None, "avg_ms": None, "peak_abs_ms": None}
    source = _parquet_source(files)
    min_ms, max_ms, avg_ms = connection.execute(
        f"""
        SELECT
            MIN(recv_ts - exchange_ts),
            MAX(recv_ts - exchange_ts),
            AVG(recv_ts - exchange_ts)
        FROM {source}
        """
    ).fetchone()
    return {
        "min_ms": int(min_ms) if min_ms is not None else None,
        "max_ms": int(max_ms) if max_ms is not None else None,
        "avg_ms": int(round(avg_ms)) if avg_ms is not None else None,
        "peak_abs_ms": (
            max(abs(int(min_ms)), abs(int(max_ms)))
            if min_ms is not None and max_ms is not None
            else None
        ),
    }


def _calculate_quarantine_rate(*, valid_count: int, quarantined_count: int) -> float:
    total = valid_count + quarantined_count
    if total == 0:
        return 0.0
    return quarantined_count / total


def _classify_skew(
    *,
    summary: dict[str, int | None],
    warning_ms: int,
    severe_ms: int,
) -> str:
    peak_abs_ms = summary.get("peak_abs_ms")
    if peak_abs_ms is None:
        return "empty"
    if peak_abs_ms >= severe_ms:
        return "severe"
    if peak_abs_ms >= warning_ms:
        return "warning"
    return "ok"


def _count_crossed_books(
    *,
    connection: duckdb.DuckDBPyConnection,
    files: list[Path],
) -> int:
    if not files:
        return 0
    source = _parquet_source(files)
    return int(
        connection.execute(
            f"""
            WITH best_quotes AS (
                SELECT
                    book_event_id,
                    MAX(CASE WHEN side = 'bid' THEN price END) AS best_bid,
                    MIN(CASE WHEN side = 'ask' THEN price END) AS best_ask
                FROM {source}
                GROUP BY 1
            )
            SELECT COUNT(*)
            FROM best_quotes
            WHERE best_bid IS NOT NULL
              AND best_ask IS NOT NULL
              AND best_bid >= best_ask
            """
        ).fetchone()[0]
    )


def _count_non_positive_values(
    *,
    connection: duckdb.DuckDBPyConnection,
    files: list[Path],
    column_name: str,
) -> int:
    if not files:
        return 0
    source = _parquet_source(files)
    return int(
        connection.execute(
            f"SELECT COUNT(*) FROM {source} WHERE {column_name} <= 0"
        ).fetchone()[0]
    )


def _count_stale_asset_ctx(
    *,
    connection: duckdb.DuckDBPyConnection,
    files: list[Path],
    stale_threshold_ms: int,
) -> int:
    if not files:
        return 0
    source = _parquet_source(files)
    return int(
        connection.execute(
            f"""
            SELECT COUNT(*)
            FROM {source}
            WHERE ABS(recv_ts - exchange_ts) > ?
            """,
            [stale_threshold_ms],
        ).fetchone()[0]
    )
