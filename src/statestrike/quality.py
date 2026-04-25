from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb
from pydantic import BaseModel, ConfigDict, Field

from statestrike.paths import build_normalized_path, build_quarantine_path
from statestrike.storage import _parquet_source

# DuckDB stays here as the analytics query engine over immutable parquet files,
# not as a write-path sink.


class QualityAuditReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    thresholds: "QualityAuditThresholds"
    row_counts: dict[str, int]
    quarantine_row_counts: dict[str, int]
    quarantine_reason_counts: dict[str, dict[str, int]]
    quarantine_rates: dict[str, float]
    quarantine_alerts: dict[str, str]
    gap_count: int = Field(ge=0)
    skew_summary: dict[str, dict[str, int | None]]
    symbol_skew_summary: dict[str, dict[str, dict[str, int | None]]]
    skew_alerts: dict[str, str]
    crossed_book_count: int = Field(ge=0)
    zero_or_negative_qty_count: int = Field(ge=0)
    asset_ctx_stale_count: int = Field(ge=0)
    duplicate_trade_count: int = Field(ge=0)
    book_continuity_gap_count: int = Field(ge=0)
    recoverable_book_gap_count: int = Field(ge=0)
    non_recoverable_book_gap_count: int = Field(ge=0)
    book_epoch_switch_count: int = Field(ge=0)
    non_monotonic_exchange_ts_count: int = Field(ge=0)
    non_monotonic_recv_ts_count: int = Field(ge=0)
    empty_snapshot_count: int = Field(ge=0)
    trade_gap_count: int = Field(ge=0)
    asset_ctx_gap_count: int = Field(ge=0)


class QualityAuditThresholds(BaseModel):
    model_config = ConfigDict(frozen=True)

    skew_warning_ms: int = Field(ge=0)
    skew_severe_ms: int = Field(ge=0)
    asset_ctx_stale_threshold_ms: int = Field(ge=0)
    trade_gap_threshold_ms: int = Field(ge=0)
    quarantine_warning_rate: float = Field(ge=0.0)
    quarantine_severe_rate: float = Field(ge=0.0)


def run_quality_audit(
    *,
    normalized_root: Path,
    quarantine_root: Path | None = None,
    trading_date: date,
    symbols: tuple[str, ...],
    skew_warning_ms: int = 250,
    skew_severe_ms: int = 1_000,
    asset_ctx_stale_threshold_ms: int = 300_000,
    trade_gap_threshold_ms: int = 60_000,
    quarantine_warning_rate: float = 0.001,
    quarantine_severe_rate: float = 0.01,
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
        quarantine_alerts = {
            table: _classify_quarantine_rate(
                rate=quarantine_rates[table],
                warning_rate=quarantine_warning_rate,
                severe_rate=quarantine_severe_rate,
            )
            for table in tables
        }
        quarantine_reason_counts = {
            table: _summarize_quarantine_reasons(
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
        symbol_skew_summary = {
            symbol: {
                table: _summarize_skew(
                    connection=connection,
                    files=_table_files(
                        normalized_root=normalized_root,
                        table=table,
                        trading_date=trading_date,
                        symbols=(symbol,),
                    ),
                )
                for table in ("book_events", "trades", "asset_ctx")
            }
            for symbol in symbols
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
        duplicate_trade_count = _count_duplicate_trades(
            connection=connection,
            files=_table_files(
                normalized_root=normalized_root,
                table="trades",
                trading_date=trading_date,
                symbols=symbols,
            ),
        )
        recoverable_book_gap_count = _count_recoverable_book_gaps(
            connection=connection,
            files=_table_files(
                normalized_root=normalized_root,
                table="book_events",
                trading_date=trading_date,
                symbols=symbols,
            ),
        )
        book_epoch_switch_count = _count_book_epoch_switches(
            connection=connection,
            files=_table_files(
                normalized_root=normalized_root,
                table="book_events",
                trading_date=trading_date,
                symbols=symbols,
            ),
        )
        trade_gap_count = _count_trade_gaps(
            connection=connection,
            files=_table_files(
                normalized_root=normalized_root,
                table="trades",
                trading_date=trading_date,
                symbols=symbols,
            ),
            gap_threshold_ms=trade_gap_threshold_ms,
        )
        asset_ctx_gap_count = _count_asset_ctx_gaps(
            connection=connection,
            files=_table_files(
                normalized_root=normalized_root,
                table="asset_ctx",
                trading_date=trading_date,
                symbols=symbols,
            ),
            gap_threshold_ms=asset_ctx_stale_threshold_ms,
        )
        non_monotonic_exchange_ts_count = sum(
            _count_non_monotonic_timestamps(
                connection=connection,
                files=_table_files(
                    normalized_root=normalized_root,
                    table=table,
                    trading_date=trading_date,
                    symbols=symbols,
                ),
                target_column="exchange_ts",
            )
            for table in ("book_events", "trades", "asset_ctx")
        )
        non_monotonic_recv_ts_count = sum(
            _count_non_monotonic_timestamps(
                connection=connection,
                files=_table_files(
                    normalized_root=normalized_root,
                    table=table,
                    trading_date=trading_date,
                    symbols=symbols,
                ),
                target_column="recv_ts",
            )
            for table in ("book_events", "trades", "asset_ctx")
        )
        empty_snapshot_count = _count_empty_snapshots(
            connection=connection,
            files=_table_files(
                normalized_root=normalized_root,
                table="book_events",
                trading_date=trading_date,
                symbols=symbols,
            ),
        )
    finally:
        connection.close()

    non_recoverable_book_gap_count = _count_non_recoverable_book_gaps_from_manifests(
        manifest_files=_manifest_files(
            root=normalized_root,
            trading_date=trading_date,
        ),
        symbols=symbols,
    )
    book_continuity_gap_count = (
        recoverable_book_gap_count + non_recoverable_book_gap_count
    )

    return QualityAuditReport(
        thresholds=QualityAuditThresholds(
            skew_warning_ms=skew_warning_ms,
            skew_severe_ms=skew_severe_ms,
            asset_ctx_stale_threshold_ms=asset_ctx_stale_threshold_ms,
            trade_gap_threshold_ms=trade_gap_threshold_ms,
            quarantine_warning_rate=quarantine_warning_rate,
            quarantine_severe_rate=quarantine_severe_rate,
        ),
        row_counts=row_counts,
        quarantine_row_counts=quarantine_row_counts,
        quarantine_reason_counts=quarantine_reason_counts,
        quarantine_rates=quarantine_rates,
        quarantine_alerts=quarantine_alerts,
        gap_count=book_continuity_gap_count + trade_gap_count + asset_ctx_gap_count,
        skew_summary=skew_summary,
        symbol_skew_summary=symbol_skew_summary,
        skew_alerts=skew_alerts,
        crossed_book_count=crossed_book_count,
        zero_or_negative_qty_count=non_positive_size_count,
        asset_ctx_stale_count=asset_ctx_stale_count,
        duplicate_trade_count=duplicate_trade_count,
        book_continuity_gap_count=book_continuity_gap_count,
        recoverable_book_gap_count=recoverable_book_gap_count,
        non_recoverable_book_gap_count=non_recoverable_book_gap_count,
        book_epoch_switch_count=book_epoch_switch_count,
        non_monotonic_exchange_ts_count=non_monotonic_exchange_ts_count,
        non_monotonic_recv_ts_count=non_monotonic_recv_ts_count,
        empty_snapshot_count=empty_snapshot_count,
        trade_gap_count=trade_gap_count,
        asset_ctx_gap_count=asset_ctx_gap_count,
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


def _manifest_files(*, root: Path, trading_date: date) -> list[Path]:
    manifest_root = root / "manifests" / f"date={trading_date.isoformat()}"
    if not manifest_root.exists():
        return []
    return sorted(manifest_root.glob("session=*/*.json"))


def _count_rows(*, connection: duckdb.DuckDBPyConnection, files: list[Path]) -> int:
    if not files:
        return 0
    source = _parquet_source(files)
    return int(connection.execute(f"SELECT COUNT(*) FROM {source}").fetchone()[0])


def _summarize_quarantine_reasons(
    *,
    connection: duckdb.DuckDBPyConnection,
    files: list[Path],
) -> dict[str, int]:
    if not files:
        return {}
    source = _parquet_source(files)
    columns = set(connection.execute(f"SELECT * FROM {source} LIMIT 0").fetchdf().columns)
    if "quarantine_reason" not in columns:
        return {}
    rows = connection.execute(
        f"""
        SELECT quarantine_reason, COUNT(*)
        FROM {source}
        GROUP BY 1
        ORDER BY 2 DESC, 1 ASC
        """
    ).fetchall()
    return {str(reason): int(count) for reason, count in rows if reason is not None}


def _summarize_skew(
    *,
    connection: duckdb.DuckDBPyConnection,
    files: list[Path],
) -> dict[str, int | None]:
    if not files:
        return {"min_ms": None, "max_ms": None, "avg_ms": None, "peak_abs_ms": None}
    source = _quality_metric_source(
        connection=connection,
        files=files,
        require_exact_exchange_ts=True,
    )
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


def _classify_quarantine_rate(
    *,
    rate: float,
    warning_rate: float,
    severe_rate: float,
) -> str:
    if rate >= severe_rate:
        return "severe"
    if rate >= warning_rate:
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
    source = _quality_metric_source(
        connection=connection,
        files=files,
        require_exact_exchange_ts=True,
    )
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


def _count_duplicate_trades(
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
            SELECT COALESCE(SUM(duplicate_count - 1), 0)
            FROM (
                SELECT dedup_hash, COUNT(*) AS duplicate_count
                FROM {source}
                GROUP BY 1
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
    )


def _count_recoverable_book_gaps(
    *,
    connection: duckdb.DuckDBPyConnection,
    files: list[Path],
) -> int:
    if not files:
        return 0
    source = _parquet_source(files)
    columns = set(connection.execute(f"SELECT * FROM {source} LIMIT 0").fetchdf().columns)
    if "recovery_classification" not in columns:
        return int(
            connection.execute(
                f"""
                SELECT COUNT(*)
                FROM {source}
                WHERE event_kind = 'recovery_snapshot'
                """
            ).fetchone()[0]
        )
    return int(
        connection.execute(
            f"""
            SELECT COUNT(*)
            FROM {source}
            WHERE event_kind = 'recovery_snapshot'
              AND recovery_classification = 'recoverable'
            """
        ).fetchone()[0]
    )


def _count_book_epoch_switches(
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
            SELECT COALESCE(SUM(epoch_count - 1), 0)
            FROM (
                SELECT symbol, COUNT(DISTINCT book_epoch) AS epoch_count
                FROM {source}
                GROUP BY 1
            )
            """
        ).fetchone()[0]
    )


def _count_non_recoverable_book_gaps_from_manifests(
    *,
    manifest_files: list[Path],
    symbols: tuple[str, ...],
) -> int:
    allowed_symbols = set(symbols)
    count = 0
    for path in manifest_files:
        payload = json.loads(path.read_text(encoding="utf-8"))
        for gap_flag in payload.get("gap_flags", []):
            if not isinstance(gap_flag, str):
                continue
            if not gap_flag.startswith("l2_book_non_recoverable:"):
                continue
            _, _, symbol = gap_flag.partition(":")
            if not allowed_symbols or symbol in allowed_symbols:
                count += 1
    return count


def _count_trade_gaps(
    *,
    connection: duckdb.DuckDBPyConnection,
    files: list[Path],
    gap_threshold_ms: int,
) -> int:
    if not files:
        return 0
    source = _parquet_source(files)
    return int(
        connection.execute(
            f"""
            WITH ordered AS (
                SELECT
                    symbol,
                    exchange_ts,
                    LAG(exchange_ts) OVER (
                        PARTITION BY symbol
                        ORDER BY exchange_ts, recv_ts, dedup_hash
                    ) AS prev_exchange_ts
                FROM {source}
            )
            SELECT COUNT(*)
            FROM ordered
            WHERE prev_exchange_ts IS NOT NULL
              AND exchange_ts - prev_exchange_ts > ?
            """,
            [gap_threshold_ms],
        ).fetchone()[0]
    )


def _count_asset_ctx_gaps(
    *,
    connection: duckdb.DuckDBPyConnection,
    files: list[Path],
    gap_threshold_ms: int,
) -> int:
    if not files:
        return 0
    source = _quality_metric_source(
        connection=connection,
        files=files,
        require_exact_exchange_ts=True,
    )
    return int(
        connection.execute(
            f"""
            WITH ordered AS (
                SELECT
                    symbol,
                    exchange_ts,
                    LAG(exchange_ts) OVER (
                        PARTITION BY symbol
                        ORDER BY exchange_ts, recv_ts, dedup_hash
                    ) AS prev_exchange_ts
                FROM {source}
            )
            SELECT COUNT(*)
            FROM ordered
            WHERE prev_exchange_ts IS NOT NULL
              AND exchange_ts - prev_exchange_ts > ?
            """,
            [gap_threshold_ms],
        ).fetchone()[0]
    )


def _count_non_monotonic_timestamps(
    *,
    connection: duckdb.DuckDBPyConnection,
    files: list[Path],
    target_column: str,
) -> int:
    if not files:
        return 0
    source = _quality_metric_source(
        connection=connection,
        files=files,
        require_exact_exchange_ts=(target_column == "exchange_ts"),
    )
    order_by = (
        "recv_ts, exchange_ts, dedup_hash"
        if target_column == "exchange_ts"
        else "exchange_ts, recv_ts, dedup_hash"
    )
    return int(
        connection.execute(
            f"""
            WITH ordered AS (
                SELECT
                    symbol,
                    {target_column} AS current_value,
                    LAG({target_column}) OVER (
                        PARTITION BY symbol
                        ORDER BY {order_by}
                    ) AS previous_value
                FROM {source}
            )
            SELECT COUNT(*)
            FROM ordered
            WHERE previous_value IS NOT NULL
              AND current_value < previous_value
            """
        ).fetchone()[0]
    )


def _count_empty_snapshots(
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
            SELECT COUNT(*)
            FROM {source}
            WHERE event_kind IN ('snapshot', 'recovery_snapshot')
              AND (n_bids = 0 OR n_asks = 0)
            """
        ).fetchone()[0]
    )


def _quality_metric_source(
    *,
    connection: duckdb.DuckDBPyConnection,
    files: list[Path],
    require_exact_exchange_ts: bool,
) -> str:
    source = _parquet_source(files)
    if not require_exact_exchange_ts:
        return source
    columns = set(connection.execute(f"SELECT * FROM {source} LIMIT 0").fetchdf().columns)
    if "exchange_ts_quality" not in columns:
        return source
    return (
        f"(SELECT * FROM {source} "
        "WHERE exchange_ts_quality = 'exact' AND exchange_ts IS NOT NULL)"
    )
