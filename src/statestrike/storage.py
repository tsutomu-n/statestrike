from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import zstandard

from statestrike.models import ManifestRecord
from statestrike.paths import (
    build_normalized_path,
    build_quarantine_path,
    build_raw_path,
)


class RawWriter:
    """Writes compressed raw payloads and batch manifests."""

    def __init__(self, *, root: Path) -> None:
        self.root = root

    def write_batch(
        self,
        *,
        trading_date: date,
        channel: str,
        symbol: str,
        capture_session_id: str,
        batch_id: str,
        messages: list[dict[str, Any]],
    ) -> Path:
        path = build_raw_path(
            root=self.root,
            channel=channel,
            trading_date=trading_date,
            symbol=symbol,
            capture_session_id=capture_session_id,
            batch_id=batch_id,
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        with zstandard.open(path, "wt", encoding="utf-8") as handle:
            for message in messages:
                handle.write(json.dumps(message, ensure_ascii=True, separators=(",", ":")))
                handle.write("\n")
        return path

    def write_manifest(
        self,
        *,
        trading_date: date,
        manifest: ManifestRecord,
    ) -> Path:
        manifest_root = (
            self.root
            / "manifests"
            / f"date={trading_date.isoformat()}"
            / f"session={manifest.capture_session_id}"
        )
        manifest_root.mkdir(parents=True, exist_ok=True)
        path = manifest_root / f"{manifest.capture_session_id}.json"
        path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
        return path


class NormalizedWriter:
    """Writes normalized parquet partitions under the phase-1 layout."""

    def __init__(self, *, root: Path) -> None:
        self.root = root

    def write_rows(
        self,
        *,
        table: str,
        trading_date: date,
        symbol: str,
        rows: list[dict[str, Any]],
    ) -> Path:
        base_dir = build_normalized_path(
            root=self.root,
            channel=table,
            trading_date=trading_date,
            symbol=symbol,
        )
        return _write_partitioned_parquet(base_dir=base_dir, rows=rows)


class QuarantineWriter:
    """Writes quarantined rows into table/date/symbol parquet partitions."""

    def __init__(self, *, root: Path) -> None:
        self.root = root

    def write_rows(
        self,
        *,
        table: str,
        trading_date: date,
        symbol: str,
        rows: list[dict[str, Any]],
    ) -> Path:
        base_dir = build_quarantine_path(
            root=self.root,
            table=table,
            trading_date=trading_date,
            symbol=symbol,
        )
        return _write_partitioned_parquet(base_dir=base_dir, rows=rows)


def _write_partitioned_parquet(
    *,
    base_dir: Path,
    rows: list[dict[str, Any]],
) -> Path:
    if not rows:
        raise ValueError("rows must not be empty")

    base_dir.mkdir(parents=True, exist_ok=True)
    path = _next_part_path(base_dir)
    frame = pd.DataFrame(rows)
    _write_parquet_frame(path=path, frame=frame)
    return path


def _next_part_path(base_dir: Path) -> Path:
    next_index = len(list(base_dir.glob("part-*.parquet"))) + 1
    return base_dir / f"part-{next_index:04d}.parquet"


def _write_parquet_frame(*, path: Path, frame: pd.DataFrame) -> None:
    connection = duckdb.connect()
    try:
        connection.register("rows_df", frame)
        escaped_path = path.as_posix().replace("'", "''")
        connection.execute(
            f"COPY rows_df TO '{escaped_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        connection.close()


def _parquet_source(files: list[Path]) -> str:
    escaped = [path.as_posix().replace("'", "''") for path in files]
    if len(escaped) == 1:
        return f"read_parquet('{escaped[0]}')"
    joined = ", ".join(f"'{path}'" for path in escaped)
    return f"read_parquet([{joined}])"
