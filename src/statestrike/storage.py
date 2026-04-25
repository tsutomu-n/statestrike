from __future__ import annotations

import json
from datetime import date
from pathlib import Path
import secrets
import time
from typing import Any

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import zstandard

from statestrike.models import ManifestRecord
from statestrike.paths import (
    build_capture_log_path,
    build_normalized_path,
    build_quarantine_path,
    build_raw_path,
)
from statestrike.recovery import MessageCaptureContext, MessageIngressMeta


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


class CaptureLogWriter:
    """Writes session-global ordered capture logs with ingress metadata."""

    def __init__(self, *, root: Path) -> None:
        self.root = root

    def write_batch(
        self,
        *,
        trading_date: date,
        capture_session_id: str,
        batch_id: str,
        messages: list[dict[str, Any]],
        ingress_metadata: list[MessageIngressMeta] | tuple[MessageIngressMeta, ...],
        message_contexts: list[MessageCaptureContext] | tuple[MessageCaptureContext, ...],
    ) -> Path:
        path = build_capture_log_path(
            root=self.root,
            trading_date=trading_date,
            capture_session_id=capture_session_id,
            batch_id=batch_id,
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        with zstandard.open(path, "wt", encoding="utf-8") as handle:
            for message, ingress_meta, message_context in zip(
                messages, ingress_metadata, message_contexts, strict=True
            ):
                row = {
                    "message": message,
                    "ingress": ingress_meta.model_dump(mode="json"),
                    "message_context": message_context.model_dump(mode="json"),
                }
                handle.write(json.dumps(row, ensure_ascii=True, separators=(",", ":")))
                handle.write("\n")
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
    while True:
        candidate = base_dir / (
            f"part-{time.time_ns()}-{secrets.token_hex(4)}.parquet"
        )
        if not candidate.exists():
            return candidate


def _write_parquet_frame(*, path: Path, frame: pd.DataFrame) -> None:
    table = pa.Table.from_pandas(frame, preserve_index=False)
    pq.write_table(table, path, compression="zstd", use_dictionary=False)


def _read_parquet_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    table = pq.ParquetFile(path).read()
    return table.to_pandas()


def _read_parquet_frames(files: list[Path]) -> pd.DataFrame:
    if not files:
        return pd.DataFrame()
    frames = [_read_parquet_frame(path) for path in files]
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


def _parquet_source(files: list[Path]) -> str:
    escaped = [path.as_posix().replace("'", "''") for path in files]
    if len(escaped) == 1:
        return f"read_parquet('{escaped[0]}', union_by_name=True)"
    joined = ", ".join(f"'{path}'" for path in escaped)
    return f"read_parquet([{joined}], union_by_name=True)"
