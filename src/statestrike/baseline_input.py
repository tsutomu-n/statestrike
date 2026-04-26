from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
import shutil

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from statestrike.exports import export_nautilus_catalog
from statestrike.paths import build_normalized_path
from statestrike.storage import _read_parquet_frames, _write_parquet_frame


BASELINE_NORMALIZED_TABLES = ("trades", "book_events", "book_levels", "asset_ctx")


class BaselineInputManifest(BaseModel):
    model_config = ConfigDict(frozen=True)

    source_root: str
    output_root: str
    trading_date: date
    symbols: tuple[str, ...]
    input_sessions: tuple[str, ...]
    removed_duplicate_count: int = Field(ge=0)
    removed_duplicate_classification: str
    unexplained_duplicate_count: int = Field(ge=0)
    session_replay_dedup_applied: bool
    generated_at: str


class BaselineInputResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    output_root: Path
    manifest_path: Path
    manifest: BaselineInputManifest
    nautilus_export_paths: dict[str, Path]


def build_nautilus_baseline_input(
    *,
    source_root: Path,
    output_root: Path,
    trading_date: date,
    symbols: tuple[str, ...],
) -> BaselineInputResult:
    if source_root.resolve() == output_root.resolve():
        raise ValueError("baseline input output_root must be separate from source_root")
    normalized_symbols = tuple(symbol.upper() for symbol in symbols)
    if not normalized_symbols:
        raise ValueError("baseline input requires at least one symbol")

    _copy_supporting_artifacts(
        source_root=source_root,
        output_root=output_root,
        artifact_roots=("capture_log", "manifests", "enriched"),
    )

    removed_duplicate_count = 0
    unexplained_duplicate_count = 0
    input_sessions: set[str] = set()

    for table in BASELINE_NORMALIZED_TABLES:
        for symbol in normalized_symbols:
            frame = _read_source_table(
                source_root=source_root,
                table=table,
                trading_date=trading_date,
                symbol=symbol,
            )
            if frame.empty:
                continue
            if "capture_session_id" in frame:
                input_sessions.update(str(value) for value in frame["capture_session_id"].dropna())
            if table == "trades":
                frame, removed, unexplained = _drop_session_replay_duplicate_trades(frame)
                removed_duplicate_count += removed
                unexplained_duplicate_count += unexplained
            _write_baseline_table(
                output_root=output_root,
                table=table,
                trading_date=trading_date,
                symbol=symbol,
                frame=frame,
            )

    nautilus_export_paths = {
        symbol: export_nautilus_catalog(
            normalized_root=output_root,
            export_root=output_root,
            trading_date=trading_date,
            symbol=symbol,
        )
        for symbol in normalized_symbols
    }
    manifest = BaselineInputManifest(
        source_root=source_root.as_posix(),
        output_root=output_root.as_posix(),
        trading_date=trading_date,
        symbols=normalized_symbols,
        input_sessions=tuple(sorted(input_sessions)),
        removed_duplicate_count=removed_duplicate_count,
        removed_duplicate_classification="session_replay",
        unexplained_duplicate_count=unexplained_duplicate_count,
        session_replay_dedup_applied=True,
        generated_at=_utc_now_isoformat(),
    )
    manifest_path = _write_baseline_input_manifest(
        output_root=output_root,
        trading_date=trading_date,
        manifest=manifest,
    )
    return BaselineInputResult(
        output_root=output_root,
        manifest_path=manifest_path,
        manifest=manifest,
        nautilus_export_paths=nautilus_export_paths,
    )


def _read_source_table(
    *,
    source_root: Path,
    table: str,
    trading_date: date,
    symbol: str,
) -> pd.DataFrame:
    source_dir = build_normalized_path(
        root=source_root,
        channel=table,
        trading_date=trading_date,
        symbol=symbol,
    )
    return _read_parquet_frames(sorted(source_dir.glob("*.parquet")))


def _write_baseline_table(
    *,
    output_root: Path,
    table: str,
    trading_date: date,
    symbol: str,
    frame: pd.DataFrame,
) -> None:
    output_dir = build_normalized_path(
        root=output_root,
        channel=table,
        trading_date=trading_date,
        symbol=symbol,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    existing_files = sorted(output_dir.glob("*.parquet"))
    if existing_files:
        raise ValueError(f"baseline input partition already exists: {output_dir}")
    _write_parquet_frame(path=output_dir / "baseline-input.parquet", frame=frame)


def _drop_session_replay_duplicate_trades(
    frame: pd.DataFrame,
) -> tuple[pd.DataFrame, int, int]:
    if frame.empty or "dedup_hash" not in frame:
        return frame, 0, 0
    sort_columns = [
        column
        for column in (
            "exchange_ts",
            "recv_ts_ns",
            "recv_seq",
            "capture_session_id",
            "trade_event_id",
        )
        if column in frame
    ]
    ordered = frame.sort_values(by=sort_columns).reset_index(drop=True)
    grouped = ordered.groupby("dedup_hash", dropna=False).agg(
        duplicate_count=("dedup_hash", "size"),
        session_count=("capture_session_id", "nunique"),
        reconnect_epoch_count=("reconnect_epoch", "nunique"),
    )
    session_replay_hashes = set(
        grouped[
            (grouped["duplicate_count"] > 1)
            & (grouped["session_count"] > 1)
            & (grouped["reconnect_epoch_count"] <= 1)
        ].index
    )
    remove_mask = (
        ordered["dedup_hash"].isin(session_replay_hashes)
        & ordered.duplicated("dedup_hash", keep="first")
    )
    deduped = ordered.loc[~remove_mask].reset_index(drop=True)
    unexplained_duplicate_count = _count_unexplained_duplicate_trades(deduped)
    return deduped, int(remove_mask.sum()), unexplained_duplicate_count


def _count_unexplained_duplicate_trades(frame: pd.DataFrame) -> int:
    if frame.empty or "dedup_hash" not in frame:
        return 0
    grouped = frame.groupby("dedup_hash", dropna=False).agg(
        duplicate_count=("dedup_hash", "size"),
        session_count=("capture_session_id", "nunique"),
        reconnect_epoch_count=("reconnect_epoch", "nunique"),
    )
    unexplained = grouped[
        (grouped["duplicate_count"] > 1)
        & (grouped["session_count"] <= 1)
        & (grouped["reconnect_epoch_count"] <= 1)
    ]
    if unexplained.empty:
        return 0
    return int((unexplained["duplicate_count"] - 1).sum())


def _copy_supporting_artifacts(
    *,
    source_root: Path,
    output_root: Path,
    artifact_roots: tuple[str, ...],
) -> None:
    for artifact_root in artifact_roots:
        source = source_root / artifact_root
        if not source.exists():
            continue
        destination = output_root / artifact_root
        shutil.copytree(source, destination, dirs_exist_ok=True)


def _write_baseline_input_manifest(
    *,
    output_root: Path,
    trading_date: date,
    manifest: BaselineInputManifest,
) -> Path:
    manifest_dir = output_root / "baseline_input" / f"date={trading_date.isoformat()}"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    path = manifest_dir / "baseline_input_manifest.json"
    path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
    return path


def _utc_now_isoformat() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )
