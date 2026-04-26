from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
import os
import shutil
import subprocess
from typing import Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field
import zstandard

from statestrike.exports import export_nautilus_catalog
from statestrike.paths import build_normalized_path
from statestrike.storage import _read_parquet_frames, _write_parquet_frame


BASELINE_NORMALIZED_TABLES = ("trades", "book_events", "book_levels", "asset_ctx")


class BaselineInputManifest(BaseModel):
    model_config = ConfigDict(frozen=True)

    schema_version: str = "baseline_input_manifest.v1"
    source_root: str
    output_root: str
    source_commit_or_revision: str | None = None
    trading_date: date
    profile: str = "nautilus_baseline_candidate"
    symbols: tuple[str, ...]
    source_sessions: tuple[str, ...]
    input_sessions: tuple[str, ...]
    removed_duplicate_count: int = Field(ge=0)
    removed_duplicate_classification: str
    unexplained_duplicate_count: int = Field(ge=0)
    session_replay_dedup_applied: bool
    assumed_taker_fee_rate: float = Field(default=0.0004, ge=0.0)
    assumed_maker_fee_rate: float = Field(default=0.0004, ge=0.0)
    fill_model_name: str = "project_taker_fill_v1"
    slippage_assumption: float = Field(default=0.0, ge=0.0)
    funding_treatment: Literal["ignored"] = "ignored"
    source_capture_log_row_count: int = Field(ge=0)
    derived_trade_row_count: int = Field(ge=0)
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
    strict: bool = True,
    copy_mode: Literal["copy", "hardlink", "symlink"] = "copy",
    profile: str = "nautilus_baseline_candidate",
    assumed_taker_fee_rate: float = 0.0004,
    assumed_maker_fee_rate: float = 0.0004,
    fill_model_name: str = "project_taker_fill_v1",
    slippage_assumption: float = 0.0,
    funding_treatment: Literal["ignored"] = "ignored",
) -> BaselineInputResult:
    if source_root.resolve() == output_root.resolve():
        raise ValueError("baseline input output_root must be separate from source_root")
    if copy_mode not in {"copy", "hardlink", "symlink"}:
        raise ValueError(f"unsupported baseline input copy_mode: {copy_mode}")
    normalized_symbols = tuple(symbol.upper() for symbol in symbols)
    if not normalized_symbols:
        raise ValueError("baseline input requires at least one symbol")

    _copy_supporting_artifacts(
        source_root=source_root,
        output_root=output_root,
        artifact_roots=("capture_log", "manifests", "enriched"),
        copy_mode=copy_mode,
    )

    removed_duplicate_count = 0
    unexplained_duplicate_count = 0
    derived_trade_row_count = 0
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
                if strict and unexplained_duplicate_count > 0:
                    raise ValueError("baseline input has unexplained duplicate trades")
                derived_trade_row_count += len(frame)
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
        source_commit_or_revision=_source_commit_or_revision(),
        trading_date=trading_date,
        profile=profile,
        symbols=normalized_symbols,
        source_sessions=tuple(sorted(input_sessions)),
        input_sessions=tuple(sorted(input_sessions)),
        removed_duplicate_count=removed_duplicate_count,
        removed_duplicate_classification="session_replay",
        unexplained_duplicate_count=unexplained_duplicate_count,
        session_replay_dedup_applied=True,
        assumed_taker_fee_rate=assumed_taker_fee_rate,
        assumed_maker_fee_rate=assumed_maker_fee_rate,
        fill_model_name=fill_model_name,
        slippage_assumption=slippage_assumption,
        funding_treatment=funding_treatment,
        source_capture_log_row_count=_count_capture_log_rows(
            root=source_root,
            trading_date=trading_date,
        ),
        derived_trade_row_count=derived_trade_row_count,
        generated_at=_utc_now_isoformat(),
    )
    manifest_path = _write_baseline_input_manifest(
        output_root=output_root,
        trading_date=trading_date,
        manifest=manifest,
    )
    if strict:
        _require_warning_free_candidate(
            root=output_root,
            trading_date=trading_date,
            symbols=normalized_symbols,
            profile=profile,
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
    copy_mode: Literal["copy", "hardlink", "symlink"],
) -> None:
    for artifact_root in artifact_roots:
        source = source_root / artifact_root
        if not source.exists():
            continue
        destination = output_root / artifact_root
        if copy_mode == "copy":
            shutil.copytree(source, destination, dirs_exist_ok=True)
        else:
            _linktree(source=source, destination=destination, copy_mode=copy_mode)


def _linktree(
    *,
    source: Path,
    destination: Path,
    copy_mode: Literal["hardlink", "symlink"],
) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    for source_path in source.rglob("*"):
        destination_path = destination / source_path.relative_to(source)
        if source_path.is_dir():
            destination_path.mkdir(parents=True, exist_ok=True)
            continue
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        if destination_path.exists():
            continue
        if copy_mode == "hardlink":
            os.link(source_path, destination_path)
        else:
            destination_path.symlink_to(source_path.resolve())


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


def _count_capture_log_rows(*, root: Path, trading_date: date) -> int:
    capture_root = root / "capture_log" / f"date={trading_date.isoformat()}"
    count = 0
    for path in sorted(capture_root.glob("session=*/capture-log*.jsonl.zst")):
        with zstandard.open(path, "rt", encoding="utf-8") as handle:
            for _ in handle:
                count += 1
    return count


def _require_warning_free_candidate(
    *,
    root: Path,
    trading_date: date,
    symbols: tuple[str, ...],
    profile: str,
) -> None:
    from statestrike.readiness import run_backtest_readiness

    report = run_backtest_readiness(
        root=root,
        trading_date=trading_date,
        symbols=symbols,
        profile=profile,  # type: ignore[arg-type]
    )
    if report.status != "ready" or report.warning_reasons:
        reasons = report.blocking_reasons or report.warning_reasons
        raise ValueError(
            "baseline input strict readiness did not reach warning-free ready: "
            + ", ".join(reasons)
        )


def _source_commit_or_revision() -> str | None:
    repo_root = Path(__file__).resolve().parents[2]
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return completed.stdout.strip() or None
