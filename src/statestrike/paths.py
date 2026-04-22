from __future__ import annotations

from datetime import date
from pathlib import Path


def build_normalized_path(
    *,
    root: Path,
    channel: str,
    trading_date: date,
    symbol: str,
) -> Path:
    return (
        root
        / "normalized"
        / channel
        / f"date={trading_date.isoformat()}"
        / f"symbol={symbol.upper()}"
    )


def build_export_path(
    *,
    root: Path,
    target: str,
    trading_date: date,
    symbol: str,
) -> Path:
    return (
        root
        / "exports"
        / target
        / f"date={trading_date.isoformat()}"
        / f"symbol={symbol.upper()}"
    )


def build_raw_path(
    *,
    root: Path,
    channel: str,
    trading_date: date,
    symbol: str,
    capture_session_id: str,
    batch_id: str,
) -> Path:
    return (
        root
        / "raw_ws"
        / f"date={trading_date.isoformat()}"
        / f"channel={channel}"
        / f"symbol={symbol.upper()}"
        / f"session={capture_session_id}"
        / f"batch-{batch_id}.jsonl.zst"
    )


def build_quarantine_path(
    *,
    root: Path,
    table: str,
    trading_date: date,
    symbol: str,
) -> Path:
    return (
        root
        / "quarantine"
        / table
        / f"date={trading_date.isoformat()}"
        / f"symbol={symbol.upper()}"
    )
