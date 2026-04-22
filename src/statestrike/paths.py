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
