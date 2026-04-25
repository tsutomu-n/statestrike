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
    category: str,
    target: str,
    trading_date: date,
    symbol: str,
) -> Path:
    return (
        root
        / "exports"
        / category
        / target
        / f"date={trading_date.isoformat()}"
        / f"symbol={symbol.upper()}"
    )


def build_quality_report_path(
    *,
    root: Path,
    trading_date: date,
    suffix: str,
) -> Path:
    return (
        root
        / "reports"
        / "quality"
        / f"date={trading_date.isoformat()}"
        / f"audit.{suffix}"
    )


def build_export_validation_report_path(
    *,
    root: Path,
    trading_date: date,
    symbol: str,
) -> Path:
    return (
        root
        / "reports"
        / "export_validation"
        / f"date={trading_date.isoformat()}"
        / f"symbol={symbol.upper()}"
        / "bundle.json"
    )


def build_smoke_campaign_report_path(
    *,
    root: Path,
    campaign_id: str,
    suffix: str,
) -> Path:
    return (
        root
        / "reports"
        / "smoke_campaign"
        / f"campaign={campaign_id}"
        / f"summary.{suffix}"
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


def build_capture_log_path(
    *,
    root: Path,
    trading_date: date,
    capture_session_id: str,
    batch_id: str,
) -> Path:
    return (
        root
        / "capture_log"
        / f"date={trading_date.isoformat()}"
        / f"session={capture_session_id}"
        / f"capture-log-{batch_id}.jsonl.zst"
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
