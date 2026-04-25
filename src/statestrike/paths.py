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


def build_derived_capture_path(
    *,
    root: Path,
    channel: str,
    trading_date: date,
    symbol: str,
    capture_session_id: str,
    batch_id: str,
) -> Path:
    """Builds the derived per-channel raw payload path.

    The on-disk `raw_ws` directory is kept for compatibility, but this artifact is
    derived from the session-global `capture_log` and must not be treated as truth
    capture.
    """
    return (
        root
        / "raw_ws"
        / f"date={trading_date.isoformat()}"
        / f"channel={channel}"
        / f"symbol={symbol.upper()}"
        / f"session={capture_session_id}"
        / f"batch-{batch_id}.jsonl.zst"
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
    """Legacy alias for `build_derived_capture_path()`."""
    return build_derived_capture_path(
        root=root,
        channel=channel,
        trading_date=trading_date,
        symbol=symbol,
        capture_session_id=capture_session_id,
        batch_id=batch_id,
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
        / "capture-log.jsonl.zst"
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
