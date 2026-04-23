from datetime import date
from pathlib import Path

from statestrike.paths import (
    build_export_path,
    build_smoke_campaign_report_path,
    build_export_validation_report_path,
    build_normalized_path,
    build_quality_report_path,
    build_quarantine_path,
    build_raw_path,
)


def test_normalized_path_uses_channel_date_symbol_partitions() -> None:
    path = build_normalized_path(
        root=Path("/data"),
        channel="l2_book",
        trading_date=date(2026, 4, 22),
        symbol="btc",
    )

    assert path == Path("/data/normalized/l2_book/date=2026-04-22/symbol=BTC")


def test_export_path_separates_target_date_and_symbol() -> None:
    path = build_export_path(
        root=Path("/data"),
        target="nautilus",
        trading_date=date(2026, 4, 22),
        symbol="eth",
    )

    assert path == Path("/data/exports/nautilus/date=2026-04-22/symbol=ETH")


def test_raw_path_includes_session_and_jsonl_zst_filename() -> None:
    path = build_raw_path(
        root=Path("/data"),
        channel="trades",
        trading_date=date(2026, 4, 22),
        symbol="btc",
        capture_session_id="018f0dce-7b9f-7b8f-bfd6-65c9a3fe5b1b",
        batch_id="0001",
    )

    assert path == Path(
        "/data/raw_ws/date=2026-04-22/channel=trades/symbol=BTC/"
        "session=018f0dce-7b9f-7b8f-bfd6-65c9a3fe5b1b/batch-0001.jsonl.zst"
    )


def test_quarantine_path_uses_table_date_and_symbol_partitions() -> None:
    path = build_quarantine_path(
        root=Path("/data"),
        table="trades",
        trading_date=date(2026, 4, 22),
        symbol="sol",
    )

    assert path == Path("/data/quarantine/trades/date=2026-04-22/symbol=SOL")


def test_quality_report_path_uses_report_kind_and_date() -> None:
    path = build_quality_report_path(
        root=Path("/data"),
        trading_date=date(2026, 4, 22),
        suffix="json",
    )

    assert path == Path("/data/reports/quality/date=2026-04-22/audit.json")


def test_export_validation_report_path_uses_date_and_symbol() -> None:
    path = build_export_validation_report_path(
        root=Path("/data"),
        trading_date=date(2026, 4, 22),
        symbol="eth",
    )

    assert path == Path(
        "/data/reports/export_validation/date=2026-04-22/symbol=ETH/bundle.json"
    )


def test_smoke_campaign_report_path_uses_campaign_partition() -> None:
    path = build_smoke_campaign_report_path(
        root=Path("/data"),
        campaign_id="session-campaign",
        suffix="json",
    )

    assert path == Path(
        "/data/reports/smoke_campaign/campaign=session-campaign/summary.json"
    )
