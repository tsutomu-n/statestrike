from datetime import date
from pathlib import Path

from statestrike.paths import build_export_path, build_normalized_path


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
