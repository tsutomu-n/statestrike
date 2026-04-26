from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta

from statestrike.funding import (
    build_funding_history_sidecar,
    parse_funding_history,
)
from statestrike.storage import _read_parquet_frame


def funding_history_payload(symbol: str, trading_date: date) -> list[dict[str, str | int]]:
    start = datetime.combine(trading_date, datetime.min.time(), tzinfo=UTC)
    return [
        {
            "coin": symbol,
            "fundingRate": "0.0000125",
            "premium": "0.0000010",
            "time": int((start + timedelta(hours=hour)).timestamp() * 1000),
        }
        for hour in range(24)
    ]


def test_parse_funding_history_normalizes_official_rows() -> None:
    rows = parse_funding_history(
        funding_history_payload("BTC", date(2026, 4, 25)),
        symbol="BTC",
        funding_interval_hours=1,
    )

    assert len(rows) == 24
    assert rows[0]["symbol"] == "BTC"
    assert rows[0]["source_type"] == "fundingHistory"
    assert rows[0]["endpoint_type"] == "official_info"
    assert rows[0]["enrichment_kind"] == "funding_history"
    assert rows[0]["funding_interval_hours"] == 1


def test_funding_history_sidecar_builds_for_btc_eth(tmp_path) -> None:
    trading_date = date(2026, 4, 25)

    result = build_funding_history_sidecar(
        root=tmp_path,
        trading_date=trading_date,
        symbols=("BTC", "ETH"),
        funding_history_payloads={
            "BTC": funding_history_payload("BTC", trading_date),
            "ETH": funding_history_payload("ETH", trading_date),
        },
        source_root=tmp_path / "source-truth",
        copied_into_baseline_root=True,
    )

    frame = _read_parquet_frame(result.history_path)
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))

    assert result.manifest.source_type == "fundingHistory"
    assert result.manifest.endpoint_type == "official_info"
    assert result.manifest.row_count == 48
    assert result.manifest.missing_symbol_count == 0
    assert result.manifest.unsupported_symbol_count == 0
    assert result.manifest.coin_coverage == {"BTC": 24, "ETH": 24}
    assert len(frame) == 48
    assert set(frame["symbol"]) == {"BTC", "ETH"}
    assert manifest["source_type"] == "fundingHistory"
    assert manifest["copied_into_baseline_root"] is True


def test_source_truth_root_not_mutated_by_funding_sidecar_build(tmp_path) -> None:
    trading_date = date(2026, 4, 25)
    source_root = tmp_path / "source-truth"
    source_root.mkdir()
    marker = source_root / "marker.txt"
    marker.write_text("unchanged", encoding="utf-8")

    build_funding_history_sidecar(
        root=tmp_path / "derived",
        trading_date=trading_date,
        symbols=("BTC",),
        funding_history_payloads={"BTC": funding_history_payload("BTC", trading_date)},
        source_root=source_root,
        copied_into_baseline_root=False,
    )

    assert sorted(path.name for path in source_root.iterdir()) == ["marker.txt"]
    assert marker.read_text(encoding="utf-8") == "unchanged"
