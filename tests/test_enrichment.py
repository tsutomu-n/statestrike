from __future__ import annotations

from datetime import date

import pandas as pd

from statestrike.enrichment import (
    enrich_funding_schedule_from_predicted_fundings,
    parse_predicted_fundings,
)


PREDICTED_FUNDINGS = [
    [
        "BTC",
        [
            [
                "HlPerp",
                {
                    "fundingRate": "0.0000125",
                    "nextFundingTime": 1713819600000,
                    "fundingIntervalHours": 1,
                },
            ],
            [
                "BinPerp",
                {
                    "fundingRate": "0.00005",
                    "nextFundingTime": 1713823200000,
                    "fundingIntervalHours": 4,
                },
            ],
        ],
    ],
    [
        "ETH",
        [
            [
                "HlPerp",
                {
                    "fundingRate": "-0.000003",
                    "nextFundingTime": 1713819600000,
                    "fundingIntervalHours": 1,
                },
            ],
        ],
    ],
]


def test_parse_predicted_fundings_extracts_hyperliquid_schedule() -> None:
    rows = parse_predicted_fundings(
        PREDICTED_FUNDINGS,
        symbols=("BTC",),
        enrichment_asof_ts=1713819000000,
    )

    assert len(rows) == 1
    assert rows[0]["symbol"] == "BTC"
    assert rows[0]["next_funding_ts"] == 1713819600000
    assert rows[0]["funding_interval_hours"] == 1
    assert rows[0]["enrichment_source"] == "hyperliquid_predictedFundings"
    assert rows[0]["enrichment_kind"] == "predicted_funding"


def test_enrich_funding_schedule_writes_sidecar_with_provenance(tmp_path) -> None:
    result = enrich_funding_schedule_from_predicted_fundings(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        symbols=("BTC",),
        predicted_fundings=PREDICTED_FUNDINGS,
        enrichment_asof_ts=1713819000000,
    )

    assert result.status == "completed"
    assert result.enriched_count == 1
    assert result.unsupported_dex is None
    assert result.path is not None
    frame = pd.read_parquet(result.path)
    assert frame.loc[0, "symbol"] == "BTC"
    assert frame.loc[0, "next_funding_ts"] == 1713819600000
    assert frame.loc[0, "enrichment_asof_ts"] == 1713819000000


def test_enrich_funding_schedule_records_unsupported_dex_without_fallback(tmp_path) -> None:
    result = enrich_funding_schedule_from_predicted_fundings(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        symbols=("BTC",),
        predicted_fundings=PREDICTED_FUNDINGS,
        dex="builder-dex",
        enrichment_asof_ts=1713819000000,
    )

    assert result.status == "unsupported"
    assert result.enriched_count == 0
    assert result.unsupported_dex == "builder-dex"
    assert result.path is not None
    frame = pd.read_parquet(result.path)
    assert frame.loc[0, "enrichment_kind"] == "unsupported"
    assert frame.loc[0, "unsupported_reason"] == "predictedFundings_first_perp_dex_only"
