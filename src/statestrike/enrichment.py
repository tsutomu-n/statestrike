from __future__ import annotations

from datetime import date
import json
from pathlib import Path
import time
from typing import Any, Literal
from urllib import request

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from statestrike.storage import _write_parquet_frame

HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"
HYPERLIQUID_PREDICTED_FUNDING_VENUE = "HlPerp"
SUPPORTED_PREDICTED_FUNDING_DEX = ""


class FundingEnrichmentResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    status: Literal["completed", "partial", "unsupported"]
    path: Path | None = None
    enriched_count: int = Field(ge=0)
    missing_symbols: tuple[str, ...] = ()
    unsupported_dex: str | None = None


def fetch_predicted_fundings(
    *,
    info_url: str = HYPERLIQUID_INFO_URL,
    timeout_seconds: float = 10.0,
) -> Any:
    body = json.dumps({"type": "predictedFundings"}).encode("utf-8")
    http_request = request.Request(
        info_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(http_request, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def enrich_funding_schedule_from_predicted_fundings(
    *,
    root: Path,
    trading_date: date,
    symbols: tuple[str, ...],
    predicted_fundings: Any | None = None,
    dex: str = SUPPORTED_PREDICTED_FUNDING_DEX,
    enrichment_asof_ts: int | None = None,
    info_url: str = HYPERLIQUID_INFO_URL,
) -> FundingEnrichmentResult:
    normalized_symbols = tuple(symbol.upper() for symbol in symbols)
    asof_ts = enrichment_asof_ts if enrichment_asof_ts is not None else int(time.time() * 1000)
    if dex != SUPPORTED_PREDICTED_FUNDING_DEX:
        path = _write_funding_sidecar(
            root=root,
            trading_date=trading_date,
            rows=[
                {
                    "symbol": symbol,
                    "next_funding_ts": None,
                    "funding_interval_hours": None,
                    "enrichment_source": "hyperliquid_predictedFundings",
                    "enrichment_asof_ts": asof_ts,
                    "enrichment_kind": "unsupported",
                    "unsupported_dex": dex,
                    "unsupported_reason": "predictedFundings_first_perp_dex_only",
                }
                for symbol in normalized_symbols
            ],
        )
        return FundingEnrichmentResult(
            status="unsupported",
            path=path,
            enriched_count=0,
            missing_symbols=normalized_symbols,
            unsupported_dex=dex,
        )

    payload = (
        predicted_fundings
        if predicted_fundings is not None
        else fetch_predicted_fundings(info_url=info_url)
    )
    rows = parse_predicted_fundings(
        payload,
        symbols=normalized_symbols,
        enrichment_asof_ts=asof_ts,
    )
    found_symbols = {str(row["symbol"]).upper() for row in rows}
    missing_symbols = tuple(symbol for symbol in normalized_symbols if symbol not in found_symbols)
    path = _write_funding_sidecar(
        root=root,
        trading_date=trading_date,
        rows=rows,
    )
    status: Literal["completed", "partial"] = "completed" if not missing_symbols else "partial"
    return FundingEnrichmentResult(
        status=status,
        path=path,
        enriched_count=len(rows),
        missing_symbols=missing_symbols,
    )


def parse_predicted_fundings(
    payload: Any,
    *,
    symbols: tuple[str, ...],
    enrichment_asof_ts: int,
) -> list[dict[str, object]]:
    requested = {symbol.upper() for symbol in symbols}
    rows: list[dict[str, object]] = []
    for coin_entry in payload:
        if not isinstance(coin_entry, list | tuple) or len(coin_entry) != 2:
            continue
        coin, venues = coin_entry
        symbol = str(coin).upper()
        if symbol not in requested:
            continue
        venue_info = _find_hyperliquid_predicted_funding(venues)
        if venue_info is None:
            continue
        rows.append(
            {
                "symbol": symbol,
                "next_funding_ts": int(venue_info["nextFundingTime"]),
                "funding_interval_hours": int(venue_info["fundingIntervalHours"]),
                "enrichment_source": "hyperliquid_predictedFundings",
                "enrichment_asof_ts": enrichment_asof_ts,
                "enrichment_kind": "predicted_funding",
            }
        )
    return rows


def _find_hyperliquid_predicted_funding(venues: Any) -> dict[str, Any] | None:
    if not isinstance(venues, list | tuple):
        return None
    for venue_entry in venues:
        if not isinstance(venue_entry, list | tuple) or len(venue_entry) != 2:
            continue
        venue_name, venue_info = venue_entry
        if venue_name == HYPERLIQUID_PREDICTED_FUNDING_VENUE and isinstance(
            venue_info,
            dict,
        ):
            return venue_info
    return None


def _write_funding_sidecar(
    *,
    root: Path,
    trading_date: date,
    rows: list[dict[str, object]],
) -> Path:
    base_dir = root / "enriched" / "funding_schedule" / f"date={trading_date.isoformat()}"
    base_dir.mkdir(parents=True, exist_ok=True)
    path = base_dir / "predicted_funding.parquet"
    _write_parquet_frame(path=path, frame=pd.DataFrame(rows))
    return path
