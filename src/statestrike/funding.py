from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
import time
from typing import Any, Literal
from urllib import request

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from statestrike.enrichment import HYPERLIQUID_INFO_URL
from statestrike.storage import _write_parquet_frame


FUNDING_HISTORY_SCHEMA_VERSION = "funding_history_sidecar.v1"
FUNDING_HISTORY_COLUMNS = (
    "symbol",
    "funding_time_ms",
    "funding_rate",
    "premium",
    "funding_interval_hours",
    "enrichment_source",
    "enrichment_kind",
    "source_type",
    "endpoint_type",
)


class FundingHistorySidecarManifest(BaseModel):
    model_config = ConfigDict(frozen=True)

    schema_version: str = FUNDING_HISTORY_SCHEMA_VERSION
    source_type: Literal["fundingHistory"] = "fundingHistory"
    endpoint_type: Literal["official_info"] = "official_info"
    trading_date: date
    symbols: tuple[str, ...]
    start_time_ms: int
    end_time_ms: int
    funding_interval_hours: int = Field(default=1, ge=1)
    row_count: int = Field(ge=0)
    missing_symbol_count: int = Field(ge=0)
    unsupported_symbol_count: int = Field(default=0, ge=0)
    generated_at: str
    copied_into_baseline_root: bool
    source_root: str | None = None
    output_root: str
    coin_coverage: dict[str, int] = Field(default_factory=dict)
    min_timestamp_ms: int | None = None
    max_timestamp_ms: int | None = None
    hour_bucket_count_by_symbol: dict[str, int] = Field(default_factory=dict)


class FundingHistorySidecarResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    root: Path
    history_path: Path
    manifest_path: Path
    manifest: FundingHistorySidecarManifest


def fetch_funding_history(
    *,
    symbol: str,
    start_time_ms: int,
    end_time_ms: int,
    info_url: str = HYPERLIQUID_INFO_URL,
    timeout_seconds: float = 10.0,
) -> Any:
    body = json.dumps(
        {
            "type": "fundingHistory",
            "coin": symbol.upper(),
            "startTime": start_time_ms,
            "endTime": end_time_ms,
        }
    ).encode("utf-8")
    http_request = request.Request(
        info_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(http_request, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def parse_funding_history(
    payload: Any,
    *,
    symbol: str,
    funding_interval_hours: int = 1,
) -> list[dict[str, object]]:
    normalized_symbol = symbol.upper()
    if not isinstance(payload, list | tuple):
        return []

    rows: list[dict[str, object]] = []
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        entry_symbol = str(entry.get("coin", normalized_symbol)).upper()
        if entry_symbol != normalized_symbol:
            continue
        timestamp = entry.get("time")
        funding_rate = entry.get("fundingRate")
        if timestamp is None or funding_rate is None:
            continue
        rows.append(
            {
                "symbol": normalized_symbol,
                "funding_time_ms": int(timestamp),
                "funding_rate": float(funding_rate),
                "premium": _optional_float(entry.get("premium")),
                "funding_interval_hours": funding_interval_hours,
                "enrichment_source": "hyperliquid_fundingHistory",
                "enrichment_kind": "funding_history",
                "source_type": "fundingHistory",
                "endpoint_type": "official_info",
            }
        )
    return rows


def build_funding_history_sidecar(
    *,
    root: Path,
    trading_date: date,
    symbols: tuple[str, ...],
    funding_history_payloads: dict[str, Any] | None = None,
    source_root: Path | None = None,
    copied_into_baseline_root: bool = True,
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
    funding_interval_hours: int = 1,
    info_url: str = HYPERLIQUID_INFO_URL,
) -> FundingHistorySidecarResult:
    normalized_symbols = tuple(symbol.upper() for symbol in symbols)
    start_ms, end_ms = _date_range_ms(
        trading_date=trading_date,
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
    )

    rows: list[dict[str, object]] = []
    for symbol in normalized_symbols:
        payload = (
            funding_history_payloads.get(symbol)
            if funding_history_payloads is not None
            else fetch_funding_history(
                symbol=symbol,
                start_time_ms=start_ms,
                end_time_ms=end_ms,
                info_url=info_url,
            )
        )
        rows.extend(
            parse_funding_history(
                payload,
                symbol=symbol,
                funding_interval_hours=funding_interval_hours,
            )
        )

    base_dir = root / "enriched" / "funding_history" / f"date={trading_date.isoformat()}"
    base_dir.mkdir(parents=True, exist_ok=True)
    history_path = base_dir / "funding_history.parquet"
    frame = pd.DataFrame(rows, columns=FUNDING_HISTORY_COLUMNS)
    _write_parquet_frame(path=history_path, frame=frame)

    coin_coverage = {
        symbol: int((frame["symbol"] == symbol).sum()) if not frame.empty else 0
        for symbol in normalized_symbols
    }
    hour_bucket_count_by_symbol = _hour_bucket_counts(frame=frame)
    missing_symbols = tuple(
        symbol for symbol in normalized_symbols if coin_coverage.get(symbol, 0) == 0
    )
    min_timestamp_ms = (
        int(frame["funding_time_ms"].min()) if not frame.empty else None
    )
    max_timestamp_ms = (
        int(frame["funding_time_ms"].max()) if not frame.empty else None
    )
    manifest = FundingHistorySidecarManifest(
        trading_date=trading_date,
        symbols=normalized_symbols,
        start_time_ms=start_ms,
        end_time_ms=end_ms,
        funding_interval_hours=funding_interval_hours,
        row_count=len(frame),
        missing_symbol_count=len(missing_symbols),
        unsupported_symbol_count=0,
        generated_at=datetime.fromtimestamp(time.time(), tz=UTC).isoformat(),
        copied_into_baseline_root=copied_into_baseline_root,
        source_root=source_root.as_posix() if source_root is not None else None,
        output_root=root.as_posix(),
        coin_coverage=coin_coverage,
        min_timestamp_ms=min_timestamp_ms,
        max_timestamp_ms=max_timestamp_ms,
        hour_bucket_count_by_symbol=hour_bucket_count_by_symbol,
    )
    manifest_path = base_dir / "funding_history_manifest.json"
    manifest_path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
    return FundingHistorySidecarResult(
        root=root,
        history_path=history_path,
        manifest_path=manifest_path,
        manifest=manifest,
    )


def funding_history_sidecar_path(*, root: Path, trading_date: date) -> Path:
    return (
        root
        / "enriched"
        / "funding_history"
        / f"date={trading_date.isoformat()}"
        / "funding_history.parquet"
    )


def funding_history_manifest_path(*, root: Path, trading_date: date) -> Path:
    return (
        root
        / "enriched"
        / "funding_history"
        / f"date={trading_date.isoformat()}"
        / "funding_history_manifest.json"
    )


def _date_range_ms(
    *,
    trading_date: date,
    start_time_ms: int | None,
    end_time_ms: int | None,
) -> tuple[int, int]:
    if start_time_ms is not None and end_time_ms is not None:
        return start_time_ms, end_time_ms
    start = datetime.combine(trading_date, datetime.min.time(), tzinfo=UTC)
    default_start = int(start.timestamp() * 1000)
    default_end = int((start + timedelta(days=1)).timestamp() * 1000)
    return start_time_ms or default_start, end_time_ms or default_end


def _hour_bucket_counts(*, frame: pd.DataFrame) -> dict[str, int]:
    if frame.empty:
        return {}
    buckets = frame.assign(
        hour_bucket=frame["funding_time_ms"].astype("int64") // 3_600_000
    )
    grouped = buckets.groupby("symbol")["hour_bucket"].nunique()
    return {str(symbol): int(count) for symbol, count in grouped.items()}


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)
