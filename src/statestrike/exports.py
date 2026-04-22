from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from statestrike.paths import build_export_path, build_normalized_path
from statestrike.storage import _parquet_source, _write_parquet_frame


class ExportTableValidation(BaseModel):
    model_config = ConfigDict(frozen=True)

    row_count: int = Field(ge=0)
    symbol_count: int = Field(ge=0)
    null_count: int = Field(ge=0)
    exchange_ts_monotonic: bool


class HftbacktestValidation(BaseModel):
    model_config = ConfigDict(frozen=True)

    row_count: int = Field(ge=0)
    null_count: int = Field(ge=0)
    exchange_ts_monotonic: bool
    local_ts_monotonic: bool
    event_codes: tuple[int, ...]


class ExportValidationReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    nautilus_tables: dict[str, ExportTableValidation]
    hftbacktest: HftbacktestValidation


def export_nautilus_catalog(
    *,
    normalized_root: Path,
    export_root: Path,
    trading_date: date,
    symbol: str,
) -> Path:
    symbol = symbol.upper()
    instrument_id = _instrument_id(symbol)
    export_dir = build_export_path(
        root=export_root,
        target="nautilus",
        trading_date=trading_date,
        symbol=symbol,
    )
    export_dir.mkdir(parents=True, exist_ok=True)

    trades = _read_normalized_table(
        normalized_root=normalized_root,
        table="trades",
        trading_date=trading_date,
        symbol=symbol,
    )
    book_events = _read_normalized_table(
        normalized_root=normalized_root,
        table="book_events",
        trading_date=trading_date,
        symbol=symbol,
    )
    book_levels = _read_normalized_table(
        normalized_root=normalized_root,
        table="book_levels",
        trading_date=trading_date,
        symbol=symbol,
    )
    asset_ctx = _read_normalized_table(
        normalized_root=normalized_root,
        table="asset_ctx",
        trading_date=trading_date,
        symbol=symbol,
    )

    instrument_frame = pd.DataFrame(
        [
            {
                "instrument_id": instrument_id,
                "symbol": symbol,
                "venue": "HYPERLIQUID",
                "product_type": "PERPETUAL",
                "quote_currency": "USD",
            }
        ]
    )
    trade_ticks = trades.copy()
    if not trade_ticks.empty:
        trade_ticks.insert(0, "instrument_id", instrument_id)
        trade_ticks = trade_ticks.sort_values(
            by=["exchange_ts", "recv_ts", "trade_event_id"]
        ).reset_index(drop=True)

    book_levels_export = _join_book_levels(
        book_events=book_events,
        book_levels=book_levels,
        instrument_id=instrument_id,
    )
    asset_ctx_export = asset_ctx.copy()
    if not asset_ctx_export.empty:
        asset_ctx_export.insert(0, "instrument_id", instrument_id)
        asset_ctx_export = asset_ctx_export.sort_values(
            by=["exchange_ts", "recv_ts", "asset_ctx_event_id"]
        ).reset_index(drop=True)

    _write_parquet_frame(path=export_dir / "instrument.parquet", frame=instrument_frame)
    _write_parquet_frame(path=export_dir / "trade_ticks.parquet", frame=trade_ticks)
    _write_parquet_frame(path=export_dir / "book_levels.parquet", frame=book_levels_export)
    _write_parquet_frame(path=export_dir / "asset_ctx.parquet", frame=asset_ctx_export)
    return export_dir


def export_hftbacktest_npz(
    *,
    normalized_root: Path,
    export_root: Path,
    trading_date: date,
    symbol: str,
) -> Path:
    symbol = symbol.upper()
    export_dir = build_export_path(
        root=export_root,
        target="hftbacktest",
        trading_date=trading_date,
        symbol=symbol,
    )
    export_dir.mkdir(parents=True, exist_ok=True)

    trades = _read_normalized_table(
        normalized_root=normalized_root,
        table="trades",
        trading_date=trading_date,
        symbol=symbol,
    )
    book_levels = _join_book_levels(
        book_events=_read_normalized_table(
            normalized_root=normalized_root,
            table="book_events",
            trading_date=trading_date,
            symbol=symbol,
        ),
        book_levels=_read_normalized_table(
            normalized_root=normalized_root,
            table="book_levels",
            trading_date=trading_date,
            symbol=symbol,
        ),
        instrument_id=_instrument_id(symbol),
    )

    trade_rows = _to_hftbacktest_rows(
        frame=trades,
        event_code=2,
        side_column="side",
        price_column="price",
        size_column="size",
        buy_label="buy",
    )
    book_rows = _to_hftbacktest_rows(
        frame=book_levels,
        event_code=4,
        side_column="side",
        price_column="price",
        size_column="size",
        buy_label="bid",
    )

    arrays = [array for array in (trade_rows, book_rows) if array.size]
    if arrays:
        data = np.vstack(arrays)
        ordering = np.lexsort((data[:, 0], data[:, 2], data[:, 1]))
        data = data[ordering]
    else:
        data = np.empty((0, 6), dtype=np.float64)

    path = export_dir / f"{symbol.lower()}_market_data.npz"
    np.savez_compressed(path, data=data)
    return path


def validate_export_bundle(
    *,
    export_root: Path,
    trading_date: date,
    symbol: str,
) -> ExportValidationReport:
    symbol = symbol.upper()
    nautilus_dir = build_export_path(
        root=export_root,
        target="nautilus",
        trading_date=trading_date,
        symbol=symbol,
    )
    hft_dir = build_export_path(
        root=export_root,
        target="hftbacktest",
        trading_date=trading_date,
        symbol=symbol,
    )

    nautilus_tables = {
        table_name: _summarize_export_frame(_read_export_frame(nautilus_dir / filename))
        for table_name, filename in (
            ("instrument", "instrument.parquet"),
            ("trade_ticks", "trade_ticks.parquet"),
            ("book_levels", "book_levels.parquet"),
            ("asset_ctx", "asset_ctx.parquet"),
        )
    }
    hftbacktest = _summarize_hftbacktest_npz(
        hft_dir / f"{symbol.lower()}_market_data.npz"
    )
    return ExportValidationReport(
        nautilus_tables=nautilus_tables,
        hftbacktest=hftbacktest,
    )


def _join_book_levels(
    *,
    book_events: pd.DataFrame,
    book_levels: pd.DataFrame,
    instrument_id: str,
) -> pd.DataFrame:
    if book_levels.empty:
        return pd.DataFrame(
            columns=[
                "instrument_id",
                "book_event_id",
                "symbol",
                "exchange_ts",
                "recv_ts",
                "event_kind",
                "source",
                "side",
                "level_idx",
                "price",
                "size",
            ]
        )
    if book_events.empty:
        joined = book_levels.copy()
        joined.insert(0, "instrument_id", instrument_id)
        return joined
    joined = book_levels.merge(
        book_events[
            [
                "book_event_id",
                "symbol",
                "exchange_ts",
                "recv_ts",
                "event_kind",
                "source",
            ]
        ],
        on="book_event_id",
        how="left",
    )
    joined.insert(0, "instrument_id", instrument_id)
    return joined.sort_values(
        by=["exchange_ts", "recv_ts", "book_event_id", "side", "level_idx"]
    ).reset_index(drop=True)


def _to_hftbacktest_rows(
    *,
    frame: pd.DataFrame,
    event_code: int,
    side_column: str,
    price_column: str,
    size_column: str,
    buy_label: str,
) -> np.ndarray:
    if frame.empty:
        return np.empty((0, 6), dtype=np.float64)
    side_values = np.where(frame[side_column].to_numpy() == buy_label, 1, -1)
    return np.column_stack(
        [
            np.full(len(frame), event_code, dtype=np.int64),
            frame["exchange_ts"].to_numpy(dtype=np.int64),
            frame["recv_ts"].to_numpy(dtype=np.int64),
            side_values.astype(np.int64),
            frame[price_column].to_numpy(dtype=np.float64),
            frame[size_column].to_numpy(dtype=np.float64),
        ]
    )


def _read_export_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    connection = duckdb.connect()
    try:
        escaped_path = path.as_posix().replace("'", "''")
        return connection.execute(
            f"SELECT * FROM read_parquet('{escaped_path}')"
        ).fetchdf()
    finally:
        connection.close()


def _summarize_export_frame(frame: pd.DataFrame) -> ExportTableValidation:
    symbol_count = 0
    if "symbol" in frame:
        symbol_count = int(frame["symbol"].nunique(dropna=True))
    elif "instrument_id" in frame:
        symbol_count = int(frame["instrument_id"].nunique(dropna=True))
    exchange_ts_monotonic = True
    if "exchange_ts" in frame:
        exchange_ts_monotonic = _is_non_decreasing(
            frame["exchange_ts"].to_numpy(dtype=np.int64)
        )
    return ExportTableValidation(
        row_count=int(len(frame)),
        symbol_count=symbol_count,
        null_count=int(frame.isna().sum().sum()) if not frame.empty else 0,
        exchange_ts_monotonic=exchange_ts_monotonic,
    )


def _summarize_hftbacktest_npz(path: Path) -> HftbacktestValidation:
    if not path.exists():
        return HftbacktestValidation(
            row_count=0,
            null_count=0,
            exchange_ts_monotonic=True,
            local_ts_monotonic=True,
            event_codes=(),
        )
    with np.load(path) as archive:
        data = archive["data"]
    if data.size == 0:
        return HftbacktestValidation(
            row_count=0,
            null_count=0,
            exchange_ts_monotonic=True,
            local_ts_monotonic=True,
            event_codes=(),
        )
    return HftbacktestValidation(
        row_count=int(data.shape[0]),
        null_count=int(np.isnan(data).sum()),
        exchange_ts_monotonic=_is_non_decreasing(data[:, 1]),
        local_ts_monotonic=_is_non_decreasing(data[:, 2]),
        event_codes=tuple(sorted({int(code) for code in data[:, 0]})),
    )


def _is_non_decreasing(values: np.ndarray) -> bool:
    if values.size <= 1:
        return True
    return bool(np.all(np.diff(values) >= 0))


def _read_normalized_table(
    *,
    normalized_root: Path,
    table: str,
    trading_date: date,
    symbol: str,
) -> pd.DataFrame:
    base_dir = build_normalized_path(
        root=normalized_root,
        channel=table,
        trading_date=trading_date,
        symbol=symbol,
    )
    files = sorted(base_dir.glob("*.parquet"))
    if not files:
        return pd.DataFrame()

    connection = duckdb.connect()
    try:
        source = _parquet_source(files)
        return connection.execute(f"SELECT * FROM {source}").fetchdf()
    finally:
        connection.close()


def _instrument_id(symbol: str) -> str:
    return f"{symbol.upper()}-USD-PERP.HYPERLIQUID"
