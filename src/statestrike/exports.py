from __future__ import annotations

from contextlib import redirect_stdout
from datetime import date
import io
from pathlib import Path

import duckdb
from hftbacktest import BUY_EVENT, DEPTH_EVENT, EXCH_EVENT, LOCAL_EVENT, SELL_EVENT, TRADE_EVENT
from hftbacktest.data import correct_event_order, correct_local_timestamp, validate_event_order
from hftbacktest.types import event_dtype
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
        event_code=TRADE_EVENT,
        side_column="side",
        price_column="price",
        size_column="size",
        buy_label="buy",
    )
    book_rows = _to_hftbacktest_rows(
        frame=book_levels,
        event_code=DEPTH_EVENT,
        side_column="side",
        price_column="price",
        size_column="size",
        buy_label="bid",
    )

    arrays = [array for array in (trade_rows, book_rows) if len(array)]
    if arrays:
        data = np.concatenate(arrays)
        with redirect_stdout(io.StringIO()):
            data = correct_local_timestamp(data.copy(), 0)
        sorted_exch_index = np.argsort(data["exch_ts"], kind="stable")
        sorted_local_index = np.argsort(data["local_ts"], kind="stable")
        data = correct_event_order(
            data,
            sorted_exch_index,
            sorted_local_index,
        )
        validate_event_order(data)
    else:
        data = np.empty(0, dtype=event_dtype)

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
                "capture_session_id",
                "reconnect_epoch",
                "book_epoch",
                "book_event_id",
                "symbol",
                "exchange_ts",
                "recv_ts",
                "event_kind",
                "source",
                "raw_msg_hash",
                "dedup_hash",
                "side",
                "level_idx",
                "price",
                "size",
            ]
        )
    required_columns = {
        "capture_session_id",
        "reconnect_epoch",
        "book_epoch",
        "symbol",
        "exchange_ts",
        "recv_ts",
        "source",
        "raw_msg_hash",
        "dedup_hash",
    }
    if required_columns.issubset(set(book_levels.columns)):
        joined = book_levels.copy()
        if "event_kind" not in joined.columns:
            if book_events.empty:
                joined["event_kind"] = "snapshot"
            else:
                event_kinds = book_events[["book_event_id", "event_kind"]]
                joined = joined.merge(event_kinds, on="book_event_id", how="left")
        joined.insert(0, "instrument_id", instrument_id)
        return joined.sort_values(
            by=["exchange_ts", "recv_ts", "book_event_id", "side", "level_idx"]
        ).reset_index(drop=True)
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
        return np.empty(0, dtype=event_dtype)
    side_flags = np.where(
        frame[side_column].to_numpy() == buy_label,
        BUY_EVENT,
        SELL_EVENT,
    ).astype(np.uint64)
    rows = np.zeros(len(frame), dtype=event_dtype)
    rows["ev"] = side_flags | np.uint64(event_code)
    rows["exch_ts"] = frame["exchange_ts"].to_numpy(dtype=np.int64)
    rows["local_ts"] = frame["recv_ts"].to_numpy(dtype=np.int64)
    rows["px"] = frame[price_column].to_numpy(dtype=np.float64)
    rows["qty"] = frame[size_column].to_numpy(dtype=np.float64)
    return rows


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
        row_count=int(len(data)),
        null_count=_count_structured_nulls(data),
        exchange_ts_monotonic=_is_non_decreasing(
            data["exch_ts"][(data["ev"] & EXCH_EVENT) == EXCH_EVENT]
        ),
        local_ts_monotonic=_is_non_decreasing(
            data["local_ts"][(data["ev"] & LOCAL_EVENT) == LOCAL_EVENT]
        ),
        event_codes=tuple(sorted({int(code & 0xFF) for code in data["ev"]})),
    )


def _is_non_decreasing(values: np.ndarray) -> bool:
    if values.size <= 1:
        return True
    return bool(np.all(np.diff(values) >= 0))


def _count_structured_nulls(data: np.ndarray) -> int:
    total = 0
    for name in data.dtype.names or ():
        if np.issubdtype(data.dtype[name], np.floating):
            total += int(np.isnan(data[name]).sum())
    return total


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
