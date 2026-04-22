from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from statestrike.paths import build_export_path, build_normalized_path
from statestrike.storage import _parquet_source, _write_parquet_frame


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
        ordering = np.lexsort((data[:, 2], data[:, 1], data[:, 0]))
        data = data[ordering]
    else:
        data = np.empty((0, 6), dtype=np.float64)

    path = export_dir / f"{symbol.lower()}_market_data.npz"
    np.savez_compressed(path, data=data)
    return path


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
