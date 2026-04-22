from __future__ import annotations

import copy
import json
from datetime import date
from pathlib import Path

from statestrike.collector import CollectorConfig
from statestrike.smoke import run_smoke_batch


FIXTURES = Path(__file__).parent / "fixtures" / "hyperliquid"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_run_smoke_batch_persists_phase15_artifacts(tmp_path) -> None:
    invalid_trades = copy.deepcopy(load_fixture("trades.json"))
    invalid_trades["data"][0]["sz"] = "0.0"
    config = CollectorConfig(
        allowed_symbols=("BTC",),
        source_priority=("ws", "info", "s3", "tardis"),
        market_data_network="mainnet",
        flush_interval_ms=1000,
        snapshot_recovery_enabled=True,
        channels=("l2Book", "trades", "activeAssetCtx", "candle"),
        candle_interval="1m",
    )

    result = run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        messages=[
            load_fixture("l2_book.json"),
            invalid_trades,
            load_fixture("active_asset_ctx.json"),
            load_fixture("candle.json"),
        ],
        config=config,
        capture_session_id="session-1",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))

    assert result.capture_session_id == "session-1"
    assert result.raw_paths["trades:BTC"].exists()
    assert result.normalized_paths["trades:BTC"].exists()
    assert result.quarantine_paths["trades:BTC"].exists()
    assert manifest["row_count"] == 4
    assert set(manifest["channels"]) == {
        "activeAssetCtx",
        "candle",
        "l2Book",
        "trades",
    }
    assert result.audit_report.quarantine_row_counts["trades"] == 1
    assert result.export_validations["BTC"].nautilus_tables["trade_ticks"].row_count == 1
    assert result.export_validations["BTC"].hftbacktest.row_count == 5
