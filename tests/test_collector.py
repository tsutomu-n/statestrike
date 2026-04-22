from __future__ import annotations

import json
from pathlib import Path

from statestrike.collector import CollectorConfig, collect_market_batch


FIXTURES = Path(__file__).parent / "fixtures" / "hyperliquid"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_collect_market_batch_replays_hyperliquid_fixtures() -> None:
    config = CollectorConfig(
        allowed_symbols=("BTC",),
        source_priority=("ws", "info", "s3", "tardis"),
        market_data_network="mainnet",
        flush_interval_ms=1000,
        snapshot_recovery_enabled=True,
    )

    result = collect_market_batch(
        messages=[
            load_fixture("l2_book.json"),
            load_fixture("trades.json"),
            load_fixture("active_asset_ctx.json"),
        ],
        config=config,
        capture_session_id="session-1",
        reconnect_epoch=0,
        book_epoch=1,
        recv_ts_start=1713818880100,
    )

    assert result.raw_count == 3
    assert result.normalized_counts["book_events"] == 1
    assert result.normalized_counts["book_levels"] == 4
    assert result.normalized_counts["trades"] == 2
    assert result.normalized_counts["asset_ctx"] == 1
