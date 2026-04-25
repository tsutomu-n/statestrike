from __future__ import annotations

import copy
import json
from datetime import date
from pathlib import Path
from typing import Any

import zstandard

from statestrike.collector import CollectorConfig
from statestrike.readiness import run_backtest_readiness
from statestrike.recovery import MessageCaptureContext, MessageIngressMeta
from statestrike.smoke import run_smoke_batch


FIXTURES = Path(__file__).parent / "fixtures" / "hyperliquid"


def load_fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def p4_5_config() -> CollectorConfig:
    return CollectorConfig(
        allowed_symbols=("BTC",),
        source_priority=("ws", "info", "s3", "tardis"),
        market_data_network="mainnet",
        flush_interval_ms=1000,
        snapshot_recovery_enabled=True,
        channels=("l2Book", "trades", "activeAssetCtx"),
        candle_interval=None,
    )


def test_p4_5_fault_injection_explains_reconnect_gap_ordering_and_funding(
    tmp_path,
) -> None:
    duplicate_trades = copy.deepcopy(load_fixture("trades.json"))
    recovery_book = copy.deepcopy(load_fixture("l2_book.json"))
    recovery_book["data"]["time"] += 100
    messages = [
        recovery_book,
        load_fixture("trades.json"),
        duplicate_trades,
        load_fixture("active_asset_ctx.json"),
    ]
    result = run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 25),
        messages=messages,
        message_contexts=[
            MessageCaptureContext(
                reconnect_epoch=1,
                book_epoch=2,
                book_event_kind="recovery_snapshot",
                continuity_status="recovered",
                recovery_classification="recoverable",
                recovery_succeeded=True,
            ),
            MessageCaptureContext(
                reconnect_epoch=1,
                book_epoch=2,
                continuity_status="recovered",
            ),
            MessageCaptureContext(
                reconnect_epoch=1,
                book_epoch=2,
                continuity_status="recovered",
            ),
            MessageCaptureContext(
                reconnect_epoch=1,
                book_epoch=2,
                continuity_status="recovered",
            ),
        ],
        ingress_metadata=[
            MessageIngressMeta(
                recv_wall_ns=1713818880100000000,
                recv_mono_ns=100,
                recv_seq=0,
                connection_id="conn-1",
            ),
            MessageIngressMeta(
                recv_wall_ns=1713818880102000000,
                recv_mono_ns=102,
                recv_seq=2,
                connection_id="conn-1",
            ),
            MessageIngressMeta(
                recv_wall_ns=1713818880101000000,
                recv_mono_ns=101,
                recv_seq=1,
                connection_id="conn-1",
            ),
            MessageIngressMeta(
                recv_wall_ns=1713818880103000000,
                recv_mono_ns=103,
                recv_seq=3,
                connection_id="conn-1",
            ),
        ],
        config=p4_5_config(),
        capture_session_id="p4-5-fault-injection",
        batch_id="0001",
        recv_ts_start=1713818880100,
        reconnect_epoch=1,
        book_epoch=2,
        manifest_reconnect_count=1,
        ws_disconnect_count=1,
        gap_flags=("ws_reconnect", "l2_book_non_recoverable:BTC"),
    )

    replay = replay_capture_log(result.capture_log_path)
    readiness = run_backtest_readiness(
        root=tmp_path,
        trading_date=date(2026, 4, 25),
        symbols=("BTC",),
        profile="funding_aware_ready",
    )

    assert replay == {
        "row_count": 4,
        "missing_ingress_count": 0,
        "missing_recv_wall_ns_count": 0,
        "missing_recv_seq_count": 0,
        "recv_seq_non_monotonic_count": 1,
        "max_book_epoch": 2,
        "gap_flags": ("l2_book_non_recoverable:BTC", "ws_reconnect"),
    }
    assert result.audit_report.duplicate_trade_count > 0
    assert readiness.quality_report.non_recoverable_book_gap_count == 1
    assert readiness.status == "blocked"
    assert "recv_seq_not_monotonic" in readiness.blocking_reasons
    assert "unexplained_duplicate_trade_count_over_threshold" in readiness.blocking_reasons
    assert "non_recoverable_book_gap_count_over_threshold" in readiness.blocking_reasons
    assert "funding_enrichment_incomplete" in readiness.blocking_reasons


def test_p4_5_capture_log_offline_replay_accepts_ordered_recovery_capture(
    tmp_path,
) -> None:
    result = run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 25),
        messages=[
            load_fixture("l2_book.json"),
            load_fixture("trades.json"),
            load_fixture("active_asset_ctx.json"),
        ],
        message_contexts=[
            MessageCaptureContext(
                reconnect_epoch=1,
                book_epoch=2,
                book_event_kind="recovery_snapshot",
                continuity_status="recovered",
                recovery_classification="recoverable",
                recovery_succeeded=True,
            ),
            MessageCaptureContext(
                reconnect_epoch=1,
                book_epoch=2,
                continuity_status="recovered",
            ),
            MessageCaptureContext(
                reconnect_epoch=1,
                book_epoch=2,
                continuity_status="recovered",
            ),
        ],
        ingress_metadata=[
            MessageIngressMeta(
                recv_wall_ns=1713818880100000000,
                recv_mono_ns=100,
                recv_seq=0,
                connection_id="conn-1",
            ),
            MessageIngressMeta(
                recv_wall_ns=1713818880101000000,
                recv_mono_ns=101,
                recv_seq=1,
                connection_id="conn-1",
            ),
            MessageIngressMeta(
                recv_wall_ns=1713818880102000000,
                recv_mono_ns=102,
                recv_seq=2,
                connection_id="conn-1",
            ),
        ],
        config=p4_5_config(),
        capture_session_id="p4-5-offline-replay",
        batch_id="0001",
        recv_ts_start=1713818880100,
        reconnect_epoch=1,
        book_epoch=2,
        manifest_reconnect_count=1,
        ws_disconnect_count=1,
        gap_flags=("ws_reconnect",),
    )

    replay = replay_capture_log(result.capture_log_path)

    assert replay["row_count"] == 3
    assert replay["missing_ingress_count"] == 0
    assert replay["missing_recv_wall_ns_count"] == 0
    assert replay["missing_recv_seq_count"] == 0
    assert replay["recv_seq_non_monotonic_count"] == 0
    assert replay["max_book_epoch"] == 2
    assert replay["gap_flags"] == ("ws_reconnect",)


def replay_capture_log(path: Path) -> dict[str, Any]:
    row_count = 0
    missing_ingress_count = 0
    missing_recv_wall_ns_count = 0
    missing_recv_seq_count = 0
    recv_seq_non_monotonic_count = 0
    max_book_epoch = 1
    gap_flags: set[str] = set()
    previous_recv_seq: int | None = None
    with zstandard.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            row_count += 1
            row = json.loads(line)
            ingress = row.get("ingress") or {}
            context = row.get("message_context") or {}
            if not ingress:
                missing_ingress_count += 1
            if ingress.get("recv_wall_ns") is None:
                missing_recv_wall_ns_count += 1
            recv_seq = ingress.get("recv_seq")
            if recv_seq is None:
                missing_recv_seq_count += 1
            else:
                recv_seq = int(recv_seq)
                if previous_recv_seq is not None and recv_seq <= previous_recv_seq:
                    recv_seq_non_monotonic_count += 1
                previous_recv_seq = recv_seq
            max_book_epoch = max(max_book_epoch, int(context.get("book_epoch") or 1))
            if context.get("recovery_classification") == "non_recoverable":
                gap_flags.add("non_recoverable_context")
            if int(context.get("reconnect_epoch") or 0) > 0:
                gap_flags.add("ws_reconnect")
            if context.get("book_event_kind") == "recovery_snapshot":
                gap_flags.add("recovery_snapshot")
    manifest_path = next(path.parents[3].glob("manifests/date=*/session=*/**/*.json"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    gap_flags.update(manifest.get("gap_flags", []))
    gap_flags.discard("recovery_snapshot")
    return {
        "row_count": row_count,
        "missing_ingress_count": missing_ingress_count,
        "missing_recv_wall_ns_count": missing_recv_wall_ns_count,
        "missing_recv_seq_count": missing_recv_seq_count,
        "recv_seq_non_monotonic_count": recv_seq_non_monotonic_count,
        "max_book_epoch": max_book_epoch,
        "gap_flags": tuple(sorted(gap_flags)),
    }
