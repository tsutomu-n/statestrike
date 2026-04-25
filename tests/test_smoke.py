from __future__ import annotations

import asyncio
import copy
import json
from datetime import date
from pathlib import Path

import duckdb
import pytest
import zstandard

from statestrike.collector import CollectorConfig
from statestrike.recovery import MessageCaptureContext, MessageIngressMeta
from statestrike.settings import Settings
from statestrike.smoke import (
    SmokeTransportCapture,
    main,
    run_smoke_batch,
    run_smoke_campaign,
    run_smoke_session,
)


FIXTURES = Path(__file__).parent / "fixtures" / "hyperliquid"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def with_symbol(message: dict, symbol: str) -> dict:
    message = copy.deepcopy(message)
    data = message.get("data")
    if isinstance(data, dict):
        if "coin" in data:
            data["coin"] = symbol
        if "s" in data:
            data["s"] = symbol
    elif isinstance(data, list):
        for row in data:
            if "coin" in row:
                row["coin"] = symbol
    return message


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
        skew_warning_ms=40,
        skew_severe_ms=80,
        asset_ctx_stale_threshold_ms=123456,
    )

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    audit_json = json.loads(result.audit_report_paths["json"].read_text(encoding="utf-8"))
    export_json = json.loads(
        result.export_validation_report_paths["BTC"].read_text(encoding="utf-8")
    )

    assert result.capture_session_id == "session-1"
    assert result.capture_log_path.exists()
    assert result.raw_paths["trades:BTC"].exists()
    assert result.normalized_paths["trades:BTC"].exists()
    assert result.quarantine_paths["trades:BTC"].exists()
    assert result.audit_report_paths["json"].exists()
    assert result.audit_report_paths["md"].exists()
    assert result.export_validation_report_paths["BTC"].exists()
    assert manifest["capture_semantics_version"] == "phase1_truth_capture_v1"
    assert manifest["truth_capture_artifact"] == "capture_log"
    assert manifest["derived_capture_artifacts"] == ["raw_ws"]
    assert manifest["truth_export_targets"] == ["nautilus"]
    assert manifest["corrected_export_targets"] == ["hftbacktest"]
    assert manifest["row_count"] == 4
    assert manifest["book_epoch_count"] == 1
    assert manifest["book_continuity_gap_count"] == 0
    assert set(manifest["channels"]) == {
        "activeAssetCtx",
        "candle",
        "l2Book",
        "trades",
    }
    assert result.audit_report.quarantine_row_counts["trades"] == 1
    assert result.audit_report.thresholds.skew_warning_ms == 40
    assert result.audit_report.thresholds.skew_severe_ms == 80
    assert result.audit_report.thresholds.asset_ctx_stale_threshold_ms == 123456
    assert audit_json["thresholds"] == {
        "skew_warning_ms": 40,
        "skew_severe_ms": 80,
        "asset_ctx_stale_threshold_ms": 123456,
        "trade_gap_threshold_ms": 60000,
        "quarantine_warning_rate": 0.001,
        "quarantine_severe_rate": 0.01,
    }
    assert audit_json["quarantine_reason_counts"]["trades"] == {
        "size:greater_than(0)": 1
    }
    assert "quarantine_rate" in result.audit_report_paths["md"].read_text(encoding="utf-8")
    assert "- skew_warning_ms: 40" in result.audit_report_paths["md"].read_text(
        encoding="utf-8"
    )
    assert result.export_validations["BTC"].nautilus_tables["trade_ticks"].row_count == 1
    assert result.export_validations["BTC"].hftbacktest.row_count == 5
    assert export_json["hftbacktest"]["row_count"] == 5

    with zstandard.open(result.capture_log_path, "rt", encoding="utf-8") as handle:
        capture_log_rows = [json.loads(line) for line in handle]

    assert [row["message"]["channel"] for row in capture_log_rows] == [
        "l2Book",
        "trades",
        "activeAssetCtx",
        "candle",
    ]
    assert capture_log_rows[1]["ingress"]["recv_seq"] == 1
    assert capture_log_rows[0]["message_context"]["book_event_kind"] == "snapshot"


def test_run_smoke_batch_normalizes_full_ordered_stream_once_before_partitioning(
    tmp_path,
) -> None:
    config = CollectorConfig(
        allowed_symbols=("BTC", "ETH"),
        source_priority=("ws", "info", "s3", "tardis"),
        market_data_network="mainnet",
        flush_interval_ms=1000,
        snapshot_recovery_enabled=True,
        channels=("trades",),
        candle_interval=None,
    )
    btc_trades = load_fixture("trades.json")
    eth_trades = with_symbol(load_fixture("trades.json"), "ETH")
    eth_trades["data"][0]["time"] = 1713818880040
    eth_trades["data"][0]["tid"] = 3
    eth_trades["data"][1]["time"] = 1713818880070
    eth_trades["data"][1]["tid"] = 4
    ingress = [
        MessageIngressMeta(
            recv_wall_ns=1713818880101000000,
            recv_mono_ns=11,
            recv_seq=2,
            connection_id="conn-1",
        ),
        MessageIngressMeta(
            recv_wall_ns=1713818880100000000,
            recv_mono_ns=10,
            recv_seq=1,
            connection_id="conn-1",
        ),
    ]

    result = run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        messages=[btc_trades, eth_trades],
        ingress_metadata=ingress,
        config=config,
        capture_session_id="session-ordered",
        batch_id="0001",
        recv_ts_start=1713818880999,
    )

    connection = duckdb.connect()
    try:
        btc_rows = connection.execute(
            f"""
            SELECT recv_ts, trade_event_id
            FROM read_parquet('{result.normalized_paths["trades:BTC"].as_posix()}')
            ORDER BY exchange_ts, trade_event_id
            """
        ).fetchall()
        eth_rows = connection.execute(
            f"""
            SELECT recv_ts, trade_event_id
            FROM read_parquet('{result.normalized_paths["trades:ETH"].as_posix()}')
            ORDER BY exchange_ts, trade_event_id
            """
        ).fetchall()
    finally:
        connection.close()

    assert [recv_ts for recv_ts, _ in btc_rows] == [1713818880101, 1713818880101]
    assert [recv_ts for recv_ts, _ in eth_rows] == [1713818880100, 1713818880100]


def test_run_smoke_session_uses_transport_hook_and_tracks_reconnects(tmp_path) -> None:
    captured_args: dict[str, int] = {}

    async def fake_transport(
        *,
        config: CollectorConfig,
        max_messages: int,
        max_runtime_seconds: int,
        ping_interval_seconds: int,
        reconnect_limit: int,
    ) -> SmokeTransportCapture:
        captured_args["max_messages"] = max_messages
        captured_args["max_runtime_seconds"] = max_runtime_seconds
        captured_args["ping_interval_seconds"] = ping_interval_seconds
        captured_args["reconnect_limit"] = reconnect_limit
        return SmokeTransportCapture(
            messages=[
                load_fixture("l2_book.json"),
                load_fixture("trades.json"),
                load_fixture("active_asset_ctx.json"),
                load_fixture("candle.json"),
            ],
            recv_ts_start=1713818880100,
            started_at="2026-04-23T00:00:00Z",
            ended_at="2026-04-23T00:05:00Z",
            ws_disconnect_count=1,
            reconnect_count=1,
            gap_flags=("ws_reconnect",),
        )

    settings = Settings(
        data_root=tmp_path,
        allowed_symbols=("BTC",),
        smoke_max_messages=4,
        smoke_max_runtime_seconds=300,
        smoke_ping_interval_seconds=15,
        smoke_reconnect_limit=2,
        smoke_skew_warning_ms=35,
        smoke_skew_severe_ms=70,
        smoke_asset_ctx_stale_threshold_ms=654321,
    )

    result = asyncio.run(
        run_smoke_session(
            settings=settings,
            trading_date=date(2026, 4, 23),
            transport=fake_transport,
            capture_session_id="session-2",
        )
    )

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))

    assert captured_args == {
        "max_messages": 4,
        "max_runtime_seconds": 300,
        "ping_interval_seconds": 15,
        "reconnect_limit": 2,
    }
    assert result.audit_report_paths["json"].exists()
    assert result.audit_report.thresholds.skew_warning_ms == 35
    assert result.audit_report.thresholds.skew_severe_ms == 70
    assert result.audit_report.thresholds.asset_ctx_stale_threshold_ms == 654321
    assert manifest["capture_session_id"] == "session-2"
    assert manifest["truth_capture_artifact"] == "capture_log"
    assert manifest["corrected_export_targets"] == ["hftbacktest"]
    assert result.capture_log_path.exists()
    assert manifest["started_at"] == "2026-04-23T00:00:00Z"
    assert manifest["ended_at"] == "2026-04-23T00:05:00Z"
    assert manifest["ws_disconnect_count"] == 1
    assert manifest["reconnect_count"] == 1
    assert manifest["gap_flags"] == ["ws_reconnect"]


def test_run_smoke_session_propagates_runtime_epochs_into_normalized_rows(tmp_path) -> None:
    async def fake_transport(
        *,
        config: CollectorConfig,
        max_messages: int,
        max_runtime_seconds: int,
        ping_interval_seconds: int,
        reconnect_limit: int,
    ) -> SmokeTransportCapture:
        return SmokeTransportCapture(
            messages=[
                load_fixture("l2_book.json"),
                load_fixture("trades.json"),
                load_fixture("active_asset_ctx.json"),
            ],
            message_contexts=[
                MessageCaptureContext(
                    reconnect_epoch=2,
                    book_epoch=7,
                    book_event_kind="recovery_snapshot",
                    continuity_status="recovered",
                    recovery_classification="recoverable",
                    recovery_succeeded=True,
                ),
                MessageCaptureContext(
                    reconnect_epoch=2,
                    book_epoch=7,
                    continuity_status="recovered",
                ),
                MessageCaptureContext(
                    reconnect_epoch=2,
                    book_epoch=7,
                    continuity_status="recovered",
                ),
            ],
            recv_ts_start=1713818880100,
            started_at="2026-04-23T00:00:00Z",
            ended_at="2026-04-23T00:05:00Z",
            ws_disconnect_count=2,
            reconnect_count=2,
            reconnect_epoch=2,
            book_epoch=7,
            gap_flags=("ws_reconnect",),
        )

    settings = Settings(
        data_root=tmp_path,
        allowed_symbols=("BTC",),
        smoke_max_messages=4,
        smoke_max_runtime_seconds=300,
        smoke_ping_interval_seconds=15,
        smoke_reconnect_limit=2,
    )

    result = asyncio.run(
        run_smoke_session(
            settings=settings,
            trading_date=date(2026, 4, 23),
            transport=fake_transport,
            capture_session_id="session-epochs",
        )
    )

    book_events_path = result.normalized_paths["book_events:BTC"]
    connection = duckdb.connect()
    try:
        reconnect_epoch, book_epoch, event_kind, continuity_status = connection.execute(
            f"SELECT reconnect_epoch, book_epoch, event_kind, continuity_status FROM read_parquet('{book_events_path.as_posix()}')"
        ).fetchone()
    finally:
        connection.close()

    assert reconnect_epoch == 2
    assert book_epoch == 7
    assert event_kind == "recovery_snapshot"
    assert continuity_status == "recovered"


def test_run_smoke_session_persists_book_continuity_manifest_summary(tmp_path) -> None:
    async def fake_transport(
        *,
        config: CollectorConfig,
        max_messages: int,
        max_runtime_seconds: int,
        ping_interval_seconds: int,
        reconnect_limit: int,
    ) -> SmokeTransportCapture:
        return SmokeTransportCapture(
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
                    continuity_status="recovery_pending",
                ),
                MessageCaptureContext(
                    reconnect_epoch=1,
                    book_epoch=2,
                    continuity_status="recovery_pending",
                ),
            ],
            recv_ts_start=1713818880100,
            started_at="2026-04-23T00:00:00Z",
            ended_at="2026-04-23T00:05:00Z",
            ws_disconnect_count=1,
            reconnect_count=1,
            reconnect_epoch=1,
            book_epoch=2,
            gap_flags=("l2_book_non_recoverable:ETH",),
        )

    settings = Settings(
        data_root=tmp_path,
        allowed_symbols=("BTC", "ETH"),
        smoke_max_messages=4,
        smoke_max_runtime_seconds=300,
        smoke_ping_interval_seconds=15,
        smoke_reconnect_limit=2,
    )

    result = asyncio.run(
        run_smoke_session(
            settings=settings,
            trading_date=date(2026, 4, 23),
            transport=fake_transport,
            capture_session_id="session-gap-summary",
        )
    )

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))

    assert manifest["book_epoch_count"] == 2
    assert manifest["recoverable_book_gap_count"] == 1
    assert manifest["non_recoverable_book_gap_count"] == 1
    assert manifest["book_continuity_gap_count"] == 2


def test_run_smoke_campaign_accumulates_daily_artifacts_across_sessions(
    tmp_path,
) -> None:
    captures = [
        SmokeTransportCapture(
            messages=[
                load_fixture("l2_book.json"),
                load_fixture("trades.json"),
                load_fixture("active_asset_ctx.json"),
            ],
            recv_ts_start=1713818880100,
            started_at="2026-04-23T00:00:00Z",
            ended_at="2026-04-23T00:01:00Z",
        ),
        SmokeTransportCapture(
            messages=[
                load_fixture("l2_book.json"),
                load_fixture("trades.json"),
                load_fixture("active_asset_ctx.json"),
            ],
            recv_ts_start=1713818881100,
            started_at="2026-04-23T00:01:00Z",
            ended_at="2026-04-23T00:02:00Z",
            ws_disconnect_count=1,
            reconnect_count=1,
            gap_flags=("ws_reconnect",),
        ),
    ]

    async def fake_transport(
        *,
        config: CollectorConfig,
        max_messages: int,
        max_runtime_seconds: int,
        ping_interval_seconds: int,
        reconnect_limit: int,
    ) -> SmokeTransportCapture:
        return captures.pop(0)

    settings = Settings(
        data_root=tmp_path,
        allowed_symbols=("BTC",),
        smoke_max_messages=4,
        smoke_max_runtime_seconds=300,
        smoke_ping_interval_seconds=15,
        smoke_reconnect_limit=2,
    )

    result = asyncio.run(
        run_smoke_campaign(
            settings=settings,
            trading_date=date(2026, 4, 23),
            transport=fake_transport,
            capture_session_id="campaign",
            session_count=2,
        )
    )

    assert result.campaign_id == "campaign"
    assert result.status == "completed"
    assert result.requested_session_count == 2
    assert result.session_count == 2
    assert result.total_row_count == 6
    assert [session.capture_session_id for session in result.sessions] == [
        "campaign-0001",
        "campaign-0002",
    ]
    assert [session.row_count for session in result.sessions] == [3, 3]
    assert result.sessions[1].gap_flags == ("ws_reconnect",)
    assert Path(result.sessions[0].manifest_path).exists()
    assert Path(result.sessions[1].manifest_path).exists()
    assert result.final_audit_report.row_counts["book_events"] == 2
    assert result.final_audit_report.row_counts["book_levels"] == 8
    assert result.final_audit_report.row_counts["trades"] == 4
    assert result.final_audit_report.row_counts["asset_ctx"] == 2
    assert result.report_paths["json"].exists()
    assert result.report_paths["md"].exists()
    campaign_json = json.loads(result.report_paths["json"].read_text(encoding="utf-8"))
    assert campaign_json["campaign_id"] == "campaign"
    assert campaign_json["status"] == "completed"
    assert campaign_json["requested_session_count"] == 2
    assert campaign_json["session_count"] == 2
    assert result.final_audit_report_paths["json"].exists()
    assert result.final_export_validation_report_paths["BTC"].exists()


def test_run_smoke_campaign_recomputes_final_reports_from_campaign_scope(
    tmp_path,
) -> None:
    captures = [
        SmokeTransportCapture(
            messages=[
                load_fixture("l2_book.json"),
                load_fixture("trades.json"),
                load_fixture("active_asset_ctx.json"),
            ],
            recv_ts_start=1713818880100,
            started_at="2026-04-23T00:00:00Z",
            ended_at="2026-04-23T00:01:00Z",
        ),
        SmokeTransportCapture(
            messages=[
                with_symbol(load_fixture("l2_book.json"), "ETH"),
                with_symbol(load_fixture("trades.json"), "ETH"),
                with_symbol(load_fixture("active_asset_ctx.json"), "ETH"),
            ],
            recv_ts_start=1713818881100,
            started_at="2026-04-23T00:01:00Z",
            ended_at="2026-04-23T00:02:00Z",
        ),
    ]

    async def fake_transport(
        *,
        config: CollectorConfig,
        max_messages: int,
        max_runtime_seconds: int,
        ping_interval_seconds: int,
        reconnect_limit: int,
    ) -> SmokeTransportCapture:
        return captures.pop(0)

    settings = Settings(
        data_root=tmp_path,
        allowed_symbols=("BTC", "ETH"),
        smoke_max_messages=4,
        smoke_max_runtime_seconds=300,
        smoke_ping_interval_seconds=15,
        smoke_reconnect_limit=2,
    )

    result = asyncio.run(
        run_smoke_campaign(
            settings=settings,
            trading_date=date(2026, 4, 23),
            transport=fake_transport,
            capture_session_id="campaign-scope",
            session_count=2,
        )
    )

    summary = json.loads(result.report_paths["json"].read_text(encoding="utf-8"))

    assert result.final_audit_report.row_counts["book_events"] == 2
    assert result.final_audit_report.row_counts["book_levels"] == 8
    assert result.final_audit_report.row_counts["trades"] == 4
    assert result.final_audit_report.row_counts["asset_ctx"] == 2
    assert tuple(result.final_export_validation_report_paths) == ("BTC", "ETH")
    assert tuple(result.final_export_validations) == ("BTC", "ETH")
    assert summary["scope"]["allowed_symbols"] == ["BTC", "ETH"]
    assert summary["scope"]["observed_symbols"] == ["BTC", "ETH"]
    assert tuple(summary["final_export_validation_report_paths"]) == ("BTC", "ETH")


def test_run_smoke_campaign_persists_failed_summary_when_later_session_crashes(
    tmp_path,
) -> None:
    call_count = 0

    async def fake_transport(
        *,
        config: CollectorConfig,
        max_messages: int,
        max_runtime_seconds: int,
        ping_interval_seconds: int,
        reconnect_limit: int,
    ) -> SmokeTransportCapture:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return SmokeTransportCapture(
                messages=[
                    load_fixture("l2_book.json"),
                    load_fixture("trades.json"),
                    load_fixture("active_asset_ctx.json"),
                ],
                recv_ts_start=1713818880100,
                started_at="2026-04-23T00:00:00Z",
                ended_at="2026-04-23T00:01:00Z",
            )
        raise RuntimeError("synthetic transport failure")

    settings = Settings(
        data_root=tmp_path,
        allowed_symbols=("BTC",),
        smoke_max_messages=4,
        smoke_max_runtime_seconds=300,
        smoke_ping_interval_seconds=15,
        smoke_reconnect_limit=2,
    )

    with pytest.raises(RuntimeError, match="synthetic transport failure"):
        asyncio.run(
            run_smoke_campaign(
                settings=settings,
                trading_date=date(2026, 4, 23),
                transport=fake_transport,
                capture_session_id="campaign-failed",
                session_count=2,
            )
        )

    summary_path = (
        tmp_path
        / "reports"
        / "smoke_campaign"
        / "campaign=campaign-failed"
        / "summary.json"
    )
    markdown_path = summary_path.with_suffix(".md")
    summary = json.loads(summary_path.read_text(encoding="utf-8"))

    assert summary_path.exists()
    assert markdown_path.exists()
    assert summary["campaign_id"] == "campaign-failed"
    assert summary["status"] == "failed"
    assert summary["requested_session_count"] == 2
    assert summary["session_count"] == 1
    assert summary["total_row_count"] == 3
    assert summary["error_message"] == "synthetic transport failure"
    assert summary["sessions"][0]["capture_session_id"] == "campaign-failed-0001"
    assert Path(summary["final_audit_report_paths"]["json"]).exists()
    assert "status: failed" in markdown_path.read_text(encoding="utf-8")


def test_run_smoke_campaign_resume_continues_from_persisted_summary(
    tmp_path,
) -> None:
    async def initial_transport(
        *,
        config: CollectorConfig,
        max_messages: int,
        max_runtime_seconds: int,
        ping_interval_seconds: int,
        reconnect_limit: int,
    ) -> SmokeTransportCapture:
        return SmokeTransportCapture(
            messages=[
                load_fixture("l2_book.json"),
                load_fixture("trades.json"),
                load_fixture("active_asset_ctx.json"),
            ],
            recv_ts_start=1713818880100,
            started_at="2026-04-23T00:00:00Z",
            ended_at="2026-04-23T00:01:00Z",
        )

    resumed_calls = 0

    async def resumed_transport(
        *,
        config: CollectorConfig,
        max_messages: int,
        max_runtime_seconds: int,
        ping_interval_seconds: int,
        reconnect_limit: int,
    ) -> SmokeTransportCapture:
        nonlocal resumed_calls
        resumed_calls += 1
        return SmokeTransportCapture(
            messages=[
                load_fixture("l2_book.json"),
                load_fixture("trades.json"),
                load_fixture("active_asset_ctx.json"),
            ],
            recv_ts_start=1713818881100,
            started_at="2026-04-23T00:01:00Z",
            ended_at="2026-04-23T00:02:00Z",
        )

    settings = Settings(
        data_root=tmp_path,
        allowed_symbols=("BTC",),
        smoke_max_messages=4,
        smoke_max_runtime_seconds=300,
        smoke_ping_interval_seconds=15,
        smoke_reconnect_limit=2,
    )

    initial_result = asyncio.run(
        run_smoke_campaign(
            settings=settings,
            trading_date=date(2026, 4, 23),
            transport=initial_transport,
            capture_session_id="campaign-resume",
            session_count=1,
        )
    )
    resumed_result = asyncio.run(
        run_smoke_campaign(
            settings=settings,
            trading_date=date(2026, 4, 23),
            transport=resumed_transport,
            capture_session_id="campaign-resume",
            session_count=2,
            resume_campaign=True,
        )
    )

    summary = json.loads(resumed_result.report_paths["json"].read_text(encoding="utf-8"))

    assert initial_result.session_count == 1
    assert resumed_calls == 1
    assert resumed_result.status == "completed"
    assert resumed_result.requested_session_count == 2
    assert resumed_result.session_count == 2
    assert resumed_result.total_row_count == 6
    assert resumed_result.started_at == initial_result.started_at
    assert [session.capture_session_id for session in resumed_result.sessions] == [
        "campaign-resume-0001",
        "campaign-resume-0002",
    ]
    assert summary["status"] == "completed"
    assert summary["requested_session_count"] == 2
    assert summary["session_count"] == 2


@pytest.mark.parametrize(
    ("settings_overrides", "resume_date", "expected_fragment"),
    [
        ({"allowed_symbols": ("BTC", "ETH")}, date(2026, 4, 23), "allowed_symbols"),
        ({"market_data_network": "testnet"}, date(2026, 4, 23), "market_data_network"),
        ({"smoke_skew_warning_ms": 333}, date(2026, 4, 23), "thresholds"),
        ({}, date(2026, 4, 24), "trading_date"),
    ],
)
def test_run_smoke_campaign_resume_rejects_scope_drift(
    tmp_path,
    settings_overrides,
    resume_date,
    expected_fragment,
) -> None:
    async def initial_transport(
        *,
        config: CollectorConfig,
        max_messages: int,
        max_runtime_seconds: int,
        ping_interval_seconds: int,
        reconnect_limit: int,
    ) -> SmokeTransportCapture:
        return SmokeTransportCapture(
            messages=[
                load_fixture("l2_book.json"),
                load_fixture("trades.json"),
                load_fixture("active_asset_ctx.json"),
            ],
            recv_ts_start=1713818880100,
            started_at="2026-04-23T00:00:00Z",
            ended_at="2026-04-23T00:01:00Z",
        )

    resumed_calls = 0

    async def resumed_transport(
        *,
        config: CollectorConfig,
        max_messages: int,
        max_runtime_seconds: int,
        ping_interval_seconds: int,
        reconnect_limit: int,
    ) -> SmokeTransportCapture:
        nonlocal resumed_calls
        resumed_calls += 1
        return SmokeTransportCapture(
            messages=[
                load_fixture("l2_book.json"),
                load_fixture("trades.json"),
                load_fixture("active_asset_ctx.json"),
            ],
            recv_ts_start=1713818881100,
            started_at="2026-04-23T00:01:00Z",
            ended_at="2026-04-23T00:02:00Z",
        )

    initial_settings = Settings(
        data_root=tmp_path,
        allowed_symbols=("BTC",),
        smoke_max_messages=4,
        smoke_max_runtime_seconds=300,
        smoke_ping_interval_seconds=15,
        smoke_reconnect_limit=2,
    )
    asyncio.run(
        run_smoke_campaign(
            settings=initial_settings,
            trading_date=date(2026, 4, 23),
            transport=initial_transport,
            capture_session_id="campaign-drift",
            session_count=1,
        )
    )

    resumed_settings_kwargs = {
        "data_root": tmp_path,
        "allowed_symbols": ("BTC",),
        "smoke_max_messages": 4,
        "smoke_max_runtime_seconds": 300,
        "smoke_ping_interval_seconds": 15,
        "smoke_reconnect_limit": 2,
    }
    resumed_settings_kwargs.update(settings_overrides)
    resumed_settings = Settings(**resumed_settings_kwargs)
    with pytest.raises(ValueError, match=expected_fragment):
        asyncio.run(
            run_smoke_campaign(
                settings=resumed_settings,
                trading_date=resume_date,
                transport=resumed_transport,
                capture_session_id="campaign-drift",
                session_count=2,
                resume_campaign=True,
            )
        )

    assert resumed_calls == 0


def test_run_smoke_campaign_resume_rejects_missing_persisted_artifacts(
    tmp_path,
) -> None:
    async def initial_transport(
        *,
        config: CollectorConfig,
        max_messages: int,
        max_runtime_seconds: int,
        ping_interval_seconds: int,
        reconnect_limit: int,
    ) -> SmokeTransportCapture:
        return SmokeTransportCapture(
            messages=[
                load_fixture("l2_book.json"),
                load_fixture("trades.json"),
                load_fixture("active_asset_ctx.json"),
            ],
            recv_ts_start=1713818880100,
            started_at="2026-04-23T00:00:00Z",
            ended_at="2026-04-23T00:01:00Z",
        )

    resumed_calls = 0

    async def resumed_transport(
        *,
        config: CollectorConfig,
        max_messages: int,
        max_runtime_seconds: int,
        ping_interval_seconds: int,
        reconnect_limit: int,
    ) -> SmokeTransportCapture:
        nonlocal resumed_calls
        resumed_calls += 1
        return SmokeTransportCapture(
            messages=[
                load_fixture("l2_book.json"),
                load_fixture("trades.json"),
                load_fixture("active_asset_ctx.json"),
            ],
            recv_ts_start=1713818881100,
            started_at="2026-04-23T00:01:00Z",
            ended_at="2026-04-23T00:02:00Z",
        )

    settings = Settings(
        data_root=tmp_path,
        allowed_symbols=("BTC",),
        smoke_max_messages=4,
        smoke_max_runtime_seconds=300,
        smoke_ping_interval_seconds=15,
        smoke_reconnect_limit=2,
    )
    initial_result = asyncio.run(
        run_smoke_campaign(
            settings=settings,
            trading_date=date(2026, 4, 23),
            transport=initial_transport,
            capture_session_id="campaign-artifacts",
            session_count=1,
        )
    )

    missing_path = next(iter(initial_result.sessions[0].normalized_paths.values()))
    missing_path.unlink()

    with pytest.raises(ValueError, match="missing persisted artifact"):
        asyncio.run(
            run_smoke_campaign(
                settings=settings,
                trading_date=date(2026, 4, 23),
                transport=resumed_transport,
                capture_session_id="campaign-artifacts",
                session_count=2,
                resume_campaign=True,
            )
        )

    assert resumed_calls == 0


def test_run_smoke_campaign_writes_summary_via_atomic_replace(
    tmp_path,
    monkeypatch,
) -> None:
    replace_targets: list[Path] = []
    path_cls = type(tmp_path)
    original_replace = path_cls.replace

    def tracking_replace(self: Path, target: Path) -> Path:
        replace_targets.append(Path(target))
        return original_replace(self, target)

    monkeypatch.setattr(path_cls, "replace", tracking_replace)

    async def fake_transport(
        *,
        config: CollectorConfig,
        max_messages: int,
        max_runtime_seconds: int,
        ping_interval_seconds: int,
        reconnect_limit: int,
    ) -> SmokeTransportCapture:
        return SmokeTransportCapture(
            messages=[
                load_fixture("l2_book.json"),
                load_fixture("trades.json"),
                load_fixture("active_asset_ctx.json"),
            ],
            recv_ts_start=1713818880100,
            started_at="2026-04-23T00:00:00Z",
            ended_at="2026-04-23T00:01:00Z",
        )

    settings = Settings(
        data_root=tmp_path,
        allowed_symbols=("BTC",),
        smoke_max_messages=4,
        smoke_max_runtime_seconds=300,
        smoke_ping_interval_seconds=15,
        smoke_reconnect_limit=2,
    )
    result = asyncio.run(
        run_smoke_campaign(
            settings=settings,
            trading_date=date(2026, 4, 23),
            transport=fake_transport,
            capture_session_id="campaign-atomic",
            session_count=1,
        )
    )

    assert result.report_paths["json"] in replace_targets
    assert result.report_paths["md"] in replace_targets


def test_smoke_main_runs_fake_transport_and_prints_json_summary(
    tmp_path, capsys
) -> None:
    async def fake_transport(
        *,
        config: CollectorConfig,
        max_messages: int,
        max_runtime_seconds: int,
        ping_interval_seconds: int,
        reconnect_limit: int,
    ) -> SmokeTransportCapture:
        assert config.allowed_symbols == ("BTC",)
        assert max_messages == 4
        assert max_runtime_seconds == 300
        assert ping_interval_seconds == 15
        assert reconnect_limit == 2
        return SmokeTransportCapture(
            messages=[
                load_fixture("l2_book.json"),
                load_fixture("trades.json"),
                load_fixture("active_asset_ctx.json"),
            ],
            recv_ts_start=1713818880100,
            started_at="2026-04-23T00:00:00Z",
            ended_at="2026-04-23T00:01:00Z",
        )

    exit_code = main(
        [
            "--data-root",
            str(tmp_path),
            "--allowed-symbols",
            "BTC",
            "--date",
            "2026-04-23",
            "--capture-session-id",
            "session-cli",
            "--max-runtime-seconds",
            "300",
            "--max-messages",
            "4",
            "--ping-interval-seconds",
            "15",
            "--reconnect-limit",
            "2",
        ],
        transport=fake_transport,
    )

    summary = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert summary["capture_session_id"] == "session-cli"
    assert Path(summary["manifest_path"]).exists()
    assert Path(summary["audit_report_paths"]["json"]).exists()
    assert Path(summary["export_validation_report_paths"]["BTC"]).exists()


def test_smoke_main_runs_campaign_mode_and_prints_json_summary(
    tmp_path, capsys
) -> None:
    captures = [
        SmokeTransportCapture(
            messages=[
                load_fixture("l2_book.json"),
                load_fixture("trades.json"),
                load_fixture("active_asset_ctx.json"),
            ],
            recv_ts_start=1713818880100,
            started_at="2026-04-23T00:00:00Z",
            ended_at="2026-04-23T00:01:00Z",
        ),
        SmokeTransportCapture(
            messages=[
                load_fixture("l2_book.json"),
                load_fixture("trades.json"),
                load_fixture("active_asset_ctx.json"),
            ],
            recv_ts_start=1713818881100,
            started_at="2026-04-23T00:01:00Z",
            ended_at="2026-04-23T00:02:00Z",
        ),
    ]

    async def fake_transport(
        *,
        config: CollectorConfig,
        max_messages: int,
        max_runtime_seconds: int,
        ping_interval_seconds: int,
        reconnect_limit: int,
    ) -> SmokeTransportCapture:
        return captures.pop(0)

    exit_code = main(
        [
            "--data-root",
            str(tmp_path),
            "--allowed-symbols",
            "BTC",
            "--date",
            "2026-04-23",
            "--capture-session-id",
            "campaign-cli",
            "--session-count",
            "2",
        ],
        transport=fake_transport,
    )

    summary = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert summary["campaign_id"] == "campaign-cli"
    assert summary["status"] == "completed"
    assert summary["requested_session_count"] == 2
    assert summary["session_count"] == 2
    assert [session["capture_session_id"] for session in summary["sessions"]] == [
        "campaign-cli-0001",
        "campaign-cli-0002",
    ]
    assert Path(summary["report_paths"]["json"]).exists()
    assert Path(summary["report_paths"]["md"]).exists()
    assert Path(summary["sessions"][0]["manifest_path"]).exists()
    assert Path(summary["sessions"][1]["manifest_path"]).exists()
    assert summary["final_audit_report"]["row_counts"]["trades"] == 4


def test_smoke_main_campaign_mode_seeds_one_session_campaign_that_can_resume(
    tmp_path, capsys
) -> None:
    calls = 0

    async def fake_transport(
        *,
        config: CollectorConfig,
        max_messages: int,
        max_runtime_seconds: int,
        ping_interval_seconds: int,
        reconnect_limit: int,
    ) -> SmokeTransportCapture:
        nonlocal calls
        calls += 1
        recv_ts_start = 1713818880100 if calls == 1 else 1713818881100
        started_at = "2026-04-23T00:00:00Z" if calls == 1 else "2026-04-23T00:01:00Z"
        ended_at = "2026-04-23T00:01:00Z" if calls == 1 else "2026-04-23T00:02:00Z"
        return SmokeTransportCapture(
            messages=[
                load_fixture("l2_book.json"),
                load_fixture("trades.json"),
                load_fixture("active_asset_ctx.json"),
            ],
            recv_ts_start=recv_ts_start,
            started_at=started_at,
            ended_at=ended_at,
        )

    first_exit_code = main(
        [
            "--data-root",
            str(tmp_path),
            "--allowed-symbols",
            "BTC",
            "--date",
            "2026-04-23",
            "--capture-session-id",
            "campaign-cli-seed",
            "--session-count",
            "1",
            "--campaign-mode",
        ],
        transport=fake_transport,
    )
    first_summary = json.loads(capsys.readouterr().out)

    second_exit_code = main(
        [
            "--data-root",
            str(tmp_path),
            "--allowed-symbols",
            "BTC",
            "--date",
            "2026-04-23",
            "--capture-session-id",
            "campaign-cli-seed",
            "--session-count",
            "2",
            "--resume-campaign",
        ],
        transport=fake_transport,
    )
    second_summary = json.loads(capsys.readouterr().out)

    assert first_exit_code == 0
    assert first_summary["campaign_id"] == "campaign-cli-seed"
    assert first_summary["session_count"] == 1
    assert Path(first_summary["report_paths"]["json"]).exists()
    assert second_exit_code == 0
    assert calls == 2
    assert second_summary["session_count"] == 2
    assert [session["capture_session_id"] for session in second_summary["sessions"]] == [
        "campaign-cli-seed-0001",
        "campaign-cli-seed-0002",
    ]


def test_smoke_main_resume_campaign_continues_existing_summary(
    tmp_path, capsys
) -> None:
    resumed_calls = 0

    async def resumed_transport(
        *,
        config: CollectorConfig,
        max_messages: int,
        max_runtime_seconds: int,
        ping_interval_seconds: int,
        reconnect_limit: int,
    ) -> SmokeTransportCapture:
        nonlocal resumed_calls
        resumed_calls += 1
        return SmokeTransportCapture(
            messages=[
                load_fixture("l2_book.json"),
                load_fixture("trades.json"),
                load_fixture("active_asset_ctx.json"),
            ],
            recv_ts_start=1713818881100,
            started_at="2026-04-23T00:01:00Z",
            ended_at="2026-04-23T00:02:00Z",
        )

    settings = Settings(
        data_root=tmp_path,
        allowed_symbols=("BTC",),
        smoke_max_messages=4,
        smoke_max_runtime_seconds=300,
        smoke_ping_interval_seconds=15,
        smoke_reconnect_limit=2,
    )
    asyncio.run(
        run_smoke_campaign(
            settings=settings,
            trading_date=date(2026, 4, 23),
            transport=resumed_transport,
            capture_session_id="campaign-cli-resume",
            session_count=1,
        )
    )
    assert resumed_calls == 1
    resumed_calls = 0

    exit_code = main(
        [
            "--data-root",
            str(tmp_path),
            "--allowed-symbols",
            "BTC",
            "--date",
            "2026-04-23",
            "--capture-session-id",
            "campaign-cli-resume",
            "--session-count",
            "2",
            "--resume-campaign",
        ],
        transport=resumed_transport,
    )

    summary = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert resumed_calls == 1
    assert summary["status"] == "completed"
    assert summary["requested_session_count"] == 2
    assert summary["session_count"] == 2
    assert [session["capture_session_id"] for session in summary["sessions"]] == [
        "campaign-cli-resume-0001",
        "campaign-cli-resume-0002",
    ]


def test_smoke_main_rejects_resume_without_capture_session_id(tmp_path, capsys) -> None:
    exit_code = main(
        [
            "--data-root",
            str(tmp_path),
            "--allowed-symbols",
            "BTC",
            "--session-count",
            "2",
            "--resume-campaign",
        ],
    )

    captured = capsys.readouterr()

    assert exit_code == 1
    assert "resume_campaign requires explicit capture_session_id" in captured.err


def test_smoke_main_rejects_non_positive_session_count(tmp_path, capsys) -> None:
    exit_code = main(
        [
            "--data-root",
            str(tmp_path),
            "--allowed-symbols",
            "BTC",
            "--session-count",
            "0",
        ],
    )

    captured = capsys.readouterr()

    assert exit_code == 1
    assert "session_count must be at least 1" in captured.err


def test_smoke_main_persists_failed_campaign_summary_on_error(
    tmp_path, capsys
) -> None:
    call_count = 0

    async def fake_transport(
        *,
        config: CollectorConfig,
        max_messages: int,
        max_runtime_seconds: int,
        ping_interval_seconds: int,
        reconnect_limit: int,
    ) -> SmokeTransportCapture:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return SmokeTransportCapture(
                messages=[
                    load_fixture("l2_book.json"),
                    load_fixture("trades.json"),
                    load_fixture("active_asset_ctx.json"),
                ],
                recv_ts_start=1713818880100,
                started_at="2026-04-23T00:00:00Z",
                ended_at="2026-04-23T00:01:00Z",
            )
        raise RuntimeError("synthetic cli failure")

    exit_code = main(
        [
            "--data-root",
            str(tmp_path),
            "--allowed-symbols",
            "BTC",
            "--date",
            "2026-04-23",
            "--capture-session-id",
            "campaign-cli-failed",
            "--session-count",
            "2",
        ],
        transport=fake_transport,
    )

    captured = capsys.readouterr()
    summary_path = (
        tmp_path
        / "reports"
        / "smoke_campaign"
        / "campaign=campaign-cli-failed"
        / "summary.json"
    )
    summary = json.loads(summary_path.read_text(encoding="utf-8"))

    assert exit_code == 1
    assert "synthetic cli failure" in captured.err
    assert summary["status"] == "failed"
    assert summary["requested_session_count"] == 2
    assert summary["session_count"] == 1


def test_smoke_main_preserves_env_defaults_and_allows_cli_overrides(
    tmp_path, monkeypatch, capsys
) -> None:
    captured: dict[str, object] = {}

    async def fake_transport(
        *,
        config: CollectorConfig,
        max_messages: int,
        max_runtime_seconds: int,
        ping_interval_seconds: int,
        reconnect_limit: int,
    ) -> SmokeTransportCapture:
        captured["allowed_symbols"] = config.allowed_symbols
        captured["max_messages"] = max_messages
        captured["max_runtime_seconds"] = max_runtime_seconds
        captured["ping_interval_seconds"] = ping_interval_seconds
        captured["reconnect_limit"] = reconnect_limit
        return SmokeTransportCapture(
            messages=[
                load_fixture("l2_book.json"),
                load_fixture("trades.json"),
            ],
            recv_ts_start=1713818880100,
            started_at="2026-04-23T00:00:00Z",
            ended_at="2026-04-23T00:00:30Z",
        )

    monkeypatch.setenv("STATESTRIKE_ALLOWED_SYMBOLS", "BTC,ETH")
    monkeypatch.setenv("STATESTRIKE_SMOKE_MAX_MESSAGES", "7")
    monkeypatch.setenv("STATESTRIKE_SMOKE_MAX_RUNTIME_SECONDS", "600")
    monkeypatch.setenv("STATESTRIKE_SMOKE_PING_INTERVAL_SECONDS", "11")
    monkeypatch.setenv("STATESTRIKE_SMOKE_RECONNECT_LIMIT", "5")
    monkeypatch.setenv("STATESTRIKE_SMOKE_SKEW_WARNING_MS", "222")
    monkeypatch.setenv("STATESTRIKE_SMOKE_SKEW_SEVERE_MS", "999")
    monkeypatch.setenv("STATESTRIKE_SMOKE_ASSET_CTX_STALE_THRESHOLD_MS", "123456")

    exit_code = main(
        [
            "--data-root",
            str(tmp_path),
            "--allowed-symbols",
            "SOL",
            "--date",
            "2026-04-23",
            "--capture-session-id",
            "session-env-override",
            "--skew-severe-ms",
            "888",
        ],
        transport=fake_transport,
    )

    summary = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert captured == {
        "allowed_symbols": ("SOL",),
        "max_messages": 7,
        "max_runtime_seconds": 600,
        "ping_interval_seconds": 11,
        "reconnect_limit": 5,
    }
    assert summary["capture_session_id"] == "session-env-override"
    assert summary["audit_report"]["thresholds"] == {
        "skew_warning_ms": 222,
        "skew_severe_ms": 888,
        "asset_ctx_stale_threshold_ms": 123456,
        "trade_gap_threshold_ms": 60000,
        "quarantine_warning_rate": 0.001,
        "quarantine_severe_rate": 0.01,
    }


def test_smoke_main_keeps_stdout_json_when_negative_latency_is_corrected(
    tmp_path, capsys
) -> None:
    delayed_trades = copy.deepcopy(load_fixture("trades.json"))
    delayed_trades["data"][0]["time"] = 1713818880200
    delayed_trades["data"][1]["time"] = 1713818880210

    async def fake_transport(
        *,
        config: CollectorConfig,
        max_messages: int,
        max_runtime_seconds: int,
        ping_interval_seconds: int,
        reconnect_limit: int,
    ) -> SmokeTransportCapture:
        return SmokeTransportCapture(
            messages=[
                load_fixture("l2_book.json"),
                delayed_trades,
                load_fixture("active_asset_ctx.json"),
            ],
            recv_ts_start=1713818880100,
            started_at="2026-04-23T00:00:00Z",
            ended_at="2026-04-23T00:01:00Z",
        )

    exit_code = main(
        [
            "--data-root",
            str(tmp_path),
            "--allowed-symbols",
            "BTC",
            "--date",
            "2026-04-23",
        ],
        transport=fake_transport,
    )

    stdout = capsys.readouterr().out

    assert exit_code == 0
    assert "local_timestamp is ahead" not in stdout
    assert json.loads(stdout)["export_validations"]["BTC"]["hftbacktest"][
        "local_ts_monotonic"
    ] is True
