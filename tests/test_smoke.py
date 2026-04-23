from __future__ import annotations

import asyncio
import copy
import json
from datetime import date
from pathlib import Path

from statestrike.collector import CollectorConfig
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
    assert result.raw_paths["trades:BTC"].exists()
    assert result.normalized_paths["trades:BTC"].exists()
    assert result.quarantine_paths["trades:BTC"].exists()
    assert result.audit_report_paths["json"].exists()
    assert result.audit_report_paths["md"].exists()
    assert result.export_validation_report_paths["BTC"].exists()
    assert manifest["row_count"] == 4
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
    assert manifest["started_at"] == "2026-04-23T00:00:00Z"
    assert manifest["ended_at"] == "2026-04-23T00:05:00Z"
    assert manifest["ws_disconnect_count"] == 1
    assert manifest["reconnect_count"] == 1
    assert manifest["gap_flags"] == ["ws_reconnect"]


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
    assert campaign_json["session_count"] == 2
    assert result.final_audit_report_paths["json"].exists()
    assert result.final_export_validation_report_paths["BTC"].exists()


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
