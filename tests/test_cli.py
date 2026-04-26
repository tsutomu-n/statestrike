from __future__ import annotations

import json

import statestrike.cli as cli
from statestrike.quality import QualityAuditReport, QualityAuditThresholds


def test_cli_collect_smoke_delegates_to_smoke_main(monkeypatch, tmp_path, capsys) -> None:
    captured: dict[str, object] = {}

    def fake_smoke_main(argv: list[str] | None = None, *, transport=None) -> int:
        captured["argv"] = argv
        return 0

    monkeypatch.setattr(cli.smoke, "main", fake_smoke_main)

    exit_code = cli.main(
        [
            "collect-smoke",
            "--data-root",
            str(tmp_path),
            "--network",
            "mainnet",
            "--symbols",
            "BTC,ETH",
            "--duration-hours",
            "1",
        ]
    )

    assert exit_code == 0
    assert captured["argv"] == [
        "--market-data-network",
        "mainnet",
        "--allowed-symbols",
        "BTC,ETH",
        "--channels",
        "l2Book,trades,activeAssetCtx,candle",
        "--candle-interval",
        "1m",
        "--max-runtime-seconds",
        "3600",
        "--data-root",
        str(tmp_path),
    ]


def test_cli_run_phase1_smoke_enables_campaign_mode(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}

    def fake_smoke_main(argv: list[str] | None = None, *, transport=None) -> int:
        captured["argv"] = argv
        return 0

    monkeypatch.setattr(cli.smoke, "main", fake_smoke_main)

    exit_code = cli.main(
        [
            "run-phase1-smoke",
            "--data-root",
            str(tmp_path),
            "--symbols",
            "BTC",
            "--duration-hours",
            "2",
        ]
    )

    assert exit_code == 0
    assert "--campaign-mode" in captured["argv"]


def test_cli_audit_quality_prints_json_report(monkeypatch, tmp_path, capsys) -> None:
    report = QualityAuditReport(
        thresholds=QualityAuditThresholds(
            skew_warning_ms=250,
            skew_severe_ms=1000,
            asset_ctx_stale_threshold_ms=300000,
            trade_gap_threshold_ms=60000,
            quarantine_warning_rate=0.001,
            quarantine_severe_rate=0.01,
        ),
        row_counts={"book_events": 1, "book_levels": 2, "trades": 3, "asset_ctx": 1},
        quarantine_row_counts={"book_events": 0, "book_levels": 0, "trades": 0, "asset_ctx": 0},
        quarantine_reason_counts={"book_events": {}, "book_levels": {}, "trades": {}, "asset_ctx": {}},
        quarantine_rates={"book_events": 0.0, "book_levels": 0.0, "trades": 0.0, "asset_ctx": 0.0},
        quarantine_alerts={"book_events": "ok", "book_levels": "ok", "trades": "ok", "asset_ctx": "ok"},
        gap_count=0,
        skew_summary={"book_events": {}, "trades": {}, "asset_ctx": {}},
        symbol_skew_summary={"BTC": {"book_events": {}, "trades": {}, "asset_ctx": {}}},
        skew_alerts={"book_events": "ok", "trades": "ok", "asset_ctx": "ok"},
        crossed_book_count=0,
        zero_or_negative_qty_count=0,
        asset_ctx_stale_count=0,
        duplicate_trade_count=0,
        raw_duplicate_trade_count=0,
        reconnect_replay_duplicate_trade_count=0,
        session_replay_duplicate_trade_count=0,
        unexplained_duplicate_trade_count=0,
        book_continuity_gap_count=0,
        recoverable_book_gap_count=0,
        non_recoverable_book_gap_count=0,
        book_epoch_switch_count=0,
        non_monotonic_exchange_ts_count=0,
        non_monotonic_recv_ts_count=0,
        exchange_sorted_recv_inversion_count=0,
        capture_order_integrity="ok",
        capture_log_file_count=1,
        capture_log_row_count=1,
        capture_order_missing_recv_timestamp_count=0,
        capture_order_recv_seq_non_monotonic_count=0,
        empty_snapshot_count=0,
        trade_gap_count=0,
        asset_ctx_gap_count=0,
    )

    def fake_run_quality_audit(**kwargs) -> QualityAuditReport:
        return report

    monkeypatch.setattr(cli, "run_quality_audit", fake_run_quality_audit)

    exit_code = cli.main(
        [
            "audit-quality",
            "--data-root",
            str(tmp_path),
            "--date",
            "2026-04-24",
            "--symbols",
            "BTC",
        ]
    )

    summary = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert summary["row_counts"]["trades"] == 3


def test_cli_export_commands_print_resegmented_truth_and_corrected_paths(
    monkeypatch, tmp_path, capsys
) -> None:
    monkeypatch.setattr(
        cli,
        "export_nautilus_catalog",
        lambda **kwargs: tmp_path
        / "exports"
        / "truth"
        / "nautilus"
        / "date=2026-04-24"
        / "symbol=BTC",
    )
    monkeypatch.setattr(
        cli,
        "export_hftbacktest_npz",
        lambda **kwargs: tmp_path
        / "exports"
        / "corrected"
        / "hftbacktest"
        / "date=2026-04-24"
        / "symbol=BTC"
        / "btc_market_data.npz",
    )

    nautilus_exit = cli.main(
        [
            "export-nautilus",
            "--data-root",
            str(tmp_path),
            "--date",
            "2026-04-24",
            "--symbol",
            "BTC",
        ]
    )
    nautilus_summary = json.loads(capsys.readouterr().out)

    hft_exit = cli.main(
        [
            "export-hft",
            "--data-root",
            str(tmp_path),
            "--date",
            "2026-04-24",
            "--symbol",
            "BTC",
        ]
    )
    hft_summary = json.loads(capsys.readouterr().out)

    assert nautilus_exit == 0
    assert nautilus_summary["path"].endswith(
        "/exports/truth/nautilus/date=2026-04-24/symbol=BTC"
    )
    assert hft_exit == 0
    assert hft_summary["path"].endswith(
        "/exports/corrected/hftbacktest/date=2026-04-24/symbol=BTC/btc_market_data.npz"
    )
