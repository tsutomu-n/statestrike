from __future__ import annotations

import copy
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from statestrike.collector import CollectorConfig
from statestrike.enrichment import enrich_funding_schedule_from_predicted_fundings
from statestrike.funding import build_funding_history_sidecar
from statestrike.readiness import (
    BacktestReadinessThresholds,
    ReadinessProfile,
    _export_contract_is_valid,
    run_backtest_readiness,
)
from statestrike.recovery import MessageCaptureContext, MessageIngressMeta
from statestrike.smoke import run_smoke_batch
from statestrike.storage import _read_parquet_frame, _write_parquet_frame


FIXTURES = Path(__file__).parent / "fixtures" / "hyperliquid"
PREDICTED_FUNDINGS = [
    [
        "BTC",
        [
            [
                "HlPerp",
                {
                    "fundingRate": "0.0000125",
                    "nextFundingTime": 1713819600000,
                    "fundingIntervalHours": 1,
                },
            ]
        ],
    ]
]


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


def funding_history_payload(symbol: str, trading_date: date) -> list[dict[str, str | int]]:
    start = datetime.combine(trading_date, datetime.min.time(), tzinfo=UTC)
    return [
        {
            "coin": symbol,
            "fundingRate": "0.0000125",
            "premium": "0.0000010",
            "time": int((start + timedelta(hours=hour)).timestamp() * 1000),
        }
        for hour in range(24)
    ]


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def readiness_config() -> CollectorConfig:
    return CollectorConfig(
        allowed_symbols=("BTC",),
        source_priority=("ws", "info", "s3", "tardis"),
        market_data_network="mainnet",
        flush_interval_ms=1000,
        snapshot_recovery_enabled=True,
        channels=("l2Book", "trades", "activeAssetCtx"),
        candle_interval=None,
    )


def readiness_config_multi() -> CollectorConfig:
    return readiness_config().model_copy(update={"allowed_symbols": ("BTC", "ETH")})


def enrich_funding_sidecar(root: Path, *, trading_date: date) -> None:
    enrich_funding_schedule_from_predicted_fundings(
        root=root,
        trading_date=trading_date,
        symbols=("BTC",),
        predicted_fundings=PREDICTED_FUNDINGS,
        enrichment_asof_ts=1713819000000,
    )


def enrich_funding_history_sidecar(
    root: Path,
    *,
    trading_date: date,
    symbols: tuple[str, ...] = ("BTC",),
    funding_interval_hours: int = 1,
) -> None:
    build_funding_history_sidecar(
        root=root,
        trading_date=trading_date,
        symbols=symbols,
        funding_history_payloads={
            symbol: funding_history_payload(symbol, trading_date)
            for symbol in symbols
        },
        funding_interval_hours=funding_interval_hours,
        source_root=root / "source-truth",
        copied_into_baseline_root=True,
    )


def run_clean_btc_eth_smoke(root: Path, *, trading_date: date) -> None:
    eth_trades = with_symbol(load_fixture("trades.json"), "ETH")
    eth_trades["data"][0]["tid"] = 101
    eth_trades["data"][1]["tid"] = 102
    run_smoke_batch(
        root=root,
        trading_date=trading_date,
        messages=[
            load_fixture("l2_book.json"),
            load_fixture("trades.json"),
            load_fixture("active_asset_ctx.json"),
            with_symbol(load_fixture("l2_book.json"), "ETH"),
            eth_trades,
            with_symbol(load_fixture("active_asset_ctx.json"), "ETH"),
        ],
        config=readiness_config_multi(),
        capture_session_id="session-ready-btc-eth",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )


def test_backtest_readiness_blocks_when_funding_enrichment_is_missing(tmp_path) -> None:
    run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        messages=[
            load_fixture("l2_book.json"),
            load_fixture("trades.json"),
            load_fixture("active_asset_ctx.json"),
        ],
        config=readiness_config(),
        capture_session_id="session-ready-blocked",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )

    report = run_backtest_readiness(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        symbols=("BTC",),
    )

    assert report.status == "blocked"
    assert report.profile.name == "funding_aware_ready"
    assert "funding_enrichment_incomplete" in report.blocking_reasons
    assert report.capture_log_file_count == 1
    assert report.missing_recv_timestamp_count == 0


def test_nautilus_baseline_profile_warns_when_funding_enrichment_is_missing(tmp_path) -> None:
    run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        messages=[
            load_fixture("l2_book.json"),
            load_fixture("trades.json"),
            load_fixture("active_asset_ctx.json"),
        ],
        config=readiness_config(),
        capture_session_id="session-ready-baseline-warning",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )

    report = run_backtest_readiness(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        symbols=("BTC",),
        profile="nautilus_baseline_ready",
    )

    assert report.status == "warning"
    assert "funding_enrichment_incomplete" not in report.blocking_reasons
    assert "funding_enrichment_incomplete" in report.warning_reasons
    assert report.profile.funding_required is False


def test_backtest_readiness_ready_after_enrichment_for_clean_dataset(tmp_path) -> None:
    result = run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        messages=[
            load_fixture("l2_book.json"),
            load_fixture("trades.json"),
            load_fixture("active_asset_ctx.json"),
        ],
        config=readiness_config(),
        capture_session_id="session-ready-clean",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )
    enrich_funding_sidecar(tmp_path, trading_date=date(2026, 4, 23))

    report = run_backtest_readiness(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        symbols=("BTC",),
    )

    assert report.status == "ready"
    assert report.profile == ReadinessProfile.funding_aware_ready()
    assert report.blocking_reasons == ()
    assert report.warning_reasons == ()
    assert report.export_validations["BTC"].correction_applied == ("hftbacktest",)
    assert report.export_contract_invalid_symbols == ()


def test_backtest_readiness_warns_on_quarantine_rate_without_blocking(tmp_path) -> None:
    invalid_trades = copy.deepcopy(load_fixture("trades.json"))
    invalid_trades["data"][0]["sz"] = "0.0"
    result = run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        messages=[
            load_fixture("l2_book.json"),
            invalid_trades,
            load_fixture("active_asset_ctx.json"),
        ],
        config=readiness_config(),
        capture_session_id="session-ready-warning",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )
    enrich_funding_sidecar(tmp_path, trading_date=date(2026, 4, 23))

    report = run_backtest_readiness(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        symbols=("BTC",),
        thresholds=BacktestReadinessThresholds(
            quarantine_warning_rate=0.2,
            quarantine_blocking_rate=0.5,
        ),
    )

    assert report.status == "warning"
    assert report.blocking_reasons == ()
    assert report.warning_reasons == ("quarantine_rate_over_warning_threshold",)


def test_backtest_readiness_blocks_when_export_contract_is_invalid(tmp_path) -> None:
    result = run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        messages=[
            load_fixture("l2_book.json"),
            load_fixture("trades.json"),
            load_fixture("active_asset_ctx.json"),
        ],
        config=readiness_config(),
        capture_session_id="session-ready-invalid-export-contract",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )
    enrich_funding_sidecar(tmp_path, trading_date=date(2026, 4, 23))

    report = run_backtest_readiness(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        symbols=("BTC",),
    )

    invalid_report = report.export_validations["BTC"].model_copy(
        update={
            "corrected_exports": {
                **report.export_validations["BTC"].corrected_exports,
                "hftbacktest": report.export_validations["BTC"].corrected_exports[
                    "hftbacktest"
                ].model_copy(update={"truth_preserving": True}),
            }
        }
    )

    assert report.status == "ready"
    assert report.export_contract_invalid_symbols == ()
    assert _export_contract_is_valid(report.export_validations["BTC"]) is True
    assert _export_contract_is_valid(invalid_report) is False


def test_backtest_readiness_blocks_when_required_export_file_is_missing(tmp_path) -> None:
    result = run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        messages=[
            load_fixture("l2_book.json"),
            load_fixture("trades.json"),
            load_fixture("active_asset_ctx.json"),
        ],
        config=readiness_config(),
        capture_session_id="session-ready-missing-export",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )
    enrich_funding_sidecar(tmp_path, trading_date=date(2026, 4, 23))
    (
        tmp_path
        / "exports"
        / "truth"
        / "nautilus"
        / "date=2026-04-23"
        / "symbol=BTC"
        / "trade_ticks.parquet"
    ).unlink()

    report = run_backtest_readiness(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        symbols=("BTC",),
    )

    assert report.status == "blocked"
    assert report.export_artifact_invalid_symbols == ("BTC",)
    assert "required_export_artifact_incomplete" in report.blocking_reasons


def test_backtest_readiness_blocks_when_required_export_has_zero_rows(tmp_path) -> None:
    result = run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        messages=[
            load_fixture("l2_book.json"),
            load_fixture("trades.json"),
            load_fixture("active_asset_ctx.json"),
        ],
        config=readiness_config(),
        capture_session_id="session-ready-zero-export",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )
    enrich_funding_sidecar(tmp_path, trading_date=date(2026, 4, 23))
    trade_path = (
        tmp_path
        / "exports"
        / "truth"
        / "nautilus"
        / "date=2026-04-23"
        / "symbol=BTC"
        / "trade_ticks.parquet"
    )
    _write_parquet_frame(
        path=trade_path,
        frame=_read_parquet_frame(trade_path).iloc[0:0].copy(),
    )

    report = run_backtest_readiness(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        symbols=("BTC",),
    )

    assert report.status == "blocked"
    assert report.export_artifact_invalid_symbols == ("BTC",)
    assert "required_export_artifact_incomplete" in report.blocking_reasons


def test_backtest_readiness_blocks_when_required_normalized_table_is_missing(tmp_path) -> None:
    result = run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        messages=[
            load_fixture("l2_book.json"),
            load_fixture("trades.json"),
            load_fixture("active_asset_ctx.json"),
        ],
        config=readiness_config(),
        capture_session_id="session-ready-missing-normalized",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )
    enrich_funding_sidecar(tmp_path, trading_date=date(2026, 4, 23))
    result.normalized_paths["trades:BTC"].unlink()

    report = run_backtest_readiness(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        symbols=("BTC",),
    )

    assert report.status == "blocked"
    assert report.normalized_table_invalid_symbols == ("BTC",)
    assert "required_normalized_table_missing" in report.blocking_reasons


def test_backtest_readiness_reports_requested_and_observed_symbol_drift(tmp_path) -> None:
    result = run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        messages=[
            load_fixture("l2_book.json"),
            load_fixture("trades.json"),
            load_fixture("active_asset_ctx.json"),
        ],
        config=readiness_config(),
        capture_session_id="session-ready-symbol-drift",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )
    enrich_funding_sidecar(tmp_path, trading_date=date(2026, 4, 23))

    report = run_backtest_readiness(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        symbols=("BTC", "ETH"),
    )

    assert report.requested_symbols == ("BTC", "ETH")
    assert report.observed_symbols == ("BTC",)
    assert report.missing_requested_symbols == ("ETH",)
    assert "requested_symbol_missing" in report.blocking_reasons


def test_backtest_readiness_blocks_when_funding_source_dex_is_unsupported(tmp_path) -> None:
    run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        messages=[
            load_fixture("l2_book.json"),
            load_fixture("trades.json"),
            load_fixture("active_asset_ctx.json"),
        ],
        config=readiness_config(),
        capture_session_id="session-ready-unsupported-dex",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )
    enrich_funding_schedule_from_predicted_fundings(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        symbols=("BTC",),
        predicted_fundings=PREDICTED_FUNDINGS,
        dex="builder-dex",
        enrichment_asof_ts=1713819000000,
    )

    report = run_backtest_readiness(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        symbols=("BTC",),
    )

    assert report.status == "blocked"
    assert "funding_source_unsupported_for_dex" in report.blocking_reasons


def test_funding_candidate_blocks_when_one_symbol_missing(tmp_path) -> None:
    trading_date = date(2026, 4, 23)
    run_clean_btc_eth_smoke(tmp_path, trading_date=trading_date)
    enrich_funding_history_sidecar(
        tmp_path,
        trading_date=trading_date,
        symbols=("BTC",),
    )

    report = run_backtest_readiness(
        root=tmp_path,
        trading_date=trading_date,
        symbols=("BTC", "ETH"),
        profile="nautilus_funding_candidate",
    )

    assert report.status == "blocked"
    assert report.funding_history_missing_symbols == ("ETH",)
    assert "funding_symbol_coverage_incomplete" in report.blocking_reasons


def test_funding_candidate_blocks_when_interval_mismatch(tmp_path) -> None:
    trading_date = date(2026, 4, 23)
    run_smoke_batch(
        root=tmp_path,
        trading_date=trading_date,
        messages=[
            load_fixture("l2_book.json"),
            load_fixture("trades.json"),
            load_fixture("active_asset_ctx.json"),
        ],
        config=readiness_config(),
        capture_session_id="session-funding-interval-mismatch",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )
    enrich_funding_history_sidecar(
        tmp_path,
        trading_date=trading_date,
        funding_interval_hours=8,
    )

    report = run_backtest_readiness(
        root=tmp_path,
        trading_date=trading_date,
        symbols=("BTC",),
        profile="nautilus_funding_candidate",
    )

    assert report.status == "blocked"
    assert report.funding_history_interval_mismatch_symbols == ("BTC",)
    assert "funding_interval_mismatch" in report.blocking_reasons


def test_funding_candidate_blocks_when_predicted_funding_used_for_historical_data(
    tmp_path,
) -> None:
    trading_date = date(2026, 4, 23)
    run_smoke_batch(
        root=tmp_path,
        trading_date=trading_date,
        messages=[
            load_fixture("l2_book.json"),
            load_fixture("trades.json"),
            load_fixture("active_asset_ctx.json"),
        ],
        config=readiness_config(),
        capture_session_id="session-funding-predicted-for-history",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )
    enrich_funding_sidecar(tmp_path, trading_date=trading_date)

    report = run_backtest_readiness(
        root=tmp_path,
        trading_date=trading_date,
        symbols=("BTC",),
        profile="nautilus_funding_candidate",
    )

    assert report.status == "blocked"
    assert report.funding_history_manifest_path is None
    assert "predicted_funding_used_for_historical_baseline" in report.blocking_reasons


def test_funding_candidate_ready_for_btc_eth_with_complete_history(tmp_path) -> None:
    trading_date = date(2026, 4, 23)
    run_clean_btc_eth_smoke(tmp_path, trading_date=trading_date)
    enrich_funding_history_sidecar(
        tmp_path,
        trading_date=trading_date,
        symbols=("BTC", "ETH"),
    )

    report = run_backtest_readiness(
        root=tmp_path,
        trading_date=trading_date,
        symbols=("BTC", "ETH"),
        profile="nautilus_funding_candidate",
    )

    assert report.status == "ready"
    assert report.profile == ReadinessProfile.nautilus_funding_candidate()
    assert report.blocking_reasons == ()
    assert report.warning_reasons == ()
    assert report.funding_history_row_count == 48
    assert report.funding_history_missing_symbols == ()


def test_backtest_readiness_warns_but_does_not_block_exchange_sorted_recv_inversion(
    tmp_path,
) -> None:
    earlier_exchange_later_recv = copy.deepcopy(load_fixture("trades.json"))
    earlier_exchange_later_recv["data"] = [earlier_exchange_later_recv["data"][0]]
    earlier_exchange_later_recv["data"][0]["time"] = 1713818880000
    earlier_exchange_later_recv["data"][0]["tid"] = 9001
    later_exchange_earlier_recv = copy.deepcopy(load_fixture("trades.json"))
    later_exchange_earlier_recv["data"] = [later_exchange_earlier_recv["data"][1]]
    later_exchange_earlier_recv["data"][0]["time"] = 1713818881000
    later_exchange_earlier_recv["data"][0]["tid"] = 9002

    run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        messages=[
            load_fixture("l2_book.json"),
            earlier_exchange_later_recv,
            later_exchange_earlier_recv,
            load_fixture("active_asset_ctx.json"),
        ],
        ingress_metadata=[
            # Capture order is intact, but exchange-time sorting will expose a
            # receive timestamp inversion between the two trades.
            _ingress(recv_wall_ns=1713818880000000000, recv_seq=0),
            _ingress(recv_wall_ns=1713818882000000000, recv_seq=1),
            _ingress(recv_wall_ns=1713818881000000000, recv_seq=2),
            _ingress(recv_wall_ns=1713818883000000000, recv_seq=3),
        ],
        config=readiness_config(),
        capture_session_id="session-ready-recv-inversion",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )
    enrich_funding_sidecar(tmp_path, trading_date=date(2026, 4, 23))

    report = run_backtest_readiness(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        symbols=("BTC",),
        profile="substrate_ready",
    )

    assert report.quality_report.capture_order_integrity == "ok"
    assert report.quality_report.exchange_sorted_recv_inversion_count == 1
    assert report.status == "warning"
    assert "exchange_sorted_recv_inversion_observed" in report.warning_reasons
    assert "non_monotonic_recv_ts_over_threshold" not in report.blocking_reasons


def test_backtest_readiness_does_not_block_reconnect_replay_duplicates(
    tmp_path,
) -> None:
    first_trades = copy.deepcopy(load_fixture("trades.json"))
    replayed_trades = copy.deepcopy(load_fixture("trades.json"))

    run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        messages=[
            load_fixture("l2_book.json"),
            first_trades,
            replayed_trades,
            load_fixture("active_asset_ctx.json"),
        ],
        message_contexts=[
            MessageCaptureContext(reconnect_epoch=0, book_epoch=1, book_event_kind="snapshot"),
            MessageCaptureContext(reconnect_epoch=0, book_epoch=1),
            MessageCaptureContext(reconnect_epoch=1, book_epoch=2, continuity_status="recovered"),
            MessageCaptureContext(reconnect_epoch=1, book_epoch=2, continuity_status="recovered"),
        ],
        config=readiness_config(),
        capture_session_id="session-ready-reconnect-replay-duplicates",
        batch_id="0001",
        recv_ts_start=1713818880100,
        reconnect_epoch=1,
        book_epoch=2,
        manifest_reconnect_count=1,
        gap_flags=("ws_reconnect",),
    )
    enrich_funding_sidecar(tmp_path, trading_date=date(2026, 4, 23))

    report = run_backtest_readiness(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        symbols=("BTC",),
        profile="substrate_ready",
    )

    assert report.quality_report.raw_duplicate_trade_count == 2
    assert report.quality_report.reconnect_replay_duplicate_trade_count == 2
    assert report.quality_report.unexplained_duplicate_trade_count == 0
    assert report.status == "warning"
    assert "reconnect_replay_duplicate_trade_observed" in report.warning_reasons
    assert "duplicate_trade_count_over_threshold" not in report.blocking_reasons


def test_backtest_readiness_does_not_block_session_replay_duplicates(
    tmp_path,
) -> None:
    messages = [
        load_fixture("l2_book.json"),
        load_fixture("trades.json"),
        load_fixture("active_asset_ctx.json"),
    ]

    run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        messages=copy.deepcopy(messages),
        config=readiness_config(),
        capture_session_id="session-ready-session-replay-duplicates-0001",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )
    run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        messages=copy.deepcopy(messages),
        config=readiness_config(),
        capture_session_id="session-ready-session-replay-duplicates-0002",
        batch_id="0002",
        recv_ts_start=1713819780100,
    )
    enrich_funding_sidecar(tmp_path, trading_date=date(2026, 4, 23))

    report = run_backtest_readiness(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        symbols=("BTC",),
        profile="substrate_ready",
    )

    assert report.quality_report.raw_duplicate_trade_count == 2
    assert report.quality_report.reconnect_replay_duplicate_trade_count == 0
    assert report.quality_report.session_replay_duplicate_trade_count == 2
    assert report.quality_report.unexplained_duplicate_trade_count == 0
    assert report.status == "warning"
    assert "session_replay_duplicate_trade_observed" in report.warning_reasons
    assert "unexplained_duplicate_trade_count_over_threshold" not in report.blocking_reasons


def test_backtest_readiness_blocks_unexplained_duplicates(tmp_path) -> None:
    first_trades = copy.deepcopy(load_fixture("trades.json"))
    duplicated_trades = copy.deepcopy(load_fixture("trades.json"))

    run_smoke_batch(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        messages=[
            load_fixture("l2_book.json"),
            first_trades,
            duplicated_trades,
            load_fixture("active_asset_ctx.json"),
        ],
        config=readiness_config(),
        capture_session_id="session-ready-unexplained-duplicates",
        batch_id="0001",
        recv_ts_start=1713818880100,
    )
    enrich_funding_sidecar(tmp_path, trading_date=date(2026, 4, 23))

    report = run_backtest_readiness(
        root=tmp_path,
        trading_date=date(2026, 4, 23),
        symbols=("BTC",),
        profile="substrate_ready",
    )

    assert report.quality_report.raw_duplicate_trade_count == 2
    assert report.quality_report.reconnect_replay_duplicate_trade_count == 0
    assert report.quality_report.session_replay_duplicate_trade_count == 0
    assert report.quality_report.unexplained_duplicate_trade_count == 2
    assert report.status == "blocked"
    assert "unexplained_duplicate_trade_count_over_threshold" in report.blocking_reasons


def _ingress(*, recv_wall_ns: int, recv_seq: int) -> MessageIngressMeta:
    return MessageIngressMeta(
        recv_wall_ns=recv_wall_ns,
        recv_mono_ns=recv_seq,
        recv_seq=recv_seq,
        connection_id="test-conn",
    )
