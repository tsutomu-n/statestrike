from pathlib import Path

import pytest

from statestrike.settings import Settings


def test_settings_load_env_file_and_keep_phase_one_safe_defaults(tmp_path: Path) -> None:
    env_file = tmp_path / "phase1.env"
    env_file.write_text(
        "\n".join(
            [
                "STATESTRIKE_RELEASE_CHANNEL=paper",
                "STATESTRIKE_ALLOWED_SYMBOLS=BTC,ETH,SOL",
            ]
        ),
        encoding="utf-8",
    )

    settings = Settings(_env_file=env_file)

    assert settings.release_channel == "paper"
    assert settings.market_data_network == "mainnet"
    assert settings.enable_live_orders is False
    assert settings.enable_addon_live is False
    assert settings.source_priority == ("ws", "info", "s3", "tardis")
    assert settings.allowed_symbols == ("BTC", "ETH", "SOL")


def test_settings_default_release_channels_and_mainnet_data_path() -> None:
    settings = Settings()

    assert settings.release_channel == "research"
    assert settings.market_data_network == "mainnet"
    assert settings.release_channels == (
        "research",
        "paper",
        "tiny-live",
        "live-stable",
    )


def test_settings_reject_phase_one_live_path_enablement(tmp_path: Path) -> None:
    env_file = tmp_path / "phase1.env"
    env_file.write_text(
        "\n".join(
            [
                "STATESTRIKE_ENABLE_LIVE_ORDERS=true",
                "STATESTRIKE_RELEASE_CHANNEL=live-stable",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Phase 1 bootstrap excludes live order path"):
        Settings(_env_file=env_file)


def test_settings_expose_phase_one_storage_and_collector_defaults() -> None:
    settings = Settings()

    assert settings.data_root == Path("data")
    assert settings.raw_root == Path("data/raw_ws")
    assert settings.normalized_root == Path("data/normalized")
    assert settings.quarantine_root == Path("data/quarantine")
    assert settings.exports_root == Path("data/exports")
    assert settings.reports_root == Path("data/reports")
    assert settings.flush_interval_ms == 1000
    assert settings.heartbeat_timeout_ms == 30_000
    assert settings.reconnect_backoff_initial_ms == 1_000
    assert settings.reconnect_backoff_max_ms == 30_000
    assert settings.snapshot_recovery_enabled is True
    assert settings.book_snapshot_refresh_on_reconnect is True
    assert settings.max_subscriptions_per_connection == 64
    assert settings.max_messages_per_minute_guard is None
    assert settings.smoke_max_runtime_seconds == 3600
    assert settings.smoke_max_messages == 5000
    assert settings.smoke_ping_interval_seconds == 30
    assert settings.smoke_reconnect_limit == 3
    assert settings.smoke_skew_warning_ms == 250
    assert settings.smoke_skew_severe_ms == 1000
    assert settings.smoke_asset_ctx_stale_threshold_ms == 300_000


def test_settings_expose_phase15_smoke_defaults(tmp_path: Path) -> None:
    env_file = tmp_path / "phase15.env"
    env_file.write_text(
        "STATESTRIKE_ALLOWED_SYMBOLS=BTC,ETH,SOL",
        encoding="utf-8",
    )

    settings = Settings(_env_file=env_file)
    smoke_config = settings.build_smoke_collector_config()

    assert settings.smoke_channels == (
        "l2Book",
        "trades",
        "activeAssetCtx",
        "candle",
    )
    assert settings.smoke_candle_interval == "1m"
    assert smoke_config.allowed_symbols == ("BTC", "ETH", "SOL")
    assert smoke_config.run_mode == "network"
    assert smoke_config.channels == settings.smoke_channels
    assert smoke_config.candle_interval == "1m"
    assert smoke_config.heartbeat_timeout_ms == settings.heartbeat_timeout_ms
    assert smoke_config.reconnect_backoff_initial_ms == settings.reconnect_backoff_initial_ms
    assert smoke_config.reconnect_backoff_max_ms == settings.reconnect_backoff_max_ms
    assert (
        smoke_config.book_snapshot_refresh_on_reconnect
        == settings.book_snapshot_refresh_on_reconnect
    )
    assert (
        smoke_config.max_subscriptions_per_connection
        == settings.max_subscriptions_per_connection
    )


def test_settings_reject_phase15_smoke_without_explicit_small_allowlist(
    tmp_path: Path,
) -> None:
    empty_env = tmp_path / "phase15-empty.env"
    empty_env.write_text("", encoding="utf-8")

    with pytest.raises(ValueError, match="Phase 1.5 smoke runs require explicit allowed_symbols"):
        Settings(_env_file=empty_env).build_smoke_collector_config()

    crowded_env = tmp_path / "phase15-crowded.env"
    crowded_env.write_text(
        "STATESTRIKE_ALLOWED_SYMBOLS=BTC,ETH,SOL,XRP,AVAX,ARB,APT,LINK,DOGE,SUI,ATOM",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Phase 1.5 smoke runs support at most 10 symbols"):
        Settings(_env_file=crowded_env).build_smoke_collector_config()


def test_settings_allow_phase15_audit_threshold_overrides(tmp_path: Path) -> None:
    env_file = tmp_path / "phase15-thresholds.env"
    env_file.write_text(
        "\n".join(
            [
                "STATESTRIKE_ALLOWED_SYMBOLS=BTC,ETH",
                "STATESTRIKE_SMOKE_SKEW_WARNING_MS=400",
                "STATESTRIKE_SMOKE_SKEW_SEVERE_MS=1200",
                "STATESTRIKE_SMOKE_ASSET_CTX_STALE_THRESHOLD_MS=123456",
            ]
        ),
        encoding="utf-8",
    )

    settings = Settings(_env_file=env_file)

    assert settings.smoke_skew_warning_ms == 400
    assert settings.smoke_skew_severe_ms == 1200
    assert settings.smoke_asset_ctx_stale_threshold_ms == 123456


def test_settings_reject_phase15_inverted_audit_thresholds() -> None:
    with pytest.raises(ValueError, match="smoke_skew_severe_ms must be greater than or equal to smoke_skew_warning_ms"):
        Settings(
            allowed_symbols=("BTC",),
            smoke_skew_warning_ms=1000,
            smoke_skew_severe_ms=250,
        )


def test_settings_reject_inverted_reconnect_backoff_bounds() -> None:
    with pytest.raises(
        ValueError,
        match="reconnect_backoff_max_ms must be greater than or equal to reconnect_backoff_initial_ms",
    ):
        Settings(
            allowed_symbols=("BTC",),
            reconnect_backoff_initial_ms=5_000,
            reconnect_backoff_max_ms=1_000,
        )
