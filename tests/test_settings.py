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
