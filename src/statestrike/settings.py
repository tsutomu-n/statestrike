from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

ReleaseChannel = Literal["research", "paper", "tiny-live", "live-stable"]
SourceName = Literal["ws", "info", "s3", "tardis"]
SmokeChannel = Literal["l2Book", "trades", "activeAssetCtx", "candle"]
CandleInterval = Literal[
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "8h",
    "12h",
    "1d",
    "3d",
    "1w",
    "1M",
]

if TYPE_CHECKING:
    from statestrike.collector import CollectorConfig


class Settings(BaseSettings):
    """Phase 1 bootstrap settings anchored to the approved docs."""

    model_config = SettingsConfigDict(
        env_prefix="STATESTRIKE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    release_channel: ReleaseChannel = "research"
    market_data_network: Literal["mainnet", "testnet"] = "mainnet"
    enable_live_orders: bool = False
    enable_addon_live: bool = False
    data_root: Path = Path("data")
    flush_interval_ms: int = 1000
    snapshot_recovery_enabled: bool = True
    source_priority: tuple[SourceName, ...] = ("ws", "info", "s3", "tardis")
    allowed_symbols: Annotated[tuple[str, ...], NoDecode] = Field(default_factory=tuple)
    smoke_channels: Annotated[tuple[SmokeChannel, ...], NoDecode] = (
        "l2Book",
        "trades",
        "activeAssetCtx",
        "candle",
    )
    smoke_candle_interval: CandleInterval = "1m"

    @property
    def release_channels(self) -> tuple[ReleaseChannel, ...]:
        return ("research", "paper", "tiny-live", "live-stable")

    @property
    def raw_root(self) -> Path:
        return self.data_root / "raw_ws"

    @property
    def normalized_root(self) -> Path:
        return self.data_root / "normalized"

    @property
    def quarantine_root(self) -> Path:
        return self.data_root / "quarantine"

    @property
    def exports_root(self) -> Path:
        return self.data_root / "exports"

    @field_validator("allowed_symbols", mode="before")
    @classmethod
    def split_allowed_symbols(cls, value: object) -> object:
        if value is None or value == "":
            return ()
        if isinstance(value, str):
            return tuple(
                symbol.strip().upper()
                for symbol in value.split(",")
                if symbol.strip()
            )
        return value

    @field_validator("smoke_channels", mode="before")
    @classmethod
    def split_smoke_channels(cls, value: object) -> object:
        if value is None or value == "":
            return ("l2Book", "trades", "activeAssetCtx", "candle")
        if isinstance(value, str):
            return tuple(
                channel.strip()
                for channel in value.split(",")
                if channel.strip()
            )
        return value

    @model_validator(mode="after")
    def enforce_phase_one_scope(self) -> "Settings":
        if self.enable_live_orders:
            raise ValueError("Phase 1 bootstrap excludes live order path")
        return self

    def build_smoke_collector_config(self) -> "CollectorConfig":
        from statestrike.collector import CollectorConfig

        return CollectorConfig(
            allowed_symbols=self.require_smoke_symbols(),
            source_priority=self.source_priority,
            market_data_network=self.market_data_network,
            flush_interval_ms=self.flush_interval_ms,
            snapshot_recovery_enabled=self.snapshot_recovery_enabled,
            channels=self.smoke_channels,
            candle_interval=(
                self.smoke_candle_interval
                if "candle" in self.smoke_channels
                else None
            ),
        )

    def require_smoke_symbols(self) -> tuple[str, ...]:
        if not self.allowed_symbols:
            raise ValueError("Phase 1.5 smoke runs require explicit allowed_symbols")
        if len(self.allowed_symbols) > 10:
            raise ValueError("Phase 1.5 smoke runs support at most 10 symbols")
        return self.allowed_symbols
