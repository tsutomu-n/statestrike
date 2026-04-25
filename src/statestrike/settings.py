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
    heartbeat_timeout_ms: int = 30_000
    reconnect_backoff_initial_ms: int = 1_000
    reconnect_backoff_max_ms: int = 30_000
    snapshot_recovery_enabled: bool = True
    book_snapshot_refresh_on_reconnect: bool = True
    max_subscriptions_per_connection: int = 64
    max_messages_per_minute_guard: int | None = None
    source_priority: tuple[SourceName, ...] = ("ws", "info", "s3", "tardis")
    allowed_symbols: Annotated[tuple[str, ...], NoDecode] = Field(default_factory=tuple)
    smoke_max_runtime_seconds: int = 3600
    smoke_max_messages: int = 5000
    smoke_ping_interval_seconds: int = 30
    smoke_reconnect_limit: int = 3
    smoke_skew_warning_ms: int = Field(default=250, ge=0)
    smoke_skew_severe_ms: int = Field(default=1000, ge=0)
    smoke_asset_ctx_stale_threshold_ms: int = Field(default=300_000, ge=0)
    smoke_trade_gap_threshold_ms: int = Field(default=60_000, ge=0)
    smoke_quarantine_warning_rate: float = Field(default=0.001, ge=0.0)
    smoke_quarantine_severe_rate: float = Field(default=0.01, ge=0.0)
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
        """Legacy alias for the derived raw payload root."""
        return self.derived_capture_root

    @property
    def derived_capture_root(self) -> Path:
        """Root for derived per-channel raw payload partitions.

        The physical directory name remains `raw_ws` for compatibility.
        """
        return self.data_root / "raw_ws"

    @property
    def capture_log_root(self) -> Path:
        return self.data_root / "capture_log"

    @property
    def normalized_root(self) -> Path:
        return self.data_root / "normalized"

    @property
    def quarantine_root(self) -> Path:
        return self.data_root / "quarantine"

    @property
    def exports_root(self) -> Path:
        return self.data_root / "exports"

    @property
    def reports_root(self) -> Path:
        return self.data_root / "reports"

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
        if self.heartbeat_timeout_ms <= 0:
            raise ValueError("heartbeat_timeout_ms must be positive")
        if self.reconnect_backoff_initial_ms <= 0:
            raise ValueError("reconnect_backoff_initial_ms must be positive")
        if self.reconnect_backoff_max_ms < self.reconnect_backoff_initial_ms:
            raise ValueError(
                "reconnect_backoff_max_ms must be greater than or equal to reconnect_backoff_initial_ms"
            )
        if self.smoke_skew_severe_ms < self.smoke_skew_warning_ms:
            raise ValueError(
                "smoke_skew_severe_ms must be greater than or equal to smoke_skew_warning_ms"
            )
        if self.smoke_quarantine_severe_rate < self.smoke_quarantine_warning_rate:
            raise ValueError(
                "smoke_quarantine_severe_rate must be greater than or equal to smoke_quarantine_warning_rate"
            )
        return self

    def build_smoke_collector_config(self) -> "CollectorConfig":
        from statestrike.collector import CollectorConfig

        return CollectorConfig(
            run_mode="network",
            allowed_symbols=self.require_smoke_symbols(),
            source_priority=self.source_priority,
            market_data_network=self.market_data_network,
            flush_interval_ms=self.flush_interval_ms,
            heartbeat_timeout_ms=self.heartbeat_timeout_ms,
            reconnect_backoff_initial_ms=self.reconnect_backoff_initial_ms,
            reconnect_backoff_max_ms=self.reconnect_backoff_max_ms,
            snapshot_recovery_enabled=self.snapshot_recovery_enabled,
            book_snapshot_refresh_on_reconnect=self.book_snapshot_refresh_on_reconnect,
            max_subscriptions_per_connection=self.max_subscriptions_per_connection,
            max_messages_per_minute_guard=self.max_messages_per_minute_guard,
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
