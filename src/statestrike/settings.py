from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

ReleaseChannel = Literal["research", "paper", "tiny-live", "live-stable"]
SourceName = Literal["ws", "info", "s3", "tardis"]


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
    source_priority: tuple[SourceName, ...] = ("ws", "info", "s3", "tardis")
    allowed_symbols: Annotated[tuple[str, ...], NoDecode] = Field(default_factory=tuple)

    @property
    def release_channels(self) -> tuple[ReleaseChannel, ...]:
        return ("research", "paper", "tiny-live", "live-stable")

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

    @model_validator(mode="after")
    def enforce_phase_one_scope(self) -> "Settings":
        if self.enable_live_orders:
            raise ValueError("Phase 1 bootstrap excludes live order path")
        return self
