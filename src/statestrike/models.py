from __future__ import annotations

from typing import Literal

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True)


class BookEvent(FrozenModel):
    book_event_id: str
    capture_session_id: str
    reconnect_epoch: int = Field(ge=0)
    book_epoch: int = Field(ge=0)
    symbol: str
    exchange_ts: int = Field(ge=0)
    recv_ts: int = Field(ge=0)
    event_kind: Literal["snapshot", "delta", "recovery_snapshot"]
    source: Literal["ws", "info", "s3", "tardis"]
    raw_msg_hash: str
    n_bids: int = Field(ge=0)
    n_asks: int = Field(ge=0)

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.upper()


class BookLevel(FrozenModel):
    book_event_id: str
    side: Literal["bid", "ask"]
    level_idx: int = Field(ge=0)
    price: float = Field(gt=0)
    size: float = Field(gt=0)


class TradeEvent(FrozenModel):
    trade_event_id: str
    symbol: str
    exchange_ts: int = Field(ge=0)
    recv_ts: int = Field(ge=0)
    price: float = Field(gt=0)
    size: float = Field(gt=0)
    side: Literal["buy", "sell"]

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.upper()


class AssetContextEvent(FrozenModel):
    asset_ctx_event_id: str
    symbol: str
    exchange_ts: int = Field(ge=0)
    recv_ts: int = Field(ge=0)
    mark_px: float = Field(gt=0)
    oracle_px: float = Field(gt=0)
    funding_rate: float
    open_interest: float = Field(ge=0)
    mid_px: float = Field(gt=0)
    basis: float | None = None
    next_funding_ts: int | None = None

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.upper()

    @model_validator(mode="before")
    @classmethod
    def normalize_derived_fields(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        exchange_ts_ms = int(data["exchange_ts"])
        data = dict(data)
        data.setdefault(
            "next_funding_ts",
            ((exchange_ts_ms // 3_600_000) + 1) * 3_600_000,
        )
        if "basis" not in data or data["basis"] is None:
            data["basis"] = (float(data["mark_px"]) - float(data["oracle_px"])) / float(
                data["oracle_px"]
            )
        return data
