from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict


class GapDecision(BaseModel):
    model_config = ConfigDict(frozen=True)

    classification: Literal["recoverable", "non_recoverable", "degraded"]
    start_new_book_epoch: bool


def classify_gap(
    *,
    channel: str,
    gap_seconds: int,
    continuity_known: bool,
    recovery_succeeded: bool,
) -> GapDecision:
    if channel == "l2_book":
        if not continuity_known:
            return GapDecision(
                classification="non_recoverable",
                start_new_book_epoch=True,
            )
        return GapDecision(
            classification="recoverable",
            start_new_book_epoch=False,
        )

    if channel == "trades":
        if gap_seconds <= 60 and recovery_succeeded:
            return GapDecision(
                classification="recoverable",
                start_new_book_epoch=False,
            )
        return GapDecision(
            classification="non_recoverable",
            start_new_book_epoch=False,
        )

    if channel == "active_asset_ctx":
        if gap_seconds <= 120 and recovery_succeeded:
            return GapDecision(
                classification="recoverable",
                start_new_book_epoch=False,
            )
        return GapDecision(
            classification="degraded",
            start_new_book_epoch=False,
        )

    return GapDecision(
        classification="non_recoverable",
        start_new_book_epoch=False,
    )
