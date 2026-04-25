from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


ContinuityStatus = Literal[
    "continuous",
    "recovery_pending",
    "recovered",
    "non_recoverable",
]
RecoveryClassification = Literal["recoverable", "non_recoverable"]
BookEventKind = Literal["snapshot", "recovery_snapshot"]


class MessageCaptureContext(BaseModel):
    model_config = ConfigDict(frozen=True)

    reconnect_epoch: int = Field(default=0, ge=0)
    book_epoch: int = Field(default=1, ge=1)
    book_event_kind: BookEventKind | None = None
    continuity_status: ContinuityStatus = "continuous"
    recovery_classification: RecoveryClassification | None = None
    recovery_succeeded: bool | None = None


class MessageIngressMeta(BaseModel):
    model_config = ConfigDict(frozen=True)

    recv_wall_ns: int = Field(ge=0)
    recv_mono_ns: int = Field(ge=0)
    recv_seq: int = Field(ge=0)
    connection_id: str


class BookRecoveryTracker:
    def __init__(self, *, symbols: tuple[str, ...]) -> None:
        self._symbols = tuple(sorted(dict.fromkeys(symbol.upper() for symbol in symbols)))
        self._reconnect_epoch = 0
        self._book_epoch_by_symbol = {symbol: 1 for symbol in self._symbols}
        self._pending_symbols: set[str] = set()

    @property
    def reconnect_epoch(self) -> int:
        return self._reconnect_epoch

    def mark_reconnect(self) -> None:
        self._reconnect_epoch += 1
        self._pending_symbols = set(self._symbols)

    def classify_message(self, message: dict[str, Any]) -> MessageCaptureContext:
        symbol = extract_message_symbol(message)
        book_epoch = self._book_epoch_by_symbol.get(symbol, 1)
        channel = message.get("channel")

        if channel == "l2Book" and symbol in self._pending_symbols:
            next_epoch = book_epoch + 1
            self._book_epoch_by_symbol[symbol] = next_epoch
            self._pending_symbols.remove(symbol)
            return MessageCaptureContext(
                reconnect_epoch=self._reconnect_epoch,
                book_epoch=next_epoch,
                book_event_kind="recovery_snapshot",
                continuity_status="recovered",
                recovery_classification="recoverable",
                recovery_succeeded=True,
            )

        if channel == "l2Book":
            return MessageCaptureContext(
                reconnect_epoch=self._reconnect_epoch,
                book_epoch=book_epoch,
                book_event_kind="snapshot",
                continuity_status="continuous",
            )

        continuity_status: ContinuityStatus = (
            "recovery_pending" if symbol in self._pending_symbols else "continuous"
        )
        return MessageCaptureContext(
            reconnect_epoch=self._reconnect_epoch,
            book_epoch=book_epoch,
            continuity_status=continuity_status,
        )

    def finalize_gap_flags(self) -> tuple[str, ...]:
        return tuple(
            f"l2_book_non_recoverable:{symbol}" for symbol in sorted(self._pending_symbols)
        )


def default_message_context(
    *,
    message: dict[str, Any],
    reconnect_epoch: int,
    book_epoch: int,
) -> MessageCaptureContext:
    if message.get("channel") == "l2Book" and (reconnect_epoch > 0 or book_epoch > 1):
        return MessageCaptureContext(
            reconnect_epoch=reconnect_epoch,
            book_epoch=book_epoch,
            book_event_kind="recovery_snapshot",
            continuity_status="recovered",
            recovery_classification="recoverable",
            recovery_succeeded=True,
        )
    return MessageCaptureContext(
        reconnect_epoch=reconnect_epoch,
        book_epoch=book_epoch,
        book_event_kind="snapshot" if message.get("channel") == "l2Book" else None,
        continuity_status="continuous" if reconnect_epoch == 0 else "recovered",
    )


def resolve_message_contexts(
    *,
    messages: list[dict[str, Any]],
    message_contexts: list[MessageCaptureContext] | tuple[MessageCaptureContext, ...] | None,
    reconnect_epoch: int,
    book_epoch: int,
) -> list[MessageCaptureContext]:
    if not message_contexts:
        return [
            default_message_context(
                message=message,
                reconnect_epoch=reconnect_epoch,
                book_epoch=book_epoch,
            )
            for message in messages
        ]
    if len(message_contexts) != len(messages):
        raise ValueError("message_contexts length must match messages length")
    return list(message_contexts)


def resolve_ingress_metadata(
    *,
    messages: list[dict[str, Any]],
    ingress_metadata: list[MessageIngressMeta] | tuple[MessageIngressMeta, ...] | None,
    recv_ts_start: int,
    allow_fixture_fallback: bool,
) -> list[MessageIngressMeta]:
    if ingress_metadata:
        if len(ingress_metadata) != len(messages):
            raise ValueError("ingress_metadata length must match messages length")
        return list(ingress_metadata)
    if not allow_fixture_fallback:
        raise ValueError(
            "ingress_metadata is required for run_mode='network'; synthetic recv_ts fallback is fixture-only"
        )
    return [
        MessageIngressMeta(
            recv_wall_ns=(recv_ts_start + index) * 1_000_000,
            recv_mono_ns=index,
            recv_seq=index,
            connection_id="fixture-fallback",
        )
        for index, _ in enumerate(messages)
    ]


def count_recoverable_book_gaps(
    message_contexts: list[MessageCaptureContext] | tuple[MessageCaptureContext, ...],
) -> int:
    return sum(
        1
        for context in message_contexts
        if context.book_event_kind == "recovery_snapshot"
        and context.recovery_classification == "recoverable"
    )


def count_non_recoverable_book_gaps(gap_flags: tuple[str, ...]) -> int:
    return sum(1 for flag in gap_flags if flag.startswith("l2_book_non_recoverable:"))


def max_book_epoch(
    message_contexts: list[MessageCaptureContext] | tuple[MessageCaptureContext, ...],
    *,
    fallback: int = 1,
) -> int:
    if not message_contexts:
        return fallback
    return max(context.book_epoch for context in message_contexts)


def extract_message_symbol(message: dict[str, Any]) -> str:
    data = message.get("data", {})
    if isinstance(data, dict) and "coin" in data:
        return str(data["coin"]).upper()
    if isinstance(data, dict) and "s" in data:
        return str(data["s"]).upper()
    if isinstance(data, list) and data and "coin" in data[0]:
        return str(data[0]["coin"]).upper()
    return ""
