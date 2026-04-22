from __future__ import annotations

from collections.abc import Mapping, Sequence
from hashlib import blake2b
import json
import secrets
import time
import uuid


def build_intent_id(
    *,
    run_id: str,
    symbol: str,
    lane: str,
    side: str,
    intent_kind: str,
    seq: int,
) -> str:
    return (
        f"{run_id}-{symbol.upper()}-{lane}-{side}-{intent_kind}-{seq:04d}"
    )


def build_cloid(
    *,
    env: str,
    wallet_role: str,
    symbol: str,
    lane: str,
    side: str,
    intent_kind: str,
    ts_ms_bucket: int,
    seq: int,
) -> str:
    payload = "|".join(
        [
            env,
            wallet_role,
            symbol.upper(),
            lane,
            side,
            intent_kind,
            str(ts_ms_bucket),
            str(seq),
        ]
    )
    return blake2b(payload.encode("utf-8"), digest_size=16).hexdigest()


def canonical_hash(value: object) -> str:
    normalized = _normalize(value)
    serialized = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return blake2b(serialized.encode("utf-8"), digest_size=16).hexdigest()


def new_capture_session_id() -> str:
    unix_ts_ms = int(time.time_ns() // 1_000_000) & ((1 << 48) - 1)
    rand_a = secrets.randbits(12)
    rand_b = secrets.randbits(62)
    value = (
        (unix_ts_ms << 80)
        | (0x7 << 76)
        | (rand_a << 64)
        | (0b10 << 62)
        | rand_b
    )
    return str(uuid.UUID(int=value))


def _normalize(value: object) -> object:
    if isinstance(value, Mapping):
        return {
            str(key): _normalize(inner)
            for key, inner in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, tuple):
        return [_normalize(item) for item in value]
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_normalize(item) for item in value]
    return value
