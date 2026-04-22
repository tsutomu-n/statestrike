from uuid import UUID

from statestrike.identifiers import (
    build_cloid,
    build_intent_id,
    canonical_hash,
    new_capture_session_id,
)


def test_intent_id_is_human_readable_and_stable() -> None:
    intent_id = build_intent_id(
        run_id="run-001",
        symbol="btc",
        lane="core",
        side="long",
        intent_kind="entry",
        seq=7,
    )

    assert intent_id == "run-001-BTC-core-long-entry-0007"


def test_cloid_is_deterministic_16_byte_hex() -> None:
    cloid = build_cloid(
        env="mainnet",
        wallet_role="production",
        symbol="ETH",
        lane="reentry",
        side="short",
        intent_kind="entry",
        ts_ms_bucket=1713818880000,
        seq=2,
    )

    assert cloid == build_cloid(
        env="mainnet",
        wallet_role="production",
        symbol="ETH",
        lane="reentry",
        side="short",
        intent_kind="entry",
        ts_ms_bucket=1713818880000,
        seq=2,
    )
    assert len(cloid) == 32
    assert all(char in "0123456789abcdef" for char in cloid)


def test_canonical_hash_ignores_mapping_key_order() -> None:
    left = {
        "symbol": "BTC",
        "fields": {"px": "100.0", "qty": "1.0"},
        "tags": ["snapshot", "primary"],
    }
    right = {
        "tags": ["snapshot", "primary"],
        "fields": {"qty": "1.0", "px": "100.0"},
        "symbol": "BTC",
    }

    assert canonical_hash(left) == canonical_hash(right)


def test_capture_session_id_is_uuidv7() -> None:
    capture_session_id = new_capture_session_id()

    parsed = UUID(capture_session_id)

    assert parsed.version == 7
