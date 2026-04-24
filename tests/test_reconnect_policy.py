from __future__ import annotations

import pytest

from statestrike.reconnect import ReconnectBackoffPolicy, compute_backoff_delay_ms


def test_compute_backoff_delay_ms_applies_cap_and_jitter() -> None:
    low = compute_backoff_delay_ms(
        retry_count=2,
        initial_ms=1_000,
        max_ms=30_000,
        jitter_ratio=0.1,
        random_value=0.0,
    )
    high = compute_backoff_delay_ms(
        retry_count=8,
        initial_ms=1_000,
        max_ms=30_000,
        jitter_ratio=0.1,
        random_value=1.0,
    )

    assert low == 3_600
    assert high == 30_000


def test_reconnect_backoff_policy_rejects_invalid_bounds() -> None:
    with pytest.raises(ValueError, match="max_ms must be greater than or equal to initial_ms"):
        ReconnectBackoffPolicy(initial_ms=5_000, max_ms=1_000)
