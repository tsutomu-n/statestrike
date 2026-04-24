from __future__ import annotations

import random

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ReconnectBackoffPolicy(BaseModel):
    model_config = ConfigDict(frozen=True)

    initial_ms: int = Field(gt=0)
    max_ms: int = Field(gt=0)
    jitter_ratio: float = Field(default=0.1, ge=0.0, le=1.0)

    @model_validator(mode="after")
    def validate_bounds(self) -> "ReconnectBackoffPolicy":
        if self.max_ms < self.initial_ms:
            raise ValueError("max_ms must be greater than or equal to initial_ms")
        return self

    def delay_ms(
        self,
        *,
        retry_count: int,
        random_value: float | None = None,
    ) -> int:
        return compute_backoff_delay_ms(
            retry_count=retry_count,
            initial_ms=self.initial_ms,
            max_ms=self.max_ms,
            jitter_ratio=self.jitter_ratio,
            random_value=random_value,
        )


def compute_backoff_delay_ms(
    *,
    retry_count: int,
    initial_ms: int,
    max_ms: int,
    jitter_ratio: float = 0.1,
    random_value: float | None = None,
) -> int:
    if retry_count < 0:
        raise ValueError("retry_count must be greater than or equal to 0")
    if initial_ms <= 0:
        raise ValueError("initial_ms must be positive")
    if max_ms < initial_ms:
        raise ValueError("max_ms must be greater than or equal to initial_ms")
    if not 0.0 <= jitter_ratio <= 1.0:
        raise ValueError("jitter_ratio must be between 0.0 and 1.0")
    bounded_retry = min(retry_count, 31)
    base_delay_ms = min(initial_ms * (2**bounded_retry), max_ms)
    jitter_seed = random.random() if random_value is None else random_value
    if not 0.0 <= jitter_seed <= 1.0:
        raise ValueError("random_value must be between 0.0 and 1.0")
    jitter_multiplier = (1.0 - jitter_ratio) + (2.0 * jitter_ratio * jitter_seed)
    jittered_delay_ms = int(round(base_delay_ms * jitter_multiplier))
    return max(1, min(max_ms, jittered_delay_ms))
