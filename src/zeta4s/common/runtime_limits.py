"""Shared runtime sizing limits."""

from __future__ import annotations

WRITE_BATCH_SIZE_DEFAULT = 50_000
WRITE_ELASTICSEARCH_BATCH_SIZE_DEFAULT = 1_000
WRITE_BATCH_SIZE_MAX = 200_000
WRITE_EFFECTIVE_BATCH_SIZE_MIN = 1_000
WRITE_TARGET_ARROW_BATCH_BYTES = 64 * 1024 * 1024
NATIVE_ORACLE_BATCH_SIZE_MAX = 200_000


def validate_write_batch_size(batch_size: int, label: str = "write.batch_size") -> int:
    if batch_size < 1 or batch_size > WRITE_BATCH_SIZE_MAX:
        raise ValueError(
            f"{label} 는 1 이상 {WRITE_BATCH_SIZE_MAX} 이하이어야 한다. "
            f"기본값은 {WRITE_BATCH_SIZE_DEFAULT}, 최대값은 {WRITE_BATCH_SIZE_MAX}이다."
        )
    return batch_size


def validate_native_oracle_batch_size(batch_size: int, label: str = "native.batch.size") -> int:
    if batch_size < 1 or batch_size > NATIVE_ORACLE_BATCH_SIZE_MAX:
        raise ValueError(f"{label} 는 1 이상 {NATIVE_ORACLE_BATCH_SIZE_MAX} 이하이어야 한다.")
    return batch_size
