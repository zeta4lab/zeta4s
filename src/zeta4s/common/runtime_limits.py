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
            f"{label} must be between 1 and {WRITE_BATCH_SIZE_MAX}. "
            f"Default is {WRITE_BATCH_SIZE_DEFAULT}, maximum is {WRITE_BATCH_SIZE_MAX}."
        )
    return batch_size


def validate_native_oracle_batch_size(batch_size: int, label: str = "native.batch.size") -> int:
    if batch_size < 1 or batch_size > NATIVE_ORACLE_BATCH_SIZE_MAX:
        raise ValueError(f"{label} must be between 1 and {NATIVE_ORACLE_BATCH_SIZE_MAX}.")
    return batch_size
