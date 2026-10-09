"""File locks for zeta4s runtime operations."""

from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
import os
from pathlib import Path
import time
from typing import Iterator

ZETA4S_API_HOME = Path(os.environ.get("ZETA4S_API_HOME", "/var/lib/zeta4s"))


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return float(raw)


DEFAULT_LOCK_TIMEOUT_SECONDS = _env_float("ZETA4S_OPERATION_LOCK_TIMEOUT", 30.0)


class OperationLockTimeout(TimeoutError):
    """Raised when a runtime operation cannot acquire its file lock in time."""

    def __init__(self, lock_name: str, timeout_seconds: float) -> None:
        self.lock_name = lock_name
        self.timeout_seconds = timeout_seconds
        super().__init__(f"runtime operation is already running: {lock_name}")


def _safe_lock_token(value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]
    return f"{value[:48].replace('/', '_').replace(':', '_')}--{digest}"


def lock_path(name: str, *, home: Path = ZETA4S_API_HOME) -> Path:
    return home / "locks" / f"{name}.lock"


def project_lock_name(project: str) -> str:
    return f"project-{_safe_lock_token(project)}"


def artifact_lock_name(artifact_id: str) -> str:
    return f"artifact-{_safe_lock_token(artifact_id)}"


REGISTRATION_LOCK_NAME = "registered-dags"


@contextmanager
def file_lock(
    name: str,
    *,
    home: Path = ZETA4S_API_HOME,
    timeout_seconds: float = DEFAULT_LOCK_TIMEOUT_SECONDS,
    poll_seconds: float = 0.1,
) -> Iterator[Path]:
    path = lock_path(name, home=home)
    path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout_seconds
    with path.open("a+", encoding="utf-8") as handle:
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError as e:
                if time.monotonic() >= deadline:
                    raise OperationLockTimeout(name, timeout_seconds) from e
                time.sleep(poll_seconds)
        try:
            yield path
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def registration_lock(*, home: Path = ZETA4S_API_HOME, timeout_seconds: float = DEFAULT_LOCK_TIMEOUT_SECONDS):
    return file_lock(REGISTRATION_LOCK_NAME, home=home, timeout_seconds=timeout_seconds)


def project_operation_lock(
    project: str, *, home: Path = ZETA4S_API_HOME, timeout_seconds: float = DEFAULT_LOCK_TIMEOUT_SECONDS
):
    return file_lock(project_lock_name(project), home=home, timeout_seconds=timeout_seconds)


def artifact_operation_lock(
    artifact_id: str, *, home: Path = ZETA4S_API_HOME, timeout_seconds: float = DEFAULT_LOCK_TIMEOUT_SECONDS
):
    return file_lock(artifact_lock_name(artifact_id), home=home, timeout_seconds=timeout_seconds)
