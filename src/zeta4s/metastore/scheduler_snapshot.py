"""Read-only scheduler snapshot publish/load helpers."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from uuid import uuid4

import yaml


def scheduler_snapshot_path(home: Path) -> Path:
    return Path(
        os.environ.get(
            "ZETA4S_SCHEDULER_SNAPSHOT_FILE",
            str(home / "registered" / "registered-dags.yml"),
        )
    )


def scheduler_last_good_snapshot_path(home: Path) -> Path:
    return Path(
        os.environ.get(
            "ZETA4S_SCHEDULER_LAST_GOOD_SNAPSHOT_FILE",
            str(home / "registered" / "registered-dags.last-good.yml"),
        )
    )


def load_scheduler_snapshot(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"registrations": []}
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"scheduler snapshot must be a mapping: {path}")
    registrations = data.get("registrations") or []
    if not isinstance(registrations, list):
        raise ValueError(f"scheduler snapshot registrations must be a list: {path}")
    data["registrations"] = registrations
    return data


def _write_snapshot_file(path: Path, data: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        tmp_path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")
        with tmp_path.open("r+", encoding="utf-8") as handle:
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()
    return path


def publish_scheduler_snapshot(*, registrations: list[dict[str, Any]], home: Path) -> Path:
    path = scheduler_snapshot_path(home)
    data = {"registrations": sorted(registrations, key=lambda item: str(item.get("project_id") or ""))}
    published_path = _write_snapshot_file(path, data)
    _write_snapshot_file(scheduler_last_good_snapshot_path(home), data)
    return published_path
