"""Project-local path reference helpers."""

from __future__ import annotations

from pathlib import Path, PurePosixPath


def project_relative_ref(project_root: Path, path: Path) -> str:
    """Return a project-relative reference with POSIX separators."""
    relative_path = path.resolve().relative_to(project_root.resolve())
    return PurePosixPath(*relative_path.parts).as_posix()


def posix_path(*parts: object) -> str:
    """Join path parts into a POSIX path string for portable artifact refs."""
    return PurePosixPath(*(str(part).replace("\\", "/") for part in parts)).as_posix()


def normalize_relative_ref(value: str) -> str:
    """Normalize a serialized relative path reference from any CLI platform."""
    return PurePosixPath(str(value).replace("\\", "/")).as_posix()
