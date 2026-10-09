"""Project artifact helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re
import shutil
from pathlib import Path
from typing import Any, Callable, Iterator
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from yaml import YAMLError

from zeta4s.project.step_graph import step_graph_config_paths

PROJECT_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
PROJECT_MANIFEST_NAMES = ("project.yml",)
PROJECT_MANIFEST_KEYS = {"project_id", "display_name", "timezone", "paths"}
DBT_PROJECT_TEMPLATE_TYPES = {"clickhouse", "oracle"}


@dataclass(frozen=True)
class ProjectContext:
    project_id: str
    root: Path
    jobs_dir: Path
    assets_dir: Path
    dbt_dir: Path
    timezone: str
    display_name: str | None = None
    registered_at: datetime | None = None

    @property
    def dbt_models_dir(self) -> Path:
        return self.dbt_dir / "models"

    def dbt_project_dir(self, conn_id: str) -> Path:
        if not isinstance(conn_id, str) or not conn_id.strip():
            raise ValueError("dbt conn id must be a non-empty string")
        value = conn_id.strip()
        if "/" in value or "\\" in value or ".." in Path(value).parts or " " in value:
            raise ValueError(f"dbt conn id is not a valid project-local directory name: {conn_id}")
        return self.dbt_dir / value


def project_manifest_path(project_root: Path) -> Path | None:
    for name in PROJECT_MANIFEST_NAMES:
        path = project_root / name
        if path.exists():
            return path
    return None


def validate_project_id(project_id: Any) -> str:
    if project_id is None:
        raise ValueError("project.yml project_id is required")
    if not isinstance(project_id, str):
        raise ValueError("project.yml project_id must be a string")
    value = project_id.strip()
    if not value:
        raise ValueError("project.yml project_id is required")
    if not PROJECT_ID_PATTERN.fullmatch(value):
        raise ValueError("project.yml project_id must contain only letters, digits, dot, underscore, or hyphen")
    return value


def _validate_display_name(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("project.yml display_name must be a string")
    normalized = value.strip()
    return normalized or None


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except YAMLError as e:
        raise ValueError(f"yaml could not be parsed: {path}") from e
    if not isinstance(data, dict):
        raise ValueError(f"yaml config must be a mapping: {path}")
    return data


def _validate_relative_path(value: Any, field: str) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"project.yml {field} must be a non-empty relative path")
    path = Path(value.strip())
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"project.yml {field} must be a project-local relative path")
    return path


def _project_path(root: Path, paths: dict[str, Any], key: str) -> Path:
    if key not in paths:
        raise ValueError(f"project.yml paths.{key} is required")
    value = paths[key]
    return root / _validate_relative_path(value, f"paths.{key}")


def _validate_timezone(value: Any) -> str:
    if value is None:
        raise ValueError("project.yml timezone is required")
    if not isinstance(value, str) or not value.strip():
        raise ValueError("project.yml timezone must be a non-empty string")
    timezone = value.strip()
    try:
        ZoneInfo(timezone)
    except ZoneInfoNotFoundError as e:
        raise ValueError(f"project.yml timezone is not a valid IANA timezone: {timezone}") from e
    return timezone


def load_project_context(project_root: Path) -> ProjectContext:
    manifest_path = project_manifest_path(project_root)
    if manifest_path is None:
        raise ValueError(f"project.yml is required: {project_root}")
    manifest = _load_yaml(manifest_path)
    unknown_keys = sorted(set(manifest) - PROJECT_MANIFEST_KEYS)
    if unknown_keys:
        raise ValueError("project.yml has unsupported keys: " + ", ".join(unknown_keys))
    paths = manifest.get("paths")
    if not isinstance(paths, dict):
        raise ValueError("project.yml paths must be a mapping")
    assets_dir = (
        project_root / _validate_relative_path(paths["assets"], "paths.assets")
        if "assets" in paths
        else project_root / "assets"
    )
    return ProjectContext(
        project_id=validate_project_id(manifest.get("project_id")),
        root=project_root,
        jobs_dir=_project_path(project_root, paths, "jobs"),
        assets_dir=assets_dir,
        dbt_dir=_project_path(project_root, paths, "dbt"),
        timezone=_validate_timezone(manifest.get("timezone")),
        display_name=_validate_display_name(manifest.get("display_name")),
    )


def iter_project_configs(
    projects_dir: Path,
    on_error: Callable[[Path, Exception], None] | None = None,
) -> Iterator[tuple[ProjectContext, Path, dict[str, Any]]]:
    if not projects_dir.exists():
        return
    for project_root in sorted(path for path in projects_dir.iterdir() if path.is_dir()):
        try:
            context = load_project_context(project_root)
        except Exception as e:
            if on_error:
                on_error(project_root, e)
                continue
            raise
        for config_path in step_graph_config_paths(context.jobs_dir):
            try:
                config = _load_yaml(config_path)
            except Exception as e:
                if on_error:
                    on_error(config_path, e)
                    continue
                raise
            yield context, config_path, config


def _prepare_project_target(name: str, projects_dir: Path, force: bool) -> Path:
    name = validate_project_id(name)
    target = projects_dir / name
    if target.exists():
        if not force:
            raise FileExistsError(f"project already exists: {target}")
        shutil.rmtree(target)
    return target


def create_project_skeleton(
    *,
    name: str,
    projects_dir: Path,
    force: bool = False,
    dbt_conn_ids: list[str] | tuple[str, ...] = (),
) -> Path:
    """Create an empty project artifact directory."""
    target = _prepare_project_target(name, projects_dir, force)
    for directory in [
        target / "jobs",
        target / "docs",
    ]:
        directory.mkdir(parents=True, exist_ok=True)
    (target / "project.yml").write_text(
        yaml.safe_dump(
            {
                "project_id": validate_project_id(name),
                "display_name": validate_project_id(name),
                "timezone": "Asia/Seoul",
                "paths": {
                    "jobs": "jobs",
                    "dbt": "dbt",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    project_id = validate_project_id(name)
    (target / "docs" / "README.md").write_text(
        "\n".join(
            [
                f"# {project_id} Project Notes",
                "",
                "이 문서는 project-local 운영 노트다. Runtime 이 읽는 필수 artifact 는 `project.yml`,",
                "`jobs/*.yml`, SQL/dbt 파일이며, `docs/` 는 설계 결정과 검증 기록을 남기는 보조 문서 공간이다.",
                "",
                "## Purpose",
                "",
                "- 이 project 가 검증하거나 운영하는 workflow:",
                "- 주요 source/runtime backend/target:",
                "- 이 project 에서 의도적으로 보여주려는 contract:",
                "",
                "## Runtime Profile",
                "",
                "이 project 의 step 이 참조하는 workspace profile connection 과 각 역할을 기록한다.",
                "",
                "| Connection | Type | Role | Owner | Notes |",
                "|-------|------|------|-------|-------|",
                "| `<connection_id>` | `<type>` | source/stage/transform/write | profile/runtime | |",
                "",
                "`conn` 은 step 이 사용할 profile connection id 다. DB adapter 종류는 profile connection 의",
                "`type` 으로 해석한다.",
                "",
                "## Job Graph",
                "",
                "`jobs/<job>.yml` 의 step 흐름을 실행 순서대로 적는다.",
                "",
                "1. `<step_id>`: 수행 목적",
                "2. `<step_id>`: 수행 목적",
                "3. `<step_id>`: 수행 목적",
                "",
                "## Data Contract",
                "",
                "- Source tables/files/indexes:",
                "- Rowset outputs:",
                "- Stage tables:",
                "- Transform outputs:",
                "- Write targets:",
                "",
                "Table reference 는 `schema.table` 형태로 기록한다. ClickHouse backend 에서는 schema 위치가",
                "database 로 해석된다.",
                "",
                "## Validation Evidence",
                "",
                "- `z4s project check <project_id> --profile <profile_id>`:",
                "- `z4s api deploy <project_id> --profile <profile_id>`:",
                "- DAG run:",
                "- Runtime result checks:",
                "",
                "## Open Decisions",
                "",
                "- Runtime connection ownership:",
                "- Timezone/window policy:",
                "- Failure/retry policy:",
                "",
            ]
        ),
        encoding="utf-8",
    )
    for conn_id in dbt_conn_ids:
        create_dbt_project_skeleton(target, conn_id)
    return target


def create_dbt_project_skeleton(project_root: Path, conn_id: str) -> Path:
    project = load_project_context(project_root)
    dbt_project_dir = project.dbt_project_dir(conn_id)
    (dbt_project_dir / "models").mkdir(parents=True, exist_ok=True)
    (dbt_project_dir / "tests").mkdir(parents=True, exist_ok=True)
    (dbt_project_dir / "dbt_project.yml").write_text(
        yaml.safe_dump(
            {
                "name": conn_id,
                "version": "0.1.0",
                "config-version": 2,
                "profile": conn_id,
                "model-paths": ["models"],
                "test-paths": ["tests"],
                "target-path": "target",
                "clean-targets": ["target", "dbt_packages"],
                "models": {
                    conn_id: {
                        "+materialized": "table",
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return dbt_project_dir
