"""dbt graph discovery for step graph runtime artifacts."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from zeta4s.project.loader import ProjectContext, load_project_context

DBT_GRAPH_CACHE_FILE = "zeta4s_dbt_graph.json"


def dbt_executable() -> str:
    sibling = Path(sys.executable).parent / "dbt"
    if sibling.exists():
        return str(sibling)
    resolved = shutil.which("dbt")
    if resolved:
        return resolved
    raise RuntimeError("dbt executable not found: install dbt in the zeta4s Python environment or add dbt to PATH")


def dbt_parse_env() -> dict[str, str]:
    return os.environ.copy()


def dbt_profile_name(conn_id: str) -> str:
    value = re.sub(r"[^A-Za-z0-9_]+", "_", conn_id).strip("_")
    return value or "runtime"


@dataclass(frozen=True)
class DbtNode:
    unique_id: str
    name: str
    resource_type: str
    original_file_path: str
    depends_on: tuple[str, ...]

    @property
    def command(self) -> str:
        if self.resource_type == "model":
            return "run"
        if self.resource_type == "test":
            return "test"
        raise ValueError(f"unsupported dbt resource_type: {self.resource_type}")


@dataclass(frozen=True)
class DbtGraph:
    nodes: tuple[DbtNode, ...]

    @property
    def by_unique_id(self) -> dict[str, DbtNode]:
        return {node.unique_id: node for node in self.nodes}

    @property
    def roots(self) -> tuple[DbtNode, ...]:
        selected = self.by_unique_id
        return tuple(node for node in self.nodes if not any(dep in selected for dep in node.depends_on))

    @property
    def terminals(self) -> tuple[DbtNode, ...]:
        downstream: set[str] = set()
        selected = self.by_unique_id
        for node in self.nodes:
            downstream.update(dep for dep in node.depends_on if dep in selected)
        return tuple(node for node in self.nodes if node.unique_id not in downstream)


@dataclass(frozen=True)
class DbtStepSelection:
    conn_id: str
    selectors: tuple[str, ...]


def dbt_project_dir(project: ProjectContext, conn_id: str) -> Path:
    return project.dbt_project_dir(conn_id)


def dbt_models_dir(project: ProjectContext, conn_id: str) -> Path:
    return dbt_project_dir(project, conn_id) / "models"


def load_dbt_graph(dbt_project_path: Path, selectors: list[str]) -> DbtGraph:
    if not selectors:
        raise ValueError("dbt step requires models[]")
    cache = _load_graph_cache(dbt_project_path)
    key = _selector_key(selectors)
    graphs = cache.get("graphs")
    rows = graphs.get(key) if isinstance(graphs, dict) else None
    if rows is None:
        raise ValueError(
            "dbt graph cache is missing for selectors "
            f"{selectors}: run `z4s api deploy` to refresh the runtime artifact"
        )
    nodes = [_node_from_cache(row) for row in rows if isinstance(row, dict)]
    if not nodes:
        raise ValueError(f"dbt graph cache has no model/test nodes for selectors: {selectors}")
    return DbtGraph(nodes=tuple(nodes))


def sync_dbt_graph_cache(
    project_root: Path,
    config_items: list[tuple[Path, Any]],
    *,
    env: dict[str, str] | None = None,
    profiles_yml_by_conn: dict[str, str] | None = None,
) -> tuple[dict[str, Path], int]:
    project = load_project_context(project_root)
    graph_paths: dict[str, Path] = {}
    graph_count = 0
    by_conn: dict[str, list[tuple[str, ...]]] = {}
    for selection in selected_dbt_step_selections(config_items):
        by_conn.setdefault(selection.conn_id, [])
        if selection.selectors not in by_conn[selection.conn_id]:
            by_conn[selection.conn_id].append(selection.selectors)

    for conn_id, selector_sets in by_conn.items():
        project_path = dbt_project_dir(project, conn_id)
        graphs: dict[str, list[dict[str, Any]]] = {}
        for selectors in selector_sets:
            graph = discover_dbt_graph(
                project_path,
                list(selectors),
                conn_id=conn_id,
                env=env,
                profiles_yml=profiles_yml_by_conn.get(conn_id) if profiles_yml_by_conn else None,
            )
            graphs[_selector_key(list(selectors))] = [_node_to_cache(node) for node in graph.nodes]
            graph_count += 1
        path = project_path / DBT_GRAPH_CACHE_FILE
        _write_json_atomic(path, {"version": 1, "conn": conn_id, "graphs": graphs})
        graph_paths[conn_id] = path
    return graph_paths, graph_count


def selected_dbt_step_selections(config_items: list[tuple[Path, Any]]) -> list[DbtStepSelection]:
    selections: list[DbtStepSelection] = []
    for _, config in config_items:
        if not isinstance(config, dict):
            continue
        steps = config.get("steps")
        if not isinstance(steps, list):
            continue
        for step in steps:
            if not isinstance(step, dict) or step.get("type") not in {"dbt.run", "dbt.test"}:
                continue
            conn_id = str(step.get("conn") or "").strip()
            selectors = tuple(_model_selectors(step.get("models")))
            if conn_id and selectors:
                selections.append(DbtStepSelection(conn_id=conn_id, selectors=selectors))
    return selections


def discover_dbt_graph(
    dbt_project_path: Path,
    selectors: list[str],
    *,
    conn_id: str,
    env: dict[str, str] | None = None,
    profiles_yml: str | None = None,
) -> DbtGraph:
    if not selectors:
        raise ValueError("dbt step requires models[]")
    model_nodes = _dbt_ls(
        dbt_project_path,
        selectors,
        "model",
        conn_id=conn_id,
        env=env,
        profiles_yml=profiles_yml,
    )
    test_nodes = _dbt_ls(
        dbt_project_path,
        selectors,
        "test",
        conn_id=conn_id,
        env=env,
        profiles_yml=profiles_yml,
    )
    nodes = _dedupe_nodes([*model_nodes, *test_nodes])
    if not nodes:
        raise ValueError(f"dbt selector did not match any model/test nodes: {selectors}")
    return DbtGraph(nodes=tuple(nodes))


def _dbt_ls(
    dbt_project_path: Path,
    selectors: list[str],
    resource_type: str,
    *,
    conn_id: str,
    env: dict[str, str] | None = None,
    profiles_yml: str | None = None,
) -> list[DbtNode]:
    if not profiles_yml:
        raise ValueError(f"dbt profile is required for conn: {conn_id}")
    with tempfile.TemporaryDirectory(prefix="zeta4s-dbt-") as work_dir:
        profiles_dir = Path(work_dir) / "profiles"
        profiles_dir.mkdir(parents=True, exist_ok=True)
        (profiles_dir / "profiles.yml").write_text(profiles_yml, encoding="utf-8")
        cmd = [
            dbt_executable(),
            "--log-path",
            str(Path(work_dir) / "logs"),
            "ls",
            "--profiles-dir",
            str(profiles_dir),
            "--project-dir",
            str(dbt_project_path),
            "--target-path",
            str(Path(work_dir) / "target"),
            "--resource-type",
            resource_type,
            "--output",
            "json",
            "--select",
            *selectors,
        ]
        result = subprocess.run(
            cmd,
            cwd=str(dbt_project_path),
            text=True,
            capture_output=True,
            check=False,
            env=env or dbt_parse_env(),
        )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise RuntimeError(f"dbt ls failed for resource_type={resource_type}: {detail}")
    return [_node_from_payload(payload) for payload in _json_lines(result.stdout)]


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        temp_path = Path(handle.name)
        handle.write(content)
    temp_path.replace(path)


def _json_lines(output: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in output.splitlines():
        line = _strip_ansi(line).strip()
        if not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _node_from_payload(payload: dict[str, Any]) -> DbtNode:
    depends_on = payload.get("depends_on") if isinstance(payload.get("depends_on"), dict) else {}
    nodes = depends_on.get("nodes") if isinstance(depends_on, dict) else []
    return DbtNode(
        unique_id=str(payload["unique_id"]),
        name=str(payload["name"]),
        resource_type=str(payload["resource_type"]),
        original_file_path=str(payload.get("original_file_path") or ""),
        depends_on=tuple(str(node) for node in nodes or []),
    )


def _dedupe_nodes(nodes: list[DbtNode]) -> list[DbtNode]:
    seen: set[str] = set()
    deduped: list[DbtNode] = []
    for node in nodes:
        if node.unique_id in seen:
            continue
        seen.add(node.unique_id)
        deduped.append(node)
    return sorted(deduped, key=lambda node: (node.resource_type != "model", node.name, node.unique_id))


def _load_graph_cache(dbt_project_path: Path) -> dict[str, Any]:
    path = dbt_project_path / DBT_GRAPH_CACHE_FILE
    if not path.exists():
        raise ValueError(f"dbt graph cache is missing: {path}. Run `z4s api deploy` to refresh the runtime artifact.")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError(f"dbt graph cache is invalid: {path}") from e
    if not isinstance(payload, dict):
        raise ValueError(f"dbt graph cache must be a JSON object: {path}")
    return payload


def _selector_key(selectors: list[str]) -> str:
    return "\n".join(selectors)


def _model_selectors(models: Any) -> list[str]:
    if not isinstance(models, list):
        return []
    return [f"path:models/{model}.sql" for model in (str(model) for model in models)]


def _node_to_cache(node: DbtNode) -> dict[str, Any]:
    return {
        "unique_id": node.unique_id,
        "name": node.name,
        "resource_type": node.resource_type,
        "original_file_path": node.original_file_path,
        "depends_on": list(node.depends_on),
    }


def _node_from_cache(payload: dict[str, Any]) -> DbtNode:
    return DbtNode(
        unique_id=str(payload["unique_id"]),
        name=str(payload["name"]),
        resource_type=str(payload["resource_type"]),
        original_file_path=str(payload.get("original_file_path") or ""),
        depends_on=tuple(str(node) for node in payload.get("depends_on") or []),
    )


def _strip_ansi(value: str) -> str:
    chars: list[str] = []
    in_escape = False
    for ch in value:
        if ch == "\x1b":
            in_escape = True
            continue
        if in_escape:
            if ch.isalpha():
                in_escape = False
            continue
        chars.append(ch)
    return "".join(chars)
