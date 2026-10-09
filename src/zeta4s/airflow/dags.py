"""Airflow DAG metadata helpers for zeta4s runtime operations."""

from __future__ import annotations

import os
import time
from typing import Callable
import urllib.parse

from zeta4s.airflow.rest_client import AirflowRestClient, AirflowRestError
from zeta4s.project.loader import validate_project_id

ZETA4S_DAG_TAG = "zeta4s"
ACTIVE_DAG_RUN_STATES = ["queued", "running"]
ACTIVE_TASK_INSTANCE_STATES = [
    "scheduled",
    "queued",
    "running",
    "restarting",
    "up_for_retry",
    "up_for_reschedule",
    "deferred",
]


def _project_tag(project: str) -> str:
    return f"project:{validate_project_id(project)}"


def _artifact_tag(artifact_id: str) -> str:
    value = artifact_id.strip()
    if not value:
        raise ValueError("artifact_id must not be empty")
    return f"artifact:{value}"


def list_zeta4s_dags(*, project: str | None = None, artifact_id: str | None = None) -> list[str]:
    """zeta4s 가 등록한 DAG ID 를 돌려준다. REST 가 정본이다."""
    return _list_zeta4s_dags_via_rest(_require_rest_client(), project=project, artifact_id=artifact_id)


def _list_zeta4s_dags_via_rest(
    client: AirflowRestClient, *, project: str | None, artifact_id: str | None = None
) -> list[str]:
    tags = [ZETA4S_DAG_TAG]
    if project:
        tags.append(_project_tag(project))
    if artifact_id:
        tags.append(_artifact_tag(artifact_id))
    rows = client.collect(
        "/api/v2/dags",
        items_key="dags",
        # tag 를 모두 만족하는 DAG 만 본다.
        query={
            "tags": tags,
            "tags_match_mode": "all",
            # stale 여부와 무관하게 본다. REST 기본값은 stale 을 빼므로 그대로 두면
            # purge 대상 DAG 이 목록에서 사라진다.
            "exclude_stale": False,
        },
    )
    dag_ids = sorted({str(row["dag_id"]) for row in rows if row.get("dag_id")})
    if not project:
        return dag_ids
    prefix = f"{validate_project_id(project)}__"
    return [dag_id for dag_id in dag_ids if dag_id.startswith(prefix)]


def set_project_dag_paused(project: str, dag_id: str, *, paused: bool) -> dict[str, object]:
    """Set one zeta4s DAG paused state after project ownership validation."""
    project_name = validate_project_id(project)
    candidates = set(list_zeta4s_dags(project=project_name))
    if dag_id not in candidates:
        raise ValueError(f"not a zeta4s DAG for project {project_name}: {dag_id}")

    _patch_dag_paused(_require_rest_client(), dag_id, paused)
    return {"dag_id": dag_id, "paused": paused}


def set_project_dags_paused(project: str, *, paused: bool) -> list[dict[str, object]]:
    """Set all zeta4s DAGs for one project paused/unpaused."""
    project_name = validate_project_id(project)
    dag_ids = list_zeta4s_dags(project=project_name)
    if not dag_ids:
        return []

    client = _require_rest_client()
    for dag_id in dag_ids:
        _patch_dag_paused(client, dag_id, paused)
    return [{"dag_id": dag_id, "paused": paused} for dag_id in dag_ids]


def _rest_client() -> AirflowRestClient | None:
    """REST 경로가 꺼져 있으면 None 이다. 인증과 오류 처리는 client 가 맡는다."""
    return AirflowRestClient.from_env(os.environ)


def _require_rest_client() -> AirflowRestClient:
    """조회와 상태 변경 모두 REST 가 정본이다. metastore 폴백은 없다."""
    client = _rest_client()
    if client is None:
        raise RuntimeError(
            "Airflow REST API is not configured: set ZETA4S_AIRFLOW_REST_API_BASE_URL",
        )
    return client


def _patch_dag_paused(client: AirflowRestClient, dag_id: str, paused: bool) -> None:
    path = f"/api/v2/dags/{urllib.parse.quote(dag_id, safe='')}"
    try:
        client.patch(path, body={"is_paused": paused}, query={"update_mask": ["is_paused"]})
    except AirflowRestError as error:
        if error.status == 404:
            raise ValueError(f"Airflow DAG not found: {dag_id}") from error
        raise


def _set_project_dags_paused(project: str, dag_ids: list[str], *, paused: bool) -> list[dict[str, object]]:
    project_name = validate_project_id(project)
    candidates = set(list_zeta4s_dags(project=project_name))
    invalid = [dag_id for dag_id in dag_ids if dag_id not in candidates]
    if invalid:
        raise ValueError(f"not zeta4s DAGs for project {project_name}: {invalid}")

    client = _require_rest_client()
    for dag_id in dag_ids:
        _patch_dag_paused(client, dag_id, paused)
    return [{"dag_id": dag_id, "paused": paused} for dag_id in dag_ids]


def dag_paused_states(dag_ids: list[str]) -> dict[str, bool | None]:
    if not dag_ids:
        return {}
    return _dag_paused_states_via_rest(_require_rest_client(), dag_ids)


def _dag_paused_states_via_rest(client: AirflowRestClient, dag_ids: list[str]) -> dict[str, bool | None]:
    """dag_id 별로 조회한다. 없는 DAG 은 None 이다.

    목록 조회로 한 번에 받는 방법도 있으나 REST 에는 dag_id 목록 필터가 없어 zeta4s DAG
    전체를 페이징해야 한다. 이 함수는 converge 폴링이 2초마다 부르므로 대상만 짚는다.
    """
    states: dict[str, bool | None] = {}
    for dag_id in dag_ids:
        path = f"/api/v2/dags/{urllib.parse.quote(dag_id, safe='')}"
        try:
            row = client.get(path)
        except AirflowRestError as error:
            if error.status == 404:
                states[dag_id] = None
                continue
            raise
        states[dag_id] = bool((row or {}).get("is_paused"))
    return states


def wait_for_project_dags(
    project: str,
    expected_dag_ids: list[str],
    *,
    expected_artifact_id: str | None = None,
    timeout_seconds: float = 120.0,
    poll_interval_seconds: float = 2.0,
) -> dict[str, object]:
    """Wait until Airflow metadata sees the expected DAGs for this exact deployment."""
    project_name = validate_project_id(project)
    expected = sorted(set(expected_dag_ids))
    deadline = time.monotonic() + timeout_seconds
    found: list[str] = []
    missing = expected
    start = time.monotonic()
    while True:
        found = sorted(set(list_zeta4s_dags(project=project_name, artifact_id=expected_artifact_id)) & set(expected))
        missing = [dag_id for dag_id in expected if dag_id not in found]
        if not missing:
            break
        if time.monotonic() >= deadline:
            break
        time.sleep(poll_interval_seconds)
    return {
        "found_dags": found,
        "missing_dags": missing,
        "found_count": len(found),
        "missing_count": len(missing),
        "expected_artifact_id": expected_artifact_id,
        "elapsed_seconds": round(time.monotonic() - start, 3),
        "timeout_seconds": timeout_seconds,
        "poll_interval_seconds": poll_interval_seconds,
    }


def converge_project_dag_discovery(
    project: str,
    expected_dag_ids: list[str],
    *,
    expected_artifact_id: str | None = None,
    timeout_seconds: float = 120.0,
    poll_interval_seconds: float = 2.0,
) -> dict[str, object]:
    """Wait for expected DAG IDs to appear in Airflow metadata."""
    result = wait_for_project_dags(
        project,
        expected_dag_ids,
        expected_artifact_id=expected_artifact_id,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )
    result["status"] = "passed" if result["missing_count"] == 0 else "failed"
    return result


def converge_project_dags_paused(
    project: str,
    dag_ids: list[str],
    *,
    paused: bool,
    timeout_seconds: float = 60.0,
    poll_interval_seconds: float = 2.0,
) -> dict[str, object]:
    """Set DAG pause state and wait until Airflow metadata reflects it."""
    project_name = validate_project_id(project)
    target_dag_ids = sorted(set(dag_ids))
    start = time.monotonic()
    requested = _set_project_dags_paused(project_name, target_dag_ids, paused=paused) if target_dag_ids else []
    observed_states = dag_paused_states(target_dag_ids)
    not_converged = [dag_id for dag_id in target_dag_ids if observed_states.get(dag_id) is not paused]
    deadline = start + timeout_seconds
    while not_converged and time.monotonic() < deadline:
        time.sleep(poll_interval_seconds)
        observed_states = dag_paused_states(target_dag_ids)
        not_converged = [dag_id for dag_id in target_dag_ids if observed_states.get(dag_id) is not paused]
    converged = [dag_id for dag_id in target_dag_ids if dag_id not in not_converged]
    return {
        "status": "passed" if not not_converged else "failed",
        "requested_paused": paused,
        "requested_dags": requested,
        "converged_dags": converged,
        "not_converged_dags": not_converged,
        "observed_states": observed_states,
        "converged_count": len(converged),
        "not_converged_count": len(not_converged),
        "elapsed_seconds": round(time.monotonic() - start, 3),
        "timeout_seconds": timeout_seconds,
        "poll_interval_seconds": poll_interval_seconds,
    }


def _active_dag_run_rows(dag_ids: list[str]) -> list[dict[str, object]]:
    if not dag_ids:
        return []
    return _active_dag_run_rows_via_rest(_require_rest_client(), dag_ids)


def _collect_or_empty(
    client: AirflowRestClient,
    path: str,
    *,
    items_key: str,
    query: dict[str, object],
) -> list[dict[str, object]]:
    """없는 DAG 은 빈 목록이다.

    REST 는 없는 DAG 에 404 를 내지만 호출부에게 없는 DAG 은 오류가 아니다. undeploy 는 DAG 을 지운 뒤에도 남은 run 을 확인하므로 404 를 오류로 두면 터진다.
    """
    try:
        return client.collect(path, items_key=items_key, query=query)
    except AirflowRestError as error:
        if error.status == 404:
            return []
        raise


def _active_dag_run_rows_via_rest(client: AirflowRestClient, dag_ids: list[str]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for dag_id in sorted(dag_ids):
        runs = _collect_or_empty(
            client,
            f"/api/v2/dags/{urllib.parse.quote(dag_id, safe='')}/dagRuns",
            items_key="dag_runs",
            query={"state": ACTIVE_DAG_RUN_STATES},
        )
        rows.extend(
            {"dag_id": run.get("dag_id"), "run_id": run.get("dag_run_id"), "state": run.get("state")} for run in runs
        )
    rows.sort(key=lambda row: (str(row["dag_id"]), str(row["run_id"])))
    return rows


def _active_task_instance_rows(dag_ids: list[str]) -> list[dict[str, object]]:
    if not dag_ids:
        return []
    return _active_task_instance_rows_via_rest(_require_rest_client(), dag_ids)


def _active_task_instance_rows_via_rest(client: AirflowRestClient, dag_ids: list[str]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for dag_id in sorted(dag_ids):
        # dag_run_id 를 ~ 로 두면 그 DAG 의 모든 run 을 한 번에 본다.
        instances = _collect_or_empty(
            client,
            f"/api/v2/dags/{urllib.parse.quote(dag_id, safe='')}/dagRuns/~/taskInstances",
            items_key="task_instances",
            query={"state": ACTIVE_TASK_INSTANCE_STATES},
        )
        rows.extend(
            {
                "dag_id": row.get("dag_id"),
                # 호출부는 run_id 로 읽는다. REST 는 dag_run_id 다.
                "run_id": row.get("dag_run_id"),
                "task_id": row.get("task_id"),
                "map_index": row.get("map_index"),
                "state": row.get("state"),
            }
            for row in instances
        )
    rows.sort(key=lambda row: (str(row["dag_id"]), str(row["run_id"]), str(row["task_id"]), row["map_index"] or 0))
    return rows


def _terminate_active_dag_runs(dag_ids: list[str]) -> list[dict[str, object]]:
    if not dag_ids:
        return []
    client = _require_rest_client()
    terminated: list[dict[str, object]] = []
    for row in _active_dag_run_rows_via_rest(client, dag_ids):
        dag_id = str(row["dag_id"])
        run_id = str(row["run_id"])
        path = f"/api/v2/dags/{urllib.parse.quote(dag_id, safe='')}/dagRuns/{urllib.parse.quote(run_id, safe='')}"
        # update_mask 를 주지 않으면 body 에 없는 note 까지 덮어쓸 수 있다.
        client.patch(path, body={"state": "failed"}, query={"update_mask": ["state"]})
        terminated.append({"dag_id": dag_id, "run_id": run_id, "previous_state": row["state"], "state": "failed"})
    return terminated


def _terminate_active_task_instances(dag_ids: list[str]) -> list[dict[str, object]]:
    if not dag_ids:
        return []
    client = _require_rest_client()
    terminated: list[dict[str, object]] = []
    for row in _active_task_instance_rows_via_rest(client, dag_ids):
        dag_id = str(row["dag_id"])
        run_id = str(row["run_id"])
        task_id = str(row["task_id"])
        map_index = row["map_index"]
        path = (
            f"/api/v2/dags/{urllib.parse.quote(dag_id, safe='')}"
            f"/dagRuns/{urllib.parse.quote(run_id, safe='')}"
            f"/taskInstances/{urllib.parse.quote(task_id, safe='')}"
        )
        # mapped task 는 map_index 경로로 하나만 짚는다. -1 은 map 되지 않은 task 다.
        if isinstance(map_index, int) and map_index >= 0:
            path = f"{path}/{map_index}"
        # include_* 는 기본이 False 라 upstream/downstream 을 건드리지 않는다.
        client.patch(path, body={"new_state": "failed"}, query={"update_mask": ["new_state"]})
        terminated.append(
            {
                "dag_id": dag_id,
                "run_id": run_id,
                "task_id": task_id,
                "map_index": map_index,
                "previous_state": row["state"],
                "state": "failed",
            }
        )
    return terminated


def converge_project_active_runs_terminated(
    project: str,
    dag_ids: list[str],
    *,
    timeout_seconds: float = 300.0,
    poll_interval_seconds: float = 10.0,
) -> dict[str, object]:
    """Terminate active runs/tasks and wait until no active rows remain."""
    validate_project_id(project)
    target_dag_ids = sorted(set(dag_ids))
    start = time.monotonic()
    terminated_runs = _terminate_active_dag_runs(target_dag_ids)
    terminated_tasks = _terminate_active_task_instances(target_dag_ids)
    remaining_runs = _active_dag_run_rows(target_dag_ids)
    remaining_tasks = _active_task_instance_rows(target_dag_ids)
    deadline = start + timeout_seconds
    while (remaining_runs or remaining_tasks) and time.monotonic() < deadline:
        time.sleep(poll_interval_seconds)
        remaining_runs = _active_dag_run_rows(target_dag_ids)
        remaining_tasks = _active_task_instance_rows(target_dag_ids)
    return {
        "status": "passed" if not remaining_runs and not remaining_tasks else "failed",
        "terminated_runs": terminated_runs,
        "terminated_task_instances": terminated_tasks,
        "remaining_runs": remaining_runs,
        "remaining_task_instances": remaining_tasks,
        "terminated_run_count": len(terminated_runs),
        "terminated_task_instance_count": len(terminated_tasks),
        "remaining_run_count": len(remaining_runs),
        "remaining_task_instance_count": len(remaining_tasks),
        "elapsed_seconds": round(time.monotonic() - start, 3),
        "timeout_seconds": timeout_seconds,
        "poll_interval_seconds": poll_interval_seconds,
    }


def _delete_dag(dag_id: str) -> None:
    client = _require_rest_client()
    path = f"/api/v2/dags/{urllib.parse.quote(dag_id, safe='')}"
    try:
        client.delete(path)
    except AirflowRestError as error:
        # 이미 없으면 목표 상태와 같다. 삭제는 멱등이어야 한다.
        if error.status == 404:
            return
        raise RuntimeError(f"failed to delete Airflow DAG {dag_id}: {error}") from error


def delete_project_dag(project: str, dag_id: str, *, progress: Callable[[str], None] | None = None) -> list[str]:
    """Delete one zeta4s DAG after project ownership validation."""
    project_name = validate_project_id(project)
    candidates = set(list_zeta4s_dags(project=project_name))
    if dag_id not in candidates:
        raise ValueError(f"not a zeta4s DAG for project {project_name}: {dag_id}")
    _delete_dag(dag_id)
    if progress:
        progress(dag_id)
    return [dag_id]


def purge_project_dags(project: str, *, progress: Callable[[str], None] | None = None) -> list[str]:
    """Delete all zeta4s DAGs for one project."""
    project_name = validate_project_id(project)
    deleted: list[str] = []
    for dag_id in list_zeta4s_dags(project=project_name):
        _delete_dag(dag_id)
        deleted.append(dag_id)
        if progress:
            progress(dag_id)
    return deleted


def converge_project_dags_deleted(
    project: str,
    dag_ids: list[str],
    *,
    timeout_seconds: float = 120.0,
    poll_interval_seconds: float = 2.0,
    progress: Callable[[str], None] | None = None,
) -> dict[str, object]:
    """Delete project DAG metadata and wait until it disappears from Airflow metadata."""
    project_name = validate_project_id(project)
    target_dag_ids = sorted(set(dag_ids))
    existing = set(list_zeta4s_dags(project=project_name))
    deleted: list[str] = []
    for dag_id in target_dag_ids:
        if dag_id not in existing:
            continue
        _delete_dag(dag_id)
        deleted.append(dag_id)
        if progress:
            progress(dag_id)

    start = time.monotonic()
    remaining = sorted(set(list_zeta4s_dags(project=project_name)) & set(target_dag_ids))
    deadline = start + timeout_seconds
    while remaining and time.monotonic() < deadline:
        time.sleep(poll_interval_seconds)
        remaining = sorted(set(list_zeta4s_dags(project=project_name)) & set(target_dag_ids))
    return {
        "status": "passed" if not remaining else "failed",
        "deleted_dags": deleted,
        "remaining_dags": remaining,
        "deleted_count": len(deleted),
        "remaining_count": len(remaining),
        "elapsed_seconds": round(time.monotonic() - start, 3),
        "timeout_seconds": timeout_seconds,
        "poll_interval_seconds": poll_interval_seconds,
    }
