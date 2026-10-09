"""Airflow DAG run / task instance 조회. zeta4s-api process 에서 돈다.

`dags.py` 는 DAG 축이고 이 module 은 run 축이다. Airflow 에는 REST 로만 붙으며 airflow 를
import 하지 않는다.

**없는 run 은 None 이고 오류가 아니다.** REST 는 없는 DAG/run 에 404 를 내지만 호출부는
"아직 없음" 과 "붙을 수 없음" 을 구분해야 한다. 404 를 여기서 None 으로 바꿔 그 구분을
호출부에 넘긴다.
"""

from __future__ import annotations

import json
from typing import Any
import urllib.parse

from zeta4s.airflow.rest_client import AirflowRestClient, AirflowRestError, require_rest_client

# metastore 경로가 돌려주던 dag run field 다. REST 응답의 key 와 이름이 같고 `run_id` 만
# 다르다 — REST 는 `dag_run_id` 로 준다.
_DAG_RUN_FIELDS = (
    "state",
    "start_date",
    "end_date",
    "queued_at",
    "logical_date",
    "run_after",
    "data_interval_start",
    "data_interval_end",
)


def _quote(value: str) -> str:
    return urllib.parse.quote(value, safe="")


def _dag_run_path(dag_id: str, run_id: str) -> str:
    return f"/api/v2/dags/{_quote(dag_id)}/dagRuns/{_quote(run_id)}"


def _none_on_404(call):
    try:
        return call()
    except AirflowRestError as error:
        if error.status == 404:
            return None
        raise


def _dag_run_row(dag_id: str, row: dict[str, Any]) -> dict[str, Any]:
    """REST dag run 을 metastore 경로가 내던 모양으로 맞춘다."""
    run_type = row.get("run_type")
    conf = row.get("conf") if isinstance(row.get("conf"), dict) else {}
    mapped: dict[str, Any] = {
        "dag_id": row.get("dag_id") or dag_id,
        "run_id": row.get("dag_run_id"),
        "conf": conf,
        "run_type": str(run_type) if run_type is not None else None,
    }
    mapped.update({field: row.get(field) for field in _DAG_RUN_FIELDS})
    return mapped


def dag_run(dag_id: str, run_id: str) -> dict[str, Any] | None:
    """없으면 None 이다."""
    client = require_rest_client()
    row = _none_on_404(lambda: client.get(_dag_run_path(dag_id, run_id)))
    return _dag_run_row(dag_id, row) if isinstance(row, dict) else None


def _recency_key(row: dict[str, Any]) -> str:
    """metastore 경로가 쓰던 우선순위다. 시각이 없는 run 은 run_id 로 줄 세운다.

    **정렬을 REST 에 맡길 수 없다.** `order_by` 는 `run_after`/`start_date`/`logical_date`/
    `id` 를 받지만 `queued_at` 은 400 `Ordering with 'queued_at' is disallowed` 로 막는다.
    하필 이 우선순위의 1순위 키다. 그래서 받아서 여기서 정렬한다.
    """
    return str(
        row.get("queued_at")
        or row.get("start_date")
        or row.get("logical_date")
        or row.get("run_after")
        or row["run_id"]
    )


def dag_runs(dag_id: str, limit: int) -> list[dict[str, Any]]:
    """최신순 `limit` 개다. 없는 DAG 은 빈 목록이다."""
    client = require_rest_client()
    rows = _none_on_404(
        lambda: client.collect(
            f"/api/v2/dags/{_quote(dag_id)}/dagRuns",
            items_key="dag_runs",
            query={},
        )
    )
    mapped = [_dag_run_row(dag_id, row) for row in rows or [] if isinstance(row, dict) and row.get("dag_run_id")]
    mapped.sort(key=_recency_key, reverse=True)
    return mapped[: int(limit or 30)]


def latest_run_by_dag(dag_ids: list[str]) -> dict[str, dict[str, Any]]:
    """dag_id 별 가장 최근 run 이다. 없는 DAG 은 결과에서 빠진다.

    dag 마다 왕복하지 않고 `POST /api/v2/dags/~/dagRuns/list` 로 한 번에 받는다.
    """
    if not dag_ids:
        return {}
    client = require_rest_client()
    rows = client.collect_batch(
        "/api/v2/dags/~/dagRuns/list",
        items_key="dag_runs",
        body={"dag_ids": sorted(set(dag_ids))},
    )
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict) or not row.get("dag_id") or not row.get("dag_run_id"):
            continue
        dag_id = str(row["dag_id"])
        mapped = _dag_run_row(dag_id, row)
        current = latest.get(dag_id)
        if current is None or _recency_key(mapped) > _recency_key(current):
            latest[dag_id] = mapped
    return latest


def task_instances_by_run(keys: list[tuple[str, str]]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    """(dag_id, run_id) 별 task instance 다. run 마다 왕복하지 않고 한 번에 받는다.

    `dag_ids` 와 `dag_run_ids` 는 각각 독립으로 걸리므로 응답은 요청한 짝의 곱집합이다.
    metastore 경로의 `IN` × `IN` query 와 같은 성질이라 짝으로 다시 거른다.
    """
    if not keys:
        return {}
    client = require_rest_client()
    wanted = set(keys)
    rows = client.collect_batch(
        "/api/v2/dags/~/dagRuns/~/taskInstances/list",
        items_key="task_instances",
        body={
            "dag_ids": sorted({dag_id for dag_id, _ in wanted}),
            "dag_run_ids": sorted({run_id for _, run_id in wanted}),
        },
    )
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = (str(row.get("dag_id") or ""), str(row.get("dag_run_id") or ""))
        if key not in wanted:
            continue
        grouped.setdefault(key, []).append(
            {
                "dag_id": key[0],
                "run_id": key[1],
                "task_id": row.get("task_id"),
                "state": row.get("state"),
                "try_number": row.get("try_number"),
                "map_index": row.get("map_index"),
                "start_date": row.get("start_date"),
                "end_date": row.get("end_date"),
                "duration": row.get("duration"),
            }
        )
    return grouped


def dag_run_state(dag_id: str, run_id: str) -> dict[str, Any] | None:
    """실행 상태만 본다. 없으면 None 이다."""
    row = dag_run(dag_id, run_id)
    if row is None:
        return None
    return {
        "airflow_state": row.get("state"),
        "airflow_start_date": row.get("start_date"),
        "airflow_end_date": row.get("end_date"),
    }


def task_log_lines(
    dag_id: str,
    run_id: str,
    task_id: str,
    try_number: int,
    *,
    map_index: int | None = None,
) -> list[str] | None:
    """task 시도 하나의 로그를 줄 단위로 준다. 없으면 None 이다.

    **Airflow 3 의 task log 는 JSON line 이다.** 파일도 REST 도 같은 레코드이고, 파일을 직접
    읽던 시절의 내용과 같은 모양으로 돌려주려면 레코드를 다시 JSON line 으로 만든다.

    `text/plain` 은 406 이다. `application/json` 또는 `application/x-ndjson` 만 받는다.
    """
    client = require_rest_client()
    query: dict[str, Any] = {"full_content": True}
    if map_index is not None:
        query["map_index"] = map_index
    path = f"{_dag_run_path(dag_id, run_id)}/taskInstances/{_quote(task_id)}/logs/{int(try_number)}"
    payload = _none_on_404(lambda: client.get(path, query=query))
    if not isinstance(payload, dict):
        return None
    return [_log_line(record) for record in payload.get("content") or []]


def _log_line(record: Any) -> str:
    if isinstance(record, str):
        return record
    if not isinstance(record, dict):
        return str(record)
    return json.dumps(record, ensure_ascii=False)


def trigger_dag_run(dag_id: str, run_id: str, conf: dict[str, Any] | None = None) -> dict[str, Any]:
    """DAG run 을 만든다.

    **`logical_date` 는 값이 없어도 key 를 보내야 한다.** `TriggerDAGRunPostBody` 가
    required 로 두고 값만 nullable 이라 빼면 422 다. zeta4s 가 만드는 run 은 schedule 이
    아니라 요청 시점의 것이므로 null 을 준다.
    """
    client = require_rest_client()
    row = client.post(
        f"/api/v2/dags/{_quote(dag_id)}/dagRuns",
        body={"dag_run_id": run_id, "conf": conf or {}, "logical_date": None},
    )
    return _dag_run_row(dag_id, row) if isinstance(row, dict) else {}


def set_dag_run_state(dag_id: str, run_id: str, state: str) -> bool:
    """run 상태를 바꾼다. 없으면 False 다.

    `DagRunMutableStates` 는 `queued`/`success`/`failed` 만 받는다. `update_mask` 를 주지
    않으면 body 에 없는 note 까지 덮어쓴다.
    """
    client = require_rest_client()
    patched = _none_on_404(
        lambda: client.patch(
            _dag_run_path(dag_id, run_id),
            body={"state": state},
            query={"update_mask": ["state"]},
        )
    )
    return patched is not None


def task_instances(dag_id: str, run_id: str) -> list[dict[str, Any]] | None:
    """run 의 task instance 를 task_id, map_index 순으로 준다. 없는 run 은 None 이다."""
    client = require_rest_client()
    rows = _none_on_404(
        lambda: client.collect(
            f"{_dag_run_path(dag_id, run_id)}/taskInstances",
            items_key="task_instances",
            query={},
        )
    )
    if rows is None:
        return None
    tasks = [
        {
            "task_id": row.get("task_id"),
            "map_index": row.get("map_index"),
            "state": row.get("state"),
            "try_number": row.get("try_number"),
            "start_date": row.get("start_date"),
            "end_date": row.get("end_date"),
            "duration": row.get("duration"),
        }
        for row in rows
        if isinstance(row, dict)
    ]
    tasks.sort(
        key=lambda row: (
            str(row.get("task_id") or ""),
            row.get("map_index") if row.get("map_index") is not None else -1,
        )
    )
    return tasks


__all__ = [
    "AirflowRestClient",
    "dag_run",
    "dag_run_state",
    "dag_runs",
    "latest_run_by_dag",
    "task_instances",
    "task_instances_by_run",
]
