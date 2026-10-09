"""zeta4s note 동기화를 zeta4s-api 에 요청하는 DAG callback. Airflow worker 에서 돈다.

note 를 실제로 반영하는 쪽은 `run_notes.py` 이고 zeta4s-api process 에서 돈다. 실행 위치가
다르므로 module 을 가른다. 이 module 은 airflow 를 import 하지 않는다 — callback 이 받는
context 객체를 duck typing 으로 읽을 뿐이다.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)


def sync_task_result_notes_callback(context: dict[str, Any]) -> None:
    """Airflow DAG completion callback.

    Note synchronization is an observability convenience. It must never change the
    DAG run result, so all errors are logged and swallowed.
    """
    try:
        dag_run = context.get("dag_run") if context else None
        dag = context.get("dag") if context else None
        dag_id = _context_dag_id(context, dag_run, dag)
        airflow_run_id = _context_airflow_run_id(dag_run)
        result_run_id = _context_result_run_id(dag_run)
        if not dag_id or not airflow_run_id or not result_run_id:
            logger.warning(
                "Skipping zeta4s Airflow note sync: dag_id=%r airflow_run_id=%r result_run_id=%r",
                dag_id,
                airflow_run_id,
                result_run_id,
            )
            return
        result = request_airflow_run_note_sync(
            dag_id=dag_id,
            airflow_run_id=airflow_run_id,
            result_run_id=result_run_id,
        )
        logger.info("Synced zeta4s Airflow notes: %s", result)
    except Exception:
        logger.warning("Failed to sync zeta4s Airflow notes from DAG callback", exc_info=True)


def request_airflow_run_note_sync(
    *,
    dag_id: str,
    airflow_run_id: str,
    result_run_id: str,
) -> dict[str, Any]:
    """Ask zeta4s-api to sync notes from outside Airflow task execution context."""
    base_url = os.environ.get("ZETA4S_API_INTERNAL_URL") or os.environ.get("ZETA4S_API_URL") or "http://zeta4s-api:8088"
    timeout = float(os.environ.get("ZETA4S_AIRFLOW_NOTE_SYNC_TIMEOUT_SECONDS", "10"))
    payload = json.dumps(
        {
            "adapter_job_id": dag_id,
            "scheduler_run_id": airflow_run_id,
            "result_run_id": result_run_id,
        }
    ).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    token = os.environ.get("ZETA4S_API_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = Request(
        f"{base_url.rstrip('/')}/api/v1/runs/sync-notes",
        data=payload,
        headers=headers,
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
    except URLError as exc:
        raise RuntimeError(f"zeta4s-api note sync request failed: {exc}") from exc
    data = json.loads(body or "{}")
    return data if isinstance(data, dict) else {"response": data}


def _context_dag_id(context: dict[str, Any], dag_run: Any, dag: Any) -> str | None:
    if dag_run is not None and getattr(dag_run, "dag_id", None):
        return str(dag_run.dag_id)
    if dag is not None and getattr(dag, "dag_id", None):
        return str(dag.dag_id)
    task = context.get("task") if context else None
    if task is not None and getattr(task, "dag_id", None):
        return str(task.dag_id)
    return None


def _context_airflow_run_id(dag_run: Any) -> str | None:
    if dag_run is not None and getattr(dag_run, "run_id", None):
        return str(dag_run.run_id)
    return None


def _context_result_run_id(dag_run: Any) -> str | None:
    if dag_run is None:
        return None
    conf = getattr(dag_run, "conf", None)
    if isinstance(conf, dict) and conf.get("z4_run_id"):
        return str(conf["z4_run_id"])
    if getattr(dag_run, "run_id", None):
        return str(dag_run.run_id)
    return None
