"""Airflow runtime pool projection helpers.

Pool 은 Airflow 가 소유하는 asset 이며 zeta4s 가 Step Graph 에서 산출해 REST 로
동기화한다. Connection 은 여기서 다루지 않는다 — generated DAG 는 Airflow
Connection 을 쓰지 않고 internal API 로 step 실행을 위임하며, credential 은
`zeta4s-api` process 안에서만 resolve 된다.
"""

from __future__ import annotations

import os
import urllib.parse
from pathlib import Path
from typing import Any, Callable

from zeta4s.airflow.rest_client import AirflowRestClient, AirflowRestError
from zeta4s.project.pools import (
    project_pool_payloads,
    project_pool_names,
)
from zeta4s.project.loader import load_project_context


def _rest_client() -> AirflowRestClient:
    """Connection/Pool projection 은 REST 가 정본이다. metastore 폴백은 없다."""
    client = AirflowRestClient.from_env(os.environ)
    if client is None:
        raise RuntimeError(
            "Airflow REST API is not configured: set ZETA4S_AIRFLOW_REST_API_BASE_URL",
        )
    return client


def _bulk_upsert(client: AirflowRestClient, path: str, entities: list[dict[str, Any]], *, replace: bool) -> None:
    """실측: upsert 는 create + action_on_existence=overwrite 다.

    update action 은 없는 entity 를 만들지 않고 skip/fail 만 하므로 upsert 가 아니다.
    replace 가 아니면 이미 있는 것을 덮지 않고 실패시킨다.
    """
    if not entities:
        return
    body = {
        "actions": [
            {
                "action": "create",
                "entities": entities,
                "action_on_existence": "overwrite" if replace else "fail",
            }
        ]
    }
    result = client.patch(path, body=body) or {}
    errors = ((result.get("create") or {}).get("errors")) or []
    if errors:
        raise RuntimeError(f"Airflow bulk upsert failed for {path}: {errors}")


def apply_project_pools(
    project_root: Path,
    *,
    progress: Callable[[str], None] | None = None,
) -> list[dict[str, Any]]:
    project = load_project_context(project_root)
    payloads = project_pool_payloads(project.project_id, project.root)
    desired_names = {payload["name"] for payload in payloads}
    client = _rest_client()

    for pool_name in sorted(project_pool_names(project.project_id) - desired_names):
        if progress:
            progress(f"delete obsolete project pool: {pool_name}")
        _delete_pool(client, pool_name)

    for payload in payloads:
        if progress:
            progress(f"upsert project pool: {payload['name']}")
    _bulk_upsert(client, "/api/v2/pools", [_pool_entity(payload) for payload in payloads], replace=True)
    return payloads


def _pool_entity(payload: dict[str, Any]) -> dict[str, Any]:
    """zeta4s payload 를 PoolBody 로 바꾼다.

    payload 는 stage 처럼 zeta4s 안에서만 쓰는 field 를 싣고 다니는데 PoolBody 는
    additionalProperties 를 막아 두어 그대로 보내면 422 다. 경계를 넘을 때 PoolBody 가 받는
    field 만 고른다.
    """
    entity: dict[str, Any] = {"name": payload["name"], "slots": payload["slots"]}
    description = payload.get("description")
    if description is not None:
        entity["description"] = description
    entity["include_deferred"] = bool(payload.get("include_deferred", False))
    return entity


def _delete_pool(client: AirflowRestClient, name: str) -> None:
    # 이미 없으면 목표 상태와 같다.
    try:
        client.delete(f"/api/v2/pools/{urllib.parse.quote(name, safe='')}")
    except AirflowRestError as error:
        if error.status != 404:
            raise


if __name__ == "__main__":
    raise SystemExit("Use `z4s api ...` instead.")
