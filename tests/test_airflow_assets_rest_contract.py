"""assets.py 의 Pool projection 계약 검사.

Pool 은 adapter projection contract 다. REST 가 정본이고 metastore 폴백은 없다.
Connection 은 zeta4s 가 Airflow 에 동기화하지 않는다 — generated DAG 는 internal API 로
step 실행을 위임하고 credential 은 API process 안에서만 resolve 된다.
"""

from __future__ import annotations

import json
import unittest
import urllib.error
from unittest import mock

from zeta4s.airflow import assets
from zeta4s.airflow.rest_client import AirflowRestClient


class _Response:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *exc: object) -> bool:
        return False


class _Recorder:
    """PATCH/DELETE 를 기록하고 bulk 응답을 흉내낸다."""

    def __init__(self, *, errors: list[dict[str, object]] | None = None) -> None:
        self.requests: list[tuple[str, str, object]] = []
        self.errors = errors or []

    def __call__(self, request, timeout=None):  # noqa: ANN001
        body = None
        if request.data:
            body = json.loads(request.data.decode())
        self.requests.append((request.get_method(), request.full_url, body))
        return _Response(json.dumps({"create": {"success": [], "errors": self.errors}}).encode())


def _client(transport) -> AirflowRestClient:  # noqa: ANN001
    return AirflowRestClient(base_url="http://airflow:8080", token="t", transport=transport)


class PoolEntityShapeTest(unittest.TestCase):
    """project pool payload 는 stage 를 싣고 다니는데 PoolBody 는 거부한다."""

    def test_stage_field_is_dropped(self) -> None:
        entity = assets._pool_entity({"name": "z4p_p_extract", "stage": "extract", "slots": 8, "description": "d"})
        self.assertEqual(entity, {"name": "z4p_p_extract", "slots": 8, "description": "d", "include_deferred": False})
        self.assertNotIn("stage", entity)

    def test_real_project_pool_payload_shape_is_accepted(self) -> None:
        # project_pool_payloads 가 실제로 만드는 형태다. fake payload 로는 이 경계를 검증할 수 없다.
        payload = {
            "name": "z4p_canonical__showcase_extract",
            "stage": "extract",
            "slots": 8,
            "description": "ZETA4S project canonical_showcase extract concurrency",
        }
        entity = assets._pool_entity(payload)
        allowed = {"name", "slots", "description", "include_deferred", "team_name"}
        self.assertTrue(set(entity).issubset(allowed), f"PoolBody 가 모르는 field: {set(entity) - allowed}")


class BulkUpsertTest(unittest.TestCase):
    def test_upsert_uses_create_with_overwrite(self) -> None:
        # 실측: update action 은 없는 entity 를 만들지 않는다. upsert 는 create+overwrite 다.
        rec = _Recorder()
        assets._bulk_upsert(_client(rec), "/api/v2/pools", [{"name": "z4p_p_extract"}], replace=True)

        _method, _url, body = rec.requests[-1]
        action = body["actions"][0]
        self.assertEqual(action["action"], "create")
        self.assertEqual(action["action_on_existence"], "overwrite")

    def test_no_replace_refuses_to_overwrite(self) -> None:
        rec = _Recorder()
        assets._bulk_upsert(_client(rec), "/api/v2/pools", [{"name": "z4p_p_extract"}], replace=False)

        _method, _url, body = rec.requests[-1]
        self.assertEqual(body["actions"][0]["action_on_existence"], "fail")

    def test_bulk_errors_are_raised_not_swallowed(self) -> None:
        rec = _Recorder(errors=[{"error": "already exists"}])
        with self.assertRaises(RuntimeError):
            assets._bulk_upsert(_client(rec), "/api/v2/pools", [{"name": "z4p_p_extract"}], replace=False)

    def test_empty_entities_makes_no_request(self) -> None:
        rec = _Recorder()
        assets._bulk_upsert(_client(rec), "/api/v2/pools", [], replace=True)
        self.assertEqual(rec.requests, [])


class DeletePoolTest(unittest.TestCase):
    def test_missing_pool_is_not_an_error(self) -> None:
        def gone(request, timeout=None):  # noqa: ANN001
            raise urllib.error.HTTPError(request.full_url, 404, "Not Found", {}, None)

        assets._delete_pool(_client(gone), "absent")  # 예외가 없어야 한다

    def test_other_errors_still_raise(self) -> None:
        def boom(request, timeout=None):  # noqa: ANN001
            raise urllib.error.HTTPError(request.full_url, 500, "boom", {}, None)

        with self.assertRaises(Exception):
            assets._delete_pool(_client(boom), "p")


class RestIsRequiredTest(unittest.TestCase):
    def test_projection_fails_clearly_without_rest(self) -> None:
        with mock.patch.dict("os.environ", {"ZETA4S_AIRFLOW_REST_API_BASE_URL": ""}, clear=False):
            with self.assertRaises(RuntimeError) as ctx:
                assets._rest_client()
            self.assertIn("ZETA4S_AIRFLOW_REST_API_BASE_URL", str(ctx.exception))


class AssetsRestOnlyTest(unittest.TestCase):
    """assets 는 Airflow CLI subprocess 나 metastore 경로를 두지 않는다.

    asset 조작은 REST 로만 한다. REST 는 password 를 마스킹하므로 export 는 제공하지 않는다.
    """

    def test_assets_have_no_subprocess_helpers(self) -> None:
        for name in ("_run", "export_assets", "delete_assets", "apply_assets", "validate_assets"):
            self.assertFalse(hasattr(assets, name), f"{name} 이 남아 있다")


if __name__ == "__main__":
    unittest.main()
