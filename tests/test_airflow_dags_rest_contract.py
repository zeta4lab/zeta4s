"""dags.py 의 REST 경로 계약 검사.

metastore 경로와 REST 경로는 없는 DAG 을 다르게 다룬다. metastore 는 행이 없을 뿐이고
REST 는 404 다. 이 차이를 흡수하지 않으면 undeploy 처럼 DAG 을 지운 뒤 남은 run 을
확인하는 흐름이 터진다.
"""

from __future__ import annotations

import json
import unittest
import urllib.error
from unittest import mock

from zeta4s.airflow import dags
from zeta4s.airflow.rest_client import AirflowRestClient

REST_ENV = {
    "ZETA4S_AIRFLOW_REST_API_BASE_URL": "http://airflow:8080",
    "ZETA4S_AIRFLOW_REST_API_TOKEN": "t",
}


class _Response:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *exc: object) -> bool:
        return False


def _transport(routes: dict[str, object]):
    """path prefix -> payload | HTTP status. 없는 경로는 404 다."""

    def transport(request, timeout=None):  # noqa: ANN001
        url = request.full_url
        for prefix, outcome in routes.items():
            if prefix in url:
                if isinstance(outcome, int):
                    raise urllib.error.HTTPError(url, outcome, "err", {}, None)
                return _Response(json.dumps(outcome).encode())
        raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)

    return transport


def _client(routes: dict[str, object]) -> AirflowRestClient:
    return AirflowRestClient(base_url="http://airflow:8080", token="t", transport=_transport(routes))


class MissingDagIsEmptyNotErrorTest(unittest.TestCase):
    def test_active_dag_run_rows_treats_missing_dag_as_empty(self) -> None:
        # 실측 회귀: REST 는 없는 DAG 의 dagRuns 에 404 를 낸다.
        client = _client({"/dagRuns": 404})
        self.assertEqual(dags._active_dag_run_rows_via_rest(client, ["gone"]), [])

    def test_active_task_instance_rows_treats_missing_dag_as_empty(self) -> None:
        client = _client({"/taskInstances": 404})
        self.assertEqual(dags._active_task_instance_rows_via_rest(client, ["gone"]), [])

    def test_non_404_still_raises(self) -> None:
        # 404 만 흡수한다. 500 을 빈 목록으로 삼키면 장애가 조용해진다.
        client = _client({"/dagRuns": 500})
        with self.assertRaises(Exception):
            dags._active_dag_run_rows_via_rest(client, ["boom"])

    def test_delete_dag_is_idempotent(self) -> None:
        with mock.patch.dict("os.environ", REST_ENV, clear=False):
            with mock.patch.object(dags, "_rest_client", return_value=_client({"/api/v2/dags/": 404})):
                dags._delete_dag("gone")  # 예외가 없어야 한다


class ActiveRowShapeTest(unittest.TestCase):
    def test_dag_run_rows_map_dag_run_id_to_run_id(self) -> None:
        client = _client(
            {
                "/dagRuns": {
                    "dag_runs": [{"dag_id": "d", "dag_run_id": "r1", "state": "running"}],
                    "total_entries": 1,
                }
            }
        )
        self.assertEqual(
            dags._active_dag_run_rows_via_rest(client, ["d"]),
            [{"dag_id": "d", "run_id": "r1", "state": "running"}],
        )

    def test_task_instance_rows_map_dag_run_id_to_run_id(self) -> None:
        client = _client(
            {
                "/taskInstances": {
                    "task_instances": [
                        {"dag_id": "d", "dag_run_id": "r1", "task_id": "t", "map_index": -1, "state": "running"}
                    ],
                    "total_entries": 1,
                }
            }
        )
        self.assertEqual(
            dags._active_task_instance_rows_via_rest(client, ["d"]),
            [{"dag_id": "d", "run_id": "r1", "task_id": "t", "map_index": -1, "state": "running"}],
        )


class DeploymentIdentityDiscoveryTest(unittest.TestCase):
    def test_list_filters_by_project_and_artifact_tags(self) -> None:
        client = mock.Mock()
        client.collect.return_value = [{"dag_id": "retail__daily"}]

        self.assertEqual(
            dags._list_zeta4s_dags_via_rest(client, project="retail", artifact_id="sha256:new"),
            ["retail__daily"],
        )
        self.assertEqual(
            client.collect.call_args.kwargs["query"]["tags"],
            ["zeta4s", "project:retail", "artifact:sha256:new"],
        )

    def test_wait_rejects_stale_artifact_until_new_tag_is_visible(self) -> None:
        with mock.patch.object(
            dags,
            "list_zeta4s_dags",
            side_effect=[[], ["retail__daily"]],
        ) as list_dags:
            result = dags.wait_for_project_dags(
                "retail",
                ["retail__daily"],
                expected_artifact_id="sha256:new",
                timeout_seconds=1,
                poll_interval_seconds=0,
            )

        self.assertEqual(result["missing_count"], 0)
        self.assertEqual(result["expected_artifact_id"], "sha256:new")
        self.assertEqual(list_dags.call_count, 2)
        for call in list_dags.call_args_list:
            self.assertEqual(call.kwargs["artifact_id"], "sha256:new")


class RestIsRequiredTest(unittest.TestCase):
    """조회와 상태 변경 모두 metastore 폴백이 없다.

    폴백이 남아 있으면 REST 환경변수가 빠졌을 때 조용히 metastore 로 새고, headless 가
    아닌 상태를 아무 신호 없이 되돌린다. 붙을 곳이 없으면 실패해야 한다.
    """

    def test_paths_fail_clearly_without_rest(self) -> None:
        with mock.patch.object(dags, "_rest_client", return_value=None):
            for label, call in (
                ("list_zeta4s_dags", lambda: dags.list_zeta4s_dags(project="p")),
                ("dag_paused_states", lambda: dags.dag_paused_states(["d"])),
                ("_active_dag_run_rows", lambda: dags._active_dag_run_rows(["d"])),
                ("_active_task_instance_rows", lambda: dags._active_task_instance_rows(["d"])),
                ("_delete_dag", lambda: dags._delete_dag("d")),
                ("_terminate_active_dag_runs", lambda: dags._terminate_active_dag_runs(["d"])),
                ("_terminate_active_task_instances", lambda: dags._terminate_active_task_instances(["d"])),
            ):
                with self.subTest(label):
                    with self.assertRaises(RuntimeError) as ctx:
                        call()
                    self.assertIn("ZETA4S_AIRFLOW_REST_API_BASE_URL", str(ctx.exception))

    def test_empty_input_short_circuits_before_rest(self) -> None:
        """빈 목록은 REST 없이도 빈 결과다. 물어볼 대상이 없으면 붙을 이유도 없다."""
        with mock.patch.object(dags, "_rest_client", return_value=None):
            self.assertEqual(dags.dag_paused_states([]), {})
            self.assertEqual(dags._active_dag_run_rows([]), [])
            self.assertEqual(dags._active_task_instance_rows([]), [])


if __name__ == "__main__":
    unittest.main()
