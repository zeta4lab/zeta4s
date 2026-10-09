"""`runs.py` 의 REST 조회 계약.

없는 run 은 None 이고 오류가 아니다. 그리고 Airflow 가 느린 것과 붙을 수 없는 것은 다른
상황이므로 timeout 을 다른 전송 실패와 구분한다.
"""

from __future__ import annotations

import json
import unittest
import urllib.error
from unittest import mock

from zeta4s.airflow import runs
from zeta4s.airflow.rest_client import AirflowRestClient, AirflowRestError, AirflowRestTimeout

# 실측한 REST dag run row 다.
DAG_RUN = {
    "dag_id": "p__j",
    "dag_run_id": "r1",
    "state": "failed",
    "conf": {"z4_run_id": "r1"},
    "run_type": "manual",
    "start_date": "2026-07-15T15:40:58.887331Z",
    "end_date": "2026-07-15T15:41:16.712540Z",
    "queued_at": "2026-07-15T15:40:58.740582Z",
    "logical_date": None,
    "run_after": "2026-07-15T15:40:58.734982Z",
    "data_interval_start": None,
    "data_interval_end": None,
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


def _client(routes: dict[str, object]) -> AirflowRestClient:
    def transport(request, timeout=None):  # noqa: ANN001
        for prefix, outcome in routes.items():
            if prefix in request.full_url:
                if isinstance(outcome, Exception):
                    raise outcome
                if isinstance(outcome, int):
                    raise urllib.error.HTTPError(request.full_url, outcome, "err", {}, None)
                return _Response(json.dumps(outcome).encode("utf-8"))
        raise urllib.error.HTTPError(request.full_url, 404, "not found", {}, None)

    return AirflowRestClient(base_url="http://airflow:8080", token="t", transport=transport)


def _with(routes: dict[str, object]):
    return mock.patch.object(runs, "require_rest_client", return_value=_client(routes))


class MissingRunIsNoneTest(unittest.TestCase):
    def test_404_becomes_none_not_error(self) -> None:
        with _with({"/dagRuns/": 404}):
            self.assertIsNone(runs.dag_run("p__j", "nope"))
            self.assertIsNone(runs.dag_run_state("p__j", "nope"))
            self.assertIsNone(runs.task_instances("p__j", "nope"))

    def test_missing_dag_lists_no_runs(self) -> None:
        with _with({"/dagRuns": 404}):
            self.assertEqual(runs.dag_runs("gone", 30), [])

    def test_non_404_still_raises(self) -> None:
        with _with({"/dagRuns/": 500}):
            with self.assertRaises(AirflowRestError):
                runs.dag_run("p__j", "r1")


class TimeoutIsDistinctTest(unittest.TestCase):
    def test_timeout_is_not_a_plain_transport_failure(self) -> None:
        with _with({"/dagRuns/": TimeoutError("timed out")}):
            with self.assertRaises(AirflowRestTimeout):
                runs.dag_run("p__j", "r1")

    def test_timeout_wrapped_in_urlerror_is_still_a_timeout(self) -> None:
        """urllib 은 timeout 을 URLError 로 감싸기도 한다. 놓치면 조용히 강등된다."""
        with _with({"/dagRuns/": urllib.error.URLError(TimeoutError("timed out"))}):
            with self.assertRaises(AirflowRestTimeout):
                runs.dag_run("p__j", "r1")

    def test_connection_failure_is_not_a_timeout(self) -> None:
        with _with({"/dagRuns/": urllib.error.URLError(ConnectionRefusedError("refused"))}):
            with self.assertRaises(AirflowRestError) as ctx:
                runs.dag_run("p__j", "r1")
            self.assertNotIsInstance(ctx.exception, AirflowRestTimeout)


class DagRunShapeTest(unittest.TestCase):
    def test_rest_dag_run_id_maps_to_run_id(self) -> None:
        with _with({"/dagRuns/r1": DAG_RUN}):
            row = runs.dag_run("p__j", "r1")

        self.assertEqual(row["run_id"], "r1")
        self.assertEqual(row["dag_id"], "p__j")
        self.assertEqual(row["state"], "failed")
        self.assertEqual(row["conf"], {"z4_run_id": "r1"})
        self.assertEqual(row["run_type"], "manual")
        self.assertEqual(row["queued_at"], "2026-07-15T15:40:58.740582Z")

    def test_dag_run_state_keeps_only_execution_fields(self) -> None:
        with _with({"/dagRuns/r1": DAG_RUN}):
            state = runs.dag_run_state("p__j", "r1")

        self.assertEqual(
            state,
            {
                "airflow_state": "failed",
                "airflow_start_date": "2026-07-15T15:40:58.887331Z",
                "airflow_end_date": "2026-07-15T15:41:16.712540Z",
            },
        )

    def test_dag_runs_are_newest_first_and_limited(self) -> None:
        rows = {
            "dag_runs": [
                {**DAG_RUN, "dag_run_id": "old", "queued_at": "2026-07-14T00:00:00Z"},
                {**DAG_RUN, "dag_run_id": "new", "queued_at": "2026-07-16T00:00:00Z"},
                {**DAG_RUN, "dag_run_id": "mid", "queued_at": "2026-07-15T00:00:00Z"},
            ],
            "total_entries": 3,
        }
        with _with({"/dagRuns": rows}):
            self.assertEqual([row["run_id"] for row in runs.dag_runs("p__j", 30)], ["new", "mid", "old"])
            self.assertEqual([row["run_id"] for row in runs.dag_runs("p__j", 2)], ["new", "mid"])


class BatchTest(unittest.TestCase):
    """metrics 는 dag 마다 왕복하지 않고 `.../list` 배치로 한 번에 받는다."""

    def test_latest_run_is_picked_per_dag_by_recency(self) -> None:
        rows = {
            "dag_runs": [
                {**DAG_RUN, "dag_id": "a", "dag_run_id": "a-old", "queued_at": "2026-07-14T00:00:00Z"},
                {**DAG_RUN, "dag_id": "a", "dag_run_id": "a-new", "queued_at": "2026-07-16T00:00:00Z"},
                {**DAG_RUN, "dag_id": "b", "dag_run_id": "b-only", "queued_at": "2026-07-15T00:00:00Z"},
            ],
            "total_entries": 3,
        }
        with _with({"/dagRuns/list": rows}):
            latest = runs.latest_run_by_dag(["a", "b", "c"])

        self.assertEqual(sorted(latest), ["a", "b"])
        self.assertEqual(latest["a"]["run_id"], "a-new")
        self.assertEqual(latest["b"]["run_id"], "b-only")

    def test_batch_pages_with_page_limit_not_limit(self) -> None:
        """배치 endpoint 는 POST body 의 `page_limit`/`page_offset` 으로 페이징한다.

        query 의 `limit`/`offset` 을 쓰면 조용히 첫 페이지만 돌아온다.
        """
        seen: list[dict] = []

        def transport(request, timeout=None):  # noqa: ANN001
            body = json.loads(request.data.decode())
            seen.append(body)
            offset = body["page_offset"]
            page = [{**DAG_RUN, "dag_id": "a", "dag_run_id": f"r{offset}"}] if offset < 2 else []
            return _Response(json.dumps({"dag_runs": page, "total_entries": 2}).encode("utf-8"))

        client = AirflowRestClient(base_url="http://airflow:8080", token="t", transport=transport)
        with mock.patch.object(runs, "require_rest_client", return_value=client):
            runs.latest_run_by_dag(["a"])

        self.assertIn("page_limit", seen[0])
        self.assertIn("page_offset", seen[0])
        self.assertNotIn("limit", seen[0])
        self.assertEqual(seen[0]["dag_ids"], ["a"])

    def test_batch_task_instances_filter_the_cross_product(self) -> None:
        """`dag_ids` 와 `dag_run_ids` 는 각각 독립으로 걸려 곱집합이 온다.

        metastore 의 `IN` × `IN` query 와 같은 성질이라 짝으로 다시 걸러야 한다.
        """
        rows = {
            "task_instances": [
                {"dag_id": "a", "dag_run_id": "r-a", "task_id": "t1", "map_index": -1, "state": "success"},
                {"dag_id": "b", "dag_run_id": "r-b", "task_id": "t2", "map_index": -1, "state": "success"},
                # 요청하지 않은 짝이다. 곱집합으로 섞여 온다.
                {"dag_id": "a", "dag_run_id": "r-b", "task_id": "t3", "map_index": -1, "state": "success"},
            ],
            "total_entries": 3,
        }
        with _with({"/taskInstances/list": rows}):
            grouped = runs.task_instances_by_run([("a", "r-a"), ("b", "r-b")])

        self.assertEqual(sorted(grouped), [("a", "r-a"), ("b", "r-b")])
        self.assertEqual([t["task_id"] for t in grouped[("a", "r-a")]], ["t1"])
        self.assertEqual([t["task_id"] for t in grouped[("b", "r-b")]], ["t2"])

    def test_empty_input_skips_the_call(self) -> None:
        with mock.patch.object(runs, "require_rest_client", side_effect=AssertionError("불러선 안 된다")):
            self.assertEqual(runs.latest_run_by_dag([]), {})
            self.assertEqual(runs.task_instances_by_run([]), {})


class WritePathTest(unittest.TestCase):
    def test_trigger_sends_logical_date_even_when_empty(self) -> None:
        """`TriggerDAGRunPostBody` 는 `logical_date` 를 required 로 두고 값만 nullable 이다.

        key 를 빼면 422 다. zeta4s 가 만드는 run 은 schedule 이 아니라 요청 시점의 것이므로
        null 을 준다.
        """
        sent: list[dict] = []

        def transport(request, timeout=None):  # noqa: ANN001
            sent.append(json.loads(request.data.decode()))
            return _Response(json.dumps({**DAG_RUN, "dag_run_id": "r1"}).encode("utf-8"))

        client = AirflowRestClient(base_url="http://airflow:8080", token="t", transport=transport)
        with mock.patch.object(runs, "require_rest_client", return_value=client):
            row = runs.trigger_dag_run("p__j", "r1", {"z4_run_id": "r1"})

        self.assertIn("logical_date", sent[0])
        self.assertIsNone(sent[0]["logical_date"])
        self.assertEqual(sent[0]["dag_run_id"], "r1")
        self.assertEqual(sent[0]["conf"], {"z4_run_id": "r1"})
        self.assertEqual(row["run_id"], "r1")

    def test_set_state_patches_with_update_mask(self) -> None:
        seen: list[tuple[str, dict]] = []

        def transport(request, timeout=None):  # noqa: ANN001
            seen.append((request.full_url, json.loads(request.data.decode())))
            return _Response(json.dumps(DAG_RUN).encode("utf-8"))

        client = AirflowRestClient(base_url="http://airflow:8080", token="t", transport=transport)
        with mock.patch.object(runs, "require_rest_client", return_value=client):
            self.assertTrue(runs.set_dag_run_state("p__j", "r1", "failed"))

        url, body = seen[0]
        self.assertEqual(body, {"state": "failed"})
        # mask 를 주지 않으면 body 에 없는 note 까지 덮어쓴다.
        self.assertIn("update_mask=state", url)

    def test_set_state_on_missing_run_is_false_not_error(self) -> None:
        with _with({"/dagRuns/": 404}):
            self.assertFalse(runs.set_dag_run_state("p__j", "gone", "failed"))


class TaskLogTest(unittest.TestCase):
    """Airflow 3 의 task log 는 JSON line 이다. 파일도 REST 도 같은 레코드다."""

    def test_records_become_json_lines(self) -> None:
        payload = {
            "content": [
                {"event": "::group::Log message source details"},
                {"timestamp": "2026-07-15T15:41:03.032671Z", "level": "info", "event": "Pre Execute"},
            ],
            "continuation_token": None,
        }
        with _with({"/logs/1": payload}):
            lines = runs.task_log_lines("p__j", "r1", "t", 1)

        self.assertEqual(len(lines), 2)
        self.assertEqual(json.loads(lines[0]), {"event": "::group::Log message source details"})
        self.assertEqual(json.loads(lines[1])["event"], "Pre Execute")

    def test_missing_log_is_none_not_error(self) -> None:
        with _with({"/logs/": 404}):
            self.assertIsNone(runs.task_log_lines("p__j", "r1", "t", 9))

    def test_full_content_and_map_index_are_sent(self) -> None:
        seen: list[str] = []

        def transport(request, timeout=None):  # noqa: ANN001
            seen.append(request.full_url)
            return _Response(json.dumps({"content": []}).encode("utf-8"))

        client = AirflowRestClient(base_url="http://airflow:8080", token="t", transport=transport)
        with mock.patch.object(runs, "require_rest_client", return_value=client):
            runs.task_log_lines("p__j", "r1", "t", 2, map_index=3)

        self.assertIn("/taskInstances/t/logs/2", seen[0])
        # rest_client 가 bool 을 JSON 표기로 직렬화한다. python 의 True 가 아니다.
        self.assertIn("full_content=true", seen[0])
        self.assertIn("map_index=3", seen[0])


class TaskInstanceShapeTest(unittest.TestCase):
    def test_tasks_are_ordered_by_task_id_then_map_index(self) -> None:
        rows = {
            "task_instances": [
                {"task_id": "b", "map_index": -1, "state": "success", "try_number": 1, "duration": 1.0},
                {"task_id": "a", "map_index": 1, "state": "success", "try_number": 1, "duration": 2.0},
                {"task_id": "a", "map_index": 0, "state": "failed", "try_number": 2, "duration": 3.0},
            ],
            "total_entries": 3,
        }
        with _with({"/taskInstances": rows}):
            tasks = runs.task_instances("p__j", "r1")

        self.assertEqual([(t["task_id"], t["map_index"]) for t in tasks], [("a", 0), ("a", 1), ("b", -1)])
        self.assertEqual(tasks[0]["state"], "failed")
        self.assertEqual(tasks[0]["try_number"], 2)
        self.assertEqual(tasks[0]["duration"], 3.0)


if __name__ == "__main__":
    unittest.main()
