from __future__ import annotations

import ast
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from uuid import UUID

from prefect.exceptions import ObjectNotFound

from zeta4s.prefect import ScheduleIdentity, ScheduleState
from zeta4s.prefect.prefect_engine import (
    _current_task_attempts,
    build_step_tasks,
    delete_prefect_job,
    deploy_prefect_job,
    prefect_task_policy,
)
from zeta4s.project.execution_plan import build_step_graph_execution_plan
from zeta4s.project.pools import project_pool_name
from zeta4s.project.step_graph import StepGraphJob


class _MissingPrefectClient:
    async def __aenter__(self) -> "_MissingPrefectClient":
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        return None

    async def read_deployment_by_name(self, name: str):
        raise ObjectNotFound(Exception(f"missing deployment: {name}"))


class _ExistingPrefectClient:
    def __init__(self) -> None:
        self.deployment = SimpleNamespace(id=UUID("00000000-0000-0000-0000-000000000003"))
        self.deleted = []

    async def __aenter__(self) -> "_ExistingPrefectClient":
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        return None

    async def read_deployment_by_name(self, name: str):
        return self.deployment

    async def delete_deployment(self, deployment_id) -> None:
        self.deleted.append(deployment_id)


def _plan():
    return build_step_graph_execution_plan(
        StepGraphJob.model_validate(
            {
                "job_id": "scheduled_job",
                "schedule": {"cron": "0 2 * * *", "timezone": "Asia/Seoul"},
                "steps": [
                    {"step_id": "start", "type": "noop"},
                    {
                        "step_id": "work",
                        "type": "noop",
                        "depends_on": ["start"],
                        "retry": {"max_attempts": 3, "delay_seconds": 4},
                        "timeout": {"seconds": 9},
                    },
                ],
            }
        )
    )


def _manual_plan():
    return build_step_graph_execution_plan(
        StepGraphJob.model_validate(
            {
                "job_id": "manual_job",
                "steps": [{"step_id": "start", "type": "noop"}],
            }
        )
    )


def _auto_pool_plan(*, pool: str | None = None):
    step = {
        "step_id": "select_one",
        "type": "clickhouse.sql",
        "conn": "analytics",
        "sql": "select 1",
    }
    if pool is not None:
        step["pool"] = pool
    return build_step_graph_execution_plan(
        StepGraphJob.model_validate(
            {
                "job_id": "auto_pool_job",
                "steps": [step],
            }
        )
    )


class PrefectDeploymentTest(unittest.TestCase):
    def test_prefect_task_uses_automatic_project_pool(self) -> None:
        occupied = []
        calls = []

        @contextmanager
        def record_concurrency(name: str, occupy: int, *, strict: bool):
            occupied.append((name, occupy, strict))
            yield

        try:
            tasks = build_step_tasks(
                _auto_pool_plan(),
                lambda step_id, run_id: calls.append((step_id, run_id)),
                project_id="retail",
            )
        except TypeError as exc:
            self.fail(f"build_step_tasks must accept project_id for automatic pool binding: {exc}")

        with patch("zeta4s.prefect.prefect_engine.concurrency", record_concurrency):
            tasks["select_one"].fn("run-1")

        self.assertEqual(occupied, [(project_pool_name("retail", "transform"), 1, True)])
        self.assertEqual(calls, [("select_one", "run-1")])

    def test_prefect_task_prefers_explicit_pool_override(self) -> None:
        occupied = []

        @contextmanager
        def record_concurrency(name: str, occupy: int, *, strict: bool):
            occupied.append((name, occupy, strict))
            yield

        tasks = build_step_tasks(
            _auto_pool_plan(pool="custom_transform_pool"),
            lambda *_: None,
            project_id="retail",
        )

        with patch("zeta4s.prefect.prefect_engine.concurrency", record_concurrency):
            tasks["select_one"].fn("run-1")

        self.assertEqual(occupied, [("custom_transform_pool", 1, True)])

    def test_prefect_projects_run_count_as_canonical_and_adapter_attempt(self) -> None:
        with patch(
            "zeta4s.prefect.prefect_engine.get_run_context",
            return_value=SimpleNamespace(task_run=SimpleNamespace(run_count=3)),
        ):
            attempts = _current_task_attempts()

        self.assertEqual(attempts, (3, 1))

    def test_delete_prefect_job_returns_false_when_deployment_is_missing(self) -> None:
        with patch(
            "zeta4s.prefect.prefect_engine.get_client",
            return_value=_MissingPrefectClient(),
        ):
            deleted = delete_prefect_job(ScheduleIdentity("retail", "daily", "prod"))

        self.assertFalse(deleted)

    def test_delete_prefect_job_deletes_matching_deployment(self) -> None:
        client = _ExistingPrefectClient()
        with patch("zeta4s.prefect.prefect_engine.get_client", return_value=client):
            deleted = delete_prefect_job(ScheduleIdentity("retail", "daily", "prod"))

        self.assertTrue(deleted)
        self.assertEqual(client.deleted, [client.deployment.id])

    def test_deploy_prefect_job_creates_scheduled_deployment(self) -> None:
        identity = ScheduleIdentity("retail", "scheduled_job", "prod")
        deployment = SimpleNamespace(apply=lambda: UUID("00000000-0000-0000-0000-000000000002"))

        with patch(
            "zeta4s.prefect.prefect_engine.scheduled_job_flow.to_deployment",
            return_value=deployment,
        ) as to_deployment:
            state = deploy_prefect_job(
                identity=identity,
                plan=_plan(),
                artifact_id="sha256:test-artifact",
            )

        kwargs = to_deployment.call_args.kwargs
        self.assertEqual(kwargs["name"], identity.key)
        self.assertEqual(kwargs["schedule"].cron, "0 2 * * *")
        self.assertEqual(kwargs["parameters"]["project_id"], "retail")
        self.assertEqual(kwargs["parameters"]["artifact_id"], "sha256:test-artifact")
        self.assertEqual(
            [step["id"] for step in kwargs["parameters"]["projection"]["steps"]],
            ["start", "work"],
        )
        self.assertEqual(state, ScheduleState(identity, str(deployment.apply()), False))

    def test_deploy_prefect_job_creates_manual_deployment_without_schedule(self) -> None:
        identity = ScheduleIdentity("retail", "manual_job", "prod")
        deployment = SimpleNamespace(apply=lambda: "deployment-manual")

        with patch(
            "zeta4s.prefect.prefect_engine.scheduled_job_flow.to_deployment",
            return_value=deployment,
        ) as to_deployment:
            state = deploy_prefect_job(
                identity=identity,
                plan=_manual_plan(),
                artifact_id="sha256:test-artifact",
            )

        self.assertIsNone(to_deployment.call_args.kwargs["schedule"])
        self.assertFalse(to_deployment.call_args.kwargs["paused"])
        self.assertEqual(state, ScheduleState(identity, "deployment-manual", False))

    def test_task_policy_maps_canonical_retry_and_timeout(self) -> None:
        policy = prefect_task_policy(_plan().step_by_id["work"])

        self.assertEqual(
            policy,
            {"retries": 2, "retry_delay_seconds": 4, "timeout_seconds": 9},
        )

    def test_projection_builds_one_native_task_per_execution_step(self) -> None:
        plan = _plan()
        calls = []
        tasks = build_step_tasks(
            plan,
            lambda step_id, run_id: calls.append((step_id, run_id)),
            project_id="retail",
        )

        self.assertEqual(tuple(tasks), ("start", "work"))
        self.assertEqual(tasks["work"].retries, 2)
        self.assertEqual(tasks["work"].retry_delay_seconds, 4)
        self.assertEqual(tasks["work"].timeout_seconds, 9)
        self.assertEqual(calls, [])

    def test_prefect_import_is_confined_to_engine_module(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        offenders = []
        for path in (repo_root / "src" / "zeta4s").rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            imports_prefect = any(
                (isinstance(node, ast.Import) and any(alias.name.split(".", 1)[0] == "prefect" for alias in node.names))
                or (isinstance(node, ast.ImportFrom) and (node.module or "").split(".", 1)[0] == "prefect")
                for node in ast.walk(tree)
            )
            if imports_prefect and path.name != "prefect_engine.py":
                offenders.append(str(path.relative_to(repo_root)))

        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
