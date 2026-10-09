from __future__ import annotations

import unittest
from unittest.mock import patch


from zeta4s.api.app import RunCreateRequest, create_app
from zeta4s.api.services.scheduler_runs import RunSnapshot


def _route_endpoint(path: str, method: str):
    for route in create_app().routes:
        if route.path == path and method in (route.methods or set()):
            return route.endpoint
    raise AssertionError(f"route not found: {method} {path}")


class _FakeAdapter:
    scheduler = "prefect"

    def __init__(self) -> None:
        self.created = None
        self.cancelled = None

    def create_run(self, **kwargs):
        self.created = kwargs
        return RunSnapshot(
            project_id=kwargs["project_id"],
            job_id=kwargs["job_id"],
            run_id=kwargs["run_id"],
            scheduler="prefect",
            scheduler_run_id="native-1",
            state="queued",
            created_at="2026-07-17T00:00:00+00:00",
            parameters=kwargs["parameters"],
        )

    def get_run(self, run):
        return RunSnapshot(
            project_id=run["project_id"],
            job_id=run["job_id"],
            run_id=run["run_id"],
            scheduler=run["scheduler"],
            scheduler_run_id=run["scheduler_run_id"],
            state=run["state"],
            created_at=run["created_at"],
            parameters=run["parameters"],
        )

    def cancel_run(self, run):
        self.cancelled = run
        return RunSnapshot(
            project_id=run["project_id"],
            job_id=run["job_id"],
            run_id=run["run_id"],
            scheduler=run["scheduler"],
            scheduler_run_id=run["scheduler_run_id"],
            state="cancelled",
            created_at=run["created_at"],
            parameters=run["parameters"],
        )


class SchedulerRunApiTest(unittest.TestCase):
    def test_create_uses_active_scheduler_adapter_and_canonical_payload(self) -> None:
        endpoint = _route_endpoint("/api/v1/projects/{project_id}/jobs/{job_id}/runs", "POST")
        adapter = _FakeAdapter()
        stored = []
        registration = {
            "project_id": "retail",
            "profile_id": "prod",
            "scheduler_backend": "prefect",
            "artifact_id": "sha256:abc",
            "dags": [{"job_id": "daily"}],
        }
        with (
            patch("zeta4s.api.app._active_project_registration", return_value=registration),
            patch("zeta4s.api.app.adapter_for_registration", return_value=adapter),
            patch("zeta4s.api.app.new_run_id", return_value="canonical-run"),
            patch("zeta4s.api.app._artifact_id_for_project", return_value="sha256:abc"),
            patch("zeta4s.api.app.create_run", side_effect=stored.append),
        ):
            response = endpoint(
                "retail",
                "daily",
                RunCreateRequest(parameters={"region": "kr"}),
                authorization=None,
                timezone="UTC",
            )

        self.assertEqual(adapter.created["parameters"], {"region": "kr"})
        self.assertEqual(response["run_id"], "canonical-run")
        self.assertEqual(response["scheduler_run_id"], "native-1")
        self.assertEqual(response["state"], "queued")
        self.assertNotIn("dag_id", response)
        self.assertNotIn("airflow_run_id", response)
        self.assertEqual(stored[0]["parameters"], {"region": "kr"})

    def test_create_rolls_back_native_run_when_metadata_write_fails(self) -> None:
        endpoint = _route_endpoint("/api/v1/projects/{project_id}/jobs/{job_id}/runs", "POST")
        adapter = _FakeAdapter()
        registration = {
            "project_id": "retail",
            "profile_id": "prod",
            "scheduler_backend": "prefect",
            "artifact_id": "sha256:abc",
            "dags": [{"job_id": "daily"}],
        }
        with (
            patch("zeta4s.api.app._active_project_registration", return_value=registration),
            patch("zeta4s.api.app.adapter_for_registration", return_value=adapter),
            patch("zeta4s.api.app.new_run_id", return_value="canonical-run"),
            patch("zeta4s.api.app._artifact_id_for_project", return_value="sha256:abc"),
            patch("zeta4s.api.app.create_run", side_effect=RuntimeError("metadata unavailable")),
            self.assertRaisesRegex(RuntimeError, "metadata unavailable"),
        ):
            endpoint(
                "retail",
                "daily",
                RunCreateRequest(parameters={}),
                authorization=None,
                timezone="UTC",
            )

        self.assertEqual(adapter.cancelled["run_id"], "canonical-run")


if __name__ == "__main__":
    unittest.main()
