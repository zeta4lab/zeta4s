from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from zeta4s.api.app import (
    DEPLOY_PROGRESS_STEPS,
    UNDEPLOY_PROGRESS_STEPS,
    DeployRequest,
    _deploy_prefect_jobs,
    _registered_runtime_jobs,
    _runtime_connection_policy_by_conn_id,
    create_app,
)
from zeta4s.prefect import ScheduleIdentity, ScheduleState


def _route_endpoint(app, path: str, method: str):
    for route in app.routes:
        if route.path == path and method in route.methods:
            return route.endpoint
    raise AssertionError(f"route not found: {method} {path}")


class _ReadyAdapter:
    def inspect_schema(self):
        return {"status": "ok"}


def _write_project(root: Path) -> None:
    (root / "jobs").mkdir(parents=True)
    (root / "project.yml").write_text(
        "project_id: retail\ntimezone: Asia/Seoul\npaths:\n  jobs: jobs\n  dbt: dbt\n",
        encoding="utf-8",
    )
    (root / "jobs" / "manual.yml").write_text(
        "job_id: manual\nschedule: null\nsteps:\n  - step_id: start\n    type: noop\n",
        encoding="utf-8",
    )
    (root / "jobs" / "scheduled.yml").write_text(
        "job_id: scheduled\nschedule:\n  cron: '0 1 * * *'\n  timezone: Asia/Seoul\nsteps:\n  - step_id: start\n    type: noop\n",
        encoding="utf-8",
    )


class SchedulerBackendRoutingTest(unittest.TestCase):
    def test_prefect_deploy_rolls_back_every_attempted_job_after_partial_failure(self) -> None:
        deleted: list[ScheduleIdentity] = []

        def deploy_prefect_job(**kwargs):
            identity = kwargs["identity"]
            if identity.job_id == "scheduled":
                raise RuntimeError("prefect unavailable")
            return ScheduleState(identity, "deployment-manual", False)

        with TemporaryDirectory() as tmp:
            project_root = Path(tmp) / "retail"
            _write_project(project_root)
            with (
                patch(
                    "zeta4s.prefect.prefect_engine.deploy_prefect_job",
                    side_effect=deploy_prefect_job,
                ),
                patch(
                    "zeta4s.prefect.prefect_engine.delete_prefect_job",
                    side_effect=lambda identity: deleted.append(identity) or True,
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "prefect unavailable"):
                    _deploy_prefect_jobs(
                        plan={"dags": [{"job_id": "manual"}, {"job_id": "scheduled"}]},
                        artifact_id="sha256:prefect",
                        project_root=project_root,
                        profile_id="prod",
                    )

        self.assertEqual(
            deleted,
            [
                ScheduleIdentity("retail", "scheduled", "prod"),
                ScheduleIdentity("retail", "manual", "prod"),
            ],
        )

    def test_default_prefect_deploys_every_job_before_registration(self) -> None:
        events: list[str] = []
        deployed = []

        def deploy_prefect_job(**kwargs):
            deployed.append(kwargs)
            events.append(f"deploy:{kwargs['identity'].job_id}")
            return ScheduleState(kwargs["identity"], f"deployment-{len(deployed)}", False)

        def register(**kwargs):
            events.append("register")
            return Path("/registration.yml")

        with TemporaryDirectory() as tmp:
            project_root = Path(tmp) / "retail"
            _write_project(project_root)
            request = DeployRequest(
                project_id="retail",
                bundle_base64="not-used",
                profile_id="prod",
                profile={"connections": {}},
            )
            with (
                patch("zeta4s.api.app.metastore_adapter_factory", return_value=_ReadyAdapter()),
                patch("zeta4s.api.app.decode_bundle", return_value=(b"bundle", "sha256:prefect")),
                patch("zeta4s.api.app.extract_bundle"),
                patch("zeta4s.api.app._project_root_for_artifact", return_value=project_root),
                patch("zeta4s.api.app.ensure_artifact_runtime_permissions"),
                patch("zeta4s.api.app.project_operation_lock", return_value=nullcontext()),
                patch("zeta4s.api.app._emit_operation_report", side_effect=lambda report: report),
                patch("zeta4s.api.app.record_artifact_metadata"),
                patch(
                    "zeta4s.api.app._sync_backend_registry",
                    return_value={"active_backend_count": 0, "removed_backend_count": 0},
                ),
                patch("zeta4s.api.app.upsert_project_registration", side_effect=register) as upsert,
                patch(
                    "zeta4s.prefect.prefect_engine.deploy_prefect_job",
                    side_effect=deploy_prefect_job,
                ),
                patch(
                    "zeta4s.api.app._airflow_pause_and_terminate",
                    side_effect=AssertionError("Prefect deploy must not reset Airflow"),
                ),
                patch(
                    "zeta4s.api.app._airflow_register_prepare",
                    side_effect=AssertionError("Prefect deploy must not project Airflow resources"),
                ),
                patch(
                    "zeta4s.api.app._airflow_discover_and_unpause",
                    side_effect=AssertionError("Prefect deploy must not discover Airflow DAGs"),
                ),
            ):
                endpoint = _route_endpoint(create_app(), "/api/v1/deploy", "POST")
                response = endpoint(request, authorization=None)

        self.assertEqual(response["status"], "passed")
        self.assertEqual([step["name"] for step in response["steps"]], DEPLOY_PROGRESS_STEPS)
        self.assertEqual({item["identity"].job_id for item in deployed}, {"manual", "scheduled"})
        self.assertIsNone(next(item["plan"].schedule for item in deployed if item["identity"].job_id == "manual"))
        self.assertEqual(events[-1], "register")
        upsert.assert_called_once()
        self.assertEqual(upsert.call_args.kwargs["scheduler_backend"], "prefect")
        scheduler_step = next(step for step in response["steps"] if step["name"] == "scheduler_deploy")
        self.assertEqual(
            scheduler_step["summary"],
            {"scheduler_backend": "prefect", "deployment_count": 2},
        )
        for name in (
            "dag_pause",
            "active_run_terminate",
            "airflow_register_prepare",
            "connections_apply",
            "project_pools_apply",
            "dag_discovery",
            "dag_unpause",
        ):
            self.assertEqual(next(step for step in response["steps"] if step["name"] == name)["status"], "skipped")

    def test_prefect_undeploy_uses_stored_backend_profile_and_jobs(self) -> None:
        events: list[str] = []
        registration = {
            "project_id": "retail",
            "artifact_id": "sha256:prefect",
            "profile_id": "stored-profile",
            "scheduler_backend": "prefect",
            "dags": [
                {"job_id": "manual", "dag_id": "retail__manual"},
                {"job_id": "scheduled", "dag_id": "retail__scheduled"},
            ],
        }

        def remove(project_id):
            events.append("remove")
            return Path("/registration.yml"), registration

        def delete_prefect_job(identity):
            events.append(f"delete:{identity.key}")
            return True

        with (
            patch("zeta4s.api.app.metastore_adapter_factory", return_value=_ReadyAdapter()),
            patch("zeta4s.api.app._active_project_registration", return_value=registration),
            patch(
                "zeta4s.prefect.prefect_engine.delete_prefect_job",
                side_effect=delete_prefect_job,
            ) as delete_job,
            patch("zeta4s.api.app.remove_project_registration", side_effect=remove),
            patch("zeta4s.api.app.project_operation_lock", return_value=nullcontext()),
            patch("zeta4s.api.app._emit_operation_report", side_effect=lambda report: report),
            patch(
                "zeta4s.api.app._airflow_pause_and_terminate",
                side_effect=AssertionError("Prefect undeploy must not call Airflow cleanup"),
            ),
        ):
            endpoint = _route_endpoint(create_app(), "/api/v1/undeploy", "POST")
            response = endpoint({"project_id": "retail", "profile_id": "ignored"}, authorization=None)

        self.assertEqual(
            [call.args[0] for call in delete_job.call_args_list],
            [
                ScheduleIdentity("retail", "manual", "stored-profile"),
                ScheduleIdentity("retail", "scheduled", "stored-profile"),
            ],
        )
        self.assertEqual(events[-1], "remove")
        self.assertEqual(response["status"], "passed")
        self.assertEqual([step["name"] for step in response["steps"]], UNDEPLOY_PROGRESS_STEPS)
        self.assertEqual(response["steps"][1]["status"], "skipped")
        self.assertEqual(response["steps"][2]["status"], "skipped")

    def test_undeploy_rejects_unknown_stored_backend_without_cleanup(self) -> None:
        registration = {
            "project_id": "retail",
            "artifact_id": "sha256:corrupt",
            "profile_id": "stored-profile",
            "scheduler_backend": "unknown",
            "dags": [{"job_id": "scheduled", "dag_id": "retail__scheduled"}],
        }

        with (
            patch("zeta4s.api.app.metastore_adapter_factory", return_value=_ReadyAdapter()),
            patch("zeta4s.api.app._active_project_registration", return_value=registration),
            patch("zeta4s.api.app.project_operation_lock", return_value=nullcontext()),
            patch("zeta4s.api.app._emit_operation_report", side_effect=lambda report: report),
            patch(
                "zeta4s.api.app._airflow_pause_and_terminate",
                side_effect=AssertionError("unknown backend must not call Airflow cleanup"),
            ),
            patch(
                "zeta4s.api.app.remove_project_registration",
                side_effect=AssertionError("unknown backend must keep registration"),
            ),
        ):
            endpoint = _route_endpoint(create_app(), "/api/v1/undeploy", "POST")
            response = endpoint({"project_id": "retail"}, authorization=None)

        self.assertEqual(response["status"], "failed")
        self.assertEqual(response["issues"][0]["code"], "Z4E_SCHEDULER_BACKEND_001")
        self.assertEqual(response["issues"][0]["details"]["scheduler_backend"], "unknown")
        self.assertEqual(response["steps"][0]["name"], "project_registration")
        self.assertEqual(response["steps"][0]["status"], "failed")
        self.assertTrue(all(step["status"] == "skipped" for step in response["steps"][1:]))

    def test_airflow_undeploy_keeps_registration_until_scheduler_cleanup_succeeds(self) -> None:
        registration = {
            "project_id": "retail",
            "artifact_id": "sha256:airflow",
            "profile_id": "stored-profile",
            "scheduler_backend": "airflow",
            "dags": [{"job_id": "daily", "dag_id": "retail__daily"}],
        }
        airflow_result = {
            "dag_ids": ["retail__daily"],
            "dag_pause": {"status": "passed"},
            "active_run_terminate": {
                "status": "passed",
                "remaining_runs": [],
                "remaining_task_instances": [],
                "remaining_run_count": 0,
                "remaining_task_instance_count": 0,
                "terminated_run_count": 0,
                "terminated_task_instance_count": 0,
            },
        }
        delete_result = {
            "status": "failed",
            "remaining_count": 1,
            "remaining_dags": ["retail__daily"],
        }

        with (
            patch("zeta4s.api.app.metastore_adapter_factory", return_value=_ReadyAdapter()),
            patch("zeta4s.api.app._active_project_registration", return_value=registration),
            patch("zeta4s.api.app._airflow_pause_and_terminate", return_value=airflow_result),
            patch(
                "zeta4s.airflow.dags.converge_project_dags_deleted",
                return_value=delete_result,
            ),
            patch("zeta4s.api.app.remove_project_registration") as remove,
            patch("zeta4s.api.app.project_operation_lock", return_value=nullcontext()),
            patch("zeta4s.api.app._emit_operation_report", side_effect=lambda report: report),
        ):
            endpoint = _route_endpoint(create_app(), "/api/v1/undeploy", "POST")
            response = endpoint({"project_id": "retail"}, authorization=None)

        remove.assert_not_called()
        self.assertEqual(response["status"], "failed")
        self.assertEqual(response["issues"][0]["code"], "Z4E_AIRFLOW_DAG_DELETE_TIMEOUT")
        self.assertEqual(response["steps"][3]["name"], "scheduler_cleanup")
        self.assertEqual(response["steps"][3]["status"], "failed")
        self.assertEqual(response["steps"][4]["name"], "registration_remove")
        self.assertEqual(response["steps"][4]["status"], "skipped")

    def test_airflow_only_registration_consumers_ignore_prefect_projects(self) -> None:
        registrations = {
            "registrations": [
                {
                    "project_id": "prefect-project",
                    "artifact_id": "sha256:prefect",
                    "scheduler_backend": "prefect",
                    "dags": [{"dag_id": "prefect-project__daily", "job_id": "daily"}],
                },
                {
                    "project_id": "airflow-project",
                    "artifact_id": "sha256:airflow",
                    "scheduler_backend": "airflow",
                    "dags": [{"dag_id": "airflow-project__daily", "job_id": "daily"}],
                },
            ]
        }

        def artifact_metadata(artifact_id):
            if artifact_id != "sha256:airflow":
                raise AssertionError("Prefect artifact must not feed Airflow connection projection")
            return {"runtime_connections": [{"conn_id": "analytics", "type": "clickhouse"}]}

        with (
            patch("zeta4s.api.app.load_registrations", return_value=registrations),
            patch("zeta4s.api.services.registration_store.load_registrations", return_value=registrations),
            patch("zeta4s.api.app._load_artifact_metadata", side_effect=artifact_metadata),
        ):
            jobs = _registered_runtime_jobs()
            policy = _runtime_connection_policy_by_conn_id("analytics")

        self.assertEqual([job["project"] for job in jobs], ["prefect-project", "airflow-project"])
        self.assertEqual(policy["conn_id"], "analytics")


if __name__ == "__main__":
    unittest.main()
