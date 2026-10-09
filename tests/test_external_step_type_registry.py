"""외부 step type descriptor 의 등록 e2e.

fixture descriptor 를 module-level 로 두고 runtime_callable 을 이 모듈의 dotted path 로
가리켜, 등록·schema·실행·계약위반 경로를 검증한다. 설치 discovery 는 `entry_points` 를
monkeypatch 해 가짜 entry point 로 시뮬레이션한다.
"""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from zeta4s.api.app import _project_check_report
from zeta4s.core import ExecutionContext, LocalRunner, RuntimeCallableStepExecutor, StepExecutionState
from zeta4s.core.step_executors import built_in_step_executor
from zeta4s.project import step_graph
from zeta4s.project.execution_plan import build_step_graph_execution_plan
from zeta4s.project.loader import ProjectContext
from zeta4s.project.step_graph import StepGraphJob, StepTypeDescriptor, validate_step_graph_config
from zeta4s.project.step_types import (
    STEP_TYPE_ENTRY_POINT_GROUP,
    StepTypeContractError,
    discover_installed_step_type_factories,
    register_installed_step_types,
    register_step_type_descriptors,
    registered_step_type_descriptor,
    reset_step_type_registry,
)


class _FakeEntryPoint:
    """importlib.metadata.EntryPoint 의 `.value` 만 시뮬레이션한다."""

    def __init__(self, value: str) -> None:
        self.value = value


def _patch_entry_points(refs: list[str]):
    """`zeta4s.step_types` group 에 대해 지정한 factory ref 를 반환하도록 entry_points 를 대체한다."""

    def fake_entry_points(*, group: str):
        assert group == STEP_TYPE_ENTRY_POINT_GROUP
        return [_FakeEntryPoint(ref) for ref in refs]

    return patch("zeta4s.project.step_types.entry_points", fake_entry_points)


def _write_echo_project(root: Path) -> None:
    (root / "jobs").mkdir()
    (root / "project.yml").write_text(
        "project_id: external_test\ntimezone: Asia/Seoul\npaths:\n  jobs: jobs\n  dbt: dbt\n",
        encoding="utf-8",
    )
    (root / "jobs" / "echo.yml").write_text(
        "job_id: echo_job\nsteps:\n  - step_id: say\n    type: demo.echo\n    params:\n      message: hi\n",
        encoding="utf-8",
    )


# --- fixture descriptor (외부 저자 모듈 시뮬레이션) ---


def run_echo(**kwargs):
    return {"status": "success", "details": {"outputs": {"echo": kwargs.get("message")}}}


def _validate_echo(step) -> None:
    if not (step.params or {}).get("message"):
        raise ValueError(f"step {step.id} type=demo.echo requires params.message")


def _echo_payload(project, plan, step, common):
    return {"message": step.params.get("message"), "project_id": project.project_id, **common}


def build_descriptors():
    return (
        StepTypeDescriptor(
            type="demo.echo",
            pool_stage="transform",
            schema_validator=_validate_echo,
            runtime_callable=f"{__name__}:run_echo",
            payload_builder=_echo_payload,
            connection_id_fields=(),
        ),
    )


class NestedFactory:
    """표준 entry-point 의 dotted attr 경로를 검증하는 fixture."""

    @staticmethod
    def build_descriptors():
        return build_descriptors()


def build_conflicting_descriptors():
    return (
        StepTypeDescriptor(
            type="demo.echo",
            pool_stage="write",
            schema_validator=_validate_echo,
            runtime_callable=f"{__name__}:run_echo",
            payload_builder=_echo_payload,
            connection_id_fields=(),
        ),
    )


def build_bad_runtime_descriptors():
    return (
        StepTypeDescriptor(
            type="demo.bad",
            pool_stage="transform",
            schema_validator=_validate_echo,
            runtime_callable=f"{__name__}:does_not_exist",
            payload_builder=_echo_payload,
            connection_id_fields=(),
        ),
    )


def build_non_iterable_descriptors():
    return None


def _echo_job_config(*, message: str | None = "hi") -> dict:
    params = {"message": message} if message is not None else {}
    return {
        "job_id": "echo_job",
        "schedule": None,
        "steps": [{"step_id": "say", "type": "demo.echo", "params": params}],
    }


def _project() -> ProjectContext:
    root = Path("/tmp/zeta4s-external-step-type-test")
    return ProjectContext(
        project_id="external_test",
        root=root,
        jobs_dir=root / "jobs",
        assets_dir=root / "assets",
        dbt_dir=root / "dbt",
        timezone="Asia/Seoul",
    )


class _Reporter:
    def run_started(self, context):
        return None

    def run_succeeded(self, context, result):
        return None

    def run_failed(self, context, result):
        return None

    def run_skipped(self, context, result):
        return None

    def step_started(self, context, step):
        return None

    def step_succeeded(self, context, result):
        return None

    def step_failed(self, context, result):
        return None


class ExternalStepTypeRegistryTest(unittest.TestCase):
    def tearDown(self) -> None:
        reset_step_type_registry()

    def test_unregistered_external_type_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            validate_step_graph_config(Path("echo.yml"), _echo_job_config())

    def test_registered_external_type_passes_schema(self) -> None:
        register_step_type_descriptors(build_descriptors(), source="test")
        job = validate_step_graph_config(Path("echo.yml"), _echo_job_config())
        self.assertEqual(job.steps[0].type, "demo.echo")
        self.assertIn("demo.echo", step_graph.STEP_TYPE_VALUES)

    def test_registered_external_type_derives_pool_stage_in_place(self) -> None:
        register_step_type_descriptors(build_descriptors(), source="test")
        # execution_plan/pools 가 이름으로 import 한 dict 도 in-place 갱신을 본다.
        self.assertEqual(step_graph.STEP_TYPE_POOL_STAGES["demo.echo"], "transform")

    def test_external_type_builds_runtime_callable_executor(self) -> None:
        register_step_type_descriptors(build_descriptors(), source="test")
        job = validate_step_graph_config(Path("echo.yml"), _echo_job_config())
        plan = build_step_graph_execution_plan(job)
        executor = built_in_step_executor(
            project=_project(),
            plan=plan,
            plan_step=plan.step_by_id["say"],
            runtime_home="/tmp/zeta4s",
        )
        self.assertIsInstance(executor, RuntimeCallableStepExecutor)
        self.assertEqual(executor.runtime_callable, f"{__name__}:run_echo")
        self.assertEqual(executor.kwargs["message"], "hi")

    def test_external_type_executes_via_local_runner(self) -> None:
        register_step_type_descriptors(build_descriptors(), source="test")
        job = validate_step_graph_config(Path("echo.yml"), _echo_job_config())
        plan = build_step_graph_execution_plan(job)
        plan_step = plan.step_by_id["say"]
        executor = built_in_step_executor(
            project=_project(), plan=plan, plan_step=plan_step, runtime_home="/tmp/zeta4s"
        )
        context = ExecutionContext(
            project_id="external_test", job_id="echo_job", run_id="echo_job__test", reporter=_Reporter()
        )
        result = LocalRunner({plan_step.id: executor}).run_step(plan, plan_step, context)
        self.assertEqual(result.state, StepExecutionState.SUCCEEDED)
        self.assertEqual(result.raw_result["details"]["outputs"]["echo"], "hi")

    def test_unregistered_external_type_falls_through_to_unsupported(self) -> None:
        # 미등록 type 은 pydantic 이전 단계를 우회해도 실행 경로에서 unsupported 로 보고된다.
        job = StepGraphJob.model_construct(
            job_id="echo_job",
            steps=[step_graph.StepGraphStep.model_construct(step_id="say", type="demo.echo", params={})],
        )
        plan = build_step_graph_execution_plan(job)
        executor = built_in_step_executor(
            project=_project(), plan=plan, plan_step=plan.step_by_id["say"], runtime_home="/tmp/zeta4s"
        )
        self.assertIsInstance(executor, RuntimeCallableStepExecutor)
        self.assertEqual(executor.runtime_callable, "zeta4s.core:run_unsupported_step")

    def test_schema_violation_fails(self) -> None:
        register_step_type_descriptors(build_descriptors(), source="test")
        with self.assertRaises(ValueError):
            validate_step_graph_config(Path("echo.yml"), _echo_job_config(message=None))

    def test_duplicate_external_type_is_contract_error(self) -> None:
        register_step_type_descriptors(build_descriptors(), source="test")
        with self.assertRaises(StepTypeContractError):
            register_step_type_descriptors(build_conflicting_descriptors(), source="test")

    def test_failed_batch_registration_is_atomic(self) -> None:
        with self.assertRaises(StepTypeContractError):
            register_step_type_descriptors(
                (*build_descriptors(), *build_bad_runtime_descriptors()),
                source="test",
            )

        self.assertIsNone(registered_step_type_descriptor("demo.echo"))
        self.assertNotIn("demo.echo", step_graph.STEP_TYPE_VALUES)

    def test_factory_must_return_an_iterable(self) -> None:
        with _patch_entry_points([f"{__name__}:build_non_iterable_descriptors"]):
            with self.assertRaises(StepTypeContractError):
                register_installed_step_types()

    def test_identical_re_registration_is_idempotent(self) -> None:
        register_step_type_descriptors(build_descriptors(), source="test")
        register_step_type_descriptors(build_descriptors(), source="test")
        self.assertIn("demo.echo", step_graph.STEP_TYPE_VALUES)

    def test_builtin_type_cannot_be_overridden(self) -> None:
        override = (
            StepTypeDescriptor(
                type="noop",
                pool_stage=None,
                schema_validator=_validate_echo,
                runtime_callable=f"{__name__}:run_echo",
                payload_builder=_echo_payload,
            ),
        )
        with self.assertRaises(StepTypeContractError):
            register_step_type_descriptors(override, source="test")

    def test_unresolvable_runtime_callable_is_contract_error(self) -> None:
        with self.assertRaises(StepTypeContractError):
            register_step_type_descriptors(build_bad_runtime_descriptors(), source="test")

    def test_invalid_pool_stage_is_contract_error(self) -> None:
        bad = (
            StepTypeDescriptor(
                type="demo.badstage",
                pool_stage="not_a_stage",
                schema_validator=_validate_echo,
                runtime_callable=f"{__name__}:run_echo",
                payload_builder=_echo_payload,
            ),
        )
        with self.assertRaises(StepTypeContractError):
            register_step_type_descriptors(bad, source="test")

    def test_invalid_connection_id_field_is_contract_error(self) -> None:
        bad = (
            StepTypeDescriptor(
                type="demo.badconn",
                pool_stage="transform",
                schema_validator=_validate_echo,
                runtime_callable=f"{__name__}:run_echo",
                payload_builder=_echo_payload,
                connection_id_fields=("missing_conn_field",),
            ),
        )
        with self.assertRaises(StepTypeContractError):
            register_step_type_descriptors(bad, source="test")

    def test_reset_removes_external_type(self) -> None:
        register_step_type_descriptors(build_descriptors(), source="test")
        self.assertIn("demo.echo", step_graph.STEP_TYPE_VALUES)
        reset_step_type_registry()
        self.assertNotIn("demo.echo", step_graph.STEP_TYPE_VALUES)
        self.assertIsNone(registered_step_type_descriptor("demo.echo"))
        with self.assertRaises(ValueError):
            validate_step_graph_config(Path("echo.yml"), _echo_job_config())

    def test_discover_installed_step_type_factories(self) -> None:
        with _patch_entry_points([f"{__name__}:build_descriptors"]):
            self.assertEqual(
                discover_installed_step_type_factories(),
                [f"{__name__}:build_descriptors"],
            )

    def test_register_installed_discovers_entry_points(self) -> None:
        with _patch_entry_points([f"{__name__}:build_descriptors"]):
            registered_count = register_installed_step_types()
        self.assertEqual(registered_count, 1)
        self.assertIsNotNone(registered_step_type_descriptor("demo.echo"))

    def test_register_installed_resolves_dotted_entry_point_attribute(self) -> None:
        with _patch_entry_points([f"{__name__}:NestedFactory.build_descriptors"]):
            registered_count = register_installed_step_types()

        self.assertEqual(registered_count, 1)
        self.assertIsNotNone(registered_step_type_descriptor("demo.echo"))

    def test_identical_installed_registration_does_not_rebuild_live_tables(self) -> None:
        with _patch_entry_points([f"{__name__}:build_descriptors"]):
            register_installed_step_types()
            with patch("zeta4s.project.step_types.rebuild_step_type_tables") as rebuild:
                register_installed_step_types()

        rebuild.assert_not_called()

    def test_register_installed_replaces_previous_registry(self) -> None:
        with _patch_entry_points([f"{__name__}:build_descriptors"]):
            register_installed_step_types()
        with _patch_entry_points([]):
            register_installed_step_types()
        self.assertNotIn("demo.echo", step_graph.STEP_TYPE_VALUES)
        self.assertIsNone(registered_step_type_descriptor("demo.echo"))

    def test_api_project_check_registers_installed_before_schema_validation(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_echo_project(root)
            with _patch_entry_points([f"{__name__}:build_descriptors"]):
                report = _project_check_report(
                    root,
                    profile_id="external",
                    profile={"scheduler": "prefect", "connections": {}},
                )

        self.assertEqual(report["status"], "passed")
        self.assertEqual(report["steps"][0]["name"], "step_types")

    def test_api_project_check_allows_external_type_for_airflow(self) -> None:
        # entry-point 모델에서는 설치가 곧 등록이므로 Airflow backend 도 조기 거부되지 않는다.
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_echo_project(root)
            with _patch_entry_points([f"{__name__}:build_descriptors"]):
                report = _project_check_report(
                    root,
                    profile_id="external",
                    profile={"scheduler": "airflow", "connections": {}},
                )

        self.assertEqual(report["steps"][0]["name"], "step_types")
        self.assertEqual(report["steps"][0]["status"], "passed")
        self.assertEqual(report["status"], "passed")


if __name__ == "__main__":
    unittest.main()
