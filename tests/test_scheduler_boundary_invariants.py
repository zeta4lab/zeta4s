from __future__ import annotations

import ast
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class SchedulerBoundaryInvariantTest(unittest.TestCase):
    def test_core_project_runtime_have_no_scheduler_backend_imports(self) -> None:
        # step-type runtime 은 scheduler 와 무관하게 만들어지고 실행된다. runtime 을
        # 빼면 함수 안 lazy import 가 어떤 gate 에도 걸리지 않는다. import 를 실행하는
        # test 는 airflow 가 설치되지 않았다는 환경 사실에만 기대므로 회귀를 막지 못한다.
        offenders = []
        for package in ("core", "project", "runtime"):
            for path in (ROOT / "src" / "zeta4s" / package).rglob("*.py"):
                imports = _import_roots(path)
                if imports.intersection({"airflow", "prefect"}):
                    offenders.append(str(path.relative_to(ROOT)))
        self.assertEqual(offenders, [])

    def test_flow_control_semantics_have_one_core_definition(self) -> None:
        names = {
            "step_skip_reason",
            "join_skip_reason",
            "evaluate_step_expression",
            "aggregate_run_result",
        }
        definitions = {name: [] for name in names}
        for path in (ROOT / "src" / "zeta4s").rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef) and node.name in names:
                    definitions[node.name].append(str(path.relative_to(ROOT)))
        self.assertEqual(
            definitions,
            {name: ["src/zeta4s/core/semantics.py"] for name in names},
        )

    def test_prefect_is_confined_and_not_exposed_by_cli(self) -> None:
        prefect_importers = []
        for path in (ROOT / "src" / "zeta4s").rglob("*.py"):
            if "prefect" in _import_roots(path):
                prefect_importers.append(str(path.relative_to(ROOT)))
        self.assertEqual(prefect_importers, ["src/zeta4s/prefect/prefect_engine.py"])
        cli_source = (ROOT / "src" / "zeta4s" / "cli" / "main.py").read_text(encoding="utf-8").lower()
        self.assertNotIn("prefect", cli_source)

    def test_scheduler_adapter_does_not_run_full_plan(self) -> None:
        source = "\n".join(path.read_text(encoding="utf-8") for path in (ROOT / "src" / "zeta4s").rglob("*.py"))
        self.assertNotIn("RunnerBackedSchedulerAdapter", source)
        self.assertNotIn("SchedulerRunRequest", source)


class AirflowHeadlessInvariantTest(unittest.TestCase):
    """zeta4s-api 는 Airflow 에 REST 로만 붙고 Airflow 는 standalone DAG만 읽는다.

    **파일 목록으로는 판단할 수 없다.** `api/app.py` 가 airflow 를 직접 import 하지 않아도
    다른 module 을 거쳐 airflow 에 닿을 수 있고, 그런 경로는 어떤 파일 단위 검사에도 걸리지
    않는다. 그래서 import 그래프로 본다.

    worker-side module은 distribution에 남아 있어도 공식 Airflow image에서 import되지
    않는다. zeta4s-api process도 airflow package를 import하지 않는다.
    """

    def test_api_cannot_reach_airflow_by_any_import_path(self) -> None:
        offenders = sorted(
            module
            for module in _reachable_modules("zeta4s.api.app")
            if "airflow" in _import_roots(_module_path(module))
        )
        self.assertEqual(offenders, [])

    def test_worker_side_modules_are_the_only_airflow_importers(self) -> None:
        """distribution 안에서 airflow 를 import하는 legacy adapter 경계를 못박는다.

        여기 없는 module 이 airflow 를 import 하면 실행 위치를 다시 따져야 한다는 뜻이다.
        """
        importers = sorted(
            str(path.relative_to(ROOT))
            for path in (ROOT / "src" / "zeta4s").rglob("*.py")
            if "airflow" in _import_roots(path)
        )
        self.assertEqual(
            importers,
            [
                # task 실행 — operators.py 가 부른다
                "src/zeta4s/airflow/connections.py",
                # dagbag parse
                "src/zeta4s/airflow/dag_generator.py",
                "src/zeta4s/airflow/dynamic_loader.py",
                # task 실행
                "src/zeta4s/airflow/operators.py",
                # legacy connection projection plugin; official image에서는 import하지 않는다
                # generic task 바인딩 — PythonOperator 를 만든다
                "src/zeta4s/airflow/step_binding.py",
            ],
        )

    def test_no_module_opens_the_airflow_metastore(self) -> None:
        """`create_session` 과 `airflow.settings.Session` 두 경로 모두다.

        DSN 만 있으면 어느 쪽이든 metastore 를 직접 연다. 6단계에서 DSN 을 끊으면 붙을 곳이
        없어지지만, 그 전까지는 이 검사가 유일한 방어다.
        """
        offenders = []
        for path in (ROOT / "src" / "zeta4s").rglob("*.py"):
            source = path.read_text(encoding="utf-8")
            if "create_session" in source or "settings import Session" in source or "settings.Session" in source:
                offenders.append(str(path.relative_to(ROOT)))
        self.assertEqual(sorted(offenders), [])

    def test_no_module_shells_out_to_the_airflow_cli(self) -> None:
        self.assertEqual(_airflow_cli_subprocess_offenders(), [])

    def test_api_does_not_inject_python_code_into_a_subprocess(self) -> None:
        """app.py 는 Airflow 에 REST 로만 붙으므로 subprocess 에 code 를 주입하지 않는다.

        주입하면 airflow import 가 문자열 안으로 숨어 import 그래프 검사를 통째로 우회한다.
        """
        source = (ROOT / "src" / "zeta4s" / "api" / "app.py").read_text(encoding="utf-8")
        self.assertNotIn("_airflow_metadata_json", source)
        self.assertNotIn("sys.executable", source)


def _import_roots(path: Path) -> set[str]:
    roots = set()
    for node in ast.walk(_tree(path)):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            roots.add((node.module or "").split(".", 1)[0])
    return roots


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _module_path(module: str) -> Path | None:
    base = ROOT / "src" / Path(*module.split("."))
    if base.with_suffix(".py").exists():
        return base.with_suffix(".py")
    init = base / "__init__.py"
    return init if init.exists() else None


def _zeta4s_imports(path: Path) -> set[str]:
    """이 파일이 끌어오는 zeta4s module 이름이다.

    `from zeta4s.airflow import dags` 는 module 이 `zeta4s.airflow` 이고 이름이 `dags` 라
    둘을 합쳐야 실제 module 이 된다. 함수 안 lazy import 도 AST 라 그대로 잡힌다.
    """
    modules: set[str] = set()
    for node in ast.walk(_tree(path)):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names if alias.name.startswith("zeta4s"))
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if not module.startswith("zeta4s"):
                continue
            modules.add(module)
            modules.update(f"{module}.{alias.name}" for alias in node.names)
    return modules


def _reachable_modules(entry: str) -> set[str]:
    """entry 에서 import 로 도달하는 zeta4s module 의 전이 폐포다.

    **부모 package 를 함께 넣는다.** `zeta4s.airflow.dags` 를 import 하면 python 이
    `zeta4s/airflow/__init__.py` 를 먼저 실행하므로, 거기서 worker 측 module 을 끌어오면
    API process 도 airflow 를 import 하게 된다. 부모를 빼면 그 경로를 놓친다.
    """
    seen: set[str] = set()
    queue = [entry]
    while queue:
        module = queue.pop()
        if module in seen:
            continue
        path = _module_path(module)
        if path is None:
            continue
        seen.add(module)
        for found in _zeta4s_imports(path):
            queue.append(found)
            parts = found.split(".")
            queue.extend(".".join(parts[:index]) for index in range(1, len(parts)))
    return seen


def _airflow_cli_subprocess_offenders() -> list[str]:
    """`subprocess.run(["airflow", ...])` 같은 CLI 호출을 찾는다."""
    offenders = []
    for path in (ROOT / "src" / "zeta4s").rglob("*.py"):
        for node in ast.walk(_tree(path)):
            if not isinstance(node, ast.Call) or not node.args:
                continue
            first = node.args[0]
            if not isinstance(first, (ast.List, ast.Tuple)) or not first.elts:
                continue
            head = first.elts[0]
            if isinstance(head, ast.Constant) and head.value == "airflow":
                offenders.append(str(path.relative_to(ROOT)))
    return sorted(set(offenders))


if __name__ == "__main__":
    unittest.main()
