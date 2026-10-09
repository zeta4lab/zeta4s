from __future__ import annotations

import ast
import importlib
import importlib.abc
from pathlib import Path
import pkgutil
import sys
import unittest

import zeta4s.runtime
import zeta4s.core
import zeta4s.prefect


class _BlockedSchedulerFinder(importlib.abc.MetaPathFinder):
    """Raise on any import of the given root modules."""

    def __init__(self, blocked: tuple[str, ...]) -> None:
        self._blocked = blocked

    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".", 1)[0] in self._blocked:
            raise ImportError(f"scheduler backend must not be imported here: {fullname}")
        return None


class RuntimeSchedulerIndependenceTest(unittest.TestCase):
    def test_runtime_modules_import_without_scheduler_backends(self) -> None:
        # airflow/prefect 를 import 자체가 불가능하게 막고 runtime 전체를 다시 import
        # 한다. sys.modules 에서 지우기만 하면 backend 가 설치된 환경에서 재 import 가
        # 그냥 성공해 검사가 무력해진다. 이 test 는 환경에 의존하면 안 된다.
        blocked = ("airflow", "prefect")
        saved = {name: module for name, module in sys.modules.items() if name.split(".", 1)[0] in blocked}
        saved_runtime = {name: module for name, module in sys.modules.items() if name.startswith("zeta4s.runtime")}
        for name in (*saved, *saved_runtime):
            sys.modules.pop(name, None)
        finder = _BlockedSchedulerFinder(blocked)
        sys.meta_path.insert(0, finder)
        try:
            importlib.import_module("zeta4s.runtime")
            for module_info in pkgutil.walk_packages(
                sys.modules["zeta4s.runtime"].__path__,
                prefix="zeta4s.runtime.",
            ):
                importlib.import_module(module_info.name)
        finally:
            sys.meta_path.remove(finder)
            for name in [n for n in sys.modules if n.startswith("zeta4s.runtime")]:
                sys.modules.pop(name, None)
            sys.modules.update(saved)
            sys.modules.update(saved_runtime)

    def test_core_project_runtime_do_not_import_external_schedulers(self) -> None:
        package_root = Path(zeta4s.core.__file__).parent.parent
        package_roots = (
            package_root / "core",
            package_root / "project",
            package_root / "runtime",
        )
        forbidden = ("airflow", "prefect")
        for package_root in package_roots:
            for path in package_root.rglob("*.py"):
                tree = ast.parse(path.read_text(), filename=str(path))
                for node in ast.walk(tree):
                    if isinstance(node, ast.Import):
                        imported = tuple(alias.name for alias in node.names)
                    elif isinstance(node, ast.ImportFrom):
                        imported = (node.module or "",)
                    else:
                        continue
                    for module_name in imported:
                        root_module = module_name.split(".", 1)[0]
                        self.assertNotIn(root_module, forbidden, msg=f"{path} must not depend on {root_module}")

    def test_cli_import_does_not_load_airflow_adapter_namespace(self) -> None:
        airflow_modules = {
            name: module
            for name, module in sys.modules.items()
            if name == "zeta4s.airflow" or name.startswith("zeta4s.airflow.")
        }
        cli_modules = {name: module for name, module in sys.modules.items() if name == "zeta4s.cli.main"}
        for name in [*airflow_modules, *cli_modules]:
            sys.modules.pop(name, None)
        try:
            importlib.import_module("zeta4s.cli.main")
            loaded_airflow_modules = [
                name for name in sys.modules if name == "zeta4s.airflow" or name.startswith("zeta4s.airflow.")
            ]
            self.assertEqual(loaded_airflow_modules, [])
        finally:
            for name in list(sys.modules):
                if name == "zeta4s.airflow" or name.startswith("zeta4s.airflow."):
                    sys.modules.pop(name, None)
            sys.modules.update(airflow_modules)
            sys.modules.update(cli_modules)


if __name__ == "__main__":
    unittest.main()
