from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from click.testing import CliRunner

from zeta4s.api.app import create_app
from zeta4s.cli.main import cli


class ScheduleCliApiContractTest(unittest.TestCase):
    def test_cli_has_no_schedule_group(self) -> None:
        with TemporaryDirectory() as home:
            with patch.object(Path, "home", return_value=Path(home)):
                result = CliRunner().invoke(cli, ["--help"])

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertNotIn("schedule", cli.commands)
        self.assertNotIn("  schedule ", result.output)

    def test_api_has_no_schedule_routes(self) -> None:
        paths = {route.path for route in create_app().routes}

        self.assertFalse(any(path.startswith("/api/v1/schedules") for path in paths))
        self.assertFalse(any(path.startswith("/api/v1/schedule-runs") for path in paths))

    def test_release_gate_does_not_validate_future_schedule_operations(self) -> None:
        release_gate = (Path(__file__).resolve().parents[1] / "scripts/check_release_runtime_showcases.sh").read_text(
            encoding="utf-8"
        )

        self.assertNotIn("verify_scheduler_backend_runs", release_gate)
        self.assertNotIn('"$Z4S_BIN" schedule ', release_gate)

    def test_current_docs_do_not_restore_deleted_schedule_or_internal_scheduler_contract(self) -> None:
        repository_root = Path(__file__).resolve().parents[1]
        current_docs = [
            repository_root / "README.md",
            repository_root / "docs/design/metastore.md",
            repository_root / "docs/roadmap/00-step-graph-runtime-roadmap.md",
            repository_root / "docs/roadmap/README.md",
            *sorted((repository_root / "docs/roadmap/backlog").glob("*.md")),
        ]

        for path in current_docs:
            with self.subTest(path=path.relative_to(repository_root)):
                content = path.read_text(encoding="utf-8")
                self.assertNotIn("z4s schedule apply", content)
                self.assertNotIn("z4s schedule show", content)
                self.assertNotIn("내부 scheduler", content)
                self.assertNotIn("Airflow/Prefect deploy 및 run", content)

        readme = (repository_root / "README.md").read_text(encoding="utf-8")
        self.assertNotIn("첫 정식 기준선", readme)


if __name__ == "__main__":
    unittest.main()
