from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import yaml

from zeta4s.project.bundle import inspect_project
from zeta4s.project.loader import create_project_skeleton, load_project_context


class ProjectLayoutContractTest(unittest.TestCase):
    def test_create_project_skeleton_uses_target_contract_layout(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp) / "sample_project"

            created = create_project_skeleton(name="sample_project", projects_dir=Path(tmp))

            self.assertEqual(created, root)
            self.assertTrue((root / "project.yml").exists())
            self.assertTrue((root / "jobs").is_dir())
            self.assertTrue((root / "docs" / "README.md").exists())
            self.assertFalse((root / "assets").exists())
            self.assertFalse((root / "jobs" / ".gitkeep").exists())
            self.assertFalse((root / "docs" / ".gitkeep").exists())
            self.assertFalse((root / "jobs" / "quickstart.yml").exists())
            self.assertFalse((root / "dbt" / "dbt_project.yml").exists())
            manifest = yaml.safe_load((root / "project.yml").read_text(encoding="utf-8"))
            self.assertEqual(manifest["paths"], {"jobs": "jobs", "dbt": "dbt"})

    def test_inspect_project_uses_configured_nested_jobs_path(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp) / "nested_project"
            jobs_dir = root / "workflows" / "jobs"
            jobs_dir.mkdir(parents=True)
            (root / "dbt").mkdir(parents=True)
            (root / "project.yml").write_text(
                yaml.safe_dump(
                    {
                        "project_id": "nested_project",
                        "timezone": "Asia/Seoul",
                        "paths": {
                            "jobs": "workflows/jobs",
                            "dbt": "dbt",
                        },
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            (jobs_dir / "daily.yml").write_text(
                yaml.safe_dump(
                    {
                        "job_id": "daily",
                        "schedule": None,
                        "steps": [
                            {
                                "step_id": "start",
                                "type": "noop",
                            }
                        ],
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            result = inspect_project(root)

        self.assertEqual(result["dags"][0]["config"], "workflows/jobs/daily.yml")
        self.assertEqual(result["job_graphs"][0]["config"], "workflows/jobs/daily.yml")
        self.assertIn("graph", result)
        self.assertEqual(result["graph"]["nodes"][0]["id"], "job:daily")

    def test_project_manifest_requires_yml_extension(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp) / "nested_project"
            jobs_dir = root / "workflows" / "jobs"
            jobs_dir.mkdir(parents=True)
            (root / "dbt").mkdir(parents=True)
            (root / "project.yaml").write_text(
                yaml.safe_dump(
                    {
                        "project_id": "nested_project",
                        "timezone": "Asia/Seoul",
                        "paths": {
                            "jobs": "workflows/jobs",
                            "dbt": "dbt",
                        },
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            (jobs_dir / "daily.yaml").write_text(
                yaml.safe_dump(
                    {
                        "job_id": "daily",
                        "schedule": None,
                        "steps": [
                            {
                                "step_id": "start",
                                "type": "noop",
                            }
                        ],
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "project.yml is required"):
                inspect_project(root)

    def test_project_paths_are_required(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp) / "missing_paths"
            root.mkdir()
            (root / "project.yml").write_text(
                yaml.safe_dump(
                    {
                        "project_id": "missing_paths",
                        "timezone": "Asia/Seoul",
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "project.yml paths must be a mapping"):
                load_project_context(root)

    def test_project_paths_reject_unsupported_keys(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp) / "extra_paths"
            root.mkdir()
            (root / "project.yml").write_text(
                yaml.safe_dump(
                    {
                        "project_id": "extra_paths",
                        "timezone": "Asia/Seoul",
                        "paths": {"jobs": "jobs", "assets": "assets", "dbt": "dbt"},
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "project.yml paths has unsupported keys: assets"):
                load_project_context(root)

    def test_project_manifest_requires_project_id(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp) / "invalid_name_project"
            root.mkdir()
            (root / "project.yml").write_text(
                yaml.safe_dump(
                    {
                        "name": "invalid_name_project",
                        "timezone": "Asia/Seoul",
                        "paths": {"jobs": "jobs", "dbt": "dbt"},
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "unsupported keys: name"):
                load_project_context(root)

    def test_project_manifest_rejects_unsupported_identity_keys(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp) / "invalid_extra_project"
            root.mkdir()
            (root / "project.yml").write_text(
                yaml.safe_dump(
                    {
                        "project_id": "invalid_extra_project",
                        "name": "invalid_extra_project",
                        "version": "0.1.0",
                        "timezone": "Asia/Seoul",
                        "paths": {"jobs": "jobs", "dbt": "dbt"},
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "unsupported keys: name, version"):
                load_project_context(root)


if __name__ == "__main__":
    unittest.main()
