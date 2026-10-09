from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import yaml

from zeta4s.config import profile_config


class ProfileSchedulerBackendTest(unittest.TestCase):
    def test_sample_local_profile_explicitly_selects_airflow(self) -> None:
        root = Path(__file__).resolve().parents[1]
        profile = yaml.safe_load((root / "zeta4s-work/profiles/airflow.yml").read_text(encoding="utf-8"))

        self.assertEqual(profile["scheduler"], "airflow")

    def test_validate_profile_accepts_airflow_and_prefect(self) -> None:
        for scheduler_backend in ("airflow", "prefect"):
            with self.subTest(scheduler_backend=scheduler_backend):
                profile_config.validate_profile(
                    {"scheduler": scheduler_backend, "connections": {}},
                )

    def test_validate_profile_rejects_invalid_scheduler_backend(self) -> None:
        for scheduler_backend in ("other", 1, None, {}):
            with self.subTest(scheduler_backend=scheduler_backend):
                with self.assertRaises(ValueError):
                    profile_config.validate_profile(
                        {"scheduler": scheduler_backend, "connections": {}},
                    )

    def test_scheduler_backend_defaults_to_prefect(self) -> None:
        self.assertEqual(
            profile_config.scheduler_backend_from_profile({"connections": {}}),
            "prefect",
        )

    def test_validate_profile_rejects_step_types_top_level_field(self) -> None:
        # 외부 step type 은 설치 패키지 entry-point 로 등록되므로 profile 에 나열하지 않는다.
        with self.assertRaises(ValueError):
            profile_config.validate_profile(
                {"connections": {}, "step_types": ["acme_zeta_steps:descriptors"]},
            )

    def test_init_profile_writes_explicit_prefect_backend(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir)
            with patch.object(profile_config, "workspace_path", return_value=workspace):
                path = profile_config.init_profile("dev")

            self.assertEqual(
                yaml.safe_load(path.read_text(encoding="utf-8")),
                {"scheduler": "prefect", "connections": {}},
            )


if __name__ == "__main__":
    unittest.main()
