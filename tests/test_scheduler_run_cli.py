from __future__ import annotations

import unittest
from unittest.mock import patch

from click.testing import CliRunner

from zeta4s.cli.main import cli


class SchedulerRunCliTest(unittest.TestCase):
    def test_create_posts_parameters_to_canonical_endpoint(self) -> None:
        with (
            patch("zeta4s.cli.main._resolve_api_alias", return_value="local"),
            patch(
                "zeta4s.cli.main._check_stale_deployment_if_available",
                return_value={"status": "not_checked"},
            ),
            patch(
                "zeta4s.cli.main._post_json",
                return_value={"project_id": "retail", "job_id": "daily", "run_id": "run-1", "state": "queued"},
            ) as post,
        ):
            result = CliRunner().invoke(
                cli,
                ["api", "run", "create", "retail", "daily", "--parameters", '{"region":"kr"}'],
            )

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("run_id: run-1", result.output)
        self.assertEqual(post.call_args.args[2], {"parameters": {"region": "kr"}})
        self.assertIn("/api/v1/projects/retail/jobs/daily/runs", post.call_args.args[1])

    def test_create_rejects_non_object_parameters(self) -> None:
        result = CliRunner().invoke(
            cli,
            ["api", "run", "create", "retail", "daily", "--parameters", "[]"],
        )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("--parameters must be a JSON object", result.output)


if __name__ == "__main__":
    unittest.main()
