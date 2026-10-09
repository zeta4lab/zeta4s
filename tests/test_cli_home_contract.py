from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import yaml
from click.testing import CliRunner

from zeta4s.cli.main import _stream_api_report, cli
from zeta4s.config.cli_config import cli_home, config_path, ensure_cli_home, load_config, resolve_api
from zeta4s.core import StepExecutionState, StepFailure, StepResult


class CliHomeContractTest(unittest.TestCase):
    def test_cli_home_uses_account_zeta4s_and_ignores_runtime_home_env(self) -> None:
        with TemporaryDirectory() as tmp, TemporaryDirectory() as runtime_tmp:
            account_home = Path(tmp)
            with (
                patch.object(Path, "home", return_value=account_home),
                patch.dict(
                    os.environ,
                    {
                        "ZETA4S_HOME": runtime_tmp,
                        "ZETA4S_API_HOME": runtime_tmp,
                    },
                    clear=False,
                ),
            ):
                self.assertEqual(cli_home(), account_home / ".zeta4s")
                self.assertEqual(config_path(), account_home / ".zeta4s" / "config.yml")

                home = ensure_cli_home()

                self.assertEqual(home, account_home / ".zeta4s")
                self.assertTrue((home / "config.yml").exists())
                self.assertTrue((home / "secrets").is_dir())
                self.assertTrue((home / "cache").is_dir())
                self.assertTrue((home / "reports").is_dir())
                self.assertFalse((Path(runtime_tmp) / "config.yml").exists())
                self.assertEqual(load_config(), {"apis": {}, "workspaces": {}})

    def test_cli_home_can_be_overridden_with_zeta4s_cli_home(self) -> None:
        with TemporaryDirectory() as tmp, TemporaryDirectory() as account_tmp:
            cli_home_root = Path(tmp) / "cli-home"
            with (
                patch.object(Path, "home", return_value=Path(account_tmp)),
                patch.dict(
                    os.environ,
                    {"ZETA4S_CLI_HOME": str(cli_home_root)},
                    clear=False,
                ),
            ):
                self.assertEqual(cli_home(), cli_home_root)
                self.assertEqual(config_path(), cli_home_root / "config.yml")

                home = ensure_cli_home()

                self.assertEqual(home, cli_home_root)
                self.assertTrue((home / "config.yml").exists())
                self.assertTrue((home / "secrets").is_dir())
                self.assertTrue((home / "cache").is_dir())
                self.assertTrue((home / "reports").is_dir())

    def test_cli_startup_bootstraps_home_before_command(self) -> None:
        with TemporaryDirectory() as tmp:
            account_home = Path(tmp)
            with patch.object(Path, "home", return_value=account_home):
                result = CliRunner().invoke(cli, ["api", "list"])

                self.assertEqual(result.exit_code, 0, result.output)
                home = account_home / ".zeta4s"
                self.assertTrue((home / "config.yml").exists())
                self.assertTrue((home / "secrets").is_dir())
                self.assertTrue((home / "cache").is_dir())
                self.assertTrue((home / "reports").is_dir())
                self.assertEqual(yaml.safe_load((home / "config.yml").read_text(encoding="utf-8")), {"apis": {}})

    def test_api_connect_generates_token_and_registers_default_alias(self) -> None:
        with TemporaryDirectory() as home_tmp, TemporaryDirectory() as cwd_tmp:
            account_home = Path(home_tmp)
            previous_cwd = Path.cwd()
            with patch.object(Path, "home", return_value=account_home):
                try:
                    os.chdir(cwd_tmp)

                    result = CliRunner().invoke(
                        cli,
                        ["api", "connect", "local", "--url", "http://127.0.0.1:18088"],
                    )

                    self.assertEqual(result.exit_code, 0, result.output)
                    token_path = account_home / ".zeta4s" / "secrets" / "api.token"
                    config = yaml.safe_load((account_home / ".zeta4s" / "config.yml").read_text(encoding="utf-8"))
                    env = (Path(cwd_tmp) / ".env").read_text(encoding="utf-8")
                    self.assertTrue(token_path.exists())
                    self.assertIn("ZETA4S_API_TOKEN=", env)
                    self.assertEqual(config["default_api"], "local")
                    self.assertEqual(config["apis"]["local"]["url"], "http://127.0.0.1:18088")
                    self.assertEqual(config["apis"]["local"]["token_file"], str(token_path.resolve()))
                finally:
                    os.chdir(previous_cwd)

    def test_api_commands_manage_api_config(self) -> None:
        with TemporaryDirectory() as home_tmp:
            account_home = Path(home_tmp)
            with patch.object(Path, "home", return_value=account_home):
                runner = CliRunner()

                added = runner.invoke(
                    cli, ["api", "connect", "dev", "--url", "http://127.0.0.1:18088", "--no-env-file"]
                )
                listed = runner.invoke(cli, ["api", "list"])
                used = runner.invoke(cli, ["api", "use", "dev"])
                removed = runner.invoke(cli, ["api", "remove", "dev"])

                self.assertEqual(added.exit_code, 0, added.output)
                self.assertIn("* dev\thttp://127.0.0.1:18088", listed.output)
                self.assertEqual(used.exit_code, 0, used.output)
                self.assertEqual(removed.exit_code, 0, removed.output)

    def test_api_connect_can_register_environment_token(self) -> None:
        with TemporaryDirectory() as home_tmp, TemporaryDirectory() as cwd_tmp:
            account_home = Path(home_tmp)
            previous_cwd = Path.cwd()
            with (
                patch.object(Path, "home", return_value=account_home),
                patch.dict(
                    os.environ,
                    {"ZETA4S_API_TOKEN": "from-env"},
                    clear=False,
                ),
            ):
                try:
                    os.chdir(cwd_tmp)

                    result = CliRunner().invoke(
                        cli,
                        [
                            "api",
                            "connect",
                            "ci",
                            "--url",
                            "https://zeta4s.example.com",
                            "--token-env",
                            "ZETA4S_API_TOKEN",
                        ],
                    )

                    self.assertEqual(result.exit_code, 0, result.output)
                    token_path = account_home / ".zeta4s" / "secrets" / "api.token"
                    config = yaml.safe_load((account_home / ".zeta4s" / "config.yml").read_text(encoding="utf-8"))
                    self.assertFalse(token_path.exists())
                    self.assertFalse((Path(cwd_tmp) / ".env").exists())
                    self.assertEqual(config["apis"]["ci"]["token_env"], "ZETA4S_API_TOKEN")
                    self.assertNotIn("token_file", config["apis"]["ci"])
                    self.assertEqual(resolve_api("ci")["token"], "from-env")
                finally:
                    os.chdir(previous_cwd)

    def test_project_graph_renders_workspace_graph(self) -> None:
        with TemporaryDirectory() as home_tmp, TemporaryDirectory() as cwd_tmp:
            account_home = Path(home_tmp)
            previous_cwd = Path.cwd()
            with patch.object(Path, "home", return_value=account_home):
                try:
                    os.chdir(cwd_tmp)
                    runner = CliRunner()

                    self.assertEqual(runner.invoke(cli, ["work", "init"]).exit_code, 0)
                    self.assertEqual(runner.invoke(cli, ["project", "init", "retail"]).exit_code, 0)
                    job_path = Path(cwd_tmp) / "zeta4s-work" / "projects" / "retail" / "jobs" / "daily.yml"
                    job_path.write_text(
                        yaml.safe_dump(
                            {
                                "job_id": "daily",
                                "steps": [
                                    {"step_id": "start", "type": "noop"},
                                    {"step_id": "finish", "type": "noop", "depends_on": ["start"]},
                                ],
                            },
                            sort_keys=False,
                        ),
                        encoding="utf-8",
                    )

                    text_result = runner.invoke(cli, ["project", "graph", "retail"])
                    json_result = runner.invoke(cli, ["project", "graph", "retail", "--format", "json"])
                    mermaid_result = runner.invoke(cli, ["project", "graph", "retail", "--format", "mermaid"])

                    self.assertEqual(text_result.exit_code, 0, text_result.output)
                    self.assertIn("scope: workspace", text_result.output)
                    self.assertIn("job: daily", text_result.output)
                    self.assertIn("start [noop]", text_result.output)
                    self.assertEqual(json_result.exit_code, 0, json_result.output)
                    self.assertEqual(json.loads(json_result.output)["scope"], "workspace")
                    self.assertEqual(mermaid_result.exit_code, 0, mermaid_result.output)
                    self.assertIn("flowchart TD", mermaid_result.output)
                finally:
                    os.chdir(previous_cwd)

    def test_project_graph_api_reads_deployed_graph_without_workspace(self) -> None:
        deployed_graph = {
            "project_id": "retail",
            "job_graphs": [
                {
                    "job_id": "daily",
                    "config": "jobs/daily.yml",
                    "nodes": [{"id": "job:daily:step:start", "label": "start", "step_type": "noop"}],
                    "edges": [],
                }
            ],
        }
        with TemporaryDirectory() as home_tmp:
            account_home = Path(home_tmp)
            captured = {}

            def fake_get_json(api_alias, endpoint):
                captured["api_alias"] = api_alias
                captured["endpoint"] = endpoint
                return dict(deployed_graph)

            with (
                patch.object(Path, "home", return_value=account_home),
                patch("zeta4s.cli.main._get_json", side_effect=fake_get_json),
            ):
                result = CliRunner().invoke(cli, ["project", "graph", "retail", "--api", "local", "--format", "json"])

                self.assertEqual(result.exit_code, 0, result.output)
                self.assertEqual(captured["api_alias"], "local")
                self.assertEqual(captured["endpoint"], "/api/v1/projects/retail/graph")
                payload = json.loads(result.output)
                self.assertEqual(payload["scope"], "deployed")
                self.assertEqual(payload["api"], "local")

    def test_top_level_help_lists_current_command_groups(self) -> None:
        result = CliRunner().invoke(cli, ["--help"])

        self.assertEqual(result.exit_code, 0, result.output)
        for command in ("api", "profile", "project", "report", "work"):
            self.assertIn(f"  {command}", result.output)
        self.assertNotIn("  runtime", result.output)

    def test_work_init_creates_workspace_and_registers_single_path(self) -> None:
        with TemporaryDirectory() as home_tmp, TemporaryDirectory() as cwd_tmp:
            account_home = Path(home_tmp)
            previous_cwd = Path.cwd()
            with patch.object(Path, "home", return_value=account_home):
                try:
                    os.chdir(cwd_tmp)
                    result = CliRunner().invoke(cli, ["work", "init"])

                    self.assertEqual(result.exit_code, 0, result.output)
                    workspace = Path(cwd_tmp) / "zeta4s-work"
                    self.assertTrue((workspace / "profiles").is_dir())
                    self.assertTrue((workspace / "projects").is_dir())
                    config = yaml.safe_load((account_home / ".zeta4s" / "config.yml").read_text(encoding="utf-8"))
                    self.assertEqual(config["workspaces"]["zeta4s-work"], str(workspace.resolve()))
                    self.assertEqual(config["active_workspace"], "zeta4s-work")

                    second = CliRunner().invoke(cli, ["work", "init", "another-work"])

                    self.assertEqual(second.exit_code, 0, second.output)
                    another_workspace = Path(cwd_tmp) / "another-work"
                    self.assertTrue((another_workspace / "profiles").is_dir())
                    self.assertTrue((another_workspace / "projects").is_dir())
                    config2 = yaml.safe_load((account_home / ".zeta4s" / "config.yml").read_text(encoding="utf-8"))
                    self.assertEqual(config2["workspaces"]["another-work"], str(another_workspace.resolve()))
                    self.assertEqual(config2["active_workspace"], "another-work")
                    self.assertEqual(config2["workspaces"]["zeta4s-work"], str(workspace.resolve()))
                finally:
                    os.chdir(previous_cwd)

    def test_work_init_replaces_deleted_registered_workspace(self) -> None:
        with TemporaryDirectory() as home_tmp, TemporaryDirectory() as cwd_tmp:
            account_home = Path(home_tmp)
            previous_cwd = Path.cwd()
            with patch.object(Path, "home", return_value=account_home):
                try:
                    os.chdir(cwd_tmp)
                    first = CliRunner().invoke(cli, ["work", "init", "old-work"])
                    self.assertEqual(first.exit_code, 0, first.output)
                    old_workspace = Path(cwd_tmp) / "old-work"
                    shutil.rmtree(old_workspace)

                    second = CliRunner().invoke(cli, ["work", "init", "new-work"])

                    self.assertEqual(second.exit_code, 0, second.output)
                    new_workspace = Path(cwd_tmp) / "new-work"
                    self.assertTrue((new_workspace / "profiles").is_dir())
                    self.assertTrue((new_workspace / "projects").is_dir())
                    config = yaml.safe_load((account_home / ".zeta4s" / "config.yml").read_text(encoding="utf-8"))
                    self.assertEqual(config["workspaces"]["new-work"], str(new_workspace.resolve()))
                    self.assertEqual(config["active_workspace"], "new-work")
                finally:
                    os.chdir(previous_cwd)

    def test_work_init_recreates_deleted_registered_workspace_with_same_name(self) -> None:
        with TemporaryDirectory() as home_tmp, TemporaryDirectory() as cwd_tmp:
            account_home = Path(home_tmp)
            previous_cwd = Path.cwd()
            with patch.object(Path, "home", return_value=account_home):
                try:
                    os.chdir(cwd_tmp)
                    first = CliRunner().invoke(cli, ["work", "init"])
                    self.assertEqual(first.exit_code, 0, first.output)
                    workspace = Path(cwd_tmp) / "zeta4s-work"
                    shutil.rmtree(workspace)

                    second = CliRunner().invoke(cli, ["work", "init"])

                    self.assertEqual(second.exit_code, 0, second.output)
                    self.assertTrue((workspace / "profiles").is_dir())
                    self.assertTrue((workspace / "projects").is_dir())
                    config = yaml.safe_load((account_home / ".zeta4s" / "config.yml").read_text(encoding="utf-8"))
                    self.assertEqual(config["workspaces"]["zeta4s-work"], str(workspace.resolve()))
                    self.assertEqual(config["active_workspace"], "zeta4s-work")
                finally:
                    os.chdir(previous_cwd)

    def test_work_show_reads_registered_workspace_without_directory_search(self) -> None:
        with TemporaryDirectory() as home_tmp, TemporaryDirectory() as cwd_tmp:
            account_home = Path(home_tmp)
            previous_cwd = Path.cwd()
            with patch.object(Path, "home", return_value=account_home):
                try:
                    os.chdir(cwd_tmp)
                    unregistered = Path(cwd_tmp) / "zeta4s-work"
                    (unregistered / "profiles").mkdir(parents=True)
                    (unregistered / "projects").mkdir()

                    result = CliRunner().invoke(cli, ["work", "show"])

                    self.assertEqual(result.exit_code, 0, result.output)
                    output = yaml.safe_load(result.output)
                    self.assertEqual(output["home"], str(account_home / ".zeta4s"))
                    self.assertIsNone(output["workspace"])
                    self.assertFalse(output["exists"])
                    self.assertIsNone(output["profiles_dir"])
                    self.assertIsNone(output["projects_dir"])
                    self.assertEqual(output["profile_count"], 0)
                    self.assertEqual(output["project_count"], 0)
                finally:
                    os.chdir(previous_cwd)

    def test_work_show_reports_registered_workspace_counts(self) -> None:
        with TemporaryDirectory() as home_tmp, TemporaryDirectory() as cwd_tmp:
            account_home = Path(home_tmp)
            previous_cwd = Path.cwd()
            with patch.object(Path, "home", return_value=account_home):
                try:
                    os.chdir(cwd_tmp)
                    init_result = CliRunner().invoke(cli, ["work", "init", "analytics-work"])
                    self.assertEqual(init_result.exit_code, 0, init_result.output)
                    workspace = Path(cwd_tmp) / "analytics-work"
                    (workspace / "profiles" / "dev.yml").write_text("connections: {}\n", encoding="utf-8")
                    (workspace / "profiles" / "prod.yaml").write_text("connections: {}\n", encoding="utf-8")
                    (workspace / "projects" / "retail").mkdir()

                    result = CliRunner().invoke(cli, ["work", "show"])

                    self.assertEqual(result.exit_code, 0, result.output)
                    output = yaml.safe_load(result.output)
                    self.assertEqual(output["workspace"], str(workspace.resolve()))
                    self.assertTrue(output["exists"])
                    self.assertEqual(output["profiles_dir"], str((workspace / "profiles").resolve()))
                    self.assertEqual(output["projects_dir"], str((workspace / "projects").resolve()))
                    self.assertEqual(output["profile_count"], 2)
                    self.assertEqual(output["project_count"], 1)
                finally:
                    os.chdir(previous_cwd)

    def test_profile_init_check_and_list_use_registered_workspace(self) -> None:
        with TemporaryDirectory() as home_tmp, TemporaryDirectory() as cwd_tmp:
            account_home = Path(home_tmp)
            previous_cwd = Path.cwd()
            with patch.object(Path, "home", return_value=account_home):
                try:
                    os.chdir(cwd_tmp)
                    init_work = CliRunner().invoke(cli, ["work", "init"])
                    self.assertEqual(init_work.exit_code, 0, init_work.output)

                    init_profile = CliRunner().invoke(cli, ["profile", "init", "dev"])

                    self.assertEqual(init_profile.exit_code, 0, init_profile.output)
                    profile_path = Path(cwd_tmp) / "zeta4s-work" / "profiles" / "dev.yml"
                    self.assertTrue(profile_path.exists())
                    self.assertEqual(
                        yaml.safe_load(profile_path.read_text(encoding="utf-8")),
                        {"scheduler": "prefect", "connections": {}},
                    )

                    profile_path.write_text(
                        yaml.safe_dump(
                            {
                                "connections": {
                                    "oracle-source": {
                                        "type": "oracle",
                                        "host": "oracle",
                                        "port": 1521,
                                        "username": "metastore",
                                        "password_ref": "dev.oracle-source.password",
                                        "database": "freepdb1",
                                        "schema": "metastore",
                                        "options": {"service_name": "freepdb1"},
                                    }
                                },
                                "variables": {"timezone": "Asia/Seoul"},
                            },
                            sort_keys=False,
                        ),
                        encoding="utf-8",
                    )

                    captured = {}

                    def fake_post_json(api_alias, endpoint, payload):
                        captured["api_alias"] = api_alias
                        captured["endpoint"] = endpoint
                        captured["payload"] = payload
                        return {
                            "status": "passed",
                            "command": "z4s profile check",
                            "summary": {"connection_count": 1},
                            "steps": [],
                            "issues": [],
                        }

                    with patch("zeta4s.cli.main._post_json", side_effect=fake_post_json):
                        check = CliRunner().invoke(cli, ["profile", "check", "dev"])
                    show = CliRunner().invoke(cli, ["profile", "show", "dev"])
                    listed = CliRunner().invoke(cli, ["profile", "list"])

                    self.assertEqual(check.exit_code, 0, check.output)
                    self.assertIn("[profile][check] dev passed: 0 issues", check.output)
                    self.assertEqual(captured["endpoint"], "/api/v1/profiles/check")
                    self.assertEqual(
                        captured["payload"]["profile"], yaml.safe_load(profile_path.read_text(encoding="utf-8"))
                    )
                    report_path = account_home / ".zeta4s" / "reports" / "dev" / "profile-check.latest.json"
                    self.assertTrue(report_path.exists())
                    report = yaml.safe_load(report_path.read_text(encoding="utf-8"))
                    self.assertEqual(report["profile"], "dev")
                    self.assertNotIn("project", report)
                    self.assertNotIn("project_id", report)
                    self.assertEqual(show.exit_code, 0, show.output)
                    self.assertEqual(
                        yaml.safe_load(show.output), yaml.safe_load(profile_path.read_text(encoding="utf-8"))
                    )
                    self.assertEqual(listed.exit_code, 0, listed.output)
                    list_output = yaml.safe_load(listed.output)
                    self.assertEqual(
                        list_output["profiles"],
                        [{"profile_id": "dev", "path": str(profile_path.resolve())}],
                    )
                finally:
                    os.chdir(previous_cwd)

    def test_profile_edit_validates_saved_yaml_and_delete_removes_profile(self) -> None:
        with TemporaryDirectory() as home_tmp, TemporaryDirectory() as cwd_tmp:
            account_home = Path(home_tmp)
            previous_cwd = Path.cwd()
            with patch.object(Path, "home", return_value=account_home):
                try:
                    os.chdir(cwd_tmp)
                    self.assertEqual(CliRunner().invoke(cli, ["work", "init"]).exit_code, 0)
                    profile_path = Path(cwd_tmp) / "zeta4s-work" / "profiles" / "dev.yml"
                    self.assertEqual(CliRunner().invoke(cli, ["profile", "init", "dev"]).exit_code, 0)

                    def fake_edit(filename):
                        Path(filename).write_text(
                            yaml.safe_dump(
                                {
                                    "connections": {
                                        "analytics_clickhouse": {
                                            "type": "clickhouse",
                                            "host": "clickhouse",
                                        }
                                    },
                                    "variables": {"timezone": "Asia/Seoul"},
                                },
                                sort_keys=False,
                            ),
                            encoding="utf-8",
                        )
                        return None

                    with patch("click.edit", side_effect=fake_edit):
                        edited = CliRunner().invoke(cli, ["profile", "edit", "dev"])
                    shown = CliRunner().invoke(cli, ["profile", "show", "dev"])
                    deleted = CliRunner().invoke(cli, ["profile", "delete", "dev", "--yes"])

                    self.assertEqual(edited.exit_code, 0, edited.output)
                    self.assertIn("profile dev: saved", edited.output)
                    self.assertEqual(yaml.safe_load(shown.output)["variables"]["timezone"], "Asia/Seoul")
                    self.assertEqual(deleted.exit_code, 0, deleted.output)
                    self.assertFalse(profile_path.exists())
                finally:
                    os.chdir(previous_cwd)

    def test_profile_check_and_auto_select_accept_yaml_profile_file(self) -> None:
        with TemporaryDirectory() as home_tmp, TemporaryDirectory() as cwd_tmp:
            account_home = Path(home_tmp)
            previous_cwd = Path.cwd()
            with patch.object(Path, "home", return_value=account_home):
                try:
                    os.chdir(cwd_tmp)
                    self.assertEqual(CliRunner().invoke(cli, ["work", "init"]).exit_code, 0)
                    workspace = Path(cwd_tmp) / "zeta4s-work"
                    profile_path = workspace / "profiles" / "dev.yaml"
                    profile_path.write_text(
                        yaml.safe_dump(
                            {
                                "connections": {
                                    "analytics_clickhouse": {
                                        "type": "clickhouse",
                                        "host": "clickhouse",
                                    }
                                }
                            },
                            sort_keys=False,
                        ),
                        encoding="utf-8",
                    )
                    self.assertEqual(CliRunner().invoke(cli, ["project", "init", "retail"]).exit_code, 0)
                    project_root = workspace / "projects" / "retail"
                    (project_root / "sql" / "clickhouse").mkdir(parents=True)
                    (project_root / "sql" / "clickhouse" / "select_one.sql").write_text("SELECT 1\n", encoding="utf-8")
                    (project_root / "jobs" / "daily.yml").write_text(
                        yaml.safe_dump(
                            {
                                "job_id": "daily",
                                "schedule": None,
                                "steps": [
                                    {
                                        "step_id": "select_one",
                                        "type": "clickhouse.sql",
                                        "conn": "analytics_clickhouse",
                                        "query": "sql/clickhouse/select_one.sql",
                                    }
                                ],
                            },
                            sort_keys=False,
                        ),
                        encoding="utf-8",
                    )

                    with patch(
                        "zeta4s.cli.main._post_json",
                        return_value={
                            "status": "passed",
                            "command": "z4s profile check",
                            "summary": {"connection_count": 1},
                            "steps": [],
                            "issues": [],
                        },
                    ):
                        profile_check = CliRunner().invoke(cli, ["profile", "check", "dev"])
                    project_check = CliRunner().invoke(cli, ["project", "check", "retail"])

                    self.assertEqual(profile_check.exit_code, 0, profile_check.output)
                    self.assertIn("[profile][check] dev passed: 0 issues", profile_check.output)
                    self.assertEqual(project_check.exit_code, 0, project_check.output)
                    latest = account_home / ".zeta4s" / "reports" / "retail" / "project-check.latest.json"
                    report = yaml.safe_load(latest.read_text(encoding="utf-8"))
                    self.assertEqual(report["summary"]["profile"], "dev")
                finally:
                    os.chdir(previous_cwd)

    def test_profile_check_rejects_forbidden_connection_fields(self) -> None:
        with TemporaryDirectory() as home_tmp, TemporaryDirectory() as cwd_tmp:
            account_home = Path(home_tmp)
            previous_cwd = Path.cwd()
            with patch.object(Path, "home", return_value=account_home):
                try:
                    os.chdir(cwd_tmp)
                    init_work = CliRunner().invoke(cli, ["work", "init"])
                    self.assertEqual(init_work.exit_code, 0, init_work.output)
                    profile_path = Path(cwd_tmp) / "zeta4s-work" / "profiles" / "dev.yml"
                    profile_path.write_text(
                        yaml.safe_dump(
                            {
                                "connections": {
                                    "oracle_source": {
                                        "type": "oracle",
                                        "login": "metastore",
                                        "password": "plain",
                                    }
                                }
                            },
                            sort_keys=False,
                        ),
                        encoding="utf-8",
                    )

                    result = CliRunner().invoke(cli, ["profile", "check", "dev"])

                    self.assertIn("forbidden fields: login, password", result.output)
                finally:
                    os.chdir(previous_cwd)

    def test_profile_init_rejects_extension_in_profile_id(self) -> None:
        with TemporaryDirectory() as home_tmp, TemporaryDirectory() as cwd_tmp:
            account_home = Path(home_tmp)
            previous_cwd = Path.cwd()
            with patch.object(Path, "home", return_value=account_home):
                try:
                    os.chdir(cwd_tmp)
                    init_work = CliRunner().invoke(cli, ["work", "init"])
                    self.assertEqual(init_work.exit_code, 0, init_work.output)

                    result = CliRunner().invoke(cli, ["profile", "init", "dev.yml"])

                    self.assertIn("profile id must not include a file extension", result.output)
                    self.assertFalse((Path(cwd_tmp) / "zeta4s-work" / "profiles" / "dev.yml").exists())
                finally:
                    os.chdir(previous_cwd)

    def test_profile_check_rejects_secure_connection_field(self) -> None:
        with TemporaryDirectory() as home_tmp, TemporaryDirectory() as cwd_tmp:
            account_home = Path(home_tmp)
            previous_cwd = Path.cwd()
            with patch.object(Path, "home", return_value=account_home):
                try:
                    os.chdir(cwd_tmp)
                    init_work = CliRunner().invoke(cli, ["work", "init"])
                    self.assertEqual(init_work.exit_code, 0, init_work.output)
                    profile_path = Path(cwd_tmp) / "zeta4s-work" / "profiles" / "dev.yml"
                    profile_path.write_text(
                        yaml.safe_dump(
                            {
                                "connections": {
                                    "analytics_clickhouse": {
                                        "type": "clickhouse",
                                        "host": "clickhouse",
                                        "secure": True,
                                    }
                                }
                            },
                            sort_keys=False,
                        ),
                        encoding="utf-8",
                    )

                    result = CliRunner().invoke(cli, ["profile", "check", "dev"])

                    self.assertIn("unsupported fields: secure", result.output)
                finally:
                    os.chdir(previous_cwd)

    def test_project_init_creates_project_under_registered_workspace(self) -> None:
        with TemporaryDirectory() as home_tmp, TemporaryDirectory() as cwd_tmp:
            account_home = Path(home_tmp)
            previous_cwd = Path.cwd()
            with patch.object(Path, "home", return_value=account_home):
                try:
                    os.chdir(cwd_tmp)
                    init_work = CliRunner().invoke(cli, ["work", "init"])
                    self.assertEqual(init_work.exit_code, 0, init_work.output)

                    result = CliRunner().invoke(cli, ["project", "init", "retail"])

                    self.assertEqual(result.exit_code, 0, result.output)
                    project_root = Path(cwd_tmp) / "zeta4s-work" / "projects" / "retail"
                    self.assertTrue((project_root / "project.yml").exists())
                    self.assertTrue((project_root / "jobs").is_dir())
                    self.assertTrue((project_root / "docs" / "README.md").exists())
                    self.assertFalse((project_root / "assets").exists())
                    manifest = yaml.safe_load((project_root / "project.yml").read_text(encoding="utf-8"))
                    self.assertEqual(manifest["paths"], {"jobs": "jobs", "dbt": "dbt"})
                finally:
                    os.chdir(previous_cwd)

    def test_project_init_requires_registered_workspace(self) -> None:
        with TemporaryDirectory() as home_tmp, TemporaryDirectory() as cwd_tmp:
            account_home = Path(home_tmp)
            previous_cwd = Path.cwd()
            with patch.object(Path, "home", return_value=account_home):
                try:
                    os.chdir(cwd_tmp)

                    result = CliRunner().invoke(cli, ["project", "init", "retail"])

                    self.assertIn("workspace is not initialized", result.output)
                    self.assertFalse((Path(cwd_tmp) / "projects" / "retail").exists())
                finally:
                    os.chdir(previous_cwd)

    def test_project_check_uses_workspace_project_and_profile(self) -> None:
        with TemporaryDirectory() as home_tmp, TemporaryDirectory() as cwd_tmp:
            account_home = Path(home_tmp)
            previous_cwd = Path.cwd()
            with patch.object(Path, "home", return_value=account_home):
                try:
                    os.chdir(cwd_tmp)
                    self.assertEqual(CliRunner().invoke(cli, ["work", "init"]).exit_code, 0)
                    self.assertEqual(CliRunner().invoke(cli, ["profile", "init", "dev"]).exit_code, 0)
                    self.assertEqual(CliRunner().invoke(cli, ["project", "init", "retail"]).exit_code, 0)
                    project_root = Path(cwd_tmp) / "zeta4s-work" / "projects" / "retail"
                    (project_root / "jobs" / "daily.yml").write_text(
                        yaml.safe_dump(
                            {
                                "job_id": "daily",
                                "schedule": None,
                                "steps": [{"step_id": "start", "type": "noop"}],
                            },
                            sort_keys=False,
                        ),
                        encoding="utf-8",
                    )

                    result = CliRunner().invoke(cli, ["project", "check", "retail", "--profile", "dev"])

                    self.assertEqual(result.exit_code, 0, result.output)
                    self.assertIn("[project][check] project check 통과", result.output)
                    latest = account_home / ".zeta4s" / "reports" / "retail" / "project-check.latest.json"
                    report = yaml.safe_load(latest.read_text(encoding="utf-8"))
                    self.assertEqual(report["summary"]["profile"], "dev")
                    self.assertEqual(report["command"], "z4s project check")
                finally:
                    os.chdir(previous_cwd)

    def test_project_init_with_dbt_uses_profile_dbt_capable_connections(self) -> None:
        with TemporaryDirectory() as home_tmp, TemporaryDirectory() as cwd_tmp:
            account_home = Path(home_tmp)
            previous_cwd = Path.cwd()
            with patch.object(Path, "home", return_value=account_home):
                try:
                    os.chdir(cwd_tmp)
                    runner = CliRunner()
                    self.assertEqual(runner.invoke(cli, ["work", "init"]).exit_code, 0)
                    profile_path = Path(cwd_tmp) / "zeta4s-work" / "profiles" / "dev.yml"
                    profile_path.write_text(
                        yaml.safe_dump(
                            {
                                "connections": {
                                    "analytics_clickhouse": {"type": "clickhouse"},
                                    "erp_oracle": {"type": "oracle"},
                                    "search_elasticsearch": {"type": "elasticsearch"},
                                }
                            },
                            sort_keys=False,
                        ),
                        encoding="utf-8",
                    )

                    result = runner.invoke(cli, ["project", "init", "retail", "--profile", "dev", "--with-dbt"])

                    self.assertEqual(result.exit_code, 0, result.output)
                    project_root = Path(cwd_tmp) / "zeta4s-work" / "projects" / "retail"
                    self.assertTrue((project_root / "dbt" / "analytics_clickhouse" / "dbt_project.yml").exists())
                    self.assertTrue((project_root / "dbt" / "erp_oracle" / "dbt_project.yml").exists())
                    self.assertFalse((project_root / "dbt" / "search_elasticsearch").exists())
                finally:
                    os.chdir(previous_cwd)

    def test_project_init_with_dbt_requires_profile(self) -> None:
        with TemporaryDirectory() as home_tmp, TemporaryDirectory() as cwd_tmp:
            account_home = Path(home_tmp)
            previous_cwd = Path.cwd()
            with patch.object(Path, "home", return_value=account_home):
                try:
                    os.chdir(cwd_tmp)
                    runner = CliRunner()
                    self.assertEqual(runner.invoke(cli, ["work", "init"]).exit_code, 0)

                    result = runner.invoke(cli, ["project", "init", "retail", "--with-dbt"])

                    self.assertIn("--profile is required with --with-dbt", result.output)
                finally:
                    os.chdir(previous_cwd)

    def test_project_check_fails_when_profile_connection_is_missing(self) -> None:
        with TemporaryDirectory() as home_tmp, TemporaryDirectory() as cwd_tmp:
            account_home = Path(home_tmp)
            previous_cwd = Path.cwd()
            with patch.object(Path, "home", return_value=account_home):
                try:
                    os.chdir(cwd_tmp)
                    self.assertEqual(CliRunner().invoke(cli, ["work", "init"]).exit_code, 0)
                    self.assertEqual(CliRunner().invoke(cli, ["profile", "init", "dev"]).exit_code, 0)
                    self.assertEqual(CliRunner().invoke(cli, ["project", "init", "retail"]).exit_code, 0)
                    project_root = Path(cwd_tmp) / "zeta4s-work" / "projects" / "retail"
                    (project_root / "sql" / "clickhouse").mkdir(parents=True)
                    (project_root / "sql" / "clickhouse" / "select_one.sql").write_text("SELECT 1\n", encoding="utf-8")
                    (project_root / "jobs" / "daily.yml").write_text(
                        yaml.safe_dump(
                            {
                                "job_id": "daily",
                                "schedule": None,
                                "steps": [
                                    {
                                        "step_id": "select_one",
                                        "type": "clickhouse.sql",
                                        "conn": "analytics_clickhouse",
                                        "query": "sql/clickhouse/select_one.sql",
                                    }
                                ],
                            },
                            sort_keys=False,
                        ),
                        encoding="utf-8",
                    )

                    result = CliRunner().invoke(cli, ["project", "check", "retail", "--profile", "dev"])

                    self.assertIn("[project][check] project check 실패", result.output)
                    self.assertIn("missing profile connections: analytics_clickhouse", result.output)
                finally:
                    os.chdir(previous_cwd)

    def test_project_run_executes_noop_job_with_core_runner(self) -> None:
        with TemporaryDirectory() as home_tmp, TemporaryDirectory() as cwd_tmp:
            account_home = Path(home_tmp)
            previous_cwd = Path.cwd()
            with patch.object(Path, "home", return_value=account_home):
                try:
                    os.chdir(cwd_tmp)
                    runner = CliRunner()
                    self.assertEqual(runner.invoke(cli, ["work", "init"]).exit_code, 0)
                    self.assertEqual(runner.invoke(cli, ["profile", "init", "dev"]).exit_code, 0)
                    self.assertEqual(runner.invoke(cli, ["project", "init", "retail"]).exit_code, 0)
                    job_path = Path(cwd_tmp) / "zeta4s-work" / "projects" / "retail" / "jobs" / "daily.yml"
                    job_path.write_text(
                        yaml.safe_dump(
                            {
                                "job_id": "daily",
                                "steps": [
                                    {"step_id": "start", "type": "noop"},
                                    {"step_id": "finish", "type": "noop", "depends_on": ["start"]},
                                ],
                            },
                            sort_keys=False,
                        ),
                        encoding="utf-8",
                    )

                    result = runner.invoke(cli, ["run", "retail", "daily", "--profile", "dev"])

                    self.assertEqual(result.exit_code, 0, result.output)
                    self.assertIn("[project][run] passed: project=retail job=daily", result.output)
                    self.assertIn("profile=dev", result.output)
                    self.assertRegex(
                        result.output,
                        r"report: reports/retail/project-run\.\d{8}T\d{6}[+-]\d{4}\.[0-9a-f]{8}\.json",
                    )
                    self.assertIn("latest: reports/retail/project-run.latest.json", result.output)
                    report_path = account_home / ".zeta4s" / "reports" / "retail" / "project-run.latest.json"
                    report = json.loads(report_path.read_text(encoding="utf-8"))
                    self.assertTrue((account_home / ".zeta4s" / report["report_path"]).exists())
                    self.assertEqual(report["latest_report_path"], "reports/retail/project-run.latest.json")
                    self.assertEqual(report["command"], "z4s run")
                    self.assertEqual(report["status"], "passed")
                    self.assertEqual(report["profile"], "dev")
                    self.assertEqual(report["summary"]["steps"], 2)
                    self.assertEqual([step["state"] for step in report["steps"]], ["succeeded", "succeeded"])
                    self.assertEqual(
                        [event["event_type"] for event in report["events"]],
                        [
                            "run_started",
                            "step_started",
                            "step_succeeded",
                            "step_started",
                            "step_succeeded",
                            "run_succeeded",
                        ],
                    )
                    self.assertEqual(report["events"][1]["step_id"], "start")
                    self.assertEqual(report["events"][-1]["event"]["state"], "succeeded")
                finally:
                    os.chdir(previous_cwd)

    def test_project_run_stops_before_runner_when_static_check_fails(self) -> None:
        with TemporaryDirectory() as home_tmp, TemporaryDirectory() as cwd_tmp:
            account_home = Path(home_tmp)
            previous_cwd = Path.cwd()
            with patch.object(Path, "home", return_value=account_home):
                try:
                    os.chdir(cwd_tmp)
                    runner = CliRunner()
                    self.assertEqual(runner.invoke(cli, ["work", "init"]).exit_code, 0)
                    self.assertEqual(runner.invoke(cli, ["profile", "init", "dev"]).exit_code, 0)
                    self.assertEqual(runner.invoke(cli, ["project", "init", "retail"]).exit_code, 0)
                    job_path = Path(cwd_tmp) / "zeta4s-work" / "projects" / "retail" / "jobs" / "daily.yml"
                    job_path.write_text(
                        yaml.safe_dump(
                            {
                                "job_id": "daily",
                                "steps": [
                                    {
                                        "step_id": "count_orders",
                                        "type": "sql.scalar",
                                        "conn": "analytics",
                                        "sql": "select 1",
                                        "outputs": {"row_count": {"kind": "scalar", "type": "int"}},
                                    },
                                ],
                            },
                            sort_keys=False,
                        ),
                        encoding="utf-8",
                    )

                    with patch("zeta4s.cli.main.built_in_step_executor") as build_executor:
                        result = runner.invoke(cli, ["run", "retail", "daily", "--profile", "dev"])

                    self.assertEqual(result.exit_code, 1, result.output)
                    self.assertIn("[project][check] project check 실패", result.output)
                    self.assertIn("missing profile connections: analytics", result.output)
                    build_executor.assert_not_called()
                    self.assertFalse((cli_home() / "reports" / "retail" / "project-run.latest.json").exists())
                    self.assertTrue((cli_home() / "reports" / "retail" / "project-check.latest.json").exists())
                finally:
                    os.chdir(previous_cwd)

    def test_project_run_report_includes_terminal_outputs(self) -> None:
        with TemporaryDirectory() as home_tmp, TemporaryDirectory() as cwd_tmp:
            account_home = Path(home_tmp)
            previous_cwd = Path.cwd()
            with patch.object(Path, "home", return_value=account_home):
                try:
                    os.chdir(cwd_tmp)
                    runner = CliRunner()
                    self.assertEqual(runner.invoke(cli, ["work", "init"]).exit_code, 0)
                    profile_path = Path(cwd_tmp) / "zeta4s-work" / "profiles" / "dev.yml"
                    profile_path.write_text(
                        yaml.safe_dump(
                            {"connections": {"analytics": {"type": "clickhouse"}}},
                            sort_keys=False,
                        ),
                        encoding="utf-8",
                    )
                    self.assertEqual(runner.invoke(cli, ["project", "init", "retail"]).exit_code, 0)
                    job_path = Path(cwd_tmp) / "zeta4s-work" / "projects" / "retail" / "jobs" / "daily.yml"
                    job_path.write_text(
                        yaml.safe_dump(
                            {
                                "job_id": "daily",
                                "steps": [
                                    {
                                        "step_id": "count_orders",
                                        "type": "sql.scalar",
                                        "conn": "analytics",
                                        "sql": "select 3",
                                        "outputs": {"row_count": {"kind": "scalar", "type": "int"}},
                                    },
                                ],
                            },
                            sort_keys=False,
                        ),
                        encoding="utf-8",
                    )

                    test_case = self

                    class TerminalOutputExecutor:
                        def execute(self, step, context):
                            connection = context.connection_resolver.resolve("analytics")
                            test_case.assertEqual(connection.conn_type, "clickhouse")
                            return StepResult(
                                step_id=step.id,
                                step_type=step.type,
                                state=StepExecutionState.SUCCEEDED,
                                outputs={"row_count": 3},
                            )

                    with patch("zeta4s.cli.main.built_in_step_executor", return_value=TerminalOutputExecutor()):
                        result = runner.invoke(cli, ["run", "retail", "daily", "--profile", "dev"])

                    self.assertEqual(result.exit_code, 0, result.output)
                    report_path = cli_home() / "reports" / "retail" / "project-run.latest.json"
                    report = json.loads(report_path.read_text(encoding="utf-8"))
                    terminal_output = report["terminal_outputs"]["count_orders"]["row_count"]
                    self.assertEqual(terminal_output["step_id"], "count_orders")
                    self.assertEqual(terminal_output["output_name"], "row_count")
                    self.assertEqual(terminal_output["kind"], "scalar")
                    self.assertEqual(terminal_output["value"], 3)
                    self.assertEqual(
                        terminal_output["ref"],
                        {"kind": "scalar", "type": "int"},
                    )
                finally:
                    os.chdir(previous_cwd)

    def test_project_run_report_preserves_skipped_terminal_state(self) -> None:
        with TemporaryDirectory() as home_tmp, TemporaryDirectory() as cwd_tmp:
            account_home = Path(home_tmp)
            previous_cwd = Path.cwd()
            with patch.object(Path, "home", return_value=account_home):
                try:
                    os.chdir(cwd_tmp)
                    runner = CliRunner()
                    self.assertEqual(runner.invoke(cli, ["work", "init"]).exit_code, 0)
                    profile_path = Path(cwd_tmp) / "zeta4s-work" / "profiles" / "dev.yml"
                    profile_path.write_text(
                        yaml.safe_dump(
                            {"connections": {"analytics": {"type": "clickhouse"}}},
                            sort_keys=False,
                        ),
                        encoding="utf-8",
                    )
                    self.assertEqual(runner.invoke(cli, ["project", "init", "retail"]).exit_code, 0)
                    job_path = Path(cwd_tmp) / "zeta4s-work" / "projects" / "retail" / "jobs" / "daily.yml"
                    job_path.write_text(
                        yaml.safe_dump(
                            {
                                "job_id": "daily",
                                "steps": [
                                    {
                                        "step_id": "count_orders",
                                        "type": "sql.scalar",
                                        "conn": "analytics",
                                        "sql": "select 0",
                                        "outputs": {"row_count": {"kind": "scalar", "type": "int"}},
                                    },
                                    {
                                        "step_id": "publish_orders",
                                        "type": "noop",
                                        "depends_on": ["count_orders"],
                                        "when": {"expr": "$steps.count_orders.outputs.row_count > 0"},
                                    },
                                ],
                            },
                            sort_keys=False,
                        ),
                        encoding="utf-8",
                    )

                    class SkippedTerminalExecutor:
                        def execute(self, step, context):
                            return StepResult(
                                step_id=step.id,
                                step_type=step.type,
                                state=StepExecutionState.SUCCEEDED,
                                outputs={"row_count": 0} if step.id == "count_orders" else {},
                            )

                    with patch("zeta4s.cli.main.built_in_step_executor", return_value=SkippedTerminalExecutor()):
                        result = runner.invoke(cli, ["run", "retail", "daily", "--profile", "dev"])

                    self.assertEqual(result.exit_code, 0, result.output)
                    self.assertIn("[project][run] skipped: project=retail job=daily", result.output)
                    report_path = cli_home() / "reports" / "retail" / "project-run.latest.json"
                    report = json.loads(report_path.read_text(encoding="utf-8"))
                    self.assertEqual(report["status"], "skipped")
                    self.assertEqual(report["result_state"], "skipped")
                    self.assertEqual(report["summary"]["skipped"], 1)
                    self.assertEqual(report["steps"][1]["state"], "skipped")
                    self.assertEqual(report["events"][-1]["event_type"], "run_skipped")
                    self.assertEqual(
                        report["steps"][1]["skipped_reason"],
                        "when.expr evaluated false: $steps.count_orders.outputs.row_count > 0",
                    )
                finally:
                    os.chdir(previous_cwd)

    def test_project_run_report_preserves_failed_terminal_state(self) -> None:
        with TemporaryDirectory() as home_tmp, TemporaryDirectory() as cwd_tmp:
            account_home = Path(home_tmp)
            previous_cwd = Path.cwd()
            with patch.object(Path, "home", return_value=account_home):
                try:
                    os.chdir(cwd_tmp)
                    runner = CliRunner()
                    self.assertEqual(runner.invoke(cli, ["work", "init"]).exit_code, 0)
                    profile_path = Path(cwd_tmp) / "zeta4s-work" / "profiles" / "dev.yml"
                    profile_path.write_text(
                        yaml.safe_dump(
                            {"connections": {}},
                            sort_keys=False,
                        ),
                        encoding="utf-8",
                    )
                    self.assertEqual(runner.invoke(cli, ["project", "init", "retail"]).exit_code, 0)
                    job_path = Path(cwd_tmp) / "zeta4s-work" / "projects" / "retail" / "jobs" / "daily.yml"
                    job_path.write_text(
                        yaml.safe_dump(
                            {
                                "job_id": "daily",
                                "steps": [
                                    {"step_id": "publish_orders", "type": "noop"},
                                ],
                            },
                            sort_keys=False,
                        ),
                        encoding="utf-8",
                    )

                    class FailedTerminalExecutor:
                        def execute(self, step, context):
                            return StepResult(
                                step_id=step.id,
                                step_type=step.type,
                                state=StepExecutionState.FAILED,
                                failure=StepFailure("publish failed", type="PublishFailure"),
                            )

                    with patch("zeta4s.cli.main.built_in_step_executor", return_value=FailedTerminalExecutor()):
                        result = runner.invoke(cli, ["run", "retail", "daily", "--profile", "dev"])

                    self.assertEqual(result.exit_code, 1, result.output)
                    self.assertIn("[project][run] failed: project=retail job=daily", result.output)
                    report_path = cli_home() / "reports" / "retail" / "project-run.latest.json"
                    report = json.loads(report_path.read_text(encoding="utf-8"))
                    self.assertEqual(report["status"], "failed")
                    self.assertEqual(report["result_state"], "failed")
                    self.assertEqual(report["summary"]["failed"], 1)
                    self.assertEqual(report["steps"][0]["state"], "failed")
                    self.assertEqual(report["steps"][0]["failure"]["type"], "PublishFailure")
                    self.assertEqual(report["issues"][0]["message"], "publish failed")
                    self.assertEqual(report["events"][-1]["event_type"], "run_failed")
                finally:
                    os.chdir(previous_cwd)

    def test_api_deploy_sends_workspace_profile_without_secret_literals(self) -> None:
        with TemporaryDirectory() as home_tmp, TemporaryDirectory() as cwd_tmp:
            account_home = Path(home_tmp)
            previous_cwd = Path.cwd()
            captured: dict[str, object] = {}
            with patch.object(Path, "home", return_value=account_home):
                try:
                    os.chdir(cwd_tmp)
                    self.assertEqual(CliRunner().invoke(cli, ["work", "init"]).exit_code, 0)
                    self.assertEqual(
                        CliRunner()
                        .invoke(
                            cli,
                            ["api", "connect", "local", "--url", "http://localhost:8000", "--no-env-file"],
                        )
                        .exit_code,
                        0,
                    )
                    self.assertEqual(CliRunner().invoke(cli, ["project", "init", "retail"]).exit_code, 0)
                    workspace = Path(cwd_tmp) / "zeta4s-work"
                    profile_path = workspace / "profiles" / "dev.yml"
                    profile_path.write_text(
                        yaml.safe_dump(
                            {
                                "connections": {
                                    "analytics_clickhouse": {
                                        "type": "clickhouse",
                                        "host": "clickhouse",
                                        "port": 8123,
                                        "username": "metastore",
                                        "password_ref": "dev.analytics_clickhouse.password",
                                        "database": "default",
                                    }
                                }
                            },
                            sort_keys=False,
                        ),
                        encoding="utf-8",
                    )
                    project_root = workspace / "projects" / "retail"
                    (project_root / "jobs" / "daily.yml").write_text(
                        yaml.safe_dump(
                            {
                                "job_id": "daily",
                                "schedule": None,
                                "steps": [
                                    {
                                        "step_id": "select_one",
                                        "type": "clickhouse.sql",
                                        "conn": "analytics_clickhouse",
                                        "query": "sql/clickhouse/select_one.sql",
                                    }
                                ],
                            },
                            sort_keys=False,
                        ),
                        encoding="utf-8",
                    )
                    (project_root / "sql" / "clickhouse").mkdir(parents=True)
                    (project_root / "sql" / "clickhouse" / "select_one.sql").write_text("SELECT 1\n", encoding="utf-8")

                    def fake_stream_api_report(**kwargs):
                        captured.update(kwargs)
                        return {}, None, 0

                    with patch("zeta4s.cli.main._stream_api_report", side_effect=fake_stream_api_report):
                        result = CliRunner().invoke(cli, ["api", "deploy", "retail", "--profile", "dev"])

                    self.assertEqual(result.output, "")
                    self.assertIn("payload", captured)
                    payload = captured["payload"]
                    self.assertIsInstance(payload, dict)
                    self.assertEqual(captured["endpoint"], "/api/v1/deploy/stream")
                    self.assertEqual(captured["report_name"], "api-deploy")
                    self.assertEqual(captured["command"], "z4s api deploy")
                    self.assertEqual(payload["project_id"], "retail")
                    self.assertNotIn("project", payload)
                    self.assertEqual(payload["profile_id"], "dev")
                    self.assertNotIn("airflow_assets_config", payload)
                    serialized_payload = yaml.safe_dump(payload, sort_keys=True)
                    self.assertNotIn("password:", serialized_payload)
                    self.assertIn("password_ref:", serialized_payload)
                finally:
                    os.chdir(previous_cwd)

    def test_stream_api_report_keeps_parent_report_name_after_nested_reports(self) -> None:
        class FakeResponse:
            def __enter__(self):
                return iter(
                    [
                        (
                            b'{"event":"complete","report":{"status":"passed","nested_operations":'
                            b'[{"operation":"deploy","report_name":"api-deploy","report":{"status":"passed"}}]}}\n'
                        )
                    ]
                )

            def __exit__(self, exc_type, exc, tb):
                return False

        saved: list[tuple[str, str]] = []

        def fake_save_api_report(project_name, report_name, report, command):
            saved.append((report_name, command))
            return report, Path("/tmp") / f"{report_name}.latest.json"

        with (
            patch("zeta4s.cli.main._post_api_stream", return_value=FakeResponse()),
            patch(
                "zeta4s.cli.main._save_api_report",
                side_effect=fake_save_api_report,
            ),
            patch("zeta4s.cli.main._print_report_summary", return_value=0),
        ):
            _stream_api_report(
                prefix="[api][redeploy]",
                project_name="retail",
                report_name="api-redeploy",
                command="z4s api redeploy",
                api_alias="local",
                endpoint="/api/v1/redeploy/stream",
                payload={"project_id": "retail"},
            )

        self.assertEqual(saved, [("api-deploy", "z4s api deploy"), ("api-redeploy", "z4s api redeploy")])


if __name__ == "__main__":
    unittest.main()
