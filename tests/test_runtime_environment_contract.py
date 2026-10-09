from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tomllib
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]


class RuntimeEnvironmentContractTest(unittest.TestCase):
    def test_api_distribution_installs_postgres_metastore_driver(self) -> None:
        root = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        dependency = "psycopg[binary]>=3.2,<4.0"

        for group in ("api", "all", "dev"):
            self.assertIn(dependency, root["project"]["optional-dependencies"][group])
        self.assertIn(dependency, root["dependency-groups"]["dev"])

    def test_install_cli_dry_run_accepts_empty_pip_options_on_macos_bash(self) -> None:
        result = subprocess.run(
            [
                "/bin/bash",
                str(ROOT / "scripts/install_cli.sh"),
                "--venv-dir",
                ".venv-contract-test",
                "--package",
                "dist/zeta4s.whl",
                "--no-upgrade-pip",
                "--dry-run",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, result.stderr)

    def test_build_images_dry_run_accepts_empty_platform_on_macos_bash(self) -> None:
        result = subprocess.run(
            [
                "/bin/bash",
                str(ROOT / "scripts/build_images.sh"),
                "--load",
                "--dry-run",
            ],
            cwd=ROOT,
            env={**os.environ, "ZETA4S_API_IMAGE": "zeta4s-api:test"},
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, result.stderr)

    def test_build_images_builds_only_zeta4s_api(self) -> None:
        result = subprocess.run(
            ["/bin/bash", str(ROOT / "scripts/build_images.sh"), "--load", "--dry-run"],
            cwd=ROOT,
            env={**os.environ, "ZETA4S_API_IMAGE": "zeta4s-api:test"},
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("docker/zeta4s-api/Dockerfile", result.stdout)
        self.assertNotIn("AIRFLOW_BASE_IMAGE", result.stdout)
        self.assertFalse((ROOT / "docker" / "airflow" / "Dockerfile").exists())

    def test_zeta4s_api_image_proves_airflow_is_absent(self) -> None:
        """이미지가 스스로 headless 임을 증명한다.

        누가 의존을 되살리면 배포된 뒤 조용히 결합이 돌아오는 대신 build 가 깨진다.
        """
        dockerfile = (ROOT / "docker/zeta4s-api/Dockerfile").read_text(encoding="utf-8")
        self.assertIn("import airflow", dockerfile)
        self.assertNotIn("apache/airflow", dockerfile)

    def test_engine_profiles_are_symmetric_and_neither_is_default(self) -> None:
        """engine 은 외부다. 어느 쪽도 기본이 아니고, engine 없이도 zeta4s-api 가 뜬다.

        Airflow 만 기본으로 뜨면 "교체 가능한 backend" 가 아니라 "Airflow 스택에 Prefect 를
        곁들인 것" 이 된다. 대칭은 배포에서 드러나야 한다.
        """
        compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
        services = compose["services"]

        def profiles(name: str) -> list[str]:
            return services[name].get("profiles") or []

        for name in (
            "airflow-auth-init",
            "airflow-init",
            "airflow-apiserver",
            "airflow-scheduler",
            "airflow-dag-processor",
        ):
            self.assertEqual(profiles(name), ["airflow"], name)
        for name in ("prefect-server", "prefect-worker"):
            self.assertEqual(profiles(name), ["prefect"], name)
        # Airflow 만 쓰는 metrics 경로다. engine 과 함께 뜨고 함께 사라져야 한다.
        self.assertEqual(profiles("statsd-exporter"), ["airflow"])

        default_services = sorted(name for name, spec in services.items() if not spec.get("profiles"))
        self.assertIn("zeta4s-api", default_services)
        for name in default_services:
            self.assertFalse(name.startswith(("airflow-", "prefect-")), f"{name} 이 기본으로 뜬다")

    def test_engines_do_not_leak_into_each_other(self) -> None:
        """Prefect engine 이 Airflow metastore DSN 을 입고 있던 것이 대칭이 깨진 자리였다."""
        compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
        worker = compose["services"]["prefect-worker"]
        state_init = compose["services"]["z4s-state-init"]

        self.assertEqual([key for key in worker["environment"] if key.startswith("AIRFLOW")], [])
        self.assertIn("--create-pool-if-not-found", worker["command"])
        self.assertEqual(
            state_init["image"],
            "${ZETA4S_API_IMAGE:-ghcr.io/zeta4lab/zeta4s-api:${ZETA4S_IMAGE_TAG:-latest}}",
        )
        # prefect-server 는 zeta4s 를 전혀 쓰지 않는다. 공식 이미지를 그대로 쓴다.
        self.assertIn("prefecthq/prefect", compose["services"]["prefect-server"]["image"])
        self.assertEqual(
            compose["services"]["prefect-server"]["environment"]["PREFECT_UI_API_URL"],
            "/api",
        )
        self.assertEqual(
            compose["services"]["prefect-server"]["environment"]["PREFECT_SERVER_UI_SHOW_PROMOTIONAL_CONTENT"],
            "false",
        )

        airflow = compose["x-airflow-common"]
        self.assertIn("apache/airflow", airflow["image"])
        self.assertNotIn("zeta4s-state:/var/lib/zeta4s", airflow["volumes"])
        self.assertIn("airflow-dags:/opt/airflow/dags", airflow["volumes"])
        self.assertIn("airflow-auth:/opt/airflow/auth", airflow["volumes"])

    def test_prefect_server_image_matches_the_sdk_pin(self) -> None:
        """공식 이미지를 쓰면 version 이 compose 와 wheel 두 곳에 생긴다.

        어긋나면 worker 와 server 가 다른 Prefect 가 되어 조용히 깨진다. 정본은 wheel 이다.
        """
        api_project = tomllib.loads((ROOT / "packages/zeta4s-api/pyproject.toml").read_text(encoding="utf-8"))
        pinned = next(dep for dep in api_project["project"]["dependencies"] if dep.startswith("prefect=="))
        sdk_version = pinned.split("==", 1)[1]

        root_project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        dev_pinned = next(dep for dep in root_project["dependency-groups"]["dev"] if dep.startswith("prefect=="))
        self.assertEqual(dev_pinned, pinned)

        env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
        self.assertIn(f"PREFECT_VERSION={sdk_version}", env_example)

        compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
        server_image = compose["services"]["prefect-server"]["image"]
        self.assertIn(f"${{PREFECT_VERSION:-{sdk_version}}}", server_image)

    def test_airflow_images_match_the_env_pin(self) -> None:
        env_values = {}
        for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines():
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                env_values[key] = value

        airflow_version = env_values["AIRFLOW_VERSION"]
        python_version = env_values["PYTHON_VERSION"]
        expected_image = f"apache/airflow:{airflow_version}-python{python_version}"
        self.assertEqual(env_values["AIRFLOW_IMAGE"], expected_image)

        compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
        for image in (compose["x-airflow-common"]["image"], compose["services"]["airflow-auth-init"]["image"]):
            self.assertIn(f"${{AIRFLOW_VERSION:-{airflow_version}}}", image)

        release = (ROOT / "scripts/check_release_runtime_showcases.sh").read_text(encoding="utf-8")
        self.assertIn(f"apache/airflow:{airflow_version}-python{python_version}", release)

    def test_cli_and_api_entrypoints_are_separate_distributions(self) -> None:
        root_project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        cli_project = tomllib.loads((ROOT / "packages/zeta4s-cli/pyproject.toml").read_text(encoding="utf-8"))
        api_project = tomllib.loads((ROOT / "packages/zeta4s-api/pyproject.toml").read_text(encoding="utf-8"))

        self.assertNotIn("scripts", root_project["project"])
        self.assertEqual(cli_project["project"]["scripts"], {"z4s": "zeta4s.cli.main:main"})
        self.assertEqual(api_project["project"]["scripts"], {"zeta4s-api": "zeta4s.api.app:main"})
        # version 은 src/zeta4s/__init__.py 하나에서만 온다. sub-package 가 zeta4s 를
        # 고정 version 으로 pin 하면 bump 때마다 어긋나 wheel 이 설치되지 않는다.
        self.assertIn("zeta4s[cli]", cli_project["project"]["dependencies"])
        self.assertIn("zeta4s[api]", api_project["project"]["dependencies"])
        self.assertEqual(root_project["project"]["dynamic"], ["version"])
        self.assertEqual(cli_project["project"]["dynamic"], ["version"])
        self.assertEqual(api_project["project"]["dynamic"], ["version"])
        self.assertNotIn("version", root_project["project"])

    def test_docker_compose_wires_generated_dag_api_boundary(self) -> None:
        self.assertFalse((ROOT / "docker-compose.dev.yml").exists())
        compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
        env = compose["x-airflow-common"]["environment"]
        api_env = compose["services"]["zeta4s-api"]["environment"]
        state_init_command = "\n".join(compose["services"]["z4s-state-init"]["command"])

        self.assertEqual(env["ZETA4S_API_INTERNAL_URL"], "http://zeta4s-api:8088")
        self.assertEqual(
            env["AIRFLOW__SCHEDULER__TASK_INSTANCE_HEARTBEAT_TIMEOUT"],
            "${AIRFLOW_TASK_INSTANCE_HEARTBEAT_TIMEOUT:-300}",
        )
        self.assertEqual(
            env["AIRFLOW__SCHEDULER__TASK_INSTANCE_HEARTBEAT_TIMEOUT_DETECTION_INTERVAL"],
            "${AIRFLOW_TASK_INSTANCE_HEARTBEAT_TIMEOUT_DETECTION_INTERVAL:-10}",
        )
        self.assertEqual(api_env["ZETA4S_API_HOME"], "/var/lib/zeta4s")
        self.assertEqual(api_env["ZETA4S_AIRFLOW_DAGS_DIR"], "/var/lib/zeta4s/airflow-dags")
        self.assertNotIn("AIRFLOW__SECRETS__BACKEND", env)
        self.assertNotIn("ZETA4S_HOME", env)
        self.assertNotIn("ZETA4S_HOME", api_env)
        self.assertEqual(
            env["ZETA4S_RUNTIME_INTERNAL_TOKEN"],
            "${ZETA4S_RUNTIME_INTERNAL_TOKEN:?Set ZETA4S_RUNTIME_INTERNAL_TOKEN by running scripts/configure_open_env.sh}",
        )
        self.assertIn("/var/lib/zeta4s-keyring/master.json", state_init_command)
        self.assertIn("init_master_key_file", state_init_command)
        self.assertIn("chmod 400 /var/lib/zeta4s-keyring/master.json", state_init_command)

    def test_master_keyring_is_isolated_from_shared_runtime_state(self) -> None:
        """keyring 은 zeta4s-api 만 받는다.

        runtime state volume 은 Prefect worker 와 공유된다. keyring 을 그 안에 두면
        worker 가 master key 를 읽을 수 있다. worker 는 step 실행을 internal API 로
        위임하므로 secret 을 직접 풀지 않는다 — 필요 없는 권한이다.
        """
        compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
        services = compose["services"]

        def volume_targets(service: str) -> list[str]:
            return [str(item).split(":")[1] for item in services[service].get("volumes", []) if ":" in str(item)]

        self.assertIn("/var/lib/zeta4s-keyring", volume_targets("zeta4s-api"))
        self.assertNotIn("/var/lib/zeta4s-keyring", volume_targets("prefect-worker"))

        api_keyring_mounts = [str(item) for item in services["zeta4s-api"]["volumes"] if "zeta4s-keyring" in str(item)]
        self.assertTrue(all(item.endswith(":ro") for item in api_keyring_mounts))

        # keyring 은 runtime state volume 경로 아래에 있으면 안 된다.
        state_targets = [target for target in volume_targets("zeta4s-api") if target.startswith("/var/lib/zeta4s/")]
        self.assertTrue(state_targets)  # runtime state mount 자체는 있어야 검사가 의미를 갖는다
        for target in volume_targets("zeta4s-api"):
            if "keyring" in target:
                self.assertFalse(target.startswith("/var/lib/zeta4s/"))

    def test_compose_uses_postgres_as_default_metastore_and_clickhouse_as_asset(self) -> None:
        compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
        airflow_env = compose["x-airflow-common"]["environment"]
        api_env = compose["services"]["zeta4s-api"]["environment"]

        self.assertEqual(api_env["ZETA4S_METASTORE_TYPE"], "${ZETA4S_METASTORE_TYPE:-postgres}")
        self.assertEqual(
            api_env["ZETA4S_METASTORE_DSN"],
            "${ZETA4S_METASTORE_DSN:-postgresql://airflow:airflow@postgres:5432/zeta4s_metastore}",
        )
        self.assertNotIn("ZETA4S_METASTORE_TYPE", airflow_env)
        self.assertNotIn("ZETA4S_METASTORE_DSN", airflow_env)
        self.assertEqual(
            airflow_env["AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL"],
            "${AIRFLOW_DAG_PROCESSOR_REFRESH_INTERVAL:-10}",
        )
        self.assertEqual(
            airflow_env["AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL"],
            "${AIRFLOW_DAG_PROCESSOR_MIN_FILE_PROCESS_INTERVAL:-10}",
        )
        self.assertEqual(compose["services"]["metastore"]["profiles"], ["asset"])
        self.assertEqual(
            compose["services"]["postgres"]["image"],
            "${POSTGRES_IMAGE:-postgres:${POSTGRES_VERSION:-18.4-alpine}}",
        )

        for service_name in (
            "airflow-init",
            "airflow-apiserver",
            "airflow-scheduler",
            "airflow-dag-processor",
            "zeta4s-api",
            "prefect-worker",
        ):
            self.assertNotIn("metastore", compose["services"][service_name].get("depends_on", {}))

    def test_postgres_init_creates_zeta4s_databases(self) -> None:
        init_sql = (ROOT / "docker/postgres/01-create-databases.sql").read_text(encoding="utf-8")

        self.assertIn("CREATE DATABASE prefect", init_sql)
        self.assertIn("CREATE DATABASE zeta4s_metastore", init_sql)
        self.assertIn("CREATE DATABASE lakekeeper", init_sql)
        self.assertEqual(init_sql.count("\\gexec"), 3)

    def test_checkpoint_compose_profile_pins_and_isolates_iceberg_services(self) -> None:
        compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
        services = compose["services"]

        expected_images = {
            "lakekeeper": "${LAKEKEEPER_IMAGE:-quay.io/lakekeeper/catalog:v0.13.1}",
            "lakekeeper-migrate": "${LAKEKEEPER_IMAGE:-quay.io/lakekeeper/catalog:v0.13.1}",
            "minio": "${MINIO_IMAGE:-quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z}",
            "minio-init": "${MINIO_CLIENT_IMAGE:-quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z}",
        }
        for service_name, image in expected_images.items():
            service = services[service_name]
            self.assertEqual(service["image"], image)
            self.assertEqual(service["profiles"], ["checkpoint"])
            self.assertNotIn(":latest", image)

        self.assertIn("healthcheck", services["lakekeeper"])
        self.assertIn("healthcheck", services["minio"])
        self.assertIn("minio-data:/data", services["minio"]["volumes"])
        self.assertIn("minio-data", compose["volumes"])
        self.assertEqual(services["lakekeeper-init"]["profiles"], ["checkpoint"])
        self.assertEqual(services["lakekeeper-init"]["restart"], "no")
        self.assertIn("MINIO_ROOT_USER", services["lakekeeper-init"]["environment"])
        self.assertIn("MINIO_ROOT_PASSWORD", services["lakekeeper-init"]["environment"])
        lakekeeper_init = "\n".join(services["lakekeeper-init"]["command"])
        self.assertIn('"endpoint":"http://minio:19002"', lakekeeper_init)
        self.assertIn('"sts-endpoint":"http://minio:19002"', lakekeeper_init)
        self.assertNotIn("minio.localhost", lakekeeper_init)

        api_env = compose["services"]["zeta4s-api"]["environment"]
        self.assertEqual(
            api_env["ZETA4S_ICEBERG_CATALOG_URI"],
            "${ZETA4S_ICEBERG_CATALOG_URI:-http://lakekeeper:8181/catalog}",
        )
        self.assertEqual(api_env["ZETA4S_ICEBERG_WAREHOUSE"], "${ZETA4S_ICEBERG_WAREHOUSE:-warehouse}")
        self.assertEqual(
            api_env["ZETA4S_ROWSET_CHECKPOINT_TARGET_BYTES"],
            "${ZETA4S_ROWSET_CHECKPOINT_TARGET_BYTES:-134217728}",
        )
        self.assertNotIn("asset", services["lakekeeper"]["profiles"])
        self.assertNotIn("asset", services["minio"]["profiles"])

    def test_release_reliability_scripts_use_profile_api_deploy_contract(self) -> None:
        reliability = (ROOT / "scripts/check_runtime_reliability.sh").read_text(encoding="utf-8")
        release = (ROOT / "scripts/check_release_runtime_showcases.sh").read_text(encoding="utf-8")
        install_cli = (ROOT / "scripts/install_cli.sh").read_text(encoding="utf-8")
        cli_main = (ROOT / "src/zeta4s/cli/main.py").read_text(encoding="utf-8")

        self.assertIn('PROFILE_ID="${PROFILE_ID:-airflow}"', reliability)
        self.assertIn('PROFILE_ID="${PROFILE_ID:-airflow}"', release)
        self.assertIn(
            'COMPOSE_PROFILES="${COMPOSE_PROFILES:-asset external-example-http-api checkpoint ${PROFILE_ID}}"',
            release,
        )
        self.assertIn('PREFECT_PORT="${PREFECT_PORT:-24200}"', release)
        self.assertIn('"PREFECT_PORT=${PREFECT_PORT}"', release)
        self.assertIn('reset_compose_args=("${compose_args[@]}" --profile airflow --profile prefect)', release)
        self.assertIn('"${reset_compose_args[@]}" down -v --remove-orphans', release)
        self.assertNotIn('"$Z4S_BIN" schedule ', release)
        self.assertIn("release_services=(", release)
        self.assertIn('"${compose_args[@]}" run --rm lakekeeper-init', release)
        self.assertIn('"${compose_args[@]}" up -d --wait "${release_services[@]}"', release)
        self.assertIn('CHECKPOINT_RECOVERY_GATE="${CHECKPOINT_RECOVERY_GATE:-1}"', release)
        self.assertIn("checkpoint_recovery_watchdog", release)
        self.assertIn("order by sequence desc,created_at desc limit 1", release)
        self.assertIn('CHECKPOINT_RECOVERY_PAUSE_SECONDS="${CHECKPOINT_RECOVERY_PAUSE_SECONDS:-8}"', release)
        self.assertIn("checkpoint_step_is_running", release)
        self.assertIn('docker pause "$es_container"', release)
        self.assertIn('sleep "$CHECKPOINT_RECOVERY_PAUSE_SECONDS"', release)
        self.assertNotIn("kill -KILL", release)
        self.assertIn('docker unpause "$es_container"', release)
        self.assertIn('JOBS="elasticsearch_checkpoint_recovery"', release)
        self.assertIn("checkpoint recovery evidence passed", release)
        self.assertIn("PYTHON_BIN=\"$(uv run python -c 'import sys; print(sys.executable)')\"", release)
        self.assertIn('PROJECT="${PROJECT:-zeta4s-work/projects/canonical_showcase}"', reliability)
        self.assertIn('PROJECT="${PROJECT:-zeta4s-work/projects/canonical_showcase}"', release)
        self.assertIn(
            'JOBS="${JOBS:-retail_mart_dbt clickhouse_rowset_stage_verify oracle_rowset_stage_verify elasticsearch_rowset_dual_stage_verify}"',
            reliability,
        )
        self.assertIn(
            'JOBS="${JOBS:-retail_mart_dbt clickhouse_rowset_stage_verify oracle_rowset_stage_verify elasticsearch_rowset_dual_stage_verify}"',
            release,
        )
        self.assertIn('"ZETA4S_METASTORE_TYPE=postgres"', release)
        self.assertIn('"ZETA4S_METASTORE_DSN=postgresql://airflow:airflow@postgres:5432/zeta4s_metastore"', release)
        self.assertIn('"POSTGRES_VERSION=18.4-alpine"', release)
        self.assertIn('"AIRFLOW_TASK_INSTANCE_HEARTBEAT_TIMEOUT=10"', release)
        self.assertIn('"AIRFLOW_TASK_INSTANCE_HEARTBEAT_TIMEOUT_DETECTION_INTERVAL=2"', release)
        self.assertIn('PROFILE_FILE="${PROFILE_FILE:-}"', release)
        self.assertIn('RELEASE_WORKSPACE="${RELEASE_WORKSPACE:-}"', release)
        self.assertIn('RELEASE_HOME="${RELEASE_HOME:-}"', release)
        self.assertIn("compose_env_value()", release)
        self.assertIn('"$PYTHON_BIN" - "$COMPOSE_ENV_FILE" "$1"', release)
        self.assertIn("if not path.is_file():", release)
        self.assertIn('name, value = line.split("=", 1)', release)
        self.assertIn('RELEASE_CLICKHOUSE_PASSWORD="$(compose_env_value METASTORE_PASSWORD || true)"', release)
        self.assertIn('RELEASE_CLICKHOUSE_PASSWORD="${RELEASE_CLICKHOUSE_PASSWORD:-metastore_pwd}"', release)
        self.assertIn('RELEASE_ORACLE_PASSWORD="${RELEASE_ORACLE_PASSWORD:-showcase_src}"', release)
        self.assertIn('RELEASE_API_TOKEN="${RELEASE_API_TOKEN:-${ZETA4S_API_TOKEN:-}}"', release)
        self.assertIn('RELEASE_API_TOKEN="$(compose_env_value ZETA4S_API_TOKEN || true)"', release)
        self.assertIn('export ZETA4S_API_TOKEN="$RELEASE_API_TOKEN"', release)
        self.assertIn('"$Z4S_BIN" api bootstrap --api "$API_ALIAS"', release)
        self.assertIn('"$Z4S_BIN" api status --api "$API_ALIAS"', release)
        self.assertIn('"$Z4S_BIN" profile check "$PROFILE_ID" --api "$API_ALIAS"', release)
        self.assertLess(
            release.rindex("\nbootstrap_release_secrets\n"),
            release.rindex('"$Z4S_BIN" profile check "$PROFILE_ID" --api "$API_ALIAS"'),
        )
        self.assertIn("--token-env ZETA4S_API_TOKEN --no-env-file", release)
        self.assertIn('EXAMPLE_HTTP_API_PORT="${EXAMPLE_HTTP_API_PORT:-28099}"', release)
        self.assertIn('"EXAMPLE_HTTP_API_PORT=${EXAMPLE_HTTP_API_PORT}"', release)
        self.assertIn('release_tmp_root="$(mktemp -d "${TMPDIR:-/tmp}/zeta4s-release-gate.XXXXXX")"', release)
        self.assertIn("trap cleanup_release_workspace EXIT", release)
        self.assertIn('prepare_release_workspace "$release_project_id" "$resolved_profile_file"', release)
        self.assertIn('"$Z4S_BIN" api bootstrap --api "$API_ALIAS"', release)
        self.assertIn("bootstrap_release_secrets", release)
        self.assertIn('"$Z4S_BIN" api secret set "$secret_key" --api "$API_ALIAS"', release)
        self.assertIn('"$Z4S_BIN" api secret check "$secret_key" --api "$API_ALIAS"', release)
        self.assertIn('Z4S_BIN="$(cd "$(dirname "$Z4S_BIN")" && pwd)/$(basename "$Z4S_BIN")"', release)
        self.assertIn('ZETA4S_CLI_HOME="$release_home_abs" "$Z4S_BIN" work init "$workspace_name"', release)
        self.assertIn('ZETA4S_CLI_HOME="$release_home_abs" "$Z4S_BIN" project init "$project_id_value"', release)
        self.assertIn('ZETA4S_CLI_HOME="$release_home_abs" "$Z4S_BIN" profile init "$PROFILE_ID"', release)
        self.assertIn('RELEASE_DIST_DIR="${RELEASE_DIST_DIR:-.zeta4s/release-gate/dist}"', release)
        self.assertIn("clean_build_artifacts", release)
        self.assertIn("src/zeta4s.egg-info", release)
        self.assertIn("packages/zeta4s-cli/zeta4s_cli.egg-info", release)
        self.assertIn("packages/zeta4s-api/zeta4s_api.egg-info", release)
        self.assertIn('uv build --wheel --out-dir "$RELEASE_DIST_DIR"', release)
        self.assertIn('uv build --wheel --out-dir "$RELEASE_DIST_DIR" packages/zeta4s-cli', release)
        self.assertIn('--package "$zeta4s_wheel"', release)
        self.assertIn('--package "$zeta4s_cli_wheel"', release)
        self.assertNotIn("WHEELHOUSE", release)
        self.assertNotIn("--find-links", release)
        self.assertNotIn("--no-index", release)
        self.assertIn('ZETA4S_CLI_HOME="$(cd "$RELEASE_HOME" && pwd)" "$Z4S_BIN" api connect', release)
        self.assertIn('PROJECT="${RELEASE_WORKSPACE}/projects/${release_project_id}"', release)
        self.assertIn('ZETA4S_CLI_HOME="$(cd "$RELEASE_HOME" && pwd)"', release)
        self.assertNotIn(' HOME="$(cd "$RELEASE_HOME" && pwd)"', release)
        self.assertNotIn(' HOME="$release_home_abs"', release)
        self.assertIn('workspace_root="$("${python_cmd[@]}" - "$PROJECT"', reliability)
        self.assertIn("python_cmd=(uv run python)", reliability)
        self.assertIn('"$Z4S_BIN" project check "$project_id" --profile "$PROFILE_ID"', reliability)
        self.assertIn('"$Z4S_BIN" api deploy "$project_id" --profile "$PROFILE_ID"', reliability)
        self.assertIn('"$Z4S_BIN" api run create "$project_id" "$job"', reliability)
        self.assertNotIn('"$Z4S_BIN" runtime', reliability)
        self.assertNotIn("work use", reliability)
        self.assertNotIn("pip install -e", release)
        self.assertIn("extras=(cli)", install_cli)
        self.assertIn("packages/zeta4s-cli", install_cli)
        self.assertIn("zeta4s-cli zeta4s", install_cli)
        self.assertIn("INSTALL_TARGETS", install_cli)
        self.assertNotIn("--find-links", install_cli)
        self.assertNotIn("--no-index", install_cli)
        self.assertFalse((ROOT / "docker" / "airflow" / "Dockerfile").exists())
        self.assertIn("report_ref = latest.relative_to(cli_home())", cli_main)
        self.assertNotIn("latest.relative_to(_workspace_root())", cli_main)

    def test_wheel_gate_cleans_stale_build_outputs_before_building(self) -> None:
        wheel_gate = (ROOT / "scripts/check_wheel_install.sh").read_text(encoding="utf-8")

        cleanup = wheel_gate.index("rm -rf")
        build = wheel_gate.index('uv build --wheel --out-dir "$DIST" .')
        self.assertLess(cleanup, build)
        self.assertIn("  build \\", wheel_gate)
        self.assertIn("  src/zeta4s.egg-info \\", wheel_gate)

    def test_api_bootstrap_public_surface_exists(self) -> None:
        cli_main = (ROOT / "src/zeta4s/cli/main.py").read_text(encoding="utf-8")
        api_app = (ROOT / "src/zeta4s/api/app.py").read_text(encoding="utf-8")

        self.assertIn('@api.command("bootstrap")', cli_main)
        self.assertIn('"/api/v1/platform/bootstrap"', cli_main)
        self.assertIn('"api-bootstrap"', cli_main)
        self.assertIn('@api.command("status")', cli_main)
        self.assertIn('"/api/v1/platform/status"', cli_main)
        self.assertIn('@app.post("/api/v1/platform/bootstrap")', api_app)
        self.assertIn('@app.get("/api/v1/platform/status")', api_app)
        self.assertIn("adapter.bootstrap()", api_app)

    def test_airflow_dag_source_uses_internal_api_without_zeta4s_import(self) -> None:
        source = (ROOT / "src/zeta4s/airflow/dag_source.py").read_text(encoding="utf-8")
        template = source.split("_SOURCE_TEMPLATE =", 1)[1]

        self.assertIn("/internal/v1/runtime/steps/execute", template)
        self.assertIn("/internal/v1/runtime/runs/finalize", template)
        self.assertNotIn("from zeta4s", template)
        self.assertNotIn("import zeta4s", template)

    def test_cli_run_surface_uses_project_run_endpoints(self) -> None:
        cli_main = (ROOT / "src/zeta4s/cli/main.py").read_text(encoding="utf-8")
        api_app = (ROOT / "src/zeta4s/api/app.py").read_text(encoding="utf-8")

        self.assertNotIn('"/api/v1/dag/', cli_main)
        self.assertNotIn('f"/api/v1/dag/', cli_main)
        self.assertNotIn('@app.post("/api/v1/dag/', api_app)
        self.assertNotIn('@app.get("/api/v1/dag/', api_app)
        self.assertIn("/api/v1/projects/{urllib.parse.quote(project_name, safe='')}/runs", cli_main)
        self.assertIn('@app.post("/api/v1/projects/{project_id}/jobs/{job_id}/runs")', api_app)
        self.assertIn('@app.get("/api/v1/projects/{project_id}/runs/{run_id}/tasks")', api_app)
        self.assertIn('@app.get("/api/v1/projects/{project_id}/runs/{run_id}/logs")', api_app)
        self.assertIn('@app.post("/api/v1/projects/{project_id}/runs/{run_id}/cancel")', api_app)
        self.assertIn("_inspect_metastore_schema(adapter)", api_app)

    def test_showcase_projects_live_under_default_workspace_layout(self) -> None:
        workspace = ROOT / "zeta4s-work"
        projects = workspace / "projects"
        profile = workspace / "profiles" / "airflow.yml"
        canonical = projects / "canonical_showcase"

        self.assertFalse((ROOT / "projects").exists())
        self.assertTrue(projects.is_dir())
        self.assertTrue(profile.is_file())
        self.assertFalse(list(projects.glob("*/assets/airflow.yml")))
        self.assertEqual(
            sorted(path.name for path in projects.iterdir()),
            [".gitkeep", "canonical_showcase"],
        )
        self.assertTrue((canonical / "project.yml").is_file())
        self.assertEqual(
            sorted(path.name for path in (canonical / "jobs").glob("*.yml")),
            [
                "clickhouse_rowset_stage_verify.yml",
                "elasticsearch_checkpoint_recovery.yml",
                "elasticsearch_rowset_dual_stage_verify.yml",
                "oracle_rowset_stage_verify.yml",
                "retail_mart_dbt.yml",
                "scheduler_failure.yml",
                "scheduler_noop.yml",
            ],
        )
        self.assertFalse((canonical / "config").exists())
        self.assertFalse((canonical / "assets").exists())
        self.assertFalse((canonical / "docs").exists())
        self.assertFalse((canonical / "tools").exists())
        self.assertFalse(list(canonical.rglob("profiles.yml")))
        self.assertFalse(list(canonical.rglob("profiles.yaml")))
        self.assertFalse(list(canonical.rglob("sources_raw*")))
        self.assertFalse(list(canonical.rglob("generated*")))

        profile_text = profile.read_text(encoding="utf-8")
        data = yaml.safe_load(profile_text) or {}
        self.assertIsInstance(data.get("connections"), dict)
        self.assertNotIn("password:", profile_text)


if __name__ == "__main__":
    unittest.main()
