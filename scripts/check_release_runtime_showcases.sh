#!/usr/bin/env bash
set -euo pipefail

Z4S_BIN="${Z4S_BIN:-z4s}"
PYTHON_BIN="${PYTHON_BIN:-}"
PROFILE_ID="${PROFILE_ID:-airflow}"
PROFILE_FILE="${PROFILE_FILE:-}"
API_ALIAS="${API_ALIAS:-release-gate}"
COMPOSE_ENV_FILE="${COMPOSE_ENV_FILE:-.env}"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-zeta4s_release_gate}"
COMPOSE_PROFILES="${COMPOSE_PROFILES:-asset external-example-http-api checkpoint ${PROFILE_ID}}"
BUILD_IMAGE="${BUILD_IMAGE:-1}"
START_STACK="${START_STACK:-1}"
RESET_STACK="${RESET_STACK:-0}"
CHECKPOINT_RECOVERY_GATE="${CHECKPOINT_RECOVERY_GATE:-1}"
CHECKPOINT_RECOVERY_PAUSE_SECONDS="${CHECKPOINT_RECOVERY_PAUSE_SECONDS:-8}"
HOST_CLI_VENV="${HOST_CLI_VENV:-.venv-z4s-release-gate}"
RELEASE_DIST_DIR="${RELEASE_DIST_DIR:-.zeta4s/release-gate/dist}"
RELEASE_WORKSPACE="${RELEASE_WORKSPACE:-}"
RELEASE_HOME="${RELEASE_HOME:-}"
RELEASE_ORACLE_PASSWORD="${RELEASE_ORACLE_PASSWORD:-showcase_src}"
ZETA4S_API_HOST_PORT="${ZETA4S_API_HOST_PORT:-28088}"
AIRFLOW_PORT="${AIRFLOW_PORT:-28080}"
PREFECT_PORT="${PREFECT_PORT:-24200}"
METASTORE_HTTP_PORT="${METASTORE_HTTP_PORT:-28123}"
METASTORE_TCP_PORT="${METASTORE_TCP_PORT:-29000}"
POSTGRES_PORT="${POSTGRES_PORT:-25432}"
ORACLE_PORT="${ORACLE_PORT:-21521}"
ELASTICSEARCH_PORT="${ELASTICSEARCH_PORT:-29200}"
EXAMPLE_HTTP_API_PORT="${EXAMPLE_HTTP_API_PORT:-28099}"
STATSD_EXPORTER_PORT="${STATSD_EXPORTER_PORT:-29102}"
STATSD_EXPORTER_STATSD_PORT="${STATSD_EXPORTER_STATSD_PORT:-29125}"
PROMETHEUS_PORT="${PROMETHEUS_PORT:-29090}"
ALERTMANAGER_PORT="${ALERTMANAGER_PORT:-29093}"
LAKEKEEPER_PORT="${LAKEKEEPER_PORT:-28181}"
MINIO_API_PORT="${MINIO_API_PORT:-29002}"
MINIO_CONSOLE_PORT="${MINIO_CONSOLE_PORT:-29092}"

if [[ -z "$PYTHON_BIN" ]]; then
  PYTHON_BIN="$(uv run python -c 'import sys; print(sys.executable)')"
fi

compose_env_value() {
  "$PYTHON_BIN" - "$COMPOSE_ENV_FILE" "$1" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
key = sys.argv[2]
if not path.is_file():
    raise SystemExit(1)
for raw_line in path.read_text(encoding="utf-8").splitlines():
    line = raw_line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    name, value = line.split("=", 1)
    if name.strip() != key:
        continue
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1]
    print(value)
    raise SystemExit(0)
raise SystemExit(1)
PY
}

if [[ -z "${RELEASE_CLICKHOUSE_PASSWORD:-}" ]]; then
  RELEASE_CLICKHOUSE_PASSWORD="${METASTORE_PASSWORD:-}"
fi
if [[ -z "$RELEASE_CLICKHOUSE_PASSWORD" ]]; then
  RELEASE_CLICKHOUSE_PASSWORD="$(compose_env_value METASTORE_PASSWORD || true)"
fi
RELEASE_CLICKHOUSE_PASSWORD="${RELEASE_CLICKHOUSE_PASSWORD:-metastore_pwd}"

RELEASE_API_TOKEN="${RELEASE_API_TOKEN:-${ZETA4S_API_TOKEN:-}}"
if [[ -z "$RELEASE_API_TOKEN" ]]; then
  RELEASE_API_TOKEN="$(compose_env_value ZETA4S_API_TOKEN || true)"
fi
if [[ -n "$RELEASE_API_TOKEN" ]]; then
  export ZETA4S_API_TOKEN="$RELEASE_API_TOKEN"
fi

PROJECT="${PROJECT:-zeta4s-work/projects/canonical_showcase}"
JOBS="${JOBS:-retail_mart_dbt clickhouse_rowset_stage_verify oracle_rowset_stage_verify elasticsearch_rowset_dual_stage_verify}"

release_tmp_root=""
cleanup_release_workspace() {
  if [[ -n "$release_tmp_root" ]]; then
    rm -rf "$release_tmp_root"
  fi
}
trap cleanup_release_workspace EXIT

if [[ -z "$RELEASE_WORKSPACE" ]]; then
  release_tmp_root="$(mktemp -d "${TMPDIR:-/tmp}/zeta4s-release-gate.XXXXXX")"
  RELEASE_WORKSPACE="${release_tmp_root}/zeta4s-work"
  if [[ -z "$RELEASE_HOME" ]]; then
    RELEASE_HOME="${release_tmp_root}/home"
  fi
elif [[ -z "$RELEASE_HOME" ]]; then
  RELEASE_HOME=".zeta4s/release-gate/home"
fi

# version 의 single source of truth 는 src/zeta4s/__init__.py 다. pyproject 는
# dynamic version 으로 이 값을 읽으므로 pyproject 에서 읽을 수 없다.
project_version() {
  "$PYTHON_BIN" - <<'PY'
import ast
from pathlib import Path

tree = ast.parse(Path("src/zeta4s/__init__.py").read_text(encoding="utf-8"))
for node in tree.body:
    if isinstance(node, ast.Assign) and any(
        isinstance(target, ast.Name) and target.id == "__version__" for target in node.targets
    ):
        print(node.value.value)
        break
else:
    raise SystemExit("__version__ not found in src/zeta4s/__init__.py")
PY
}

wheel_path() {
  "$PYTHON_BIN" - "$RELEASE_DIST_DIR" "$1" <<'PY'
from pathlib import Path
import sys
dist_dir = Path(sys.argv[1])
pattern = sys.argv[2]
wheels = sorted(dist_dir.glob(pattern), key=lambda path: path.stat().st_mtime)
if not wheels:
    raise SystemExit(f"no wheel matched: {pattern}")
print(wheels[-1])
PY
}

clean_build_artifacts() {
  rm -rf \
    build \
    src/zeta4s.egg-info \
    packages/zeta4s-cli/build \
    packages/zeta4s-cli/zeta4s_cli.egg-info \
    packages/zeta4s-api/build \
    packages/zeta4s-api/zeta4s_api.egg-info
}

project_id() {
  "$PYTHON_BIN" - "$PROJECT" <<'PY'
from pathlib import Path
import sys
import yaml

root = Path(sys.argv[1])
manifest = root / "project.yml"
if not manifest.exists():
    manifest = root / "project.yaml"
data = yaml.safe_load(manifest.read_text(encoding="utf-8")) or {}
print(data.get("project_id") or root.name)
PY
}

resolve_profile_file() {
  "$PYTHON_BIN" - "$PROJECT" "$PROFILE_ID" "$PROFILE_FILE" <<'PY'
from pathlib import Path
import sys

project = Path(sys.argv[1]).resolve()
profile_id = sys.argv[2]
explicit = sys.argv[3]
candidates = []
if explicit:
    candidates.append(Path(explicit))
workspace = project.parent.parent if project.parent.name == "projects" else None
if workspace is not None:
    candidates.extend([
        workspace / "profiles" / f"{profile_id}.yml",
        workspace / "profiles" / f"{profile_id}.yaml",
    ])
for path in candidates:
    if path.is_file():
        print(path.resolve())
        raise SystemExit(0)
raise SystemExit("PROFILE_FILE is required unless PROJECT is under a workspace with the requested profile")
PY
}

prepare_release_workspace() {
  local project_id_value="$1"
  local profile_file_value="$2"
  local workspace_parent
  local workspace_name
  local release_home_abs
  local workspace_abs

  workspace_parent="$(dirname "$RELEASE_WORKSPACE")"
  workspace_name="$(basename "$RELEASE_WORKSPACE")"
  release_home_abs="$(mkdir -p "$RELEASE_HOME" && cd "$RELEASE_HOME" && pwd)"
  mkdir -p "$workspace_parent"
  workspace_parent="$(cd "$workspace_parent" && pwd)"
  workspace_abs="${workspace_parent}/${workspace_name}"

  rm -rf "$RELEASE_WORKSPACE"
  (
    cd "$workspace_parent"
    ZETA4S_CLI_HOME="$release_home_abs" "$Z4S_BIN" work init "$workspace_name"
  )
  ZETA4S_CLI_HOME="$release_home_abs" "$Z4S_BIN" project init "$project_id_value"
  ZETA4S_CLI_HOME="$release_home_abs" "$Z4S_BIN" profile init "$PROFILE_ID"
  rm -rf "${workspace_abs}/projects/${project_id_value}"
  cp -R "$PROJECT" "${workspace_abs}/projects/${project_id_value}"
  cp "$profile_file_value" "${workspace_abs}/profiles/${PROFILE_ID}.yml"

  if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i '' -e "s|^api_endpoint: .*|api_endpoint: http://127.0.0.1:${ZETA4S_API_HOST_PORT}|" "${workspace_abs}/profiles/${PROFILE_ID}.yml"
  else
    sed -i -e "s|^api_endpoint: .*|api_endpoint: http://127.0.0.1:${ZETA4S_API_HOST_PORT}|" "${workspace_abs}/profiles/${PROFILE_ID}.yml"
  fi
}

bootstrap_release_secrets() {
  "$PYTHON_BIN" - "${RELEASE_WORKSPACE}/profiles/${PROFILE_ID}.yml" "$RELEASE_CLICKHOUSE_PASSWORD" "$RELEASE_ORACLE_PASSWORD" <<'PY' |
from pathlib import Path
import sys
import yaml

profile_path = Path(sys.argv[1])
clickhouse_password = sys.argv[2]
oracle_password = sys.argv[3]
profile = yaml.safe_load(profile_path.read_text(encoding="utf-8")) or {}
for item in (profile.get("connections") or {}).values():
    secret_key = item.get("password_ref")
    if not secret_key:
        continue
    connection_type = item.get("type")
    if connection_type == "clickhouse":
        print(f"{secret_key}\t{clickhouse_password}")
    elif connection_type == "oracle":
        print(f"{secret_key}\t{oracle_password}")
    else:
        raise SystemExit(f"unsupported password_ref connection type for release gate: {connection_type}")
PY
  while IFS=$'\t' read -r secret_key secret_value; do
    if [[ -z "$secret_key" ]]; then
      continue
    fi
    printf '%s' "$secret_value" | ZETA4S_CLI_HOME="$(cd "$RELEASE_HOME" && pwd)" "$Z4S_BIN" api secret set "$secret_key" --api "$API_ALIAS" >/dev/null
    ZETA4S_CLI_HOME="$(cd "$RELEASE_HOME" && pwd)" "$Z4S_BIN" api secret check "$secret_key" --api "$API_ALIAS" >/dev/null
  done
}

compose_env=(
  "COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME}"
  "ZETA4S_METASTORE_TYPE=postgres"
  "ZETA4S_METASTORE_DSN=postgresql://airflow:airflow@postgres:5432/zeta4s_metastore"
  "POSTGRES_VERSION=18.4-alpine"
  "AIRFLOW_TASK_INSTANCE_HEARTBEAT_TIMEOUT=10"
  "AIRFLOW_TASK_INSTANCE_HEARTBEAT_TIMEOUT_DETECTION_INTERVAL=2"
  "AIRFLOW_IMAGE=${AIRFLOW_IMAGE:-apache/airflow:3.3.0-python3.12}"
  "ZETA4S_API_IMAGE=${ZETA4S_API_IMAGE:-zeta4s-api:$(project_version)}"
  "ZETA4S_API_HOST_PORT=${ZETA4S_API_HOST_PORT}"
  "AIRFLOW_PORT=${AIRFLOW_PORT}"
  "PREFECT_PORT=${PREFECT_PORT}"
  "METASTORE_HTTP_PORT=${METASTORE_HTTP_PORT}"
  "METASTORE_TCP_PORT=${METASTORE_TCP_PORT}"
  "POSTGRES_PORT=${POSTGRES_PORT}"
  "ORACLE_PORT=${ORACLE_PORT}"
  "ELASTICSEARCH_PORT=${ELASTICSEARCH_PORT}"
  "EXAMPLE_HTTP_API_PORT=${EXAMPLE_HTTP_API_PORT}"
  "STATSD_EXPORTER_PORT=${STATSD_EXPORTER_PORT}"
  "STATSD_EXPORTER_STATSD_PORT=${STATSD_EXPORTER_STATSD_PORT}"
  "PROMETHEUS_PORT=${PROMETHEUS_PORT}"
  "ALERTMANAGER_PORT=${ALERTMANAGER_PORT}"
  "LAKEKEEPER_PORT=${LAKEKEEPER_PORT}"
  "MINIO_API_PORT=${MINIO_API_PORT}"
  "MINIO_CONSOLE_PORT=${MINIO_CONSOLE_PORT}"
  "ZETA4S_ROWSET_CHECKPOINT_TARGET_BYTES=${ZETA4S_ROWSET_CHECKPOINT_TARGET_BYTES:-1}"
)

compose_args=(docker compose --env-file "$COMPOSE_ENV_FILE")
for profile in $COMPOSE_PROFILES; do
  compose_args+=(--profile "$profile")
done

# backend 전환 뒤에도 이전 scheduler service와 shared volume이 남지 않아야 한다.
# reset은 선택한 profile뿐 아니라 두 scheduler profile을 모두 활성화해 내린다.
reset_compose_args=("${compose_args[@]}" --profile airflow --profile prefect)

# one-shot init services are dependency gates, not --wait targets.
release_services=()
while IFS= read -r service; do
  case "$service" in
    airflow-init|z4s-state-init|lakekeeper-migrate|lakekeeper-init|minio-init) ;;
    *) release_services+=("$service") ;;
  esac
done < <(env "${compose_env[@]}" "${compose_args[@]}" config --services)

checkpoint_step_is_running() {
  local postgres_container="$1"
  local run_id="$2"
  local result
  result="$(docker exec "$postgres_container" psql -U airflow -d zeta4s_metastore -Atc \
    "select exists (
       select 1 from step_execution
       where run_id='${run_id}'
         and step_id='extract_recovery_documents'
         and status='running'
     );" 2>/dev/null || true)"
  [[ "$result" == "t" ]]
}

checkpoint_recovery_watchdog() {
  local started_at="$1"
  local evidence_file="$2"
  local postgres_container es_container row sequence run_id
  postgres_container="$(env "${compose_env[@]}" "${compose_args[@]}" ps -q postgres)"
  es_container="$(env "${compose_env[@]}" "${compose_args[@]}" ps -q elasticsearch)"
  for _ in $(seq 1 6000); do
    row="$(docker exec "$postgres_container" psql -U airflow -d zeta4s_metastore -Atc \
      "select sequence,run_id from step_checkpoint where job_id='elasticsearch_checkpoint_recovery' and step_id='extract_recovery_documents' and created_at >= '${started_at}'::timestamptz order by sequence desc,created_at desc limit 1;" 2>/dev/null || true)"
    sequence="${row%%|*}"
    if [[ "${sequence:-0}" -ge 2 ]]; then
      run_id="${row#*|}"
      # 두 scheduler 모두 core step을 zeta4s-api에서 실행한다. Airflow worker PID를
      # 죽이면 HTTP caller만 사라지고 실제 runtime attempt에는 장애가 주입되지 않는다.
      if [[ -n "$es_container" ]] && checkpoint_step_is_running "$postgres_container" "$run_id"; then
        docker pause "$es_container" >/dev/null
        sleep "$CHECKPOINT_RECOVERY_PAUSE_SECONDS"
        docker unpause "$es_container" >/dev/null || true
        printf '%s\n' "$run_id" >"$evidence_file"
        return 0
      fi
    fi
    sleep 0.01
  done
  return 1
}

verify_checkpoint_recovery() {
  local run_id="$1"
  local postgres_container result
  postgres_container="$(env "${compose_env[@]}" "${compose_args[@]}" ps -q postgres)"
  result="$(docker exec "$postgres_container" psql -U airflow -d zeta4s_metastore -Atc "
    with checkpoints as (
      select attempt, sequence, rows from step_checkpoint
      where run_id='${run_id}' and step_id='extract_recovery_documents'
    ), evidence as (
      select
        min(sequence) = 1
        and max(sequence) = count(*)
        and max(attempt) >= 2
        and max(rows) = 12
        and exists (
          select 1 from step_output_binding
          where run_id='${run_id}' and step_id='extract_recovery_documents'
            and output_name='recovery_rows'
            and binding->'value'->>'storage'='iceberg'
            and (binding->'value'->>'rows')::integer=12
        )
        and 1 = (
          select count(*) from step_execution
          where run_id='${run_id}' and step_id='confirm_recovery' and status='success'
        ) as passed
      from checkpoints
    ) select case when passed then 'passed' else 'failed' end from evidence;")"
  [[ "$result" == "passed" ]]
}

rm -rf "$RELEASE_DIST_DIR"
mkdir -p "$RELEASE_DIST_DIR"
clean_build_artifacts
uv build --wheel --out-dir "$RELEASE_DIST_DIR"
uv build --wheel --out-dir "$RELEASE_DIST_DIR" packages/zeta4s-cli
zeta4s_wheel="$(wheel_path 'zeta4s-*.whl')"
zeta4s_cli_wheel="$(wheel_path 'zeta4s_cli-*.whl')"
release_project_id="$(project_id)"
resolved_profile_file="$(resolve_profile_file)"

bash scripts/install_cli.sh \
  --venv-dir "$HOST_CLI_VENV" \
  --package "$zeta4s_wheel" \
  --package "$zeta4s_cli_wheel" \
  --no-upgrade-pip
Z4S_BIN="${HOST_CLI_VENV}/bin/z4s"
Z4S_BIN="$(cd "$(dirname "$Z4S_BIN")" && pwd)/$(basename "$Z4S_BIN")"
prepare_release_workspace "$release_project_id" "$resolved_profile_file"

if [[ "$BUILD_IMAGE" == "1" ]]; then
  env "${compose_env[@]}" bash scripts/build_images.sh --env-file /dev/null --load
fi

env "${compose_env[@]}" "${compose_args[@]}" config > /tmp/zeta4s-release-gate-compose.yml
if rg -n '/opt/zeta4s-src|src/zeta4s' /tmp/zeta4s-release-gate-compose.yml >/dev/null; then
  echo "release stack config must not reference source paths" >&2
  exit 1
fi

if [[ "$START_STACK" == "1" ]]; then
  if [[ "$RESET_STACK" == "1" ]]; then
    env "${compose_env[@]}" "${reset_compose_args[@]}" down -v --remove-orphans
  fi
  env "${compose_env[@]}" "${compose_args[@]}" run --rm z4s-state-init
  if printf '%s\n' $COMPOSE_PROFILES | rg -x 'checkpoint' >/dev/null; then
    env "${compose_env[@]}" "${compose_args[@]}" run --rm lakekeeper-init
  fi
  env "${compose_env[@]}" "${compose_args[@]}" up -d --wait "${release_services[@]}"
fi

if [[ -n "$RELEASE_API_TOKEN" ]]; then
  ZETA4S_API_TOKEN="$RELEASE_API_TOKEN" ZETA4S_CLI_HOME="$(cd "$RELEASE_HOME" && pwd)" \
    "$Z4S_BIN" api connect "$API_ALIAS" --url "http://127.0.0.1:${ZETA4S_API_HOST_PORT}" --token-env ZETA4S_API_TOKEN --no-env-file
else
  ZETA4S_CLI_HOME="$(cd "$RELEASE_HOME" && pwd)" "$Z4S_BIN" api connect "$API_ALIAS" --url "http://127.0.0.1:${ZETA4S_API_HOST_PORT}" --no-env-file
fi
ZETA4S_CLI_HOME="$(cd "$RELEASE_HOME" && pwd)" "$Z4S_BIN" api bootstrap --api "$API_ALIAS"
ZETA4S_CLI_HOME="$(cd "$RELEASE_HOME" && pwd)" "$Z4S_BIN" api status --api "$API_ALIAS"
bootstrap_release_secrets
ZETA4S_CLI_HOME="$(cd "$RELEASE_HOME" && pwd)" "$Z4S_BIN" profile check "$PROFILE_ID" --api "$API_ALIAS"

PROJECT="${RELEASE_WORKSPACE}/projects/${release_project_id}" \
  PROFILE_ID="$PROFILE_ID" \
  API_ALIAS="$API_ALIAS" \
  JOBS="$JOBS" \
  ASSET_JOBS="${ASSET_JOBS:-}" \
  Z4S_BIN="$Z4S_BIN" \
  PYTHON_BIN="$PYTHON_BIN" \
  ZETA4S_CLI_HOME="$(cd "$RELEASE_HOME" && pwd)" \
  bash scripts/check_runtime_reliability.sh


if [[ "$CHECKPOINT_RECOVERY_GATE" == "1" ]] && printf '%s\n' $COMPOSE_PROFILES | rg -x 'checkpoint' >/dev/null; then
  checkpoint_started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  checkpoint_evidence_file="$(mktemp "${TMPDIR:-/tmp}/zeta4s-checkpoint-run.XXXXXX")"
  checkpoint_recovery_watchdog "$checkpoint_started_at" "$checkpoint_evidence_file" &
  checkpoint_watchdog_pid=$!
  PROJECT="${RELEASE_WORKSPACE}/projects/${release_project_id}" \
    PROFILE_ID="$PROFILE_ID" \
    API_ALIAS="$API_ALIAS" \
    JOBS="elasticsearch_checkpoint_recovery" \
    ASSET_JOBS="" \
    Z4S_BIN="$Z4S_BIN" \
    PYTHON_BIN="$PYTHON_BIN" \
    ZETA4S_CLI_HOME="$(cd "$RELEASE_HOME" && pwd)" \
    bash scripts/check_runtime_reliability.sh
  wait "$checkpoint_watchdog_pid"
  checkpoint_run_id="$(cat "$checkpoint_evidence_file")"
  rm -f "$checkpoint_evidence_file"
  verify_checkpoint_recovery "$checkpoint_run_id"
  echo "[release-gate] checkpoint recovery evidence passed: run_id=${checkpoint_run_id}"
fi
