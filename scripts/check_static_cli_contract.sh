#!/usr/bin/env bash
set -euo pipefail

Z4S_BIN="${Z4S_BIN:-z4s}"
CONTRACT_PATHS=(src docs)
if [ -d projects ]; then
  CONTRACT_PATHS+=(projects)
fi
CANONICAL_SHOWCASE="zeta4s-work/projects/canonical_showcase"

forbidden_pattern='dbt_executable|dbt_parse_env|subprocess|tempfile'

if rg -n "$forbidden_pattern" src/zeta4s/dbt/model_contract.py; then
  echo "static CLI contract violation: project check dbt model contract must not invoke dbt or subprocess" >&2
  exit 1
fi

if find "${CONTRACT_PATHS[@]}" -name 'sources_raw.py' -o -name 'sources_raw.yml' -o -name 'sources_raw.yml' | grep -q .; then
  echo "static CLI contract violation: step graph must not generate or require sources_raw metadata" >&2
  exit 1
fi

if find src/zeta4s -path '*/__pycache__/sources_raw*.pyc' | grep -q .; then
  echo "static CLI contract violation: stale sources_raw bytecode must not remain under src/zeta4s" >&2
  exit 1
fi

if rg -n 'clickhouse\.load_raw|oracle\.load_staging|oracle\.load|elasticsearch\.load|external_lookup\.http' "${CONTRACT_PATHS[@]}" >/dev/null; then
  echo "static CLI contract violation: unsupported step type names must not be used" >&2
  exit 1
fi

if rg -n 'load\.raw_tables' src/zeta4s/runtime >/dev/null; then
  echo "static CLI contract violation: runtime messages must not use unsupported load.raw_tables paths" >&2
  exit 1
fi

if rg -n 'result_context\("external_lookup"|stage="external_lookup"|external_lookup\.(plan|progress|cleanup)' src/zeta4s/runtime >/dev/null; then
  echo "static CLI contract violation: http.lookup runtime result/events must not use external_lookup labels" >&2
  exit 1
fi

if ! rg -n 'DAG_MAX_ACTIVE_RUNS = 1' src/zeta4s/airflow/dag_generator.py >/dev/null; then
  echo "static CLI contract violation: generated DAG max_active_runs must be fixed to 1" >&2
  exit 1
fi

if ! rg -n '_assert_dag_runtime_invariants' src/zeta4s/airflow/dag_generator.py >/dev/null; then
  echo "static CLI contract violation: generated DAG runtime invariant check is required" >&2
  exit 1
fi

if rg -n 'from airflow.*(Asset|Dataset)|outlets\s*=' src/zeta4s/airflow/dag_generator.py >/dev/null; then
  echo "static CLI contract violation: DAG generator must not use Airflow Asset/Dataset/outlets APIs" >&2
  exit 1
fi

if rg -n '_STEP_GRAPH_ADAPTERS|def _bind_|def _step_graph_.*_task|from zeta4s\.airflow\.operators import|PythonOperator' src/zeta4s/airflow/dag_generator.py >/dev/null; then
  echo "static CLI contract violation: task 생성은 src/zeta4s/airflow/step_binding.py 에 있어야 한다" >&2
  exit 1
fi

if rg -n 'builtin_step_adapters|step_adapters' src/zeta4s/airflow/dag_generator.py >/dev/null; then
  echo "static CLI contract violation: DAG generator must bind every step generically, not via a per-type adapter registry" >&2
  exit 1
fi

if ! rg -n 'single_task_binding\(core_step_operator' src/zeta4s/airflow/dag_generator.py >/dev/null; then
  echo "static CLI contract violation: DAG generator must bind steps through generic core_step_operator" >&2
  exit 1
fi

if [ ! -f src/zeta4s/airflow/step_binding.py ]; then
  echo "static CLI contract violation: generic step binding module is required" >&2
  exit 1
fi

if rg -n '\.venv/bin/python' scripts src -g '!scripts/check_static_cli_contract.sh' >/dev/null; then
  echo "static CLI contract violation: release/runtime gates must not use user .venv" >&2
  exit 1
fi

if rg -n '/opt/zeta4s-src|PYTHONPATH=.*/src|PYTHONPATH: .*/src' docker-compose.yml docker/zeta4s-api >/dev/null; then
  echo "static CLI contract violation: runtime image/compose must use installed package, not source import paths" >&2
  exit 1
fi

coupled_airflow_image_key="ZETA4S_""AIRFLOW_IMAGE"
coupled_airflow_dockerfile="docker/airflow/""Dockerfile"
if rg -n "$coupled_airflow_image_key|$coupled_airflow_dockerfile" docker-compose.yml .env.example scripts tests \
  -g '!scripts/check_static_cli_contract.sh' >/dev/null; then
  echo "static CLI contract violation: zeta4s must not build or configure an Airflow-coupled image" >&2
  exit 1
fi

if sed -n '/^x-airflow-common:/,/^services:/p' docker-compose.yml \
  | rg -n 'zeta4s-state:/var/lib/zeta4s|AIRFLOW__SECRETS__BACKEND' >/dev/null; then
  echo "static CLI contract violation: official Airflow services must not mount or import zeta4s runtime state" >&2
  exit 1
fi

if ! rg -n 'api-run-summary' src/zeta4s/cli/main.py >/dev/null; then
  echo "static CLI contract violation: api run summary must write a release-gate report" >&2
  exit 1
fi

if ! rg -n -- '--stale-deployment' src/zeta4s/cli/main.py >/dev/null; then
  echo "static CLI contract violation: api run create must expose stale deployment handling" >&2
  exit 1
fi

if [ ! -d "$CANONICAL_SHOWCASE" ]; then
  echo "static CLI contract violation: canonical showcase project is required" >&2
  exit 1
fi

if find zeta4s-work/projects -mindepth 1 -maxdepth 1 ! -name '.gitkeep' ! -name 'canonical_showcase' | grep -q .; then
  echo "static CLI contract violation: zeta4s-work/projects must contain only canonical_showcase and .gitkeep" >&2
  exit 1
fi

for job_file in \
  retail_mart_dbt.yml \
  clickhouse_rowset_stage_verify.yml \
  oracle_rowset_stage_verify.yml \
  elasticsearch_rowset_dual_stage_verify.yml
do
  if [ ! -f "$CANONICAL_SHOWCASE/jobs/$job_file" ]; then
    echo "static CLI contract violation: canonical showcase job missing: $job_file" >&2
    exit 1
  fi
done

if find "$CANONICAL_SHOWCASE" -type d \( -name config -o -name assets -o -name docs -o -name tools \) | grep -q .; then
  echo "static CLI contract violation: canonical showcase must not contain config/assets/docs/tools directories" >&2
  exit 1
fi

if find "$CANONICAL_SHOWCASE" \( -name 'profiles.yml' -o -name 'profiles.yaml' -o -name 'sources_raw*' -o -name 'generated*' \) | grep -q .; then
  echo "static CLI contract violation: canonical showcase must not contain checked-in profiles or generated metadata" >&2
  exit 1
fi

if rg -n '\{\{|\}\}' "$CANONICAL_SHOWCASE/sql" >/dev/null; then
  echo "static CLI contract violation: canonical showcase SQL files must not use runtime SQL template helpers" >&2
  exit 1
fi

if rg -n 'render_sql_template|_render_sql_template|_preserve_runtime_sql_templates|SQL_TEMPLATE|unknown SQL template expression' src tests >/dev/null; then
  echo "static CLI contract violation: runtime SQL template helper code must not exist" >&2
  exit 1
fi

for endpoint in \
  '/api/v1/deploy/stream' \
  '/api/v1/redeploy/stream'
do
  if ! rg -n "$endpoint" src/zeta4s/api/app.py >/dev/null; then
    echo "static CLI contract violation: api operation progress stream endpoint missing: $endpoint" >&2
    exit 1
  fi
done

for endpoint in \
  '/api/v1/deploy/stream' \
  '/api/v1/redeploy/stream'
do
  if ! rg -n "$endpoint" src/zeta4s/cli/main.py >/dev/null; then
    echo "static CLI contract violation: z4s api CLI must consume progress stream endpoint: $endpoint" >&2
    exit 1
  fi
done

if ! rg -n 'format_display_time\(event\.get\("event_time"\)' src/zeta4s/cli/main.py >/dev/null; then
  echo "static CLI contract violation: api progress output must prefix event time in display timezone" >&2
  exit 1
fi

# secret 체계는 AESGCM256 하나다. 별도 runtime key 는 호출부가 없으므로 배포 표면에 두지
# 않는다. 생기면 배포마다 쓰지 않는 비밀을 요구하게 된다.
if rg -n 'ZETA4S_RUNTIME_KEY' src/zeta4s docker-compose.yml >/dev/null; then
  echo "static CLI contract violation: secret contract has no runtime key; do not add ZETA4S_RUNTIME_KEY" >&2
  exit 1
fi

if rg -n '/api/v1/secrets/runtime-key' src/zeta4s >/dev/null; then
  echo "static CLI contract violation: runtime-key endpoints were removed" >&2
  exit 1
fi

# Airflow 는 zeta4s package 를 import 하지 않는다. secrets backend 는 그 경계를 깨뜨린다.
if rg -n 'Zeta4sSecretsBackend|AIRFLOW__SECRETS__BACKEND' src/zeta4s docker-compose.yml >/dev/null; then
  echo "static CLI contract violation: Airflow must not load a zeta4s secrets backend" >&2
  exit 1
fi

# master keyring 은 runtime state volume 밖이다. 그 volume 은 Prefect worker 와 공유된다.
if rg -n 'DEFAULT_SECRET_MASTER_KEY_FILE = Path\("/var/lib/zeta4s/' src/zeta4s >/dev/null; then
  echo "static CLI contract violation: master keyring must live outside the shared runtime state volume" >&2
  exit 1
fi

uv run python -m compileall -q src/zeta4s
