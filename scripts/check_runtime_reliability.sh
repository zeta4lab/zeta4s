#!/usr/bin/env bash
set -euo pipefail

Z4S_BIN="${Z4S_BIN:-z4s}"
PYTHON_BIN="${PYTHON_BIN:-}"
PROJECT="${PROJECT:-zeta4s-work/projects/canonical_showcase}"
PROFILE_ID="${PROFILE_ID:-airflow}"
API_ALIAS="${API_ALIAS:-local}"
JOBS="${JOBS:-retail_mart_dbt clickhouse_rowset_stage_verify oracle_rowset_stage_verify elasticsearch_rowset_dual_stage_verify}"
ASSET_JOBS="${ASSET_JOBS:-}"
POLL_TIMEOUT_SECONDS="${POLL_TIMEOUT_SECONDS:-900}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-5}"
SKIP_DAG_RUNS="${SKIP_DAG_RUNS:-0}"
STATIC_ONLY="${STATIC_ONLY:-0}"
STALE_DEPLOYMENT="${STALE_DEPLOYMENT:-fail}"

if [[ -z "$PYTHON_BIN" ]]; then
  python_cmd=(uv run python)
else
  # shellcheck disable=SC2206
  python_cmd=($PYTHON_BIN)
fi

if [[ -z "$PROJECT" || -z "$PROFILE_ID" || -z "$JOBS" ]]; then
  echo "PROJECT, PROFILE_ID, and JOBS must be set explicitly" >&2
  exit 1
fi

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
started_at_iso="$(date -u +%Y-%m-%dT%H:%M:%S%z)"
project_id="$("${python_cmd[@]}" - "$PROJECT" <<'PY'
import sys
from pathlib import Path
import yaml

root = Path(sys.argv[1])
manifest = root / "project.yml"
if not manifest.exists():
    manifest = root / "project.yaml"
data = yaml.safe_load(manifest.read_text(encoding="utf-8")) or {}
print(data.get("project_id") or root.name)
PY
)"
workspace_root="$("${python_cmd[@]}" - "$PROJECT" <<'PY'
import sys
from pathlib import Path

root = Path(sys.argv[1]).resolve()
projects_dir = root.parent
workspace = projects_dir.parent
if projects_dir.name != "projects" or not (workspace / "profiles").is_dir():
    raise SystemExit("PROJECT must be under a workspace projects/ directory with sibling profiles/")
print(workspace)
PY
)"
evidence_dir=".zeta4s/reliability/${project_id}/${timestamp}"
mkdir -p "$evidence_dir"

run_cmd() {
  local name="$1"
  shift
  local output="${evidence_dir}/${name}.txt"
  printf '[reliability] %s\n' "$*" | tee -a "${evidence_dir}/manifest.txt"
  "$@" >"$output" 2>&1
}

run_yaml_cmd() {
  local name="$1"
  shift
  local output="${evidence_dir}/${name}.yml"
  printf '[reliability] %s\n' "$*" | tee -a "${evidence_dir}/manifest.txt"
  "$@" >"$output" 2>&1
}

yaml_field() {
  local path="$1"
  local field="$2"
  "${python_cmd[@]}" - "$path" "$field" <<'PY'
import sys
import yaml

path, field = sys.argv[1], sys.argv[2]
data = yaml.safe_load(open(path, encoding="utf-8")) or {}
value = data
for part in field.split("."):
    if not isinstance(value, dict):
        value = None
        break
    value = value.get(part)
if value is None:
    raise SystemExit(1)
print(value)
PY
}

write_manifest() {
  {
    printf 'timestamp=%s\n' "$timestamp"
    printf 'started_at=%s\n' "$started_at_iso"
    printf 'project=%s\n' "$PROJECT"
    printf 'project_id=%s\n' "$project_id"
    printf 'workspace=%s\n' "$workspace_root"
    printf 'profile_id=%s\n' "$PROFILE_ID"
    printf 'api_alias=%s\n' "$API_ALIAS"
    printf 'jobs=%s\n' "$JOBS"
    printf 'asset_jobs=%s\n' "$ASSET_JOBS"
    printf 'z4s_bin=%s\n' "$Z4S_BIN"
    printf 'python_cmd=%s\n' "${python_cmd[*]}"
    printf 'poll_timeout_seconds=%s\n' "$POLL_TIMEOUT_SECONDS"
    printf 'poll_interval_seconds=%s\n' "$POLL_INTERVAL_SECONDS"
    printf 'static_only=%s\n' "$STATIC_ONLY"
    printf 'stale_deployment=%s\n' "$STALE_DEPLOYMENT"
    printf '\n'
  } >"${evidence_dir}/manifest.txt"
}

wait_for_success() {
  local job="$1"
  local run_id="$2"
  local deadline=$((SECONDS + POLL_TIMEOUT_SECONDS))
  local status_file="${evidence_dir}/${job}_status.yml"
  local summary_file="${evidence_dir}/${job}_summary.yml"
  local tasks_file="${evidence_dir}/${job}_tasks.yml"
  local logs_file="${evidence_dir}/${job}_failed_logs.yml"
  local state=""

  while (( SECONDS < deadline )); do
    "$Z4S_BIN" api run status "$project_id" "$job" --run-id "$run_id" --api "$API_ALIAS" >"$status_file" 2>&1 || true
    state="$(yaml_field "$status_file" state 2>/dev/null || true)"
    if [[ "$state" == "succeeded" ]]; then
      "$Z4S_BIN" api run summary "$project_id" "$job" --run-id "$run_id" --api "$API_ALIAS" >"$summary_file" 2>&1
      "$Z4S_BIN" api run tasks "$project_id" "$job" --run-id "$run_id" --api "$API_ALIAS" >"$tasks_file" 2>&1
      "$Z4S_BIN" api run logs "$project_id" "$job" --run-id "$run_id" --failed-only --latest-attempt-only --api "$API_ALIAS" >"$logs_file" 2>&1 || true
      return 0
    fi
    if [[ "$state" == "failed" || "$state" == "upstream_failed" ]]; then
      "$Z4S_BIN" api run summary "$project_id" "$job" --run-id "$run_id" --api "$API_ALIAS" >"$summary_file" 2>&1 || true
      "$Z4S_BIN" api run tasks "$project_id" "$job" --run-id "$run_id" --api "$API_ALIAS" >"$tasks_file" 2>&1 || true
      "$Z4S_BIN" api run logs "$project_id" "$job" --run-id "$run_id" --failed-only --latest-attempt-only --api "$API_ALIAS" >"$logs_file" 2>&1 || true
      printf '[reliability] DAG failed: job=%s run_id=%s state=%s\n' "$job" "$run_id" "$state" >&2
      return 1
    fi
    sleep "$POLL_INTERVAL_SECONDS"
  done

  "$Z4S_BIN" api run tasks "$project_id" "$job" --run-id "$run_id" --api "$API_ALIAS" >"$tasks_file" 2>&1 || true
  printf '[reliability] run timeout: job=%s run_id=%s last_state=%s\n' "$job" "$run_id" "${state:-unknown}" >&2
  return 1
}

run_job() {
  local job="$1"
  local run_file="${evidence_dir}/${job}_run.yml"
  "$Z4S_BIN" api run create "$project_id" "$job" --stale-deployment "$STALE_DEPLOYMENT" --api "$API_ALIAS" >"$run_file" 2>&1
  local run_id
  run_id="$(yaml_field "$run_file" run_id)"
  printf '[reliability] triggered job=%s run_id=%s\n' "$job" "$run_id" | tee -a "${evidence_dir}/manifest.txt"
  wait_for_success "$job" "$run_id"
}

latest_asset_run() {
  local path="$1"
  local job="$2"
  "${python_cmd[@]}" - "$path" "$job" "$started_at_iso" <<'PY'
from datetime import datetime
import sys
import yaml

path, job, started_at = sys.argv[1], sys.argv[2], sys.argv[3]
started = datetime.fromisoformat(started_at)
data = yaml.safe_load(open(path, encoding="utf-8")) or {}
for run in data.get("runs") or []:
    if run.get("job_name") != job or run.get("source") != "airflow":
        continue
    created_at = run.get("created_at")
    if not created_at:
        continue
    created = datetime.fromisoformat(created_at)
    if created < started:
        continue
    print(run.get("run_id") or "")
    print(run.get("state") or "")
    raise SystemExit(0)
raise SystemExit(1)
PY
}

wait_for_asset_job() {
  local job="$1"
  local deadline=$((SECONDS + POLL_TIMEOUT_SECONDS))
  local runs_file="${evidence_dir}/${job}_asset_runs.yml"
  local asset_file="${evidence_dir}/${job}_asset_latest.txt"
  local run_id=""
  local state=""

  while (( SECONDS < deadline )); do
    "$Z4S_BIN" api run list "$project_id" --api "$API_ALIAS" >"$runs_file" 2>&1 || true
    if latest_asset_run "$runs_file" "$job" >"$asset_file" 2>/dev/null; then
      run_id="$(sed -n '1p' "$asset_file")"
      state="$(sed -n '2p' "$asset_file")"
      printf '[reliability] asset job=%s run_id=%s state=%s\n' "$job" "$run_id" "${state:-unknown}" | tee -a "${evidence_dir}/manifest.txt"
      if [[ "$state" == "succeeded" ]]; then
        "$Z4S_BIN" api run summary "$project_id" "$job" --run-id "$run_id" --api "$API_ALIAS" >"${evidence_dir}/${job}_asset_summary.yml" 2>&1
        "$Z4S_BIN" api run tasks "$project_id" "$job" --run-id "$run_id" --api "$API_ALIAS" >"${evidence_dir}/${job}_asset_tasks.yml" 2>&1
        "$Z4S_BIN" api run logs "$project_id" "$job" --run-id "$run_id" --failed-only --latest-attempt-only --api "$API_ALIAS" >"${evidence_dir}/${job}_asset_failed_logs.yml" 2>&1 || true
        return 0
      fi
      if [[ "$state" == "failed" || "$state" == "upstream_failed" ]]; then
        "$Z4S_BIN" api run summary "$project_id" "$job" --run-id "$run_id" --api "$API_ALIAS" >"${evidence_dir}/${job}_asset_summary.yml" 2>&1 || true
        "$Z4S_BIN" api run tasks "$project_id" "$job" --run-id "$run_id" --api "$API_ALIAS" >"${evidence_dir}/${job}_asset_tasks.yml" 2>&1 || true
        "$Z4S_BIN" api run logs "$project_id" "$job" --run-id "$run_id" --failed-only --latest-attempt-only --api "$API_ALIAS" >"${evidence_dir}/${job}_asset_failed_logs.yml" 2>&1 || true
        printf '[reliability] asset DAG failed: job=%s run_id=%s state=%s\n' "$job" "$run_id" "$state" >&2
        return 1
      fi
    fi
    sleep "$POLL_INTERVAL_SECONDS"
  done

  printf '[reliability] asset DAG timeout: job=%s last_state=%s\n' "$job" "${state:-unknown}" >&2
  return 1
}

write_manifest

run_cmd compileall "${python_cmd[@]}" -m compileall -q src/zeta4s
run_cmd git_diff_check git diff --check
run_cmd project_check "$Z4S_BIN" project check "$project_id" --profile "$PROFILE_ID"

if [[ "$STATIC_ONLY" == "1" ]]; then
  printf '[reliability] STATIC_ONLY=1: api deploy/run gates skipped evidence_dir=%s\n' "$evidence_dir" | tee -a "${evidence_dir}/manifest.txt"
  exit 0
fi

run_cmd api_deploy "$Z4S_BIN" api deploy "$project_id" --profile "$PROFILE_ID"

if [[ "$SKIP_DAG_RUNS" != "1" ]]; then
  for job in $JOBS; do
    run_job "$job"
  done
  for job in $ASSET_JOBS; do
    wait_for_asset_job "$job"
  done
else
  printf '[reliability] SKIP_DAG_RUNS=1: DAG execution gate skipped\n' | tee -a "${evidence_dir}/manifest.txt"
fi

printf '[reliability] passed evidence_dir=%s\n' "$evidence_dir" | tee -a "${evidence_dir}/manifest.txt"
