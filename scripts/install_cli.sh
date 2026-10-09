#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  bash scripts/install_cli.sh [--env-file FILE] [--venv-dir DIR] [--package TARGET]... [--index-url URL] [--trusted-host HOST] [--with-dev] [--dry-run]

Install the zeta4s CLI into a uv-managed Python virtual environment.

Default behavior:
  - create or reuse .venv
  - install this checkout as an editable package through uv pip
  - verify the z4s command

Options:
  --env-file FILE       Load pip environment variables from FILE.
  --venv-dir DIR        Virtual environment directory. Default: .venv
  --package TARGET      Install TARGET instead of editable current checkout. Repeatable.
                        Examples: dist/zeta4s-<version>-py3-none-any.whl
                                  dist/zeta4s_cli-<version>-py3-none-any.whl
  --index-url URL       pip index URL. Overrides PIP_INDEX_URL.
  --extra-index-url URL pip extra index URL. Overrides PIP_EXTRA_INDEX_URL.
  --trusted-host HOST   pip trusted host. Overrides PIP_TRUSTED_HOST.
  --with-dev            Install editable checkout with dev extras.
  --no-upgrade-pip      Do not upgrade pip before installing zeta4s.
  --no-verify           Do not run z4s --help after install.
  --dry-run             Print commands without executing them.
  -h, --help            Show this help.

Environment:
  PIP_INDEX_URL
  PIP_EXTRA_INDEX_URL
  PIP_TRUSTED_HOST
  PYTHON_BIN            Override uv Python request. Default: 3.12.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

ENV_FILE=""
VENV_DIR=".venv"
INSTALL_TARGETS=()
WITH_DEV=0
UPGRADE_PIP=1
VERIFY=1
DRY_RUN=0
CLI_PIP_INDEX_URL=""
CLI_PIP_EXTRA_INDEX_URL=""
CLI_PIP_TRUSTED_HOST=""

require_option_value() {
  local option="$1"
  local value="${2:-}"
  if [[ -z "$value" || "$value" == --* ]]; then
    echo "Missing value for ${option}" >&2
    exit 1
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --env-file)
      require_option_value "$1" "${2:-}"
      ENV_FILE="$2"
      shift 2
      ;;
    --venv-dir)
      require_option_value "$1" "${2:-}"
      VENV_DIR="$2"
      shift 2
      ;;
    --package)
      require_option_value "$1" "${2:-}"
      INSTALL_TARGETS+=("$2")
      shift 2
      ;;
    --index-url)
      require_option_value "$1" "${2:-}"
      CLI_PIP_INDEX_URL="$2"
      shift 2
      ;;
    --extra-index-url)
      require_option_value "$1" "${2:-}"
      CLI_PIP_EXTRA_INDEX_URL="$2"
      shift 2
      ;;
    --trusted-host)
      require_option_value "$1" "${2:-}"
      CLI_PIP_TRUSTED_HOST="$2"
      shift 2
      ;;
    --with-dev)
      WITH_DEV=1
      shift
      ;;
    --no-upgrade-pip)
      UPGRADE_PIP=0
      shift
      ;;
    --no-verify)
      VERIFY=0
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

cd "$REPO_ROOT"

if [[ -n "$ENV_FILE" ]]; then
  if [[ ! -f "$ENV_FILE" ]]; then
    echo "Env file not found: ${ENV_FILE}" >&2
    exit 1
  fi
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

PYTHON_BIN="${PYTHON_BIN:-}"
PIP_INDEX_URL="${CLI_PIP_INDEX_URL:-${PIP_INDEX_URL:-}}"
PIP_EXTRA_INDEX_URL="${CLI_PIP_EXTRA_INDEX_URL:-${PIP_EXTRA_INDEX_URL:-}}"
PIP_TRUSTED_HOST="${CLI_PIP_TRUSTED_HOST:-${PIP_TRUSTED_HOST:-}}"

UNAME_S="$(uname -s)"
case "$UNAME_S" in
  MINGW*|MSYS*|CYGWIN*)
    HOST_OS="windows"
    ;;
  Linux*)
    HOST_OS="linux"
    ;;
  Darwin*)
    HOST_OS="macos"
    ;;
  *)
    HOST_OS="unknown"
    ;;
esac

if ! command -v uv >/dev/null 2>&1; then
  echo "uv was not found. Install uv and ensure it is on PATH." >&2
  exit 1
fi

UV_PYTHON_REQUEST="${PYTHON_BIN:-3.12}"

run_cmd() {
  if [[ "$DRY_RUN" == "1" ]]; then
    printf '+'
    printf ' %q' "$@"
    printf '\n'
  else
    "$@"
  fi
}

ensure_venv_python() {
  if [[ "$DRY_RUN" == "1" ]]; then
    return 0
  fi

  if [[ ! -f "$venv_python" ]]; then
    echo "Virtualenv Python was not created: ${venv_python}" >&2
    echo "Remove ${VENV_DIR} and rerun this script." >&2
    exit 1
  fi

  if [[ "$HOST_OS" == "windows" ]]; then
    chmod +x "${VENV_DIR}"/Scripts/*.exe 2>/dev/null || true
  fi

  if [[ ! -x "$venv_python" ]]; then
    echo "Virtualenv Python is not executable: ${venv_python}" >&2
    echo "Remove ${VENV_DIR} and rerun this script. On Windows, run it from Git Bash against a writable checkout path." >&2
    exit 1
  fi
}

if [[ "$HOST_OS" == "windows" ]]; then
  venv_python="${VENV_DIR}/Scripts/python.exe"
  venv_z4s="${VENV_DIR}/Scripts/z4s.exe"
  activate_hint="source ${VENV_DIR}/Scripts/activate"
else
  venv_python="${VENV_DIR}/bin/python"
  venv_z4s="${VENV_DIR}/bin/z4s"
  activate_hint="source ${VENV_DIR}/bin/activate"
fi

uv_pip_cmd=(uv pip)
pip_options=()

if [[ -n "$PIP_INDEX_URL" ]]; then
  pip_options+=(--index-url "$PIP_INDEX_URL")
fi

if [[ -n "$PIP_EXTRA_INDEX_URL" ]]; then
  pip_options+=(--extra-index-url "$PIP_EXTRA_INDEX_URL")
fi

if [[ -n "$PIP_TRUSTED_HOST" ]]; then
  pip_options+=(--trusted-host "$PIP_TRUSTED_HOST")
fi

run_uv_pip_install() {
  if [[ "${#pip_options[@]}" -gt 0 ]]; then
    run_cmd "${uv_pip_cmd[@]}" install "${pip_options[@]}" "$@"
  else
    run_cmd "${uv_pip_cmd[@]}" install "$@"
  fi
}

echo "[install_cli] detected host: ${HOST_OS} (${UNAME_S})" >&2
echo "[install_cli] venv python: ${venv_python}" >&2
echo "[install_cli] activate: ${activate_hint}" >&2

if [[ ! -f "$venv_python" ]]; then
  run_cmd uv venv --python "$UV_PYTHON_REQUEST" "$VENV_DIR"
fi
ensure_venv_python

if [[ "$UPGRADE_PIP" == "1" ]]; then
  run_uv_pip_install --python "$venv_python" --upgrade pip
fi

run_cmd "${uv_pip_cmd[@]}" uninstall --python "$venv_python" zeta4s-cli zeta4s

if [[ "${#INSTALL_TARGETS[@]}" -gt 0 ]]; then
  if [[ "$WITH_DEV" == "1" ]]; then
    echo "--with-dev is only supported for editable checkout install." >&2
    exit 1
  fi
  run_uv_pip_install --python "$venv_python" "${INSTALL_TARGETS[@]}"
else
  editable_target="."
  extras=(cli)
  if [[ "$WITH_DEV" == "1" ]]; then
    extras+=(dev)
  fi
  if [[ "${#extras[@]}" -gt 0 ]]; then
    old_ifs="$IFS"
    IFS=,
    editable_target=".[${extras[*]}]"
    IFS="$old_ifs"
  fi
  run_uv_pip_install --python "$venv_python" -e "$editable_target"
  run_cmd "${uv_pip_cmd[@]}" install --python "$venv_python" --no-deps -e packages/zeta4s-cli
fi

if [[ "$VERIFY" == "1" ]]; then
  run_cmd "$venv_z4s" --help
fi
