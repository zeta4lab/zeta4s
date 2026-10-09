#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  bash scripts/configure_open_env.sh [options]

Generate an open-network local .env file from .env.example.

Options:
  --output FILE        Output env file. Default: .env
  --template FILE      Template file. Default: .env.example
  --force              Overwrite output if it already exists.
  --dry-run            Print generated content to stdout.
  -h, --help           Show this help.

Examples:
  bash scripts/configure_open_env.sh --force
  bash scripts/configure_open_env.sh --output .env.dev --force
EOF
}

OUTPUT_FILE=".env"
TEMPLATE_FILE=".env.example"
AIRFLOW_UID="50000"
ZETA4S_RUNTIME_UID="50000"
FORCE=0
DRY_RUN=0

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
    --output)
      require_option_value "$1" "${2:-}"
      OUTPUT_FILE="${2:-}"
      shift 2
      ;;
    --template)
      require_option_value "$1" "${2:-}"
      TEMPLATE_FILE="${2:-}"
      shift 2
      ;;
    --force)
      FORCE=1
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

if [[ -z "$OUTPUT_FILE" ]]; then
  echo "--output must not be empty" >&2
  exit 1
fi

if [[ ! -f "$TEMPLATE_FILE" ]]; then
  echo "Template file not found: ${TEMPLATE_FILE}" >&2
  exit 1
fi

if [[ "$DRY_RUN" != "1" && -e "$OUTPUT_FILE" && "$FORCE" != "1" ]]; then
  echo "Output file already exists: ${OUTPUT_FILE}" >&2
  echo "Use --force to overwrite or --output FILE to write elsewhere." >&2
  exit 1
fi

sed_escape() {
  printf '%s' "$1" | sed -e 's/[\/&]/\\&/g'
}

replace_or_append_line() {
  local key="$1"
  local value="$2"
  local file="$3"
  local escaped
  escaped="$(sed_escape "$value")"
  if grep -q -E "^${key}=" "$file"; then
    sed -i -E "s/^${key}=.*/${key}=${escaped}/" "$file"
  else
    printf '\n%s=%s\n' "$key" "$value" >> "$file"
  fi
}

# version 의 single source of truth 는 src/zeta4s/__init__.py 다. pyproject 는
# dynamic version 으로 이 값을 읽으므로 pyproject 에서 읽을 수 없다.
project_version() {
  awk '
    /^__version__ = "/ {
      version = $3
      gsub(/"/, "", version)
      print version
      exit
    }
  ' src/zeta4s/__init__.py
}

require_value() {
  local label="$1"
  local value="$2"
  if [[ -z "$value" ]]; then
    echo "${label} must not be empty" >&2
    exit 1
  fi
}

generate_token() {
  python -c 'import secrets; print(secrets.token_urlsafe(32))'
}

zeta4s_version="$(project_version)"
require_value "src/zeta4s/__init__.py __version__" "$zeta4s_version"

tmp_file="$(mktemp)"
trap 'rm -f "$tmp_file"' EXIT
cp "$TEMPLATE_FILE" "$tmp_file"

replace_or_append_line "ZETA4S_VERSION" "$zeta4s_version" "$tmp_file"
replace_or_append_line "ZETA4S_API_IMAGE" "zeta4s-api:${zeta4s_version}" "$tmp_file"
replace_or_append_line "ZETA4S_RUNTIME_INTERNAL_TOKEN" "$(generate_token)" "$tmp_file"
replace_or_append_line "AIRFLOW_UID" "$AIRFLOW_UID" "$tmp_file"
replace_or_append_line "ZETA4S_RUNTIME_UID" "$ZETA4S_RUNTIME_UID" "$tmp_file"

if [[ "$DRY_RUN" == "1" ]]; then
  cat "$tmp_file"
else
  cp "$tmp_file" "$OUTPUT_FILE"
  chmod 600 "$OUTPUT_FILE"
  echo "Generated ${OUTPUT_FILE}"
  echo "ZETA4S_VERSION=${zeta4s_version}"
  echo "ZETA4S_API_IMAGE=zeta4s-api:${zeta4s_version}"
  echo "ZETA4S_RUNTIME_INTERNAL_TOKEN=<generated>"
  echo "AIRFLOW_UID=${AIRFLOW_UID}"
  echo "ZETA4S_RUNTIME_UID=${ZETA4S_RUNTIME_UID}"
  echo
  echo "Next steps:"
  echo "  bash scripts/build_images.sh --load"
  echo "  docker compose --env-file ${OUTPUT_FILE} --profile prefect --profile checkpoint up -d --wait zeta4s-api prefect-worker"
fi
