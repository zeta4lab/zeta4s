#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  bash scripts/build_images.sh [--env-file FILE] [--platform PLATFORM] [--push|--load|--auto] [--dry-run]

Build zeta4s container images with docker buildx.
If docker buildx is unavailable, fall back to docker build.

zeta4s가 빌드하는 image는 zeta4s-api 하나다. Airflow와 Prefect server는 공식 image를 쓴다.

Options:
  --env-file FILE      Load environment variables from FILE. Default: none
  --platform PLATFORM  Build platform. Default: Docker host platform.
  --push               Push built images to their tags.
  --load               Load built images into the local Docker engine.
  --auto               Push registry-qualified images and load local images. Default.
  --dry-run            Print the build commands without executing them.
  -h, --help           Show this help.

Optional environment:
  PYTHON_BASE_IMAGE         Default: python:${PYTHON_VERSION}-slim
  ZETA4S_API_IMAGE          Default: zeta4s-api:<src/zeta4s/__init__.py __version__>
  PYTHON_VERSION            Default: 3.12
  PIP_INDEX_URL
  PIP_TRUSTED_HOST
EOF
}

ENV_FILE=""
PLATFORM=""
OUTPUT_MODE="auto"
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
    --env-file)
      require_option_value "$1" "${2:-}"
      ENV_FILE="$2"
      shift 2
      ;;
    --platform)
      require_option_value "$1" "${2:-}"
      PLATFORM="$2"
      shift 2
      ;;
    --push)
      OUTPUT_MODE="--push"
      shift
      ;;
    --load)
      OUTPUT_MODE="--load"
      shift
      ;;
    --auto)
      OUTPUT_MODE="auto"
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

load_env_file() {
  local file="$1"
  local line key value
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
    [[ "$line" != *=* ]] && continue
    key="${line%%=*}"
    value="${line#*=}"
    key="${key#"${key%%[![:space:]]*}"}"
    key="${key%"${key##*[![:space:]]}"}"
    [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
    if [[ "$value" == \"*\" && "$value" == *\" ]]; then
      value="${value:1:${#value}-2}"
    elif [[ "$value" == \'*\' && "$value" == *\' ]]; then
      value="${value:1:${#value}-2}"
    fi
    export "$key=$value"
  done < "$file"
}

if [[ -n "$ENV_FILE" && -f "$ENV_FILE" ]]; then
  load_env_file "$ENV_FILE"
fi

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" || "${!name:-}" == *\<* ]]; then
    echo "Missing required env: ${name}" >&2
    exit 1
  fi
}

run_cmd() {
  if [[ "$DRY_RUN" == "1" ]]; then
    printf '+'
    printf ' %q' "$@"
    printf '\n'
  else
    "$@"
  fi
}

has_docker_buildx() {
  docker buildx version >/dev/null 2>&1
}

docker_buildx_supports_platform() {
  docker buildx build --help 2>/dev/null | grep -q -- '--platform'
}

docker_build_supports_platform() {
  docker build --help 2>/dev/null | grep -q -- '--platform'
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

is_registry_qualified_image() {
  local image="$1"
  local first_part="${image%%/*}"
  [[ "$image" == */* && ( "$first_part" == *.* || "$first_part" == *:* || "$first_part" == "localhost" ) ]]
}

PYTHON_VERSION="${PYTHON_VERSION:-3.12}"
ZETA4S_VERSION="${ZETA4S_VERSION:-$(project_version)}"
PYTHON_BASE_IMAGE="${PYTHON_BASE_IMAGE:-python:${PYTHON_VERSION}-slim}"
ZETA4S_API_IMAGE="${ZETA4S_API_IMAGE:-zeta4s-api:${ZETA4S_VERSION}}"
require_env ZETA4S_VERSION

resolve_output_mode() {
  local image="$1"
  if [[ "$OUTPUT_MODE" != "auto" ]]; then
    printf '%s' "$OUTPUT_MODE"
    return
  fi
  if is_registry_qualified_image "$image"; then
    printf '%s' "--push"
  else
    printf '%s' "--load"
  fi
}

build_image() {
  local image="$1"
  local dockerfile="$2"
  shift 2
  local extra_args=("$@")
  local output_mode
  output_mode="$(resolve_output_mode "$image")"

  local platform_args=()
  if [[ -n "$PLATFORM" ]]; then
    platform_args=(--platform "$PLATFORM")
  fi

  local build_args=(
    "${extra_args[@]}"
    --build-arg "PIP_INDEX_URL=${PIP_INDEX_URL:-}"
    --build-arg "PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST:-}"
    -t "$image"
    -f "$dockerfile"
  )
  if [[ "${#platform_args[@]}" -gt 0 ]]; then
    build_args=("${platform_args[@]}" "${build_args[@]}")
  fi

  if has_docker_buildx && docker_buildx_supports_platform; then
    run_cmd docker buildx build "${build_args[@]}" "$output_mode" .
  else
    echo "docker buildx is unavailable or does not support --platform. Falling back to docker build." >&2
    if [[ "${#platform_args[@]}" -gt 0 ]] && ! docker_build_supports_platform; then
      echo "docker build does not support --platform. Building for the Docker host platform." >&2
      build_args=("${build_args[@]:${#platform_args[@]}}")
    fi
    run_cmd docker build "${build_args[@]}" .
    if [[ "$output_mode" == "--push" ]]; then
      run_cmd docker push "$image"
    fi
  fi
}

require_env PYTHON_BASE_IMAGE
require_env ZETA4S_API_IMAGE
build_image "$ZETA4S_API_IMAGE" docker/zeta4s-api/Dockerfile \
  --build-arg "PYTHON_BASE_IMAGE=${PYTHON_BASE_IMAGE}"
