#!/usr/bin/env bash
set -euo pipefail

# release 는 wheel 로 배포된다. version gate 는 선언을 검사할 뿐이고, dependency 가
# 실제로 해석되는지는 설치를 해봐야 안다. 세 wheel 을 빌드해 version 이 같은지
# 확인하고, host CLI 설치 경로를 그대로 재현한다.
#
# zeta4s-api 는 prefect/pyarrow/oracledb 를 끌어와 무겁다. metadata 검사로 갈음하고
# 실제 설치는 가벼운 zeta4s-cli 로만 한다. Docker image 는 zeta4s-api 설치와
# uv pip check 를 이미 수행한다.

DIST="$(mktemp -d)"
VENV="$(mktemp -d)/venv"
trap 'rm -rf "$DIST" "$(dirname "$VENV")"' EXIT

rm -rf \
  build \
  src/zeta4s.egg-info \
  src/zeta4s_cli.egg-info \
  src/zeta4s_api.egg-info \
  packages/zeta4s-cli/build \
  packages/zeta4s-api/build

uv build --wheel --out-dir "$DIST" . >/dev/null
uv build --wheel --out-dir "$DIST" packages/zeta4s-cli >/dev/null
uv build --wheel --out-dir "$DIST" packages/zeta4s-api >/dev/null

expected="$(rg -N -m1 '^__version__ = "([^"]+)"' -o -r '$1' src/zeta4s/__init__.py)"

for prefix in zeta4s zeta4s_cli zeta4s_api; do
  found="$(find "$DIST" -maxdepth 1 -name "${prefix}-*.whl" | wc -l)"
  if [ "$found" -ne 1 ]; then
    echo "wheel install violation: expected exactly one ${prefix} wheel, found ${found}" >&2
    exit 1
  fi
  wheel="$(find "$DIST" -maxdepth 1 -name "${prefix}-*.whl")"
  built="$(basename "$wheel" | sed -E "s/^${prefix}-([^-]+)-.*/\1/")"
  if [ "$built" != "$expected" ]; then
    echo "wheel install violation: ${prefix} wheel is ${built} but __version__ is ${expected}" >&2
    exit 1
  fi
done

uv venv "$VENV" >/dev/null
uv pip install --python "$VENV/bin/python" --find-links "$DIST" zeta4s-cli >/dev/null
uv pip check --python "$VENV/bin/python" >/dev/null

installed="$("$VENV/bin/python" -c 'import zeta4s; print(zeta4s.__version__)')"
if [ "$installed" != "$expected" ]; then
  echo "wheel install violation: installed zeta4s is ${installed} but __version__ is ${expected}" >&2
  exit 1
fi

"$VENV/bin/z4s" --help >/dev/null

echo "wheel ${expected} builds and installs: zeta4s, zeta4s-cli, zeta4s-api"
