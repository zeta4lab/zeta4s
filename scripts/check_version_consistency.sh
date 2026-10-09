#!/usr/bin/env bash
set -euo pipefail

# version 의 single source of truth 는 src/zeta4s/__init__.py 의 __version__ 이다.
# 3개 pyproject 는 setuptools dynamic version 으로 이 값을 읽는다.
#
# 이 gate 는 single source 가 유지되는지 검사한다:
#   - __version__ 이 존재하고 semver 형식인지
#   - pyproject 가 static version 을 다시 들이지 않았는지
#   - sub-package 가 zeta4s 를 고정 version 으로 pin 하지 않았는지
#     (pin 하면 version bump 때마다 같이 고쳐야 하고, 놓치면 wheel 이 설치되지 않는다)
#
# 인자로 version 을 주면 그 값과 __version__ 이 일치하는지도 검사한다. tag push 시
# tag 명을 넘겨 tag 와 version 이 어긋난 릴리즈를 막는다.

VERSION_SOURCE="src/zeta4s/__init__.py"
PYPROJECTS=(
  pyproject.toml
  packages/zeta4s-cli/pyproject.toml
  packages/zeta4s-api/pyproject.toml
)

version="$(rg -N -m1 '^__version__ = "([^"]+)"' -o -r '$1' "$VERSION_SOURCE" || true)"
if [ -z "$version" ]; then
  echo "version contract violation: __version__ not found in ${VERSION_SOURCE}" >&2
  exit 1
fi

if ! printf '%s' "$version" | rg -q '^[0-9]+\.[0-9]+\.[0-9]+$'; then
  echo "version contract violation: __version__ is not semver: ${version}" >&2
  exit 1
fi

for file in "${PYPROJECTS[@]}"; do
  # static 은 version = "1.2.3" 이고 dynamic 은 version = {attr = ...} 다.
  if rg -qN '^version = "' "$file"; then
    echo "version contract violation: ${file} declares a static version. version 은 ${VERSION_SOURCE} 하나에서만 온다" >&2
    exit 1
  fi
  if ! rg -qN 'version = \{attr = "zeta4s.__version__"\}' "$file"; then
    echo "version contract violation: ${file} must resolve version from zeta4s.__version__" >&2
    exit 1
  fi
done

for file in "${PYPROJECTS[@]:1}"; do
  if rg -qN 'zeta4s\[[a-z]+\]==' "$file"; then
    echo "version contract violation: ${file} pins zeta4s to a fixed version. bump 때마다 어긋나 wheel 이 설치되지 않는다" >&2
    exit 1
  fi
done

# version 을 읽는 shell script 는 여러 개다. pyproject 는 dynamic version 이라
# project.version 을 갖지 않으므로 거기서 읽으면 조용히 빈 값이 되거나 KeyError 로
# 죽는다. project_version 정의 본문이 single source 를 읽는지 검사한다. 주석에만
# 언급하고 본문은 pyproject 를 읽는 경우를 잡으려면 본문만 봐야 한다.
while IFS= read -r file; do
  body="$(awk '/^project_version\(\)/ { in_fn = 1 } in_fn { print } in_fn && /^}/ { exit }' "$file")"
  if ! printf '%s' "$body" | rg -qN "$VERSION_SOURCE"; then
    echo "version contract violation: ${file} 의 project_version 이 ${VERSION_SOURCE} 를 읽지 않는다" >&2
    exit 1
  fi
  if printf '%s' "$body" | rg -qN 'pyproject\.toml'; then
    echo "version contract violation: ${file} 의 project_version 이 pyproject.toml 을 읽는다. pyproject 에는 version 이 없다" >&2
    exit 1
  fi
done < <(rg -lN '^project_version\(\)' scripts)

# 위 검사는 project_version 정의 본문만 본다. usage 문자열, 문서, workflow 주석에 남은
# 서술은 코드가 아니라 잡히지 않는다. pyproject 를 version 출처로 지목하는 표현을 금지한다.
stale_version_source="$(rg -nN -g '!scripts/check_version_consistency.sh' \
  'pyproject[^\n]{0,24}\bproject\.version\b|zeta4s:<pyproject\.toml version>|tag 명과 pyproject version' \
  scripts docs .github || true)"
if [ -n "$stale_version_source" ]; then
  echo "version contract violation: pyproject 를 version 출처로 지목하는 서술이 남아 있다. version 은 ${VERSION_SOURCE} 하나에서만 온다" >&2
  printf '%s\n' "$stale_version_source" >&2
  exit 1
fi

if [ "$#" -gt 0 ]; then
  expected="$1"
  if [ "$expected" != "$version" ]; then
    echo "version contract violation: expected ${expected} but ${VERSION_SOURCE} is ${version}" >&2
    exit 1
  fi
  echo "version ${version} matches expected ${expected}"
  exit 0
fi

echo "version ${version} resolves from ${VERSION_SOURCE} only"
