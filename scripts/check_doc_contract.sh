#!/usr/bin/env bash
set -euo pipefail

# 문서 드리프트의 대부분은 "코드에서 파생 가능한 사실을 문서에 복제" 해서 생긴다.
# 복제한 사실 중 기계로 확인할 수 있는 것은 경로다. 문서가 가리키는 경로가 사라지면
# 그 문서는 이미 틀렸다.
#
# 기계적으로 확인 가능한 것만 검사한다. 서술의 정확성은 검사 대상이 아니다.
#
#   1. 문서가 backtick 으로 지목하는 repository 경로가 실존하는가
#   2. 문서 간 상대 link 가 실존하는가
#
# glob(*) 이 든 경로는 pattern 서술이므로 건너뛴다.

violations=0

report() {
  echo "doc contract violation: $1" >&2
  violations=$((violations + 1))
}

# 1. backtick 안의 repository 경로
#
# 대상 최상위 directory 를 git 이 추적하는 것에서 산출한다. 목록을 손으로 적으면
# directory 가 늘 때 검사에서 조용히 빠진다. 반대로 모든 `a/b` 를 경로로 보면
# `Asia/Seoul` 이나 `INTEGER/BIGINT` 같은 서술을 오탐한다.
roots="$(git ls-files | rg -oN '^[^/]+/' | sort -u | tr -d '/' | paste -sd'|' -)"

while IFS= read -r path; do
  case "$path" in
    *"*"*) continue ;;
  esac
  [ -e "$path" ] || report "문서가 존재하지 않는 경로를 지목한다: ${path}"
done < <(
  git grep -ohE "\`(${roots})/[a-zA-Z0-9_./-]+\`" -- '*.md' |
    tr -d '`' | sort -u
)

# 2. 문서 간 상대 link
while IFS= read -r file; do
  [ -e "$file" ] || continue
  dir="$(dirname "$file")"
  while IFS= read -r link; do
    case "$link" in
      *"*"*) continue ;;
    esac
    [ -e "${dir}/${link}" ] || report "${file} 의 link 가 깨졌다: ${link}"
  done < <(
    rg -oN '\]\((\.\.?/[^)#]+)\)' -r '$1' "$file" || true
  )
done < <(git ls-files '*.md')

if [ "$violations" -gt 0 ]; then
  echo "doc contract violation: ${violations}건. 경로를 고치거나, 코드에서 파생 가능한 사실이면 문서에서 지운다" >&2
  exit 1
fi

echo "doc contract holds: 문서가 지목한 경로와 link 가 모두 실존한다"
