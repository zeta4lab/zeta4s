# 저장소 작업 지침

이 파일이 agent 지침의 single source of truth 다. 이 저장소는 여러 coding agent 가
함께 작업하므로 도구별로 지침을 나누지 않는다. 도구별 지침이 필요하면 각 도구의
진입 파일에 추가하고 공통 지침은 여기 둔다.

각 도구가 이 파일에 닿는 경로:

- Codex: `AGENTS.md` 를 그대로 읽는다. 설정이 필요 없다.
- Claude Code: `CLAUDE.md` 만 자동으로 읽으므로 `CLAUDE.md` 가 `@AGENTS.md` 로 import 한다.
  `AGENTS.md` 를 직접 읽지 않는다.

`.gitignore` 는 `.claude/`, `.codex/`, `.gemini/` 를 agent-local config 로 무시한다. 공유해야
하는 지침은 저장소 루트에 둔다.

## 기본 원칙

- 설명은 항상 한국어로 한다.
- 파일 경로, 명령어, 함수명, config key 같은 literal 은 원문 표기를 유지한다.
- zeta4s 의 궁극 목표는 AI Agent 가 생성한 계약(Contract)을 Step Graph 로 실행하는 범용
  Runtime Engine 이 되는 것이다.
- 구현 판단은 contract 생성/검증/정규화/배포/실행/관측이 같은 Step Graph 계약으로 이어지는지
  기준으로 한다.
- 저장소는 Apache-2.0 으로 공개돼 있고 version tag 마다 wheel 과 image 를 publish 한다.
  하위호환 보장은 사용자가 별도로 지시할 때부터다. 그 전까지 구조적 문제가 발견되면
  하위호환성을 유지하기보다 현재 목표 구조에 맞게 수정한다.
- version tag 는 구현 묶음을 구분하는 표식이며 하위호환 경계가 아니다.
- `docs/` 는 갈래별로 나눈다. `usage/` 는 사용자가 zeta4s 를 쓰는 법, `design/` 은 현재
  코드의 설계, `gate/` 는 검증, `roadmap/` 은 목표와 완성 여부, `plans/` 는 진행 중
  구현 계획이다. 각 디렉토리의 `README.md` 가 규칙을 규정한다.
- 구현이 끝나면 `usage`, `design`, `gate` 를 현행화하고 `roadmap` 의 완성된 범위에 반영한다.
  현행화는 version tag 를 붙이는 단계에서 완료한다. tag 시점의 `usage`, `design`, `gate`
  가 그 tag 의 현황이다.
- 문서는 현재 목표 계약만 설명한다. 필요한 이력은 Git commit 과 PR 기록으로 추적한다.
- 코드에서 파생 가능한 사실은 문서에 복제하지 않는다. 파일 경로, 명령 이름, version,
  gate 검사 목록, CI step 목록, step type 목록 같은 것은 코드가 정본이므로 문서는
  좌표만 가리킨다. 문서에는 코드로 확인할 수 없는 것 — 원칙, 계약, 설계 의도,
  "어디를 볼지" — 만 둔다. 복제한 사실은 코드가 바뀌면 조용히 틀린 문서가 된다.
  정본 좌표는 `docs/README.md` 의 표에 있다.

## 개발 제약

- `z4s` CLI 는 컨테이너 밖 host 환경에서 실행한다.
- repository 표준 Python 도구는 `uv` 이며 `uv.lock` 을 추적한다.
- 설치본은 host `zeta4s-cli` 와 server/adapter `zeta4s-api` distribution 으로 구분한다.
- adapter layer 는 `zeta4s-api` 설치본 내부 구현이며 세 번째 설치본으로 다루지 않는다.
- `zeta4s.core`, `zeta4s.project`, `zeta4s.runtime` 은 `airflow` 와 `prefect` 를 import
  하지 않는다. 함수 안 lazy import 도 포함한다. scheduler 를 쓰는 코드는 adapter layer
  (`zeta4s.airflow`, `zeta4s.prefect`) 에 둔다.
- 에이전트 검증에는 사용자 `.venv` 를 쓰지 않고 `uv` 로 만든 별도 가상환경을 쓴다.
- Airflow 3.x import/workaround 패턴은 scheduler 동작을 검증하지 않고 바꾸지 않는다.
- Scheduler pool 은 `zeta4s-api` 배포 흐름이 동기화하는 adapter projection contract 로 본다.
  connection credential 은 Airflow Connection 이나 Airflow secrets backend 로 넘기지 않고
  `zeta4s-api` 가 profile 과 encrypted secret store 에서 resolve 한다.

## 검증 기준

- Python 변경 후 최소 `uv run python -m compileall -q src/zeta4s` 와
  `uv run ruff check .`, `uv run ruff format .` 를 확인한다.
- pull request 는 `.github/workflows/ci.yml` 이 Docker 없이 검증한다. 검증 단계 목록의
  정본은 `ci.yml` 이다.
- Docker stack 이 필요한 release gate 는 `.github/workflows/release-gate.yml` 이 scheduler
  backend 별로 pull request, main push, version tag, nightly 에서 실행한다.
- 저장소는 공개이며 GitHub-hosted runner 만 쓴다. 공개 저장소에 self-hosted runner 를
  붙이지 않는다 — fork pull request 가 그 host 에 닿을 수 있다.
- 변경은 pull request 로 반영한다. CI 는 main push 에서도 돌아 반영된 결과를 다시 검사한다.
- version tag 를 push 하면 `.github/workflows/release.yml` 이 tag 와 `__version__` 일치를
  검증하고 wheel 을 GitHub Release 에, `zeta4s-api` image 를 ghcr 에 publish 한다.
- adapter/API 변경은 Docker stack 을 기동한 뒤 host `z4s` CLI 로 `z4s api deploy`,
  `z4s profile check`, 필요한 DAG run 을 확인한다.
- `z4s` CLI 로 필요한 adapter/API 테스트를 수행할 수 없으면 우회 스크립트를 만들지 않는다.
  필요한 실행 동작을 `zeta4s-api` endpoint 와 CLI 명령으로 노출한다.
- dbt step 이 있는 프로젝트에서 dbt model 을 바꾸면 `z4s project check` 로 dbt model
  contract 를 검증한다.
- `clickhouse.sql`, `oracle.sql`, extract/stage/write 만 쓰는 step graph 프로젝트에는 dbt
  model 이나 generated source metadata 를 요구하지 않는다.
- step graph contract 의 canonical input 은 `project.yml`, `jobs/*.yml`, SQL/dbt 파일이다.

## Git 기준

- 커밋 메시지는 `docs:`, `feat:`, `fix:`, `test:` 같은 Conventional Commit 스타일을
  사용하고 설명은 한국어로 작성한다.
- 변경 사항은 `main` 단일 장기 브랜치 기준으로 관리한다.
- 기능 변경은 `feature/*`, 수정은 `fix/*`, 문서는 `docs/*`, 그 밖의 유지보수는 `chore/*` 에서
  진행하고 pull request 로 `main` 에 반영한다. 의존성 갱신은 Dependabot 이 pull request 로 올린다.
- 릴리즈는 `main` 에 version tag 를 만드는 것으로 끝낸다. 안정화 브랜치와 back-merge 는 없다.
- version 의 single source of truth 는 `src/zeta4s/__init__.py` 의 `__version__` 이다.
  pyproject 는 dynamic version 으로 이 값을 읽으므로 static version 을 두지 않는다.
- version bump 는 별도 branch 나 pull request 를 만들지 않고 릴리즈에 포함되는
  pull request 에서 함께 올린다. tag 명은 `__version__` 과 일치해야 한다.
- GitHub `origin` 이 기준 원격 저장소다.
