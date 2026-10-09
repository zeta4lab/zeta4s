# Operations Contract

운영 기준은 release candidate 기준 검증과 local runtime 검증을 같은 contract 로 유지하는 것이다.

## Docker Stack

로컬 runtime 검증은 `docker-compose.yml` 을 사용한다.
기본 metastore는 PostgreSQL의 `zeta4s_metastore` database다. `asset` profile은 ClickHouse를
runtime data backend로 추가하며 metastore 기본값을 바꾸지 않는다.

```bash
bash scripts/configure_open_env.sh --force
docker compose --env-file .env --profile airflow --profile prefect --profile asset up -d --wait
```

Volume 을 포함해 runtime state 를 초기화하려면 다음 명령을 사용한다.

```bash
docker compose --env-file .env --profile airflow --profile prefect --profile asset down -v --remove-orphans
docker compose --env-file .env --profile airflow --profile prefect --profile asset up -d --wait
```

## Release Gate

Release gate 는 source bind 없는 Docker image 와 host wheel CLI 를 사용한다. 검증 대상 project 와
job matrix 는 workspace showcase 계약으로 명시한다.

Gate 가 확인하는 기준:

- `uv run python -m compileall -q src/zeta4s`
- `git diff --check`
- `z4s project check`
- `z4s profile check`
- `api bootstrap`
- `api status`의 `metastore.type=postgres`, `schema_status=ok`
- `api deploy`
- scheduler별 release run matrix
- local/scheduler terminal result conformance
- 실패 step 의 `attempt`/`adapter_attempt` 기록

Evidence 는 `.zeta4s/reliability/<project>/<timestamp>/` 에 저장한다. 이 경로는 repository 에
commit 하지 않는다.

Scheduler 중립 run gate는 동일한 host `z4s api run` 명령 흐름으로 Airflow와 Prefect profile을
각각 빈 runtime state에 배포한 뒤 생성, 상태, 요약, task와 log 관측을 확인한다. Backend 고유
식별자와 상태는 adapter가 canonical run 계약으로 변환하며, gate 판정은 native 응답 필드에
의존하지 않는다. 실행 matrix와 판정 로직의 정본은 `scripts/check_runtime_reliability.sh`다.

## k3s Deployment Contract

`deploy/k3s/` 는 단일 노드 k3s 배포 계약이다. `scripts/check_k3s_manifests.sh` 가 cluster
없이 파일 단계에서 경계를 검사한다. 검사 목록의 정본은 script 자체이며 다음을 지킨다.

- master keyring Secret 은 `zeta4s-api` 만 mount 하고 0400 read-only 다. 환경변수로
  싣지 않는다 — env 는 `kubectl describe pod` 와 crash dump 에 남는다.
- Secret 을 읽는 Role 은 대상 하나의 `get` 만 갖는다. `list` 는 namespace 의 다른 Secret
  이름을 드러낸다.
- internal execution endpoint 를 Ingress 로 노출하지 않고 Service 는 ClusterIP 다.
- `zeta4s-api` 는 non-root, privilege escalation 금지, `readOnlyRootFilesystem` 이다.
- `ZETA4S_API_TOKEN` 은 Secret 에서 온다. 비면 public API 가 무인증이 된다.

cluster 가 필요한 배포 검증은 이 gate 가 대신하지 않는다. 단일 노드 전제와 배포 절차는
`../../deploy/k3s/README.md` 에 있다.

## Pull Request Gate

`.github/workflows/ci.yml` 은 `main` 으로 향하는 pull request 마다 Docker 없이 도는
빠른 게이트다. 검증 항목:

- `uv sync --locked` — `uv.lock` 이 `pyproject.toml` 과 어긋나지 않음
- `uv run ruff check .` — 죽은 import 와 undefined name
- `uv run ruff format --check .` — format
- `uv run pytest` — DB 가 필요한 test 는 환경변수가 없으면 스스로 skip 한다
- `scripts/check_static_cli_contract.sh`
- `scripts/check_version_consistency.sh`
- `scripts/check_doc_contract.sh`
- `scripts/check_k3s_manifests.sh`
- `scripts/check_wheel_install.sh`

Docker stack 이 필요한 release gate 는 이 워크플로에 포함하지 않는다.

`pull_request` 는 head 를 base 에 merge 한 결과를 검사하므로 merge 후 `main` push 에서
다시 돌리지 않는다. GitHub Actions 는 job 을 분 단위로 올림 과금하므로 같은 내용을
두 번 검사하지 않는다. main 에 직접 push 한 변경은 CI 가 검사하지 않는다. 필요하면
`workflow_dispatch` 로 수동 실행한다.

`.github/workflows/release-tag.yml` 은 version tag push 시 tag 명과 `__version__`
일치를 검증한다.

## Lint Gate

`ruff` 는 `pyproject.toml` 의 `[tool.ruff]` 기준으로 돈다. 켜는 룰은 ruff 기본값인
`E4`, `E7`, `E9`, `F` 다. pyflakes 가 죽은 import 와 undefined name 을 잡는다.
줄 길이 같은 style 룰은 기존 코드 기준과 충돌하므로 켜지 않는다.

airflow 를 `sys.modules` 에 stub 한 뒤 zeta4s 를 import 하는 test 는 import 순서가
의도적이므로 `per-file-ignores` 로 `E402` 를 적용하지 않는다.

## Format Gate

`ruff format` 은 `line-length = 120` 기준으로 돈다. 기본값 88 은 기존 함수 signature
대부분을 여러 줄로 분해하므로 기존 코드 기준에 맞춘 값을 쓴다.

전체 재포맷 commit 은 `.git-blame-ignore-revs` 에 등록한다. GitHub 은 이 파일을 자동으로
적용한다. 로컬 `git blame` 에 적용하려면 다음을 한 번 설정한다.

```bash
git config blame.ignoreRevsFile .git-blame-ignore-revs
```

## Doc Contract Gate

문서 드리프트의 대부분은 코드에서 파생 가능한 사실을 문서에 복제해서 생긴다. 복제한
사실 중 기계로 확인할 수 있는 것은 경로다. 문서가 가리키는 경로가 사라지면 그 문서는
이미 틀렸다.

`scripts/check_doc_contract.sh` 는 문서가 backtick 으로 지목한 repository 경로와 문서 간
상대 link 가 실존하는지 검사한다. 대상 최상위 directory 는 git 이 추적하는 것에서
산출하므로 directory 가 늘어도 검사에서 빠지지 않는다.

서술의 정확성은 검사하지 않는다. 기계로 확인 가능한 것만 gate 로 만든다. 예를 들어
문서의 version 문자열은 대부분 "1.0.0 기준선" 같은 마일스톤 표현이라 실제 version 값과
구분할 수 없으므로 검사하지 않는다.

## Version Contract

version 의 single source of truth 는 `src/zeta4s/__init__.py` 의 `__version__` 이다.
3개 pyproject 는 setuptools dynamic version 으로 이 값을 읽으므로 static version 을
갖지 않는다. `uv.lock` 도 version 을 기록하지 않는다.

sub-package 는 `zeta4s` 를 고정 version 으로 pin 하지 않는다. pin 하면 bump 때마다
같이 고쳐야 하고, 놓치면 wheel 이 설치되지 않는다. sub-package 와 `zeta4s` 는 같은
commit 에서 함께 빌드되어 함께 설치되므로 pin 이 주는 이득이 없다.

`scripts/check_version_consistency.sh` 는 이 계약이 유지되는지 검사한다. 인자로
version 을 주면 `__version__` 과 일치하는지도 검사한다.

`scripts/check_wheel_install.sh` 는 세 wheel 을 빌드해 version 이 같은지 확인하고
host CLI 설치 경로를 재현한다. 선언만 검사하는 gate 로는 dependency 해석 오류를
잡을 수 없다.

version bump 는 별도 branch 나 pull request 를 만들지 않고 릴리즈에 포함되는
pull request 에서 `__version__` 을 함께 올린다.

## Scheduler Version Contract

zeta4s scheduler stack은 `zeta4s-cli`, `zeta4s-api`, Airflow와 Prefect를 한 release gate에서
검증한다. 정확한 version 문자열은 package metadata, `.env.example`과 `docker-compose.yml`이
정본이며 문서에 복제하지 않는다.

pytest는 Prefect SDK pin과 공식 server image pin, Airflow 공식 image와 환경 template pin이
각각 일치하는지 검사한다. upstream 최신 stable 여부는 구현 시 공식 release를 확인하고,
실제 지원 가능 여부는 scheduler별 빈 volume release gate로 판정한다. 외부 data backend와
observability image는 이 version contract의 갱신 범위가 아니다.

`deploy/k3s/`는 scheduler image를 고정하지 않는다. zeta4s는 Airflow와 Prefect를 배포하지
않고 그 배포에 붙는 환경 배선만 정의하므로(`deploy/k3s/patches/`), version 정본이 세 번째
위치에 생기지 않는다. k3s 배선에 image pin을 넣게 되면 그때 이 대조 대상에 추가한다.

## Static Contract Gate

`scripts/check_static_cli_contract.sh` 는 빠른 회귀 검증이다. 계약 위반을 rg 로 잡는
assertion 을 나열한 script 이므로, 정확한 검사 목록은 script 자체가 기준이다. 아래는
검사가 지키는 계약을 갈래로 묶은 것이다.

- project check 가 dbt 실행이나 subprocess 에 의존하지 않음
- 미지원 step type 이름과 `sources_raw` metadata 를 쓰지 않음
- generated DAG `max_active_runs` invariant 와 runtime invariant 검사 존재
- DAG generator 가 Airflow Asset/Dataset/outlets API 를 사용하지 않음
- step task 바인딩이 `src/zeta4s/airflow/step_binding.py` 의 generic `core_step_operator` 로 이뤄짐
- runtime/release gate 가 사용자 `.venv` 에 의존하지 않음
- runtime image/compose 가 source import path 를 사용하지 않음
- runtime progress stream endpoint 와 CLI consumer 존재
- `api run summary` report 저장 contract 와 stale deployment option
- canonical showcase 의 존재, job 파일, 디렉터리 구조
- runtime SQL template helper 를 쓰지 않음

## Scheduler Boundary

step-type runtime 은 scheduler 와 무관하게 만들어지고 실행된다. `zeta4s.core`,
`zeta4s.project`, `zeta4s.runtime` 은 `airflow` 와 `prefect` 를 import 하지 않는다.
adapter 는 bind 만 하고 실행 로직을 갖지 않는다.

이 경계는 pytest 가 강제한다. static gate 는 검사하지 않는다.

- `tests/test_scheduler_boundary_invariants.py` — core/project/runtime 의 import 를
  AST 로 검사한다. 함수 안 lazy import 도 잡는다. prefect 는
  `prefect/prefect_engine.py` 하나로 봉쇄한다.
- `tests/test_runtime_scheduler_independence.py` — `sys.meta_path` 로 backend import
  자체를 막고 runtime 전체를 다시 import 한다. `sys.modules` 에서 지우기만 하면
  backend 가 설치된 환경에서 재 import 가 성공해 검사가 무력해진다.

## Scheduler Pool Projection

Scheduler backend를 바꿔도 같은 Step Graph의 부하 조절 의미가 유지되어야 한다. Backend-neutral
resolver를 각 adapter가 projection 시점에 공통 사용하고 선택한 backend의 resource에 binding한다.
정확한 산출과 backend 연결의 정본은 `../README.md`의 코드 위치 포인터를 따른다.

CI는 Airflow와 Prefect가 같은 effective pool resolver를 소비하는지 검사한다. Prefect regression은
explicit binding이 없는 task가 project/stage 자동 pool을 점유하고, explicit override가 있으면 이를
우선하는지 확인한다. Prefect profile release run은 deploy가 limit resource를 동기화하고 실제 task가
해당 resource를 점유한 채 완료되는 통합 경로를 확인한다.

현재 gate는 limit보다 많은 task를 동시에 제출해 queued 대기 시간과 slot 반환을 직접 관측하지 않는다.
이 한계 때문에 unit test와 정상 release run을 실제 contention 성능 증거로 확대 해석하지 않는다.

## Checkpoint Recovery

Checkpoint recovery release gate는 scheduler가 실패한 실행 unit을 새 attempt로 다시 실행하고,
zeta4s runtime이 이전 attempt의 committed checkpoint에서 이어 실행하는지를 확인하는 목표 gate다.
실행 절차와 판정 query의 정본은 `scripts/check_release_runtime_showcases.sh`다.

Gate 는 source bind 없는 image 와 host wheel CLI 를 사용해 다음 증거가 한 run 에 함께 남는지 본다.

- 외부 data backend 장애 뒤 attempt 가 증가한다.
- checkpoint sequence 가 중복이나 누락 없이 이어진다.
- 최종 output binding 이 기대한 전체 결과를 가리킨다.
- downstream 확인 step 이 한 번 성공한다.

Prefect 경로에서 주입하는 장애는 worker crash가 아니라 외부 data backend pause timeout이다. 위 증거가
모두 남은 실행만 retry와 checkpoint resume의 결합을 통과한 것으로 판정한다.

## Airflow Headless Boundary

`zeta4s-api` 는 Airflow 에 REST 로만 붙는다. metastore 를 직접 열지 않고, CLI 를 부르지
않으며, Python code 를 subprocess 에 주입하지 않는다.

기준은 파일이 아니라 **실행 위치**다. Airflow process는 generated DAG와 Airflow package만
실행하고 zeta4s package를 import하지 않는다. Core step은 `zeta4s-api` process에서 실행된다.

**파일 목록으로는 판단할 수 없다.** `api/app.py` 는 airflow 를 직접 import 하지 않으면서도
중간 module 을 거쳐 airflow 에 닿을 수 있고, 그 경로는 어떤 파일 단위 검사에도 걸리지
않는다. 그래서 import 그래프로 본다.

- `tests/test_scheduler_boundary_invariants.py` 의 `AirflowHeadlessInvariantTest` —
  `api/app.py` 에서 도달하는 module 의 전이 폐포를 계산해 airflow import 가 없음을
  검사한다. 부모 package 를 함께 넣는다. `zeta4s.airflow.dags` 를 import 하면 python 이
  `zeta4s/airflow/__init__.py` 를 먼저 실행하므로, 거기서 worker 측 module 을 끌어오면
  `api/app.py` 는 한 줄도 바뀌지 않은 채 airflow 를 import 하게 된다.
- 같은 검사가 airflow 를 import 해도 되는 worker 측 module 목록을 못박고, `src/` 에
  `create_session`·`airflow.settings.Session`·airflow CLI subprocess·code 주입이 없음을
  본다.

**정적 검사는 근사다.** 진짜 강제는 배포에 있고 네 겹이다.

1. `zeta4s-api` 이미지에 airflow package 가 없다. `docker/zeta4s-api/Dockerfile` 은 build
   마지막에 `import airflow` 가 실패하는지 확인하므로, 의존이 되살아나면 배포된 뒤 조용히
   결합이 돌아오는 대신 build 가 깨진다.
2. Airflow service는 공식 image를 사용하고 zeta4s가 Airflow image를 빌드하지 않는다.
3. `zeta4s-api` 에 `AIRFLOW__*` 가 하나도 없다. `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` 이
   없으면 `create_session` 도 `airflow.settings.Session` 도 붙을 곳이 없다. anchor 가
   `x-zeta4s-env` 와 `x-airflow-env` 로 갈려 있고 `zeta4s-api` 는 전자만 받는다.
4. Airflow와 `zeta4s-api`가 공유하는 것은 generated DAG 전용 volume뿐이다. Airflow는
   `zeta4s-state`와 `airflow-logs`의 zeta4s 측 mount를 통해 runtime state를 읽지 않는다.

Generated DAG source는 Python 표준 라이브러리와 Airflow만 import하고, authenticated internal
endpoint로 active deployment identity와 step identity를 전달한다.

## Engine Profile Symmetry

engine 은 외부다. compose 의 `--profile airflow` 와 `--profile prefect` 는 대칭이고 어느
쪽도 기본 compose profile 이 아니다. engine 이 없어도 `zeta4s-api` 가 독립 image 로 뜬다.
반면 workspace profile 의 `scheduler` 를 생략하면 project deploy 기본값은 Prefect 다.

Airflow 만 기본으로 뜨면 교체 가능한 backend 가 아니라 "Airflow 스택에 Prefect 를 곁들인
것" 이 된다. 대칭은 배포에서 드러나야 한다.

| | 이미지 | airflow |
|---|---|---|
| `zeta4s-api` | `zeta4s-api:*` | 없다 |
| airflow process | `apache/airflow:*` (공식) | base image |
| `prefect-server` | `prefecthq/prefect:*` (공식) | 없다 |
| `prefect-worker` | `zeta4s-api:*` | 없다 |

`prefect-server` 가 공식 이미지라 version 이 compose 와 wheel 두 곳에 생긴다. 어긋나면
worker 와 server 가 다른 Prefect 가 되어 조용히 깨지므로 gate 가 대조한다 — 정본은 wheel 이다.

`tests/test_runtime_environment_contract.py` 가 profile 대칭, engine 간 누수, SDK pin 정합을
검사한다.

## Version Flow

`main` 단일 장기 브랜치 기준:

- 기능 변경: `feature/*`
- 수정: `fix/*`
- roadmap 구현 반영: pull request 로 `main` merge + 동일 version tag

정식 공개 릴리즈 전에는 version tag를 하위호환 경계로 사용하지 않는다. 현재 목표 구조에
맞는 contract 수렴과 근본 해결을 우선하며, unsupported config prefix나 Airflow Asset/Dataset
trigger를 되살리는 호환 경로는 추가하지 않는다.

## Runtime State Ownership

Airflow Connection 은 profile 로 관리되는 실행 설정이다. Scheduler pool 은 `zeta4s-api` 가
Step Graph 에서 자동 산출해 선택한 backend 에 동기화하는 projection state 다. Scheduler UI 에서
수동으로 state 를 변경하면 release gate 재현성이 떨어진다.

Project artifact registration 의 source of truth 는 metastore 다. `zeta4s-api` 는 deploy 중 metastore
registration 을 갱신하고 scheduler snapshot 을 publish 한다. Run 생성 전에 local artifact 와 metastore 의
active deployment artifact 가 다르면 stale deployment 로 처리한다.
