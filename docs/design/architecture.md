# Architecture

zeta4s 는 AI Agent 가 생성한 계약(Contract)을 Step Graph 로 실행하는 범용 Runtime Engine 이다.
AI Agent 또는 사용자는 project artifact 로 실행 계약을 선언하고, CLI/API/adapter stack 은 이
contract 를 Step Graph 로 정규화해 검증, 배포, 실행, 관측한다.

## Repository Layout

```text
src/zeta4s/        엔진 구현
  core/            scheduler 중립 실행 의미론과 step 실행 경계
  project/         project artifact 해석, step graph 정규화, ExecutionPlan 생성
  runtime/         step type 별 runtime 구현과 backend adapter
  airflow/         Airflow REST adapter와 standalone DAG source projection
  prefect/         Prefect scheduler engine adapter
  api/             zeta4s-api service
  cli/             z4s CLI
  metastore/       metastore backend
  config/          CLI/profile config 해석
  common/          공통 error, message, identifier
  dbt/             dbt model contract 정적 검증
packages/          배포 단위 wheel. zeta4s-cli, zeta4s-api
docker/            zeta4s-api image, scheduler 설정, postgres init, 관측 설정
deploy/            k3s 배포 manifest
scripts/           개발/검증 gate 와 설치 script
tests/             계약 test
zeta4s-work/       canonical showcase workspace
docs/              문서
```

의존 방향은 `airflow`, `prefect`, `api`, `cli` → `core`, `project`, `runtime` 단방향이다.
`core`, `project`, `runtime` 은 `airflow` 와 `prefect` 를 import 하지 않는다. 이 경계는
pytest 가 강제한다. 상세는 `../gate/README.md` 의 Scheduler Boundary 절에 있다.

Prefect adapter 는 `zeta4s.prefect` 에 둔다.

## Technology Stack

| 층 | 기술 |
|----|------|
| 언어 | Python. 최소 버전은 `pyproject.toml` 의 `requires-python` |
| 빌드 | setuptools. version 은 `src/zeta4s/__init__.py` 의 `__version__` 에서 dynamic 으로 읽는다 |
| 도구 | `uv`. lint/format 은 `ruff` |
| contract model | pydantic |
| project artifact | YAML |
| CLI | click |
| API | FastAPI, uvicorn |
| metastore | PostgreSQL 기본. ClickHouse 선택 |
| rowset storage | Parquet(core-orchestrated), Iceberg(scheduler-projected) |
| data backend | ClickHouse(`clickhouse-connect`), Oracle(`oracledb` thin mode), Elasticsearch(client library 없이 `urllib` HTTP 호출) |
| transform | dbt, native SQL |
| scheduler backend | Airflow, Prefect |

정확한 package 와 version 은 `pyproject.toml` 이 정본이다. 여기에 복제하지 않는다.

zeta4s scheduler stack의 release 단위는 host `zeta4s-cli`, server/adapter `zeta4s-api`,
Airflow와 Prefect다. zeta4s distribution version은 `src/zeta4s/__init__.py`에서 함께 파생한다.
Prefect는 API image의 SDK pin과 공식 server image pin을 같게 유지한다. Airflow는 zeta4s SDK를
설치하지 않으므로 공식 image pin을 독립적으로 관리한다. data backend와 observability service의
version 갱신은 scheduler stack 갱신과 별도 과제로 다룬다.

## Components

| Component | 역할 |
|-----------|------|
| `z4s` CLI | host 에서 project/profile 검증, API 명령 호출, local report 저장 |
| `zeta4s-api` | Docker stack 안에서 metastore bootstrap/갱신, artifact storage 저장, runtime connection 해석, scheduler backend 선택과 배포 해제, core step 실행, report 생성 |
| Airflow | standalone DAG source가 internal API를 호출하는 공식 scheduler backend |
| Prefect | `jobs/*.yml` 의 schedule definition 을 deployment 로 projection 하고 `ExecutionPlan` 을 실행하는 backend |
| Workspace | Git repository 로 관리하는 것을 권장하는 `profiles/`, `projects/` 상위 디렉토리 |
| Project artifact | AI Agent 또는 사용자가 생성하는 contract 파일. `project.yml`, `jobs/*.yml`, SQL/dbt 파일 |
| Profile | project step 이 사용하는 외부 connection 과 환경 값 |
| Metastore | deploy registration, artifact metadata, run/report/watermark/extract history/stage binding metadata. 기본 구현은 PostgreSQL 18이며 ClickHouse adapter도 선택 가능 |
| Artifact storage | project bundle, 압축 해제된 artifact cache, Airflow standalone DAG source 저장소 |
| Rowset storage | step 사이 row batch를 보존하는 runtime 내부 저장소. local runner 는 ephemeral Parquet, scheduler-projected 실행은 Iceberg snapshot을 사용 |
| Data backend adapters | profile connection `type` 으로 선택되는 stage/transform/write table operation adapter |

## Installation Boundary

| Install target | 포함 범위 |
|----------------|-----------|
| `zeta4s-cli` | host `z4s` CLI, workspace/project/profile command, `zeta4s-api` client |
| `zeta4s-api` | API server, metastore owner, Airflow/Prefect/backend/dbt/rowset adapter layer |

Adapter layer 는 `zeta4s-api` 설치본 내부 구현이다. 세 번째 설치본은 만들지 않는다.

## Airflow 는 외부 engine 이다

`zeta4s-api` 는 Airflow 에 **REST API 로만** 붙는다. Airflow metastore 를 직접 열지 않고,
Airflow CLI 를 부르지 않는다. 이것이 Airflow 를 교체 가능한 backend 로 만드는 조건이다 —
metastore 를 공유하면 adapter 가 아니라 같은 배포 단위가 된다.

**실행 위치가 기준이다.** Airflow process 에서 도는 코드는 `zeta4s-api` 가 publish 한
standalone DAG source 뿐이며, Airflow package 와 Python 표준 라이브러리만 import 한다.
zeta4s package 는 Airflow image 에 설치하지 않는다. `zeta4s-api` process 에서 도는 코드는
airflow 를 import 하지 않는다.

`zeta4s.airflow.rest_client` 가 인증·재발급·timeout·오류 변환을 한 곳에서 다룬다. Prefect 가
공식 SDK 를 쓰는 자리를 Airflow 쪽에서는 이 client 가 채운다. 대칭은 **경계**의 문제이지
의존 목록이 같아야 한다는 뜻이 아니다.

`zeta4s-api` 는 Airflow package 없는 자기 이미지로 실행한다. 기본 stack 은 API와 공통 상태만
기동하고, Airflow와 Prefect engine은 각각 독립 compose profile이다.

이 경계는 gate 가 강제한다 — `../gate/README.md` 의 Airflow Headless Boundary 를 본다.

## Control Flow

1. AI Agent 또는 사용자가 project artifact contract 를 생성한다.
2. 사용자가 host 에서 `z4s project check` 로 project artifact 를 정적으로 검증한다.
3. 사용자가 host 에서 `z4s profile check` 로 실행 profile 을 검증한다.
4. `z4s api deploy` 가 project bundle 과 profile 이름을 `zeta4s-api` 에 전송한다.
5. API 는 profile 의 `scheduler` 를 해석하고 bundle 과 profile 을 검증한다. Field 가 없으면
   Prefect 를 선택한다.
6. API 는 같은 Step Graph contract 를 선택한 Airflow 또는 Prefect backend 에 배포한다.
7. API 는 실제 선택값을 project 별 active deployment metadata 의 `scheduler_backend` 로 저장한다.
8. Undeploy 는 local profile 이 아니라 이 metadata 로 원래 backend 를 해석한다.
9. run/step 상태와 `attempt` 는 backend 와 무관한 같은 metastore contract 로 기록한다.
10. `z4s api run`은 active deployment에서 scheduler adapter를 선택하고 native run을 canonical
    `project_id`/`job_id`/`run_id` 상태로 정규화한다.

## Boundary

`z4s` CLI 는 컨테이너 밖 host 환경에서 실행한다. Linux/macOS 계열 shell, Windows PowerShell,
Windows Git Bash 에서 같은 command surface 로 동작해야 한다. Docker stack 내부 Python import path 나
source bind 에 의존하지 않는다. API/adapter image 는 package install 결과를 사용한다.

`zeta4s-api` 는 adapter state 를 변경하는 control plane 이다. CLI 가 필요한 실행 동작은
우회 스크립트가 아니라 API endpoint 로 노출한다.

Airflow Connection 은 profile 로 관리되는 실행 설정이다. Scheduler pool 은 `zeta4s-api` 가
Step Graph 에서 자동 산출해 선택한 backend 에 동기화하는 projection state 다. Project YAML 은
Airflow Pool 이나 Prefect Global Concurrency Limit 같은 scheduler resource 형식에 의존하지 않는다.
자동 pool 이 기본이고 explicit pool binding 은 권장하지 않는다. 현재 Airflow 는 자동 pool 을 task 에
binding 하고 Prefect 는 같은 effective pool 이름의 Global Concurrency Limit 을 task 실행 동안 점유한다.
Pool 이름 선택은 backend-neutral project contract 가 소유하고 resource projection은 각 scheduler
adapter가 소유한다.

## Supported Adapter Scope

- Project artifact 는 `project.yml`, `jobs/*.yml`, SQL/dbt 파일로 구성한다.
- Workspace 는 `profiles/` 와 `projects/` 를 포함한다.
- Profile 은 workspace 의 `profiles/*.yml` 로 관리한다.
- 현재 로컬/검증 스택은 같은 PostgreSQL server의 별도 `zeta4s_metastore` database를 기본 metastore로 사용한다.
- ClickHouse metastore adapter는 `ZETA4S_METASTORE_TYPE=clickhouse`로 선택하며 Docker `asset` profile이 필요하다.
- Data backend adapter 는 ClickHouse, Oracle, Elasticsearch 를 지원한다. table stage/transform backend 는
  ClickHouse 와 Oracle 이다.
- Transform provider 는 `dbt.run`, `clickhouse.sql`, `oracle.sql` 이다.
- Job graph 는 정적 step 목록과 명시 dependency 로 구성한다.
- Scheduler run 실행과 adapter state 변경은 `zeta4s-api`의 공용 run service를 통해 관리한다.
- Schedule 배포는 `z4s api deploy`에 포함한다. 별도 `z4s schedule` 그룹은 제공하지 않는다.
- Elasticsearch extract는 scheduler recovery가 PIT retention 안에 끝나는 동안 PIT/search-after로 exact
  resume한다. PIT가 만료되면 닫힌 실패를 반환한다. Oracle/ClickHouse extract와 append write는 restart-only다.
