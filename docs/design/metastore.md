# Metastore Contract

## 목적

zeta4s metastore 는 zeta4s 공통 상태를 저장하는 control-plane 저장소다. Stage/transform/write 가
사용하는 업무 데이터 저장소와 분리한다.

Metastore 구현체와 runtime data backend 는 같은 제품을 사용할 수 있지만 contract 상 같은 저장소로 보지
않는다.

## Backend Selection

기본 metastore backend 는 PostgreSQL 18 이다. 로컬 Docker stack 은 Airflow/Prefect 가 사용하는
`postgres:18.4-alpine` service 안에 `zeta4s_metastore` database 를 별도로 만들고, zeta4s metadata
table 은 이 database 에만 생성한다.

```text
postgres service
  airflow            # Airflow metadata
  prefect            # Prefect server metadata
  zeta4s_metastore   # zeta4s control-plane metadata
```

`ZETA4S_METASTORE_TYPE` 기본값은 `postgres` 다. PostgreSQL 연결은
`ZETA4S_METASTORE_DSN` 으로 설정하며 로컬 기본값은
`postgresql://airflow:airflow@postgres:5432/zeta4s_metastore` 다. 운영 환경은 같은 contract 의 외부
PostgreSQL DSN 을 service config/secret 으로 주입한다.

기존 ClickHouse metastore adapter 는 제거하지 않는다. `ZETA4S_METASTORE_TYPE=clickhouse` 로 선택할 수
있으며 기존 ClickHouse 전용 connection 환경 변수를 사용한다. Metastore backend 선택과 project 의
ClickHouse runtime data backend 사용 여부는 독립적이다.

Docker Compose 의 ClickHouse service 는 기본 control-plane stack 에 포함하지 않고 `asset` profile 에
둔다. ClickHouse runtime data backend showcase 또는 ClickHouse metastore adapter regression을 실행할 때만
`docker compose --profile asset` 으로 기동한다.

PostgreSQL database 자체의 생성과 role 권한 부여는 container init 또는 운영 provisioning 이 소유한다.
`z4s api bootstrap` 은 이미 존재하는 database 안에서 zeta4s table/index 를 생성하고 inspection 한다.
기존 backend 사이의 metadata migration 과 dual-write 는 제공하지 않는다.

## 용어 정리

| 용어 | 의미 |
|------|------|
| Metastore | zeta4s zeta4s 가 실행을 관리하기 위해 쓰는 metadata DB. 업무 row data 를 저장하는 DB 가 아니다. |
| Control plane | deploy, registration, run 상태, report 처럼 실행을 통제하고 추적하는 영역. |
| Runtime data backend | `stage`, `dbt.run`, `<db>.sql`, `write` step 이 실제 업무 table 을 읽고 쓰는 DB 또는 target system. |
| Data plane | Runtime data backend 에 저장되는 업무 데이터 영역. |
| Artifact storage | project bundle, 압축 해제된 artifact cache, scheduler parse snapshot 을 저장하는 파일/object 저장소. |
| Scheduler snapshot | Airflow scheduler 가 DAG parse 때 읽는 read-only 배포 상태 파일. DB/API 장애가 parse 실패로 바로 번지지 않게 하는 캐시다. |
| Source of truth | 충돌이 있을 때 최종 기준으로 삼는 저장소. Deploy metadata 의 source of truth 는 metastore 다. |
| Backend role | Project runtime connection 이 맡는 data-plane 역할. `source`, `stage`, `transform`, `write` 로 구분한다. |
| Active deployment | 특정 project 에 대해 실행 기준으로 선택된 artifact registration. |
| Step output binding | step 의 논리 output 이름과 실제 artifact/object/table 위치를 연결한 metadata. |
| Operation report | `api deploy`, `api redeploy` 같은 API operation 의 단계별 실행 결과 report. |
| Metastore adapter | metastore 구현체별 DDL, transaction, query 차이를 감추는 adapter. |
| Metastore repository | deploy, run, step state/event/output binding 같은 zeta4s metadata 를 읽고 쓰는 port. |
| Transactional write | 여러 metadata 변경이 모두 성공하거나 모두 실패해야 하는 저장 방식. 중간 상태가 active deployment 로 노출되면 안 된다. |
| Atomic publish | scheduler snapshot 파일을 쓰는 중간 상태 없이 한 번에 교체하는 방식. Scheduler 는 완성된 이전 snapshot 또는 새 snapshot 만 읽어야 한다. |
| Profile | project step 이 사용하는 외부 connection 과 환경 값을 담는 사용자별 실행 설정. Metastore connection 과 scheduler pool resource 를 포함하지 않는다. |

## 정규 실행 모델

zeta4s 의 metastore 정규 관계는 `project_id > job_id > step_id` 다. 이 세 값은 표시명이 아니라
identity 다.

- `project_id`: deploy 단위이자 artifact 생성 단위, metastore 최상위 namespace 다. 같은 metastore 안에서
  unique 해야 한다.
- `job_id`: project 안의 실행 단위 identity 다. 같은 `project_id` 안에서 unique 해야 한다.
- `step_id`: job 안의 logical 작업 단위 identity 다. 같은 `(project_id, job_id)` 안에서 unique 해야 한다.

`display_name` 은 사람이 읽는 표시명이며 optional 이다. `display_name` 은 unique key 로 쓰지 않는다.
`name` 은 identity 필드로 쓰지 않는다.

### Project authoring contract

`project.yml` 은 project 작성 계약의 필수 파일이다. `project.yaml` 은 기본 계약으로 두지 않는다.

필수 field:

- `project_id`
- `paths`

선택 field:

- `display_name`

예:

```yaml
project_id: retail
display_name: Retail Data Platform
paths:
  jobs: jobs
  dbt: dbt
```

### Job authoring contract

`jobs/*.yml` 파일 하나는 하나의 job 을 정의한다. 파일명은 identity 가 아니며 `job_id` 가 source of truth 다.

필수 field:

- `job_id`
- `steps`

선택 field:

- `display_name`
- `schedule`

예:

```yaml
job_id: daily_mart
display_name: Daily Mart Build
schedule:
  cron: "0 2 * * *"
  timezone: Asia/Seoul
  paused: false
steps:
  - step_id: extract_orders
    type: oracle.extract
```

### Step authoring contract

`steps[]` item 하나는 하나의 logical step 을 정의한다.

필수 field:

- `step_id`
- `type`

선택 field:

- `display_name`
- `depends_on`

`conn`, `source`, `output`, `map`, `query`, `params`, `watermark`, `time_window`, `batch_size` 같은 field 는
step type 별 contract 가 정의한다.

예:

```yaml
steps:
  - step_id: build_mart
    display_name: Build Mart
    type: dbt.run
```

### Runtime projection

Airflow 는 runtime projection 이다. zeta4s `job_id` 는 Airflow DAG 로 materialize 되고, zeta4s `step_id`
는 Airflow task 하나 이상으로 materialize 될 수 있다.

```text
(project_id, job_id) -> dag_id
(project_id, job_id, step_id) -> task_projection[]
```

`dag_id` 와 `task_id` 는 Airflow projection 식별자이며 metastore 의 정규 key 가 아니다. Airflow `dag_id`
는 API 조회 context 나 runtime bridge 에서만 보조 식별자로 사용한다. `dag_id` 에서 `project_id`/`job_id`
를 복원하는 처리는 Airflow projection 을 해석하는 경계에만 둔다.

Task projection 최소 field:

- `task_id`
- `task_display_name`

`task_id` 는 zeta4s 가 runtime projection 단계에서 생성한 physical task identity 다. Airflow adapter 는
core `RunReporter` 경계로 같은 값을 `step_execution` 에 기록한다.

`task_display_name` 은 UI 표시용 이름이며 adapter 가 생성한다. 사용자가 job YAML 에서 직접 작성하는
core authoring field 가 아니다.

예:

```json
{
  "project_id": "retail",
  "job_id": "daily_mart",
  "step_id": "build_mart",
  "tasks": [
    {
      "task_id": "build_mart__raw_orders",
      "task_display_name": "Build Mart / raw_orders"
    },
    {
      "task_id": "build_mart__daily_revenue",
      "task_display_name": "Build Mart / daily_revenue"
    }
  ]
}
```

단일 task 로 materialize 되는 step 도 같은 projection 형태를 가진다.

```json
{
  "step_id": "extract_orders",
  "tasks": [
    {
      "task_id": "extract_orders",
      "task_display_name": "Extract Orders"
    }
  ]
}
```

UI 는 `task_display_name`, step `display_name`, `task_id` 순서로 fallback 한다.

### Validation

Metastore 에 deploy/run/step metadata 를 쓰는 경로는 `project_id`, `job_id`, `step_id` 정규 key 를 반드시
검증해야 한다. 이 세 key 가 빠진 deploy 와 run 은 정상 runtime 에서 발생할 수 없으며, 발생하도록
보완하거나 추론하지 않는다. 검증 계층은 누락을 발견하면 metadata 를 쓰기 전에 실패시킨다.

## 역할 분리

zeta4s runtime 저장소는 다음 세 계층으로 나눈다.

```text
metastore
  zeta4s 공통 metadata 저장

artifact storage
  project bundle 과 scheduler parse cache 저장

runtime data backend
  stage, transform, write source table 저장
```

### Metastore

Metastore 는 다음 정보를 저장한다.

- deploy registration
- artifact metadata
- runtime operation report 와 step report
- project/job/step registry
- DAG registration state
- run metadata
- step execution state
- task projection metadata
- step checkpoint state
- step event history
- step output binding
- backend registry

Metastore 는 source/stage/transform target table 의 row data 를 저장하지 않는다.

### Artifact Storage

Artifact metadata 는 deploy profile 에서 생성한 runtime connection policy projection 을 포함한다. 이
projection 은 connection shape 와 `password_ref` 값만 저장하며 plaintext password 를 저장하지 않는다.
Scheduler task가 internal execution endpoint를 호출하면 `zeta4s-api`가 encrypted secret store에서
credential을 resolve하고 같은 process에서 core step을 실행한다.

Artifact storage 는 project bundle payload 와 scheduler 가 읽을 수 있는 artifact cache 를 저장한다.

- project bundle `tar.gz`
- extracted project artifact cache
- Airflow standalone DAG source

DB 에 artifact payload 전체를 BLOB 로 저장하는 것은 기본 계약으로 두지 않는다. Metastore 는 artifact
checksum 과 storage URI 만 저장한다.

### Runtime Data Backend

Runtime data backend 는 step type 의 `conn` 으로 선택한다.

- `clickhouse.stage`: ClickHouse table 을 만든다.
- `oracle.stage`: Oracle table 을 만든다.
- `clickhouse.sql`: ClickHouse 에 SQL 을 실행한다.
- `oracle.sql`: Oracle 에 SQL 을 실행한다.
- `dbt.run`: `conn` 에 해당하는 단일 backend 에서 dbt model 을 실행한다.
- `clickhouse.write`: storage-neutral rowset reader를 통해 ClickHouse target table 에 반영한다.
- `elasticsearch.command`: Elasticsearch API command 로 index/document 상태를 변경한다.

Runtime data backend 는 metastore table 을 소유하지 않는다.

## Adapter Foundation

Metastore 는 runtime data backend adapter 와 같은 방식으로 adapter boundary 를 가진다. 단, 둘은 서로
다른 adapter family 다.

```text
zeta4s.metastore
  factory / interface / repository contract

zeta4s.metastore.backends.*
  metastore 구현체

zeta4s.runtime.backends.*
  runtime data backend 구현체
```

Metastore adapter 는 profile 을 읽지 않는다. Metastore endpoint, credential, namespace 는
`zeta4s-api` service config, environment, secret 으로만 주입한다.

Profile connection 은 source/stage/transform/write backend 전용이다. 같은 물리 제품을 쓰더라도
metastore connection 과 profile connection 은 별도 lifecycle 로 본다.

Metastore interface 는 최소 다음 repository 를 제공한다.

- `DeploymentRepository`
- `ArtifactRepository`
- `OperationReportRepository`
- `RunMetadataRepository`
- `StepExecutionRepository`
- `TaskProjectionRepository`
- `StepStateRepository`
- `StepEventRepository`
- `StepOutputBindingRepository`
- `BackendRegistryRepository`

Metastore adapter 는 다음 책임만 가진다.

- metastore schema/bootstrap
- metadata transaction
- repository query/write
- metastore health/readiness

Metastore adapter 는 다음을 하지 않는다.

- project runtime backend table 생성
- project runtime backend table 삭제
- profile connection 적용
- source/stage/transform/write SQL 실행
- rowset data load/write

## Repository Contract

Metastore repository 는 DB 제품별 SQL 차이를 숨기고 같은 의미의 operation 을 제공한다. `zeta4s-api` 와
runtime code 는 ClickHouse, PostgreSQL, Oracle 같은 물리 구현체의 DDL, JSON type, latest-row query
방식을 알지 않는다.

Repository 공통 규칙:

- 모든 write 는 정규 key 를 필수로 받는다.
- Project scope key 는 `project_id`, `job_id`, `step_id` 를 사용한다.
- Airflow projection key 인 `dag_id`, `task_id` 는 projection 상태와 실행 관측에만 사용한다.
- Adapter 는 logical key 별 current state 또는 append-only event 의미를 보존한다.
- ClickHouse current-state table 은 `revision` 과 deterministic tie-breaker 로 latest row 를 선택한다.
- PostgreSQL current-state table 은 business key `PRIMARY KEY`와 `INSERT ... ON CONFLICT DO UPDATE`로
  current row 하나를 유지한다.
- list 조회는 filter key, limit, ordering 의미를 repository contract 로 고정한다.
- JSON payload 는 repository 입출력에서 structured object 로 다루고, adapter 가 DB별 저장 표현으로 변환한다.
- Adapter 는 DB별 timestamp precision 차이가 latest selection 결과를 바꾸지 않게 해야 한다.

Repository 별 최소 operation:

| Repository | 최소 operation |
|------------|----------------|
| `DeploymentRepository` | active deployment upsert, remove, project 조회, active list |
| `ArtifactRepository` | artifact upsert, artifact id 조회 |
| `OperationReportRepository` | operation report save, operation id 조회, project별 list |
| `RunMetadataRepository` | run create/update, run id 조회, project/job별 list |
| `StepExecutionRepository` | task attempt execution record, run별 execution list |
| `TaskProjectionRepository` | deploy artifact 기준 task projection record/list |
| `StepStateRepository` | state upsert, state get, project/job/step별 list |
| `StepEventRepository` | event record, project/job/step/run별 list |
| `StepOutputBindingRepository` | output binding record, upstream output resolve |
| `BackendRegistryRepository` | profile connection role record/list |
| `SecretRepository` | encrypted secret version write, active metadata 조회, active ciphertext resolve |

## Logical Schema Model

Metastore schema 의 canonical 표현은 DB-agnostic logical model 이다. PostgreSQL/ClickHouse DDL 은
adapter 구현체일 뿐이며 canonical schema 자체가 아니다.

Logical type:

| Logical type | 의미 |
|--------------|------|
| `text` | identity, status, URI, command 같은 문자열 |
| `integer` | count, attempt, version 같은 정수 |
| `timestamp` | UTC instant. Adapter 는 DB별 timestamp precision 을 정규화한다. |
| `json` | structured payload. Adapter 는 native JSON, JSONB, CLOB/String 중 DB에 맞는 저장 표현을 선택한다. |

Logical schema 규칙:

- Column 이름, required 여부, logical type, key 역할은 metastore contract 가 정의한다.
- DB별 physical type, engine, partition, index, ordering key 는 adapter 구현 세부다.
- `String`, `DateTime64(3)`, `UInt64`, `ReplacingMergeTree`, `argMax` 같은 표현은 ClickHouse adapter 내부에만 둔다.
- PostgreSQL adapter 는 `TEXT`, `INTEGER/BIGINT`, `TIMESTAMPTZ`, `JSONB`, `PRIMARY KEY`,
  `ON CONFLICT` 로 같은 logical schema 를 구현한다.
- Schema inspection 은 physical DDL 문자열 비교가 아니라 required logical column 과 capability 를 검증한다.
- Fresh bootstrap 은 logical schema 를 해당 adapter 의 physical schema 로 materialize 한다.
- 아래 table 정의의 `revision` 은 multi-row current-state adapter가 제공해야 하는 ordering capability 다.
  PostgreSQL처럼 primary-key upsert로 current row 하나만 유지하는 adapter는 physical `revision` column
  없이 같은 capability를 충족할 수 있다.

## PostgreSQL Physical Contract

`PostgresMetastoreAdapter` 는 `MetastoreAdapter` protocol 의 repository 전체를 구현한다.

- `DeploymentRepository`
- `ArtifactRepository`
- `OperationReportRepository`
- `RunMetadataRepository`
- `StepExecutionRepository`
- `StepStateRepository`
- `StepEventRepository`
- `StepOutputBindingRepository`
- `BackendRegistryRepository`
- `SecretRepository`

상태성 table 의 primary key 는 각 repository logical key 와 같다. `deploy_registration` 은
`project_id`, `artifact` 는 `artifact_id`, `run_metadata` 는 `run_id`, `step_execution` 은
`(project_id, job_id, run_id, step_id, task_id, attempt)` 를 사용한다. 나머지 상태성 table 도 현재
logical key 를 composite primary key 로 사용한다.

`step_event` 와 `operation_report` 이력은 append-only 로 저장한다. `step_event` 의 physical identity 는
adapter 내부 `BIGSERIAL` key이고 repository payload에는 노출하지 않는다. JSON payload 는 `JSONB`, UTC
instant 는 `TIMESTAMPTZ`를 사용한다. Repository method 하나의 write는 하나의 transaction 안에서
commit되고, 예외가 발생하면 rollback된 뒤 원래 database exception을 상위 API failure contract로
전달한다.

Schema inspection 은 `information_schema.columns`와 PostgreSQL catalog에서 required table, logical key,
JSON/timestamp capability를 읽기 전용으로 확인한다. `bootstrap()` 전 inspection은 `missing`, 모든
table/index 생성 후 inspection은 `ok`를 반환한다.

PostgreSQL adapter 는 `psycopg 3` sync client를 사용한다. SQL parameter는 `%s` binding으로 전달하고
identity/value를 문자열 결합으로 query에 삽입하지 않는다. Secret repository는 ClickHouse adapter와
동일하게 ciphertext envelope만 저장하고 plaintext column을 만들지 않는다.

## Bootstrap Ownership

`zeta4s-api` 는 metastore 의 owner 다. `z4s` CLI 는 metastore 에 직접 접속하지 않고
`zeta4s-api` endpoint 를 호출한다.

```text
z4s api bootstrap
  -> zeta4s-api
    -> configured metastore adapter
      -> bootstrap fresh schema
      -> inspect schema
      -> return operation report
```

Bootstrap 은 fresh schema contract 다. 기존 schema 를 목표 schema 로 변환하는 migration 은 목표
contract 에 포함하지 않는다.

`api deploy` 는 metastore bootstrap 을 암묵 실행하지 않는다. Metastore 가 준비되지 않았으면
`z4s api bootstrap` 실행을 안내하고 실패한다.

API endpoint:

```text
POST /api/v1/platform/bootstrap
GET  /api/v1/platform/status
```

`POST /api/v1/platform/bootstrap` 은 다음을 수행한다.

- configured metastore backend 확인
- adapter capability 확인
- schema bootstrap
- schema inspection
- operation report 저장

`GET /api/v1/platform/status` 는 다음을 반환한다.

- API version
- metastore backend
- bootstrap status
- schema status
- scheduler snapshot status

## Data Plane 금지사항

Project runtime database 또는 schema 안에는 zeta4s metadata table 을 만들지 않는다. Runtime 실행
상태와 event 는 metastore 의 logical table 에 project key 를 포함해 기록한다.

zeta4s metadata table:

```text
zeta4s_metastore.step_execution
zeta4s_metastore.step_state
zeta4s_metastore.step_event
zeta4s_metastore.step_output_binding
```

Project data plane object:

```text
z4p_sales_pipeline.stg_orders
z4p_sales_pipeline.dim_customer
```

`z4p_<project_id>` 계열 namespace 는 ClickHouse runtime data backend 를 선택한 project 의 data plane 이다.
Metastore 구현체가 ClickHouse 라도 zeta4s metadata 를 이 namespace 에 저장하지 않는다.

## Metadata Model

### deploy_registration

Project 의 active deployment 를 기록한다.

필수 column:

- `project_id`
- `artifact_id`
- `profile`
- `registered_at`
- `dag_ids`
- `status`
- `revision`
- `updated_at`

`project_id` 는 하나의 active deployment 를 가진다. 새 deploy 는 같은 project 의 registration 을
transactional 하게 교체한다.

### artifact

Artifact payload 의 identity 와 storage 위치를 기록한다.

필수 column:

- `artifact_id`
- `project_id`
- `checksum`
- `storage_uri`
- `created_at`
- `revision`
- `updated_at`
- `created_by`

Artifact payload 는 object storage 또는 shared artifact cache 에 둔다. Metastore 의 `storage_uri` 는
그 위치를 가리킨다.

### operation_report

Runtime operation 의 최종 report 를 저장한다.

필수 column:

- `operation_id`
- `command`
- `project_id`
- `status`
- `summary`
- `created_at`
- `revision`
- `updated_at`

### operation_step_report

Runtime operation 의 step 별 상태를 저장한다.

필수 column:

- `operation_id`
- `operation_step_id`
- `status`
- `summary`
- `issue_codes`
- `updated_at`

### secret

zeta4s-managed secret value 를 application-level encryption 후 저장한다. 이 table 은 plaintext secret 을
저장하지 않는다. Secret master key 는 metastore 밖의 master key provider 가 제공한다.

필수 column:

- `secret_key`
- `version`
- `ciphertext`
- `algorithm`
- `key_id`
- `status`
- `created_at`
- `rotated_at`
- `revision`
- `updated_at`

`secret_key` 는 profile 의 `password_ref` 와 매칭되는 logical secret key 다. `version` 은 같은
`secret_key` 의 secret rotation 순서를 나타낸다. 새 secret 등록과 rotation 은 append 방식으로 새
version row 를 기록한다.

`status` 는 다음 값을 가진다.

- `active`
- `rotated`
- `revoked`
- `deleted`

하나의 `secret_key` 에 대해 active version 은 하나여야 한다. Latest active 조회는 `updated_at` 단독이
아니라 `revision` 과 `version` 을 포함한 deterministic ordering 으로 선택한다.

`ciphertext` 는 authenticated encryption 결과다. Adapter 는 DB별 binary/string 표현을 선택할 수 있으나
repository boundary 에서는 plaintext 를 반환하지 않고 decrypt 단계에 필요한 encrypted envelope 만 다룬다.
`algorithm` 은 예를 들어 `AESGCM256` 같은 encryption suite 를 기록한다. `key_id` 는 master key provider
가 key version/alias 를 노출할 때만 채운다.

금지:

- plaintext password column
- operation report 로 secret value 복사
- Airflow metastore connection password 를 production credential 저장소로 사용하는 것

### run_metadata

zeta4s API와 scheduler runtime이 생성한 canonical run metadata를 저장한다. Native scheduler
상태는 adapter가 정규화하며 metastore는 scheduler 종류와 무관한 identity, 상태, 입력과
zeta4s context를 보관한다.

필수 column:

- `run_id`
- `project_id`
- `job_id`
- `scheduler_run_id`
- `artifact_id`
- `created_at`
- `run_json`
- `revision`
- `updated_at`

`scheduler`, `state`, `parameters`, `adapter_metadata`와 canonical timestamp는 `run_json` 계약이다.
Native scheduler object id와 원본 상태는 `adapter_metadata`에만 두고 canonical identity로 쓰지
않는다.

### step_execution

각 step attempt 의 실행 상태를 기록한다.

필수 column:

- `project_id`
- `job_id`
- `run_id`
- `step_id`
- `task_id`
- `step_type`
- `attempt`
- `metadata.adapter_attempt`: scheduler infrastructure retry 횟수. canonical `attempt` 와 분리한다.
- `status`
- `started_at`
- `ended_at`
- `revision`
- `updated_at`

`step_id` 는 zeta4s logical step identity 이고, `task_id` 는 runtime projection 단계에서 생성된
physical task identity 다. `dbt.run` 처럼 하나의 logical step 이 여러 task 로 분해될 수 있으므로 둘을
분리해 저장한다.

`step_type` 별 상세 payload 는 공통 column 을 늘리지 않고 `metadata` 또는 별도 event payload 로 기록한다.

### task_projection

runtime projection 단계에서 생성한 physical task metadata 를 기록한다. UI 는 이 projection 을 기준으로
logical step 과 physical task 목록을 표시한다.

필수 column:

- `project_id`
- `job_id`
- `step_id`
- `task_id`
- `task_display_name`
- `artifact_id`
- `revision`
- `updated_at`

`task_id` 는 zeta4s 가 projection 단계에서 생성한다. Airflow 는 실행 중 같은 값을
`TaskInstance.task_id` 로 노출한다. `task_display_name` 은 adapter 가 생성하는 UI 표시명이며 authoring
YAML 의 core field 가 아니다.

### step_checkpoint

`step_checkpoint`는 같은 `project_id/job_id/run_id/step_id/task_id/unit_id` 안에서 sequence가 단조
증가하는 append-only processing checkpoint다. `step_state`의 실행 간 watermark와 달리 한 step
attempt의 중간 진행과 재시작 위치를 기록한다.

checkpoint commit 순서는 다음으로 고정한다.

1. row batch를 Iceberg snapshot으로 commit한다.
2. commit된 `table_identifier`와 `snapshot_id`를 metastore checkpoint row에 기록한다.
3. step이 성공한 뒤에만 최종 `step_output_binding`을 공개한다.

Iceberg commit 후 metastore 기록 전에 process가 종료되면 snapshot은 orphan이다. checkpoint selection은
metastore가 참조하는 snapshot만 사용하며 orphan을 자동 재개 대상으로 선택하지 않는다. 반대로 metastore가
참조하는 snapshot이 없으면 corruption으로 닫힌 실패를 반환한다.

resume capability는 source/target adapter가 선언한다. Elasticsearch PIT/search-after extract는 PIT
retention 안의 recovery에 한해 exact이며 만료된 PIT는 닫힌 실패를 반환한다. Oracle/ClickHouse extract와
append write는 restart-only다. replace/upsert target은 checkpoint receipt로 중복 반영을 막는다.

### step_state

step 이 다음 실행에서 참조해야 하는 checkpoint/state 를 기록한다.

필수 column:

- `project_id`
- `job_id`
- `step_id`
- `state_key`
- `state_value_json`
- `state_type`
- `run_id`
- `revision`
- `updated_at`

예:

- incremental extract 의 watermark
- paginated API cursor
- external command resume token
- validation baseline version

State key 는 step adapter 가 정의한다. Metastore 는 key/value 를 저장하지만 특정 step type 의 필드 구조를
상위 schema 로 승격하지 않는다.

Extract watermark state:

- `state_key`: `watermark:{output_name}:{watermark_column}`
- `state_type`: `watermark`
- `state_value_json`: `{"value": "...", "column": "...", "output": "..."}`

### step_event

step 실행 중 발생한 이력성 event 를 기록한다.

필수 column:

- `project_id`
- `job_id`
- `run_id`
- `step_id`
- `event_type`
- `event_time`
- `status`
- `payload`

예:

- extract selection window 와 loaded row count
- write affected row count
- dbt model/test result summary
- external command response summary

### step_output_binding

step 의 논리 output 과 실제 artifact/object/table 위치를 연결한다.

필수 column:

- `project_id`
- `job_id`
- `run_id`
- `step_id`
- `output_name`
- `output_kind`
- `backend_conn`
- `backend_type`
- `object_ref`
- `artifact_id`
- `metadata`
- `created_at`

`object_ref` 는 output 종류에 따라 table name, artifact URI, external object id 를 담는다.
Metastore 는 step output reference 를 후속 step 의 data reference 로 해석할 때 이 binding 을 사용한다.
`stage` table binding 은 `output_kind=table` 인 `step_output_binding` 의 한 사례다.

### backend_registry

Profile connection 의 data-plane 역할을 기록한다. Metastore endpoint 는 backend registry 에 기록하지
않는다.

필수 column:

- `connection_id`
- `connection_type`
- `role`
- `status`
- `updated_at`

`role` 은 다음 값을 가진다.

- `source`
- `stage`
- `transform`
- `write`

하나의 connection 이 여러 role 을 가질 수 있다.

## Deploy 와 Scheduler

Metastore 는 deploy metadata 의 source of truth 다. Airflow scheduler parse 경로는 metastore 에 강하게
의존하지 않는다.

```text
z4s api deploy
  -> metastore transaction
  -> artifact storage write
  -> scheduler snapshot publish

Airflow scheduler
  -> scheduler snapshot read
  -> DAG parse
```

Scheduler snapshot 은 read-only artifact 다. DB/API 장애가 DAG parse 실패로 전파되지 않도록,
scheduler 는 마지막 정상 snapshot 을 계속 읽을 수 있어야 한다.

deploy 가 선택한 실제 backend 는 project 의 active deployment metadata 에
`scheduler_backend` 로 기록한다. 이후 local profile 의 scheduler 값이 바뀌어도 undeploy 는
이 metadata 의 backend 를 사용한다. Airflow 에만 필요한 등록 정보는 scheduler snapshot 에 둔다.

Snapshot 에 포함할 최소 정보:

- `project_id`
- `artifact_id`
- `registered_at`
- artifact cache 위치
- job 목록
- 각 job 의 `job_id`
- 각 job 의 `dag_id`
- 각 job 의 config path
- 각 job 의 task projection 목록

## 제약

- Metastore 는 stage table row data 를 저장하지 않는다.
- Runtime data backend 는 zeta4s deploy registration 을 소유하지 않는다.
- Runtime data backend 는 zeta4s metadata table 을 소유하지 않는다.
- Project database 에 `__zeta4s_*` metadata table 을 만들지 않는다.
- Metastore adapter 는 profile connection 에 의존하지 않는다.
- Airflow scheduler parse 경로에서 API 호출을 필수로 하지 않는다.
- `schema.table` 단일 문자열 표기는 quoted identifier 를 표현하지 않는다.
- metastore 구현체 선택과 ClickHouse stage/transform backend 사용 여부는 독립적이다.

## 검증

- `uv run python -m compileall -q src/zeta4s`
- PostgreSQL/ClickHouse adapter 공통 repository conformance test
- PostgreSQL 18.4 fresh database bootstrap/inspection integration test
- PostgreSQL 기본 stack 의 `z4s api bootstrap` 과 Airflow deploy/run
- Docker `asset` profile 의 ClickHouse 선택 metastore adapter focused regression
