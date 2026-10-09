# Project Contract

Project artifact 는 zeta4s 의 canonical 사용자 입력이다. 현재 기준 입력은 다음 파일들이다.

- `project.yml`
- `jobs/*.yml`
- step 이 참조하는 SQL, dbt model, project-local docs

Skeleton 과 문서의 기본 확장자는 `.yml` 이다. 수동 작성한 job 파일과 profile 은 `.yaml` 확장자도
허용한다. Project manifest 는 `project.yml` 이름만 인식한다.

## `project.yml`

`project.yml` 은 project identity, timezone, project-local path 를 정의한다. `project_id` 는 deploy
단위이자 artifact 생성 단위다.

```yaml
project_id: my_project
display_name: My Project
timezone: Asia/Seoul

paths:
  jobs: jobs
  dbt: dbt
```

`timezone` 은 project 의 business timezone 이며 필수 IANA timezone 값이다. job `schedule` 이
`timezone` 을 선언하지 않으면 scheduler 는 이 값으로 cron/interval 을 해석한다. Airflow 와 Prefect 는
같은 규칙을 따른다. Step 의 시간 parameter 는 이 값으로 재해석하지 않고 작성한 값 그대로 bind 한다.
Metadata 저장과 DB 비교용 instant 는 UTC 로 정규화한다.

`paths.jobs`, `paths.dbt` 는 필수이며 project 내부 상대 경로여야 한다. 기본 skeleton 은 각각
`jobs`, `dbt` 를 사용한다.

Project artifact 는 connection, credential, scheduler pool resource, metastore endpoint 를 포함하지 않는다.
외부 connection 과 환경 값은 workspace profile 에서 해석한다.

## Project Commands

현재 `z4s project` public command 는 `init`, `check`, `graph` 세 가지다.

- `z4s project init <project_id>`: 등록된 workspace 아래에 기본 project skeleton 을 생성한다.
- `z4s project init <project_id> --profile <profile_id> --with-dbt`: 선택 profile 의 `clickhouse`,
  `oracle` connection 에 대해 `dbt/<conn>/` skeleton 을 함께 생성한다.
- `z4s project check <project_id> --profile <profile_id>`: project artifact 를 read-only 로 정적으로
  검증한다.
- `z4s project graph <project_id>`: workspace project graph 를 조회한다.

`project check` 는 dbt CLI 를 실행하지 않고, DB connection 에 접속하지 않으며, project 파일을 생성하거나
수정하지 않는다. dbt step 이 있는 project 에서는 `dbt/<conn>/dbt_project.yml`, model SQL 존재 여부,
`dbt.run` model 의 `table` materialization 을 정적으로 확인한다.

## Job Graph YAML

Job 파일은 `jobs/*.yml` 이름을 사용한다. top-level `edges:` 는 사용하지 않는다.
step-local field 로 control/data dependency 를 표현하고, 내부에서 `ExecutionPlan` 으로 정규화한다.

```yaml
job_id: quickstart
schedule:
  cron: "0 2 * * *"
  timezone: Asia/Seoul
  paused: false
steps:
  - step_id: start
    type: noop
```

지원하는 핵심 field:

- `job_id`: project 안에서 유일한 job identity
- `steps[].step_id`: job 안에서 유일한 logical step identity
- `schedule`: 예약 실행 정의 또는 `null`. `cron`/`interval_seconds` 중 정확히 하나를 선언하며
  `paused` 기본값은 `false` 다. `timezone` 은 선택 IANA timezone 이며 scheduler 가 cron/interval 을
  해석하는 기준이다. 생략하면 `project.yml` 의 `timezone` 을 쓴다.
- `steps[]`: 실행 가능한 step 목록
- `steps[].depends_on`: 명시 control dependency
- `steps[].when.success`, `when.failed`, `when.expr`: 조건부 실행
- `steps[].join.rule`: fan-in 조건
- `steps[].retry`, `steps[].timeout`: step 실행 option
- step type 별 계약 field: data reference 와 output contract

## Scheduler Pool Contract

부하 조절의 기본 계약은 자동 pool 이다. 사용자는 일반적인 project 에 explicit pool binding 을
작성하지 않는다. zeta4s 가 Step Graph 의 실행 stage 를 기준으로 pool 이름과 slot 을 산출해 선택한
scheduler backend 에 projection 하는 것이 목표 구조다.

현재 job schema 는 step/job 수준의 explicit pool override 도 허용하지만 예외적 override surface 이며
권장 authoring contract 가 아니다. 정확한 허용 field 와 산출 규칙은 `../README.md` 의 코드 위치
포인터가 정본이다. Airflow와 Prefect adapter는 같은 effective pool resolver를 사용한다. 자동 pool
resource는 deploy가 관리하지만 explicit override resource는 자동으로 만들지 않으므로, 사용자 지정
pool을 기본 또는 portable 운영 계약으로 권장하지 않는다.

## Step Type Contract

Step type 별 YAML 계약은 `docs/usage/step-types/` 아래 문서를 기준으로 한다.

- `docs/usage/step-types/extract.md`
- `docs/usage/step-types/stage.md`
- `docs/usage/step-types/sql.md`
- `docs/usage/step-types/http-lookup.md`
- `docs/usage/step-types/dbt.md`
- `docs/usage/step-types/write.md`
- `docs/usage/step-types/elasticsearch-command.md`

DB table 이름은 step type 계약에서 별도로 허용하지 않는 한 lowercase `schema.table` 형식으로 쓴다.
현재 계약의 `schema.table` 단일 문자열 표기에서는 quoted identifier 를 표현하지 않는다.

Step 간 data reference 는 공통 field 로 강제하지 않는다. `stage.map`, `http.lookup.lookup.table`,
`write.map` 처럼 각 step type 이 정의한 field 를 사용한다.

## ExecutionPlan

`ExecutionPlan` 은 Airflow 에 독립적인 내부 contract 다. YAML 의 `depends_on`, `when.*` 과
step type 별 data reference 는 `ExecutionEdge` 로 정규화되고, `join.rule` 은 fan-in 실행 조건으로
유지된다.

정규화 기준:

- `depends_on`, `when.*` 참조는 control edge 다.
- step type 별 data reference 는 data edge 다.
- 같은 upstream/downstream 사이에 control edge 와 data edge 가 동시에 존재할 수 있다.
- terminal step 은 같은 job graph 안에서 downstream 이 없는 step 이다.
- job run result 는 terminal step 상태를 기준으로 판정한다.

## Project Graph

`z4s project graph <project_id>` 는 workspace project artifact 를 읽어 job/step graph 를 출력한다.
이 명령은 read-only 조회이며 z4s home report 나 zeta4s-api storage 에 결과를 저장하지 않는다.

```bash
z4s project graph retail
z4s project graph retail --format json
z4s project graph retail --format mermaid
```

`--api <alias>` 를 지정하면 workspace source 가 아니라 zeta4s-api 에 배포된 project artifact 를 조회한다.

```bash
z4s project graph retail --api prod
```

지원 format:

- `text`: Linux console 에서 바로 읽는 tree-like graph
- `yaml`: 전체 graph payload
- `json`: 자동화용 전체 graph payload
- `mermaid`: Markdown 문서용 Mermaid graph
- `dot`: Graphviz DOT graph

## Supported Contract Summary

- Job config path 는 `jobs/*.yml` 이다.
- Job graph 는 `steps[]` 의 정적 목록으로 정의한다.
- Step id 는 같은 `(project_id, job_id)` 안에서 유일하다.
- Step 실행 순서는 `depends_on`, `when.*`, `join.rule` 로 정의한다.
- Project artifact 는 `assets/` directory 를 포함하지 않는다.
- Project step 의 `conn` 은 profile 의 `connections` 에서 해석한다.
- Step 간 data dependency 는 step type 별 계약 field 로 정의한다.
- Runtime 에 materialize 되는 DB table 이름은 lowercase `schema.table` 형식으로 쓴다.
