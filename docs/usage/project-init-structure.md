# Project Init Structure

이 문서는 `project init` 이 workspace 안에 생성해야 하는 project artifact 의 canonical 구조와 파일
내용을 정의한다. Project 는 배포 단위이고, `jobs/*.yml` 하나는 DAG/job 단위다. Step graph 는 job 파일
하나의 `steps[]` 안에서 완결한다.

## 원칙

- `jobs/` 는 job graph 디렉토리다. `config/` 라는 포괄 이름을 쓰지 않는다.
- Step id 는 같은 `(project_id, job_id)` 안에서 유일하다.
- Step id 는 실행 동작을 표현한다: `fetch_order_rows`, `persist_order_snapshot`, `build_order_mart`.
- Step 간 실행 순서는 `depends_on` 으로 명시한다.
- Step 간 data reference 는 step type 별 계약 field 로 표현한다.
- `stage.map` 은 `upstream_step.output_name` rowset reference 를 stage 가 만들 lowercase `schema.table` DB table 이름으로 매핑한다.
- `extract` 는 staging DB work table 을 만들지 않는다. `extract` output 은 storage-neutral rowset interface 다.
- `stage` 는 named rowset map 을 transform용 table snapshot map 으로 고정한다.
- `dbt.run`/`dbt.test` 의 project root 는 `dbt/<conn>/` 다.
- DB adapter 종류는 project field 가 아니라 profile connection 의 `type` 으로 해석한다.
- 예시 project 는 ClickHouse runtime data backend 로 `conn: analytics_clickhouse` 를 명시한다.
- 현재 로컬/검증 스택의 기본 metastore 구현체는 PostgreSQL이며, ClickHouse adapter는 선택 backend다. ClickHouse runtime data backend 역할과 구분한다.
- 다중 output 은 `stage` map 과 `dbt.run` step 에서 허용한다.
- `project init` 은 project-local `assets/` directory 를 생성하지 않는다.
- `project init <project_id>` 는 z4s home config 에 등록된 workspace 의 `projects/<project_id>` 를
  생성한다.
- Project 안에 `profiles/` directory 를 만들지 않는다.
- `project init` 은 explicit pool 설정을 생성하지 않는다. 자동 pool 이 기본이며 사용자 지정 pool 은
  예외적 override 이므로 권장하지 않는다.

## 기본 구조

```text
<workspace>/
  projects/
    <project_id>/
      project.yml
      jobs/
      docs/
        README.md
```

`project init` 은 실행 job, SQL, dbt project 를 생성하지 않는다.

`project init --profile <profile_id> --with-dbt <project_id>` 는 선택한 profile 의 dbt 가능 connection 을
기준으로 `dbt/<conn>/` skeleton 을 함께 생성한다. 1.0.0 대상 dbt 가능 connection type 은
`clickhouse`, `oracle` 이다. `elasticsearch` connection 은 dbt project skeleton 생성 대상이 아니다.

```text
<workspace>/
  projects/
    <project_id>/
      project.yml
      jobs/
      docs/
        README.md
      dbt/
        analytics_clickhouse/
          dbt_project.yml
          models/
          tests/
        erp_oracle/
          dbt_project.yml
          models/
          tests/
```

Native SQL, dbt model, job 예제를 포함하려면 사용자가 다음 파일을 추가한다.

```text
<workspace>/
  projects/
    <project_id>/
      jobs/
        order_pipeline.yml
        dbt_quickstart.yml
      sql/
        oracle/
          fetch_order_rows.sql
        clickhouse/
          build_order_metrics.sql
      dbt/
        analytics_clickhouse/
          dbt_project.yml
          models/
            order_summary.sql
          tests/
            .gitkeep
```

## 파일 Contract

### `project.yml`

Project identity 와 project-local path 기본값이다. `project_id` 는 deploy 단위와 artifact identity 에
사용된다.

```yaml
project_id: sales_pipeline
display_name: Sales Pipeline
timezone: Asia/Seoul

paths:
  jobs: jobs
  dbt: dbt
```

`timezone` 은 project 의 business timezone 이며 기본값은 `Asia/Seoul` 이다. 이 파일에는 DB 종류나
dbt project 목록을 넣지 않는다.

### Profile

Project 가 `analytics_clickhouse` 같은 runtime data backend 를 쓰면 workspace profile 에 connection 을
선언한다. Metastore 는 profile connection 이 아니며 `zeta4s-api` service config 로
관리한다.

```yaml
connections:
  oracle_source:
    type: oracle
    host: oracle
    port: 1521
    username: showcase_src
    password_ref: dev.oracle_source.password
    database: freepdb1
    schema: showcase_src
    options:
      service_name: freepdb1
```

### `jobs/order_pipeline.yml`

Native extract, stage, SQL transform 흐름의 예시다.

```yaml
job_id: order_pipeline
schedule: null
steps:
  - step_id: fetch_order_rows
    type: oracle.extract
    conn: oracle_source
    source:
      kind: query
      query: sql/oracle/fetch_order_rows.sql
    output:
      order_rows:
        kind: rowset

  - step_id: persist_order_snapshot
    type: clickhouse.stage
    conn: analytics_clickhouse
    depends_on:
      - fetch_order_rows
    map:
      fetch_order_rows.order_rows: mart.stg_orders

  - step_id: build_order_metrics
    type: clickhouse.sql
    conn: analytics_clickhouse
    depends_on:
      - persist_order_snapshot
    query: sql/clickhouse/build_order_metrics.sql
```

`stage.map` 은 upstream step 의 rowset output 을 lowercase `schema.table` 이름으로 고정한다. 위 예시는
`fetch_order_rows.output.order_rows` 를 `mart.stg_orders` table 로 만든다.
실행 순서는 `depends_on` 이 만든다. 현재 구현 기준에서
`clickhouse.sql`/`oracle.sql` step 은 `input`/`output` 을 runtime binding 으로 사용하지 않으므로
SQL step 예시에는 쓰지 않는다.

### `sql/oracle/fetch_order_rows.sql`

`fetch_order_rows` step 이 source Oracle connection 으로 실행할 query file 이다.

```sql
select
  order_id,
  customer_id,
  order_date,
  amount
from sales.orders
where order_date >= :start_date
  and order_date < :end_date
```

### `sql/clickhouse/build_order_metrics.sql`

`build_order_metrics` step 이 ClickHouse connection 으로 실행할 SQL file 이다.

```sql
create table if not exists mart.order_metrics
engine = MergeTree()
order by order_id
as
select
  order_id,
  count() as order_count
from mart.stg_orders
group by order_id
```

### `jobs/dbt_quickstart.yml`

`build_order_mart` dbt step 의 job 예시다. `conn` 과 dbt project root 는 1:1 로 연결된다.

```yaml
job_id: dbt_quickstart
schedule: null
steps:
  - step_id: build_order_mart
    type: dbt.run
    conn: analytics_clickhouse
    models:
      - order_summary
```

### `dbt/analytics_clickhouse/dbt_project.yml`

`analytics_clickhouse` connection 으로 실행하는 dbt project manifest 다.

```yaml
name: analytics_clickhouse
version: "0.1.0"
config-version: 2

profile: analytics_clickhouse

model-paths: ["models"]
test-paths: ["tests"]
target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  analytics_clickhouse:
    +materialized: table
```

`profiles.yml` 은 skeleton 에 생성하지 않는다. zeta4s runtime 이 `conn` 기준으로 생성하거나 선택한다.

### `dbt/analytics_clickhouse/models/order_summary.sql`

`jobs/dbt_quickstart.yml` 의 `models[]` 가 선택하는 dbt model 이다.

```sql
select
  1 as order_id,
  'example' as order_status
```

### `docs/README.md`

Project-local 운영 문서다. Job 설명, source/target key, time window, failure policy, 검증
evidence 를 남긴다.

```markdown
# sales_pipeline

## Jobs

- `order_pipeline`: extract -> stage -> SQL transform 예시
- `dbt_quickstart`: `build_order_mart` dbt step 예시

## Runtime connections

- `analytics_clickhouse`: project 가 선언한 ClickHouse runtime data backend connection
- `oracle_source`: Oracle source connection
```

## Project Check

`z4s project check <project_id> --profile <profile_id>` 는 project artifact 를 read-only 로 검증한다.
이 명령은 파일을 생성하거나 수정하지 않고, dbt CLI 를 실행하지 않으며, profile connection 으로 DB 에
접속하지 않는다.

dbt contract 검증은 job 에 `dbt.run` 또는 `dbt.test` step 이 있을 때만 수행한다. `dbt.run.models[]`
는 `dbt/<conn>/models/<model>.sql` 존재 여부와 `table` materialization 을 정적으로 확인한다.
`dbt.test.models[]` 는 선택한 model SQL 존재 여부를 확인하지만 model materialization 을 요구하지 않는다.

`dbt.run` materialization 은 다음 순서로 해석한다.

- model SQL 의 `{{ config(materialized='table') }}`
- `models/**/*.yml` 또는 `models/**/*.yaml` 의 model `config.materialized`
- `dbt_project.yml` 의 `models.<dbt_project_name>` config chain

zeta4s 가 생성하는 dbt project 는 `dbt/<conn>/` 단위이며, `dbt_project.yml` 의 `name` 과 `profile` 은
`<conn>` 이다. `dbt_project.yml` 의 `models:` top-level config 는 이 project name 아래에만 둔다.

## 검증 기준

`project init` 결과물은 다음 정적 검증을 통과해야 한다.

- `project.yml` 이 parse 된다.
- `docs/README.md` 가 생성된다.
- 기본 skeleton 은 실행 job 을 만들지 않는다.
- 기본 skeleton 은 dbt project 를 만들지 않는다.
- `--with-dbt` skeleton 은 선택 profile 의 `clickhouse`, `oracle` connection 에 대해서만
  `dbt/<conn>/dbt_project.yml`, `models/`, `tests/` 를 만든다.
- 사용자가 추가한 `jobs/*.yml` 은 parse 되고 `ExecutionPlan` 으로 정규화된다.
- 사용자가 추가한 job 은 같은 job 안에서 step id 중복이 없다.
- 사용자가 추가한 job 의 step `conn` 은 선택한 profile 의 `connections` 에서 해석된다.
- 사용자가 추가한 job 에서 data-producing step 을 소비하는 step 은 `depends_on` 으로 실행 순서를 명시한다.
- 사용자가 추가한 job 의 모든 `query` SQL file 은 project 내부 상대 경로이고 실제 파일이 존재한다.
- `dbt.run` 이 참조하는 model 은 `table` materialization 으로 해석된다.
- skeleton 예시에 `backend` 또는 `dbt_project` field 가 없다.
