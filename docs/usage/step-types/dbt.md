# dbt Step Contract

## 대상 type

- `dbt.run`
- `dbt.test`

## 용어 정리

- dbt project: `dbt_project.yml`, `models/`, `tests/` 를 포함하는 dbt 실행 단위다. `profiles.yml` 은
  dbt 실행 profile 파일이며, 현재 계약에서는 zeta4s runtime 이 `conn` 기준으로 생성하거나 선택한다.
- dbt model: dbt 가 실행해서 DB 에 table/view 등을 만드는 SQL model 이다.
- dbt test: dbt model 결과를 검증하는 test node 다.
- dbt materialization: dbt model 이 DB 에 어떤 형태로 만들어지는지 정하는 dbt 용어다. 예: `table`,
  `view`, `incremental`.

## 역할

`dbt.run` 은 dbt model 을 실행해서 DB table/view 를 만드는 transform step 이다.

`dbt.test` 는 dbt test 를 실행해서 dbt model 결과를 검증하는 step 이다.

zeta4s 에서 dbt 는 DB 안의 데이터 변환을 쉽게 실행하기 위한 도구다. dbt 의 모든 selector 문법과 복잡한
실행 옵션을 step graph 계약으로 열어두지 않는다.

## 현재 계약

현재 계약에서 dbt step 은 `models[]` 만 사용한다.

```yaml
steps:
  - id: build_customer_mart
    type: dbt.run
    conn: analytics_clickhouse
    depends_on:
      - stage_customer_inputs
    models:
      - dim_customer
      - fct_order
```

`models[]` 의 각 model 이름이 `dbt.run` 의 graph output 이름이다.

위 step 은 다음 output 을 공개한다.

```text
build_customer_mart.dim_customer
build_customer_mart.fct_order
```

별도 `output` 블록은 쓰지 않는다.

## 후속 참조

후속 step 은 step type 별 data reference field 에서 `<step_id>.<output_name>` 문자열로 dbt output 을
참조한다. SQL step 처럼 data input field 를 갖지 않는 step 은 실행 순서만 `depends_on` 으로 연결하고,
실제 DB object 이름은 SQL file 안에 명시한다.

## dbt.run

필수 field:

- `id`
- `type`
- `conn`
- `models[]`

선택 field:

- `depends_on`
- `pool`
- `when`
- `join`
- `retry`
- `timeout`

## dbt.test

`dbt.test` 는 검증할 model 이름을 `models[]` 로 받는다.

```yaml
steps:
  - id: test_customer_mart
    type: dbt.test
    conn: analytics_clickhouse
    depends_on:
      - build_customer_mart
    models:
      - dim_customer
      - fct_order
```

`dbt.test` 는 graph output 을 만들지 않는다.

필수 field:

- `id`
- `type`
- `conn`
- `models[]`

선택 field:

- `depends_on`
- `pool`
- `when`
- `join`
- `retry`
- `timeout`

## Connection

`conn` 은 dbt 가 실행될 DB connection 이다.

하나의 `dbt.run` 은 하나의 DB connection 기준으로 실행한다. 하나의 `dbt.run` 안에서 서로 다른 DB 의
table 을 섞어 읽지 않는다.

Project 는 여러 DB connection 을 가질 수 있다. 따라서 dbt 실행도 `conn` 별로 분리한다.

- `dbt.run.conn` 은 실행할 DB connection 을 하나 선택한다.
- 같은 `conn` 을 쓰는 model 들은 같은 `dbt.run` step 에 둘 수 있다.
- 다른 `conn` 을 쓰는 model 들은 다른 `dbt.run` step 으로 나눈다.
- dbt profile target 은 `conn` 별로 생성하거나 선택해야 한다.

`dbt.run` 이 stage output 을 읽는다면 해당 stage step 의 `conn` 과 `dbt.run.conn` 은 같아야 한다.

```yaml
steps:
  - id: stage_customer_inputs
    type: clickhouse.stage
    conn: analytics_clickhouse
    depends_on:
      - extract_customers
    map:
      extract_customers.customer_rows: customers_raw

  - id: build_customer_mart
    type: dbt.run
    conn: analytics_clickhouse
    depends_on:
      - stage_customer_inputs
    models:
      - dim_customer
```

서로 다른 DB connection 의 stage output 을 하나의 `dbt.run` 에 섞지 않는다.

```yaml
steps:
  - id: build_mixed_mart
    type: dbt.run
    conn: analytics_clickhouse
    depends_on:
      - stage_orders_clickhouse
      - stage_customers_oracle
    models:
      - customer_order_mart
```

위 구조는 `stage_orders_clickhouse` 와 `stage_customers_oracle` 이 서로 다른 `conn` 으로 table 을 만들기
때문에 허용하지 않는다.

Multi-conn project 에서는 다음처럼 `conn` 별로 dbt step 을 나눈다.

```yaml
steps:
  - id: build_clickhouse_mart
    type: dbt.run
    conn: clickhouse_mart
    depends_on:
      - stage_clickhouse_inputs
    models:
      - dim_customer
      - fct_order

  - id: build_oracle_mart
    type: dbt.run
    conn: oracle_mart
    depends_on:
      - stage_oracle_inputs
    models:
      - dim_account
```

## dbt project

현재 계약에서는 DB connection 별 dbt project directory 를 둔다.

```text
project/
  dbt/
    clickhouse_mart/
      dbt_project.yml
      models/
      tests/
    oracle_mart/
      dbt_project.yml
      models/
      tests/
```

`dbt.run.conn` 은 `dbt/<conn>/` directory 를 선택한다.

`conn` 값은 dbt directory 이름으로도 쓰이므로 `/`, `..`, 공백을 포함할 수 없다.

`dbt_project.yml` 은 dbt project root 에 필요한 고정 파일명이다. zeta4s 가 dbt CLI 를 실행하더라도
이 파일명은 바꾸지 않는다.

`dbt_project.yml` 의 `name` 과 `profile` 은 `conn` 과 같아야 한다. `models:` top-level config 는
`models.<conn>` 아래에만 둔다.

`profiles.yml` 은 현재 계약에서 사용자가 project 안에 직접 두는 파일로 요구하지 않는다. zeta4s runtime
이 `dbt.run.conn` 을 기준으로 dbt profile 을 생성하거나 선택하고, dbt 실행 시 profiles directory 로
전달한다.

```yaml
steps:
  - id: build_clickhouse_mart
    type: dbt.run
    conn: clickhouse_mart
    models:
      - dim_customer
```

위 step 은 `dbt/clickhouse_mart/` 의 dbt project 를 실행한다.

현재 계약에서 step YAML 에 dbt SQL 을 쓰지 않는다. dbt SQL 은 해당 `dbt/<conn>/models/` 아래에 둔다.

## Result/metrics

`dbt.run` runtime result stage 는 `dbt_run` 이다.

`dbt.test` runtime result stage 는 `dbt_test` 이다.

최소 metrics:

- selected node count
- success count
- failure count
- skipped count

Graph output 은 runtime result 가 아니라 `dbt.run.models[]` 에서 파생된다.

## 제약

- 현재 계약에서 `selector` 는 쓰지 않는다.
- `dbt.run.models[]` 가 실행 대상이자 graph output 목록이다.
- `dbt.test` 는 graph output 을 만들지 않는다.
- `dbt.run.conn` 은 input stage step 의 `conn` 과 같아야 한다.
- dbt `sources:` YAML 생성을 필수 계약으로 두지 않는다.
- `source`, `target`, `output`, `sql`, `query`, `call` 은 dbt step 현재 계약에서 쓰지 않는다.
