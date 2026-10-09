# Stage Step Contract

## 대상 type

- `clickhouse.stage`
- `oracle.stage`

## 역할

`stage` 는 upstream step 이 만든 rowset output 을 transform 이 읽을 수 있는 table snapshot 으로
materialize 하는 step 이다.

`stage` 는 source 에서 데이터를 추출하지 않는다. `extract` 가 만든 rowset 을 받아서 `dbt.run` 또는
SQL transform 직전에 DB table 로 고정한다.

## 현재 계약

현재 계약에서 `stage` 는 `map` 을 사용한다.

```yaml
steps:
  - id: stage_sales_inputs
    type: clickhouse.stage
    conn: analytics_clickhouse
    depends_on:
      - extract_orders
      - extract_customers
    map:
      extract_orders.orders_rows: mart.stg_orders
      extract_customers.customers_rows: mart.stg_customers
```

`map` 은 다음 형식이다.

```yaml
map:
  <upstream_step>.<output_name>: <target_table>
```

`map` key 는 stage 가 읽을 rowset output reference 다. `map` value 는 stage 가 만들 target DB table
이름이다. DB table 이름은 항상 `schema.table` 형식이다.
현재 계약의 `schema.table` 단일 문자열 표기에서는 quoted identifier 를 표현하지 않는다. 예시는
lowercase 로 쓴다.

`schema.table` table 이름에는 `.` 이 들어가므로, 후속 step 이 `stage_sales_inputs.mart.stg_orders`
처럼 축약 참조를 쓸 때는 첫 번째 `.` 만 step id 와 output 이름의 구분자로 해석한다. 즉 step id 는
`stage_sales_inputs`, output 이름은 `mart.stg_orders` 다.

## Input

- 종류: rowset
- 개수: 1개 이상
- 표현: `map` key
- 형식: `<upstream_step>.<output_name>`

`map` key 의 `upstream_step` 은 `depends_on` 에 포함되어야 한다.
입력 rowset artifact 와 type metadata 는 [Rowset Contract](../../design/rowset.md)를 따른다.

## Output

- 종류: table
- 개수: `map` entry 개수와 동일
- 이름: `map` value

별도 `output` 블록은 쓰지 않는다. 후속 step 은 stage step 의 output 이름으로 `map` value 를
참조한다.

후속 step 은 step type 별 data reference field 에서 `stage_sales_inputs.mart.stg_orders` 처럼 stage
output 을 참조한다. SQL step 처럼 data input field 를 갖지 않는 step 은 실행 순서만 `depends_on` 으로
연결하고, 실제 DB object 이름은 SQL file 안에 명시한다.

## 필수 field

- `id`
- `type`
- `conn`
- `depends_on`
- `map`

## 선택 field

공통 flow-control field 만 허용한다.

- `pool`
- `when`
- `join`
- `retry`
- `timeout`

## 쓰지 않는 field

현재 계약 기준에서 `stage` 는 아래 field 를 쓰지 않는다.

- `input`
- `target`
- `output`
- `outputs`

## Connection

`conn` 은 stage 가 table 을 만들 runtime data backend connection 이다.

DB table 이름은 `schema.table` 형식으로 쓴다. ClickHouse 에서는 database 이름을 schema 위치에 쓴다.

## 실행 종속

`map` 은 data binding 이고, 실행 순서를 만들지 않는다.

`map` key 에 들어간 upstream step 은 `depends_on` 에도 명시한다.

## Result/metrics

`stage` runtime result 는 최소한 다음 정보를 남겨야 한다.

- materialized table 이름
- row count
- stage backend type

Graph output 은 runtime result 가 아니라 YAML `map` 에서 파생된다.

## 제약

`stage` 의 기본 동작은 replace snapshot 이다.

같은 input rowset 과 같은 `map` 이 주어지면 후속 transform 이 읽는 table snapshot 은 동일해야 한다.
append, upsert, merge 같은 누적/상태 병합 정책은 기본 `stage` 계약에 포함하지 않는다.

Target table type 은 rowset 의 canonical logical type 을 기준으로 만든다. 같은 backend 에서 추출한
rowset 을 같은 backend 로 stage 할 때만 source-native type hint 를 보존한다.

## 예시

### ClickHouse stage

```yaml
steps:
  - id: stage_sales_inputs
    type: clickhouse.stage
    conn: analytics_clickhouse
    depends_on:
      - extract_orders
      - extract_customers
    map:
      extract_orders.orders_rows: mart.stg_orders
      extract_customers.customers_rows: mart.stg_customers
```

### Oracle stage

```yaml
steps:
  - id: stage_sales_inputs
    type: oracle.stage
    conn: oracle_mart_store
    depends_on:
      - extract_orders
      - extract_customers
    map:
      extract_orders.orders_rows: mart.stg_orders
      extract_customers.customers_rows: mart.stg_customers
```
