# SQL Step Contract

## 대상 type

- `clickhouse.sql`
- `oracle.sql`
- `sql.check`
- `sql.scalar`
- `oracle.call`

## 역할

SQL 계열 step 은 이미 DB 에 존재하는 object 를 대상으로 SQL statement, PL/SQL block, procedure call,
check query, scalar query 를 실행하는 step 이다.

SQL 계열 step 은 rowset artifact 를 만들거나 stage table 을 자동으로 binding 하지 않는다. 선행 step 이
만든 table 을 읽어야 하면 실행 순서는 `depends_on` 으로 연결하고, 실제 table 이름은 SQL file 또는
inline `sql` 안에 명시한다.

## 현재 계약

### ClickHouse SQL

```yaml
steps:
  - step_id: build_order_metrics
    type: clickhouse.sql
    conn: analytics_clickhouse
    depends_on:
      - stage_orders
    query: sql/clickhouse/build_order_metrics.sql
```

`clickhouse.sql` 은 `conn` 이 가리키는 ClickHouse backend 에 SQL 을 실행한다.

### Oracle SQL

```yaml
steps:
  - step_id: build_order_summary
    type: oracle.sql
    conn: oracle_mart_store
    depends_on:
      - stage_orders
    query: sql/oracle/build_order_summary.sql
```

`oracle.sql` 은 `conn` 이 가리키는 Oracle connection 에 SQL 또는 PL/SQL block 을 실행한다.

### SQL check

```yaml
steps:
  - step_id: validate_order_summary
    type: sql.check
    conn: oracle_mart_store
    depends_on:
      - build_order_summary
    query: sql/oracle/validate_order_summary.sql
```

`sql.check` 은 query 결과 첫 row 의 첫 column 을 boolean predicate 로 검사한다. 값이 `true` 또는
`1` 이면 성공으로 보고, `false`, `0`, `null`, row 없음은 실패로 본다.

### SQL scalar

```yaml
steps:
  - step_id: count_order_summary
    type: sql.scalar
    conn: oracle_mart_store
    depends_on:
      - build_order_summary
    query: sql/oracle/count_order_summary.sql
    outputs:
      row_count:
        kind: scalar
        type: int
        column: 1
```

`sql.scalar` 은 query 결과 첫 row 에서 `outputs.<name>.column` 이 지정한 column 값을 읽어 scalar
output 으로 공개한다. 이 값은 후속 step 의 `when.expr` 에서 참조할 수 있다.

```yaml
when:
  expr: "$steps.count_order_summary.outputs.row_count > 0"
```

### Oracle procedure

```yaml
steps:
  - step_id: refresh_order_audit
    type: oracle.call
    conn: oracle_mart_store
    depends_on:
      - build_order_summary
    call: sales_mart.refresh_order_audit(:z4_run_id)
    params:
      z4_run_id: "$context.z4_run_id"
```

`oracle.call` 는 `call` 을 `BEGIN <call>; END;` 형태로 실행한다. Procedure return value 나 OUT
parameter 를 graph output 으로 공개하는 계약은 현재 계약에 포함하지 않는다.

## Input

### `clickhouse.sql`, `oracle.sql`, `sql.check`, `oracle.call`

- 종류: 없음
- 개수: 0

위 타입들은 graph data input 을 받지 않는다. 선행 step 이 만든 table 이 필요하면 `depends_on` 으로
실행 순서만 보장하고, SQL 안에서 table 이름을 직접 사용한다.

### `sql.scalar`

- 종류: 없음
- 개수: 0

`sql.scalar` 도 graph data input 을 받지 않는다. Scalar query 의 대상 object 는 SQL 안에 직접
명시한다.

## Output

### `clickhouse.sql`, `oracle.sql`, `sql.check`, `oracle.call`

- 종류: 없음
- 개수: 0

SQL 실행 결과로 DB object 가 생성되거나 변경될 수는 있지만, 이것은 graph output contract 가 아니다.
후속 step 이 해당 object 를 읽어야 하면 SQL 에서 사용하는 object 이름을 명시하고 `depends_on` 으로
순서를 연결한다.

### `sql.scalar`

- 종류: scalar
- 개수: 1개 이상
- 표현: `outputs.<name>`

```yaml
outputs:
  row_count:
    kind: scalar
    type: int
    column: 1
```

`column` 은 1부터 시작하는 result column index 다. 생략하면 `1` 로 본다.

지원 scalar `type`:

- `int`
- `float`
- `bool`
- `str`

`bool` 은 `true`, `false`, `1`, `0` 만 허용한다. 그 외 값은 `sql.scalar` step 실패로 처리한다.

## 필수 field

### `clickhouse.sql`

- `step_id`
- `type`
- `conn`
- `query` 또는 `sql`

### `oracle.sql`

- `step_id`
- `type`
- `conn`
- `query` 또는 `sql`

### `sql.check`

- `step_id`
- `type`
- `conn`
- `query` 또는 `sql`

### `sql.scalar`

- `step_id`
- `type`
- `conn`
- `query` 또는 `sql`
- `outputs`

### `oracle.call`

- `step_id`
- `type`
- `conn`
- `call`

## 선택 field

공통 flow-control field:

- `depends_on`
- `pool`
- `when`
- `join`
- `retry`
- `timeout`

공통 실행 field:

- `params`

`params` 는 SQL bind parameter 와 runtime context 값을 전달한다.

```yaml
params:
  start_date: "2026-01-01"
  z4_run_id: "$context.z4_run_id"
```

`$context.<name>` 은 scheduler adapter가 canonical runtime context로 정규화한 값을 bind
parameter로 전달하는 표현이다.

참조할 수 있는 runtime context key 는 `src/zeta4s/runtime/native.py` 의 `_runtime_context_params` 가
정본이다. 없는 key 를 참조하면 step 이 실패한다.

## 쓰지 않는 field

현재 계약 기준에서 SQL 계열 step 은 아래 field 를 쓰지 않는다.

- `input`
- `output`
- `target`

예외적으로 `sql.scalar` 만 `outputs` 를 사용한다.

## Query file

`query` 는 project root 기준 상대 경로다.

```yaml
query: sql/clickhouse/build_order_metrics.sql
```

허용하지 않는 경로:

- absolute path
- `..` 을 포함하는 project 외부 경로

SQL file 은 하나의 statement 또는 하나의 PL/SQL block 을 담는다. 여러 DML 을 한 파일에 넣어 순차
실행하는 계약은 현재 계약에 포함하지 않는다.

여러 statement 를 순서대로 실행해야 하면 step 을 여러 개로 나누고 `depends_on` 으로 연결한다.

```yaml
steps:
  - step_id: reset_order_summary
    type: clickhouse.sql
    conn: analytics_clickhouse
    query: sql/clickhouse/reset_order_summary.sql

  - step_id: build_order_summary
    type: clickhouse.sql
    conn: analytics_clickhouse
    depends_on:
      - reset_order_summary
    query: sql/clickhouse/build_order_summary.sql
```

## Inline SQL

짧은 SQL 은 `sql` 에 inline 으로 쓸 수 있다.

```yaml
steps:
  - step_id: optimize_order_summary
    type: clickhouse.sql
    conn: analytics_clickhouse
    sql: "OPTIMIZE TABLE order_summary FINAL"
```

긴 SQL 은 `query` file 로 분리한다.

## SQL template

목표 SQL step 계약에서는 SQL template 을 공식 계약 문법으로 두지 않는다.

SQL step 은 step 하나가 `query` file 하나 또는 inline `sql` 하나를 실행한다. 이 step 이 사용하는
DB object 이름과 조건은 SQL text 와 `params` bind parameter 로 명시한다.

하나의 project 안에는 여러 connection 과 여러 backend database/schema 가 공존할 수 있다.

목표 SQL step 계약에서 DB/schema/table 해석 기준은 `conn` 이다. SQL text 안의 object 이름은 작성자가
명시하고, backend 별 기본 database/schema 는 connection payload 또는 backend-specific resolution
규칙으로 결정한다.

특히 `source`, `target`, `input`, `output` 은 SQL step 의 data binding 계약이 아니므로 SQL template
계약으로 문서화하지 않는다.

값 조건은 template 이 아니라 bind parameter 를 사용한다.

## Bind parameter

`params` key 는 SQL 의 `:<name>` placeholder 에 전달된다.

```yaml
steps:
  - step_id: build_order_summary
    type: oracle.sql
    conn: oracle_mart_store
    query: sql/oracle/build_order_summary.sql
    params:
      start_date: "2026-01-01"
      end_date: "2026-01-02"
```

```sql
insert into order_summary
select *
from orders
where order_date >= :start_date
  and order_date < :end_date
```

SQL 에 존재하는 placeholder 에 해당하는 parameter 만 runtime execute 에 전달한다.

## SQL normalization

현재 runtime 은 SQL 을 실행하기 전에 header comment 와 trailing semicolon 을 정리한다.

### `clickhouse.sql`

허용 시작 keyword:

- `SELECT`
- `INSERT`
- `CREATE`
- `ALTER`
- `DROP`
- `TRUNCATE`
- `OPTIMIZE`

### `oracle.sql`

허용 시작 keyword:

- `SELECT`
- `INSERT`
- `UPDATE`
- `DELETE`
- `MERGE`
- `CALL`
- `BEGIN`
- `CREATE`
- `DROP`
- `ALTER`
- `TRUNCATE`

### `sql.check`, `sql.scalar`

ClickHouse connection 에서는 `clickhouse.sql` 과 같은 허용 시작 keyword 를 쓴다.

Oracle connection 에서의 허용 시작 keyword:

- `SELECT`
- `INSERT`
- `UPDATE`
- `DELETE`
- `MERGE`
- `CALL`
- `BEGIN`

Oracle connection 에서는 statement 안에 `CREATE`, `DROP`, `ALTER`, `TRUNCATE`, `GRANT`, `REVOKE` token 이
있으면 거부한다.

Check/scalar 용도에서는 `SELECT` 를 사용한다. DML/PLSQL 은 런타임 parser 가 허용하더라도 계약상
check/scalar step 의 의도와 맞지 않는다.

### `oracle.call`

`call` 에는 procedure call expression 만 쓴다.

```yaml
call: sales_mart.refresh_order_audit(:z4_run_id)
```

Runtime 은 이를 다음 형태로 실행한다.

```sql
BEGIN sales_mart.refresh_order_audit(:z4_run_id); END;
```

## Connection

- `clickhouse.sql`: ClickHouse transform/runtime data backend connection
- `oracle.sql`: Oracle connection
- `sql.check`: ClickHouse 또는 Oracle connection
- `sql.scalar`: ClickHouse 또는 Oracle connection
- `oracle.call`: Oracle connection

`sql.check` 는 `conn` 의 connection type 에 따라 ClickHouse 또는 Oracle backend 에서 실행한다.
`sql.scalar` 는 `conn` 의 connection type 에 따라 ClickHouse 또는 Oracle backend 에서 실행한다.

## 실행 종속

SQL 계열 step 은 `input` 으로 실행 순서를 만들지 않는다.

선행 step 이 만든 object 를 SQL 이 읽어야 하면 `depends_on` 을 반드시 명시한다.

```yaml
steps:
  - step_id: stage_orders
    type: clickhouse.stage
    conn: analytics_clickhouse
    depends_on:
      - extract_orders
    map:
      extract_orders.orders_rows: stg_orders

  - step_id: build_order_summary
    type: clickhouse.sql
    conn: analytics_clickhouse
    depends_on:
      - stage_orders
    query: sql/clickhouse/build_order_summary.sql
```

`sql/clickhouse/build_order_summary.sql` 은 stage output table 이름을 직접 사용한다.

```sql
create table order_summary
engine = MergeTree
order by order_id
as
select
  order_id,
  count() as order_count
from stg_orders
group by order_id
```

## Result/metrics

### `clickhouse.sql`, `oracle.sql`, `oracle.call`

Runtime result stage 는 `sql_transform` 이다.

최소 metrics:

- executed step count
- affected row count 또는 backend 가 제공하는 실행 결과

### `sql.check`

Runtime result stage 는 `sql_check` 이다.

Check query 의 첫 값이 `true` 또는 `1` 이 아니면 task 를 실패시킨다.

### `sql.scalar`

Runtime result stage 는 `sql_scalar` 이다.

`details.outputs` 에 `outputs.<name>` 값이 기록되고, 후속 `when.expr` 에서 참조할 수 있어야 한다.

## 제약

- SQL 계열 step 은 rowset/table `input`/`output` contract 를 사용하지 않는다.
- SQL file 하나는 하나의 statement 또는 하나의 PL/SQL block 을 표현한다.
- 여러 statement 실행 순서는 여러 step 과 `depends_on` 으로 표현한다.
- `oracle.call` 는 graph output 을 만들지 않는다.
- SQL 안의 object 이름은 작성자가 명시한다. zeta4s 는 SQL text 를 분석해서 table dependency 를
  자동 생성하지 않는다.

## 예시

### ClickHouse transform

```yaml
steps:
  - step_id: build_customer_summary
    type: clickhouse.sql
    conn: analytics_clickhouse
    depends_on:
      - stage_customer_inputs
    query: sql/clickhouse/build_customer_summary.sql
```

### Oracle transform

```yaml
steps:
  - step_id: build_customer_summary
    type: oracle.sql
    conn: oracle_mart_store
    depends_on:
      - stage_customer_inputs
    query: sql/oracle/build_customer_summary.sql
```

### Oracle validation branch

```yaml
steps:
  - step_id: count_customers
    type: sql.scalar
    conn: oracle_mart_store
    query: sql/oracle/count_customers.sql
    outputs:
      row_count:
        kind: scalar
        type: int

  - step_id: validate_customers
    type: sql.check
    conn: oracle_mart_store
    when:
      expr: "$steps.count_customers.outputs.row_count > 0"
    query: sql/oracle/validate_customers.sql
```

### Oracle procedure call

```yaml
steps:
  - step_id: refresh_customer_audit
    type: oracle.call
    conn: oracle_mart_store
    call: sales_mart.refresh_customer_audit(:z4_run_id)
    params:
      z4_run_id: "$context.z4_run_id"
```
