# Extract Step Contract

## 대상 type

- `oracle.extract`
- `clickhouse.extract`
- `elasticsearch.extract`

## 역할

`extract` 는 외부 source system 에서 데이터를 읽어 rowset output 을 만드는 step 이다.

`extract` 는 transform 이 읽을 DB table 을 만들지 않는다. DB table 로 고정하는 책임은 후속 `stage`
step 이 가진다.

지원 backend/source 범위는 Oracle, ClickHouse, Elasticsearch 다. 현재 로컬/검증 스택의
기본 metastore는 PostgreSQL이며 ClickHouse metastore adapter는 선택 backend다. 이는
`clickhouse.stage`, `clickhouse.sql`, dbt target으로 쓰는 runtime data backend 역할과 독립적이다.

## 현재 계약

현재 계약에서 `extract` 는 named rowset output 을 공개한다.

```yaml
steps:
  - step_id: extract_orders
    type: oracle.extract
    conn: oracle_source
    source:
      kind: query
      query: sql/oracle/extract_orders.sql
    output:
      orders_rows:
        kind: rowset
```

후속 `stage` 는 이 output 을 `map` 으로 참조한다.

```yaml
steps:
  - step_id: stage_orders
    type: clickhouse.stage
    conn: analytics_clickhouse
    depends_on:
      - extract_orders
    map:
      extract_orders.orders_rows: stg_orders
```

## Input

- 종류: 없음
- 개수: 0

`extract` 는 다른 step 의 data output 을 입력으로 받지 않는다. source 준비 step 이 필요하면
`depends_on` 으로 실행 순서만 연결한다.

## Output

- 종류: rowset
- 개수: 1개
- 표현: `output.<name>`
- physical storage: authoring field가 아니며 runtime mode가 선택

Rowset artifact 와 `column_specs` metadata 는 [Rowset Contract](../../design/rowset.md)를 따른다.

```yaml
output:
  orders_rows:
    kind: rowset
```

## 필수 field

공통 필수 field:

- `step_id`
- `type`
- `conn`
- `source`
- `output`

`oracle.extract` source 필수 field:

- `source.kind`: `table` 또는 `query`

`clickhouse.extract` source 필수 field:

- `source.kind`: `table` 또는 `query`

`elasticsearch.extract` source 필수 field:

- `source.kind`: `search`
- `source.index` 또는 `source.index_template`
- `source.fields`: 1개 이상

## 선택 field

공통 flow-control field:

- `pool`
- `when`
- `join`
- `retry`
- `timeout`

공통 실행 옵션:

- `batch_size`
- `params`
- `watermark`
- `time_window`

`oracle.extract` source 선택 field:

- `source.table`
- `source.query`
- `source.where_clause`
- `source.lob_policy`

`clickhouse.extract` source 선택 field:

- `source.table`
- `source.query`
- `source.where_clause`

`elasticsearch.extract` source 선택 field:

- `source.index_timezone`
- `source.query`
- `source.sort`
- `source.batch_size`
- `source.track_total_hits`

`elasticsearch.extract.source.fields[]` 의 mapping field:

- `column`: rowset column name
- `path`: Elasticsearch `_source` path
- `type`: rowset logical type. 허용 값은 `bool`, `int`, `float`, `decimal`, `str`, `date`, `timestamp`
- `nullable`
- `precision`
- `scale`
- `datetime_precision`
- `mode`: `scalar` 또는 `json_string`
- `required`

`type: timestamp` 는 `datetime_precision` 을 생략하면 `6` 을 사용한다.

## 쓰지 않는 field

현재 계약 기준에서 `extract` 는 아래 field 를 쓰지 않는다.

- `input`
- `target`
- `outputs`

`watermark` 는 top-level field 로 둔다. 현재 계약에서는 `target.watermark` 를 쓰지 않는다.

## Connection

`conn` 은 source system connection 이다.

- `oracle.extract`: Oracle source connection
- `clickhouse.extract`: ClickHouse source connection
- `elasticsearch.extract`: Elasticsearch source connection

`conn` 은 stage/runtime data backend 를 뜻하지 않는다.

## Source object resolution

`oracle.extract` 와 `clickhouse.extract` 의 DB table source 는 `source.table` 하나로 쓴다.

DB table 이름은 `table` 또는 `schema.table` 형식이다. ClickHouse 에서는 database 이름을 schema 위치에
쓴다. 현재 계약의 단일 문자열 표기에서는 quoted identifier 를 표현하지 않는다. 예시는 lowercase 로 쓴다.

```yaml
source:
  kind: table
  table: sales.orders
```

`source.table`, `source.query`, `source.where_clause` 는 Oracle/ClickHouse 공통 개념이다.

`source.lob_policy` 는 Oracle 전용이다. Oracle CLOB/BLOB 같은 LOB column 을 어떻게 읽을지 정하는
source reader 정책이며, ClickHouse extract 에는 대응 field 를 두지 않는다.

## Project timezone

`project.yml` 의 `timezone` 은 project 의 business timezone 이다([Project Contract](../project-contract.md)).
extract runtime 은 이 값으로 `params` 를 재해석하지 않는다.

`params` 값은 작성한 그대로 source 의 bind 값으로 전달된다. timezone 없는 datetime 문자열을 어떤
timezone 으로 해석할지는 source DB session 과 SQL 이 정한다.

```yaml
params:
  window_start: "2026-01-01 00:00"
  window_end: "2026-01-02 00:00"
```

## Watermark and time window

`watermark` 와 `time_window` 는 서로 다른 source selection 계약이다.

- `watermark`: 이전 성공 run 의 진행 위치를 저장하고 다음 incremental 범위를 계산하는 stateful 계약
- `time_window`: 이번 run 의 기준 시각에서 상대 시간 범위를 계산하는 stateless 계약

둘 다 없으면 full extract 다. 이 경우 runtime 은 이전 watermark 를 읽지 않고, watermark state 도
갱신하지 않는다.

`watermark` 와 `time_window` 는 같은 extract step 에서 동시에 쓰지 않는다.

### Watermark

`watermark` 는 incremental extract 에 사용한다.

```yaml
watermark:
  column: updated_at
  overlap_window: "5m"
```

`watermark.column` 은 source rowset 에 포함되는 column 이어야 한다. `overlap_window` 는 마지막
watermark 이후 데이터를 다시 일부 포함해 late-arriving row 를 보정하기 위한 범위다.

동작:

- runtime 이 metadata store 에서 마지막 watermark 를 읽는다.
- runtime 이 `overlap_window` 를 반영해 `:select_from` 을 계산한다.
- runtime 이 이번 run 의 upper bound 인 `:cur_wm` 을 계산한다.
- extract 성공 후 결과 rowset 의 `watermark.column` 최대값으로 watermark state 를 갱신한다.

### Time window

`time_window` 는 매 run 마다 기준 시각에서 최근 N초/분 범위를 읽는 bounded extract 에 사용한다.

```yaml
time_window:
  column: updated_at
  lookback: "30m"
```

동작:

- runtime 이 저장된 마지막 watermark 를 읽지 않는다.
- runtime 이 아래 규칙으로 `window_end` 를 결정한다.
- runtime 이 `window_end` 에서 `lookback` 을 빼 `window_start` 를 계산한다.
- `source.kind: table` 또는 `source.kind: search` 에서 runtime 이 source predicate 로 변환한다.
- extract history 는 남길 수 있지만, 마지막 watermark state 는 갱신하지 않는다.

계산 방식:

```text
window_end = 아래 규칙으로 정한 상한
window_start = window_end - lookback
```

`window_end` 결정 규칙:

1. 기본값은 extract 실행을 시작한 시각이다. runtime process 의 현재 시각을 timezone 없이 쓴다.
   Retry 는 새 attempt 의 시작 시각으로 다시 계산한다.
2. `time_window.upper_bound: data_interval_end` 를 쓰면 scheduler 가 넘긴 `data_interval_end` 에서
   timezone 정보를 떼어 쓴다. 값이 없으면 1 과 같다.

예를 들어 `lookback: "30m"` 이고 zeta4s `window_end` 가 `2026-01-01T10:00:00Z` 이면 runtime 은
최근 30분을 계산한다.

```text
window_end = 2026-01-01T10:00:00Z
window_start = window_end - 30m
updated_at >= window_start
updated_at < window_end
```

이 범위는 매 run 마다 runtime context 로 다시 계산된다.

### Time window normalization

`time_window.lookback` 은 짧은 interval string 으로 쓴다.

Canonical format:

```text
<positive integer><unit>
```

허용 unit:

- `s`: seconds
- `m`: minutes

예:

- `30s`
- `25m`
- `60m`

`h`, `d` 는 허용하지 않는다. 긴 범위의 재처리나 backfill 은 `time_window` 가 아니라 별도 재처리 run
또는 query `params` 로 명시적인 범위를 전달해 처리한다.

```yaml
time_window:
  column: updated_at
  lookback: "30m"
```

정규화 규칙:

- `lookback` 은 0보다 커야 한다.
- 계산된 `window_start` 는 inclusive lower bound 다.
- 계산된 `window_end` 는 exclusive upper bound 다.

따라서 위 예시는 다음 조건을 뜻한다.

```text
updated_at >= window_end - 30m
AND updated_at < window_end
```

### Interval normalization

`overlap_window` 는 짧은 interval string 으로 쓴다. 이 값은 마지막 watermark 이후 데이터를 얼마나
과거로 되돌려 다시 읽을지를 정한다. Runtime 은 `last_watermark - overlap_window` 를 계산해
incremental query 의 시작 bind 값으로 사용한다.

Canonical format:

```text
<non-negative integer><unit>
```

허용 unit:

- `s`: seconds
- `m`: minutes

예:

- `0s`
- `30s`
- `5m`

`h`, `d` 는 허용하지 않는다. 긴 지연 보정이나 backfill 은 `overlap_window` 가 아니라 명시적
`time_window` 또는 별도 재처리 run 으로 처리한다.

```yaml
watermark:
  column: updated_at
  overlap_window: "5m"
```

`overlap_window` 를 생략하면 `"0s"` 로 본다.

예를 들어 마지막 watermark 가 `2026-01-01T10:00:00Z` 이고 `overlap_window: "5m"` 이면 runtime 은
다음 값을 계산한다.

```text
select_from = 2026-01-01T09:55:00Z
```

사용자 SQL 은 `:select_from` 을 직접 배치하지만, 위 계산을 SQL 로 다시 작성하지 않는다.

`time_window` 는 transform table 위치나 stage table 이름을 정하지 않는다. source selection 조건으로만
쓰인다.

`source.kind: query` 에서도 `watermark` 를 사용할 수 있다. 이 경우 watermark 조건은 manual 방식만
허용한다. Runtime 이 사용자 SQL 을 subquery 로 감싸서 조건을 추가하지 않는다. Query SQL 안에서
필요한 bind parameter 를 직접 사용해야 한다.

Manual 방식은 predicate 위치와 식을 사용자가 정한다는 뜻이다. Watermark state 조회,
`overlap_window` 계산, bind value 생성은 runtime 책임이다.

```sql
select
  order_id,
  customer_id,
  amount,
  updated_at as watermark_ts
from orders
where updated_at > :select_from
  and updated_at <= :cur_wm
order by watermark_ts
```

Incremental query 에서 runtime 이 제공하는 bind parameter 는 다음이다.

- `:select_from`
- `:cur_wm`

`overlap_window` 가 있으면 runtime 은 `last_watermark - overlap_window` 를 계산해 `:select_from` 에
전달한다. 사용자는 SQL 안에서 `:select_from` 을 직접 배치하지만, overlap 계산을 SQL 로 다시 작성하지
않는다.

Query 결과에는 `watermark.column` 이 포함되어야 한다. Runtime 은 결과 rowset 의
`watermark.column` 최대값으로 watermark 를 갱신한다.

`source.kind: query` 에서 고정 시간 범위를 읽을 때는 `time_window` 대신 `params` 로 bind 값을
전달한다. Query SQL 이 이미 source selection 조건을 직접 표현하므로 별도 `time_window` 계약이
필요하지 않다.

```yaml
params:
  window_start: "2026-01-01 00:00"
  window_end: "2026-01-02 00:00"
```

```sql
select
  order_id,
  updated_at
from orders
where updated_at >= :window_start
  and updated_at < :window_end
```

조인 query 도 사용할 수 있다. 단일 table query 로 제한하지 않는다. 중요한 조건은 query 안에서
watermark 조건을 직접 작성하고, 최종 select 결과에 watermark 기준 column 을 명확히 노출하는 것이다.

```sql
select
  o.order_id,
  o.customer_id,
  c.customer_name,
  o.amount,
  o.updated_at as watermark_ts
from orders o
join customers c
  on c.customer_id = o.customer_id
where o.updated_at > :select_from
  and o.updated_at <= :cur_wm
order by o.updated_at
```

```yaml
watermark:
  column: watermark_ts
```

여러 source table 의 변경 시각을 함께 봐야 하면 query 안에서 기준 column 을 하나로 만들어야 한다.

```sql
select
  o.order_id,
  c.customer_name,
  greatest(o.updated_at, c.updated_at) as watermark_ts
from orders o
join customers c
  on c.customer_id = o.customer_id
where greatest(o.updated_at, c.updated_at) > :select_from
  and greatest(o.updated_at, c.updated_at) <= :cur_wm
order by watermark_ts
```

Metastore 는 마지막 watermark, extract history 같은 zeta4s metadata 를 저장한다. 현재 로컬/검증
스택의 기본 metastore 구현체는 PostgreSQL 이다.

## 실행 종속

`extract` 는 보통 upstream data input 이 없다.

source 준비 step 이 필요하면 `depends_on` 에 명시한다.

```yaml
steps:
  - step_id: extract_orders
    type: oracle.extract
    conn: oracle_source
    depends_on:
      - prepare_source_view
    source:
      kind: query
      query: sql/oracle/extract_orders.sql
    output:
      orders_rows:
        kind: rowset
```

## Result/metrics

`extract` runtime result 는 최소한 다음 정보를 남겨야 한다.

- row count
- rowset output name
- rowset format
- source type

Graph output 은 runtime result 가 아니라 YAML `output` 에서 정의된다.

## 제약

`extract` 는 source system 에서 rowset 을 읽는 step 이다. transform 용 DB object 이름을 결정하지
않는다.

`source.kind: table`/`search` 에 time window 가 필요하면 top-level `time_window` 로 표현한다.
`source.kind: query` 에 고정 시간 범위를 넘길 때는 `params` bind 값으로 표현한다. Metastore 는
extract history/state 를 저장할 수 있지만, transform table 위치를 결정하지 않는다.

## 예시

### Oracle table extract

```yaml
steps:
  - step_id: extract_orders
    type: oracle.extract
    conn: oracle_source
    source:
      kind: table
      table: sales.orders
      where_clause: "order_date >= :start_date AND order_date < :end_date"
    params:
      start_date: "2026-01-01"
      end_date: "2026-01-02"
    output:
      orders_rows:
        kind: rowset
```

### Oracle incremental extract

```yaml
steps:
  - step_id: extract_orders_incremental
    type: oracle.extract
    conn: oracle_source
    source:
      kind: table
      table: sales.orders
    watermark:
      column: updated_at
      overlap_window: "5m"
    output:
      orders_rows:
        kind: rowset
```

### Oracle query extract

```yaml
steps:
  - step_id: extract_customers
    type: oracle.extract
    conn: oracle_source
    source:
      kind: query
      query: sql/oracle/extract_customers.sql
    output:
      customers_rows:
        kind: rowset
```

### Oracle query time range extract

```yaml
steps:
  - step_id: extract_customers_window
    type: oracle.extract
    conn: oracle_source
    source:
      kind: query
      query: sql/oracle/extract_customers_window.sql
    params:
      window_start: "2026-01-01 00:00"
      window_end: "2026-01-02 00:00"
    output:
      customers_rows:
        kind: rowset
```

`sql/oracle/extract_customers_window.sql` 은 시간 조건을 직접 포함한다.

```sql
select
  customer_id,
  updated_at
from customers
where updated_at >= :window_start
  and updated_at < :window_end
```

### Oracle query incremental extract

```yaml
steps:
  - step_id: extract_order_customer
    type: oracle.extract
    conn: oracle_source
    source:
      kind: query
      query: sql/oracle/extract_order_customer.sql
    watermark:
      column: watermark_ts
    output:
      order_customer_rows:
        kind: rowset
```

`sql/oracle/extract_order_customer.sql` 은 watermark 조건을 직접 포함해야 한다.

```sql
select
  o.order_id,
  c.customer_name,
  greatest(o.updated_at, c.updated_at) as watermark_ts
from orders o
join customers c
  on c.customer_id = o.customer_id
where greatest(o.updated_at, c.updated_at) > :select_from
  and greatest(o.updated_at, c.updated_at) <= :cur_wm
order by watermark_ts
```

### ClickHouse query extract

```yaml
steps:
  - step_id: extract_events
    type: clickhouse.extract
    conn: clickhouse_source
    source:
      kind: query
      query: sql/clickhouse/extract_events.sql
    output:
      events_rows:
        kind: rowset
```

### ClickHouse table extract

```yaml
steps:
  - step_id: extract_events
    type: clickhouse.extract
    conn: clickhouse_source
    source:
      kind: table
      table: analytics.events
      where_clause: "event_time >= :window_start AND event_time < :window_end"
    params:
      window_start: "2026-01-01 00:00"
      window_end: "2026-01-02 00:00"
    output:
      events_rows:
        kind: rowset
```

ClickHouse table source 도 `table` 또는 `schema.table` 형식으로 쓴다. 여기서 schema 위치에는
ClickHouse database 이름을 쓴다.

ClickHouse extract 도 `where_clause`, query SQL, runtime predicate 의 bind parameter 를 `:name` 으로
쓴다. Runtime 은 ClickHouse 로 보내기 직전에 이를 server-side parameter 로 바꾸며 규칙은
[SQL step 의 `params`](sql.md) 와 같다. `datetime` bind 값은 microsecond 정밀도를 유지하므로
`DateTime64` watermark/time window 경계가 잘리지 않는다.

### Elasticsearch search extract

```yaml
steps:
  - step_id: extract_products
    type: elasticsearch.extract
    conn: elasticsearch_source
    source:
      kind: search
      index: products
      query:
        range:
          updated_at:
            gte: "2026-01-01 00:00"
            lt: "2026-01-02 00:00"
      fields:
        - column: product_id
          path: product.id
          type: str
        - column: product_name
          path: product.name
          type: str
        - column: updated_at
          path: updated_at
          type: timestamp
          datetime_precision: 6
    output:
      products_rows:
        kind: rowset
```

### Elasticsearch time window extract

```yaml
steps:
  - step_id: extract_products_window
    type: elasticsearch.extract
    conn: elasticsearch_source
    source:
      kind: search
      index: products
      fields:
        - column: product_id
          path: product.id
          type: str
        - column: updated_at
          path: updated_at
          type: timestamp
          datetime_precision: 6
    time_window:
      column: updated_at
      lookback: "30m"
    output:
      products_rows:
        kind: rowset
```
