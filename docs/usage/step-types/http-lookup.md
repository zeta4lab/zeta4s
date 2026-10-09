# HTTP Lookup Step Contract

## 대상 type

- `http.lookup`

## 역할

`http.lookup` 은 기존 DB table 을 이용해서 외부 HTTP API 로 가공한 column 을 붙인 DB table 을 만드는
transform step 이다.

현재 `http.lookup` 계약은 기존 DB table 을 읽고, 외부 API lookup 결과를 column 으로 붙여 target DB
table 을 만드는 transform 범위다.

Rowset 을 입력으로 받아 외부 API 로 추가 가공한 뒤 DB table 을 만드는 기능도 같은 transform 계약의
to-be 확장으로 다룬다.

## 현재 계약

현재 계약에서 `http.lookup` 은 `lookup` 으로 API lookup 대상 table 과 column 을 지정하고,
`target.table` 로 생성할 DB table 을 지정한다.

```yaml
steps:
  - id: enrich_ticket_priority
    type: http.lookup
    conn: analytics_clickhouse
    depends_on:
      - stage_support_tickets
    lookup:
      column: description
      table: stage_support_tickets.mart.support_tickets_raw
    target:
      table: mart.support_tickets_enriched
    api:
      conn: external_lookup_api
      path: /classify
      method: GET
      query_params:
        source: zeta4s
      request:
        query_param: text
      response:
        json_paths:
          - $.priority_label
        columns:
          - name: priority_label
            type: String
```

현재 계약의 핵심은 다음이다.

- `conn` 은 source/target DB table 을 읽고 쓸 DB connection 이다.
- `api.conn` 은 외부 HTTP API connection 이다.
- `lookup.table` 은 lookup 대상 upstream table output 을 지정한다.
- `lookup.column` 은 각 source row 에서 API request 에 넣을 column 을 지정한다.
- `target.table` 은 `conn` 이 가리키는 DB 에 만들 target table 이름이다.
- `api.response.columns` 는 API response 로 추가할 column 목록이다.
- target table 은 source table columns 에 `api.response.columns` 를 append 한 schema 로 생성한다.

`lookup` 과 `target` 은 다음 형식이다.

```yaml
lookup:
  column: <lookup_column>
  table: <upstream_step>.<output_name>
target:
  table: <target_table>
```

`lookup.table` 은 `http.lookup` 이 읽을 upstream table output 을 가리킨다. `lookup.column` 은 API
request 에 넣을 lookup column 이름이다. `target.table` 은 `conn` 이 가리키는 DB 에 만들 target table
이름이다.

DB table 이름은 항상 `schema.table` 형식이다. ClickHouse 에서는 database 이름을 schema 위치에 쓴다.
현재 계약의 `schema.table` 단일 문자열 표기에서는 quoted identifier 를 표현하지 않는다. 예시는
lowercase 로 쓴다.
`lookup.table` 은 첫 번째 `.` 만 upstream step id 와 output 이름의 구분자로 해석한다. 예를 들어
`stage_support_tickets.mart.support_tickets_raw` 에서 step id 는 `stage_support_tickets`, output
이름은 `mart.support_tickets_raw` 다.

후속 step 이 참조할 수 있는 output 이름은 `target.table` 값이다. 별도 `output` 블록은 쓰지 않는다.

`source.query` 라는 이름은 SQL query 로 오해되므로 현재 계약에서 쓰지 않는다.

Output column 은 runtime parameter 가 아니라 API response contract 이므로 `params.output_columns` 를
쓰지 않고 `api.response.columns` 를 사용한다.

## Input

- 종류: table
- 개수: 1개
- 표현: `lookup.table`
- 형식: `<upstream_step>.<output_name>`

```yaml
lookup:
  column: description
  table: stage_support_tickets.mart.support_tickets_raw
```

`lookup.table` 의 `upstream_step` 은 `depends_on` 에 포함되어야 한다.

## Output

- 종류: table
- 개수: 1개
- 이름: `target.table`

별도 `output` 블록은 쓰지 않는다. 후속 step 은 `target.table` 값을 output 이름으로 참조한다.
후속 step 이 `enrich_ticket_priority.mart.support_tickets_enriched` 처럼 축약 참조를 쓰면 첫 번째
`.` 만 step id 와 output 이름의 구분자로 해석한다.

## 필수 field

현재 계약 기준 필수 field:

- `id`
- `type`
- `conn`
- `depends_on`
- `lookup.table`
- `lookup.column`
- `target.table`
- `api.conn`
- `api.method`
- `api.response.columns`

## 선택 field

공통 flow-control field:

- `pool`
- `when`
- `join`
- `retry`
- `timeout`

실행 옵션:

- `batch_size`
- `concurrency`

HTTP API 옵션:

- `api.path`
- `api.method`
- `api.query_params`
- `api.request.query_param`
- `api.request.json_field`
- `api.response.json_paths`
- `api.response.columns`
- `api.timeout_seconds`
- `api.retries`

## 쓰지 않는 field

현재 계약 기준에서 `http.lookup` 은 아래 field 를 쓰지 않는다.

- `source`
- `map`
- `input`
- `params.output_columns`
- `output`

`source`, `map`, `params.output_columns` 는 현재 계약에서 쓰지 않는다. 현재 계약에서는 `lookup`,
`target`, `api`, `api.response.columns` 로 역할을 나눈다.

## Connection

`http.lookup` 은 connection 을 두 종류 사용한다.

- `conn`: source/target DB table 을 처리할 DB connection
- `api.conn`: HTTP API connection

`conn` 과 `api.conn` 은 서로 다른 connection 이다.

`conn` 을 생략해서 project default ClickHouse database 를 쓰는 방식은 현재 계약에 포함하지 않는다.
Project 안에 여러 DB connection 이 공존할 수 있으므로 table 위치는 `conn` 기준으로 해석한다.

## API request

`lookup.column` 값은 각 source row 마다 HTTP request payload 에 들어간다.

`api.method: GET` 은 query parameter 로 요청한다.

```yaml
api:
  path: /classify
  method: GET
  query_params:
    source: zeta4s
  request:
    query_param: text
```

위 계약은 다음 요청을 만든다.

```text
GET /classify?source=zeta4s&text=<lookup.column value>
```

`api.method: POST` 는 JSON field 로 요청한다.

```yaml
api:
  path: /classify
  method: POST
  request:
    json_field: text
```

위 계약은 다음 JSON body 를 만든다.

```json
{"text": "<lookup.column value>"}
```

`api.request.query_param` 또는 `api.request.json_field` 를 생략하면 `lookup.column` 이름을 기본 field 이름으로
사용한다.

## API response

`api.response.json_paths` 는 response 에서 output 값으로 사용할 위치 후보 목록이다. 앞의 path 부터
시도하고, 처음 성공한 값을 사용한다.

```yaml
api:
  response:
    json_paths:
      - $.result.priority_label
      - $.priority_label
```

`api.response.columns` 가 1개이면 response path 결과가 scalar 여도 된다.

```yaml
api:
  response:
    json_paths:
      - $.priority_label
    columns:
      - name: priority_label
        type: String
```

`api.response.columns` 가 여러 개이면 response path 결과는 object 여야 하며, object key 와 response
column name 을 매칭한다.

```yaml
api:
  response:
    json_paths:
      - $.result
    columns:
      - name: priority_label
        type: String
      - name: risk_score
        type: Float64
```

## Error handling

외부 API 의 에러 처리 정책은 외부 API 의 책임이다. `http.lookup` 은 API 호출 결과를 기준으로 target
table 생성 metrics 를 남긴다.

Runtime result metrics:

- `input_rows`
- `output_rows`
- `success_rows`
- `failed_rows`
- `skipped_rows`
- `error_rows`

## Target table 생성

`http.lookup` 은 `target.table` 이름으로 target table 을 생성한다.

Target table schema 는 다음 순서로 만든다.

1. Source table 의 모든 column 을 유지한다.
2. `api.response.columns` 를 source column 뒤에 append 한다.

예를 들어 source table 이 다음 schema 라면:

```text
ticket_id UInt64
description String
```

계약이 다음과 같을 때:

```yaml
api:
  response:
    columns:
      - name: priority_label
        type: String
```

target table schema 는 다음과 같다.

```text
ticket_id UInt64
description String
priority_label String
```

Target table 의 row 는 source row 에 API response column 값을 붙여 만든다.

```text
source row columns + response columns
```

`target.table` 값은 target table 이름이자 graph output 이름이다.

## 실행 종속

`lookup.table` 은 data binding 이고, 실행 순서를 만들지 않는다.

Lookup 대상 table 을 만드는 upstream step 은 `depends_on` 에 명시한다.

```yaml
steps:
  - id: stage_support_tickets
    type: clickhouse.stage
    conn: analytics_clickhouse
    depends_on:
      - extract_support_tickets
    map:
      extract_support_tickets.support_tickets_rows: mart.support_tickets_raw

  - id: enrich_ticket_priority
    type: http.lookup
    conn: analytics_clickhouse
    depends_on:
      - stage_support_tickets
    lookup:
      column: description
      table: stage_support_tickets.mart.support_tickets_raw
    target:
      table: mart.support_tickets_enriched
    api:
      conn: external_lookup_api
      path: /classify
      method: GET
      request:
        query_param: text
      response:
        json_paths:
          - $.priority_label
        columns:
          - name: priority_label
            type: String
```

## Result/metrics

Runtime result stage 는 `http.lookup` 이다.

최소 metrics:

- input row count
- output row count
- failed row count
- skipped row count
- target table name

Graph output 은 runtime result 가 아니라 YAML `target.table` 에서 파생된다.

## 제약

- `http.lookup` 은 table input 1개와 table output 1개를 받는다.
- 다른 API 또는 다른 response column 계약이 필요하면 `http.lookup` step 을 나눈다.
- `lookup.column` 은 input table 에 존재해야 한다.
- `api.response.columns[].name` 은 input table 의 기존 column 과 충돌하면 안 된다.
- 같은 target table 에 덮어쓰는 경우 runtime 은 work table 을 만든 뒤 promote 해야 한다.
- 현재 계약에서 `source.query` 라는 이름은 사용하지 않는다.
