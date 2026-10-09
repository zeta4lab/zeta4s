# Elasticsearch Command Step Contract

## 대상 type

- `elasticsearch.command`

## 역할

`elasticsearch.command` 는 Elasticsearch 자체 data-plane API 를 실행하는 side-effect step 이다.

RDBMS 의 `clickhouse.sql`, `oracle.sql` 이 SQL statement/block 을 실행한다면,
Elasticsearch 에서는 HTTP method, endpoint, JSON body, NDJSON body 조합이 native command 단위다.
따라서 bulk ingest, reindex, update-by-query, delete-by-query, index schema 변경처럼
Elasticsearch API 로 표현되는 처리는 하나의 command step type 아래 `operation` 으로 구분한다.

```text
project-local command body -> Elasticsearch API
```

이 step 은 zeta4s 안에서 만들어진 upstream table output 을 쓰지 않는다. Elasticsearch query result 를
rowset 으로 만드는 producer 는 `elasticsearch.extract` 가 담당한다.

## 지원 버전

검증 대상은 Docker stack 기본 Elasticsearch `8.15.4` 다. 다른 Elasticsearch 8.x 버전은
API 호환 범위에서 동작할 수 있지만 release validation 대상은 아니다. Elasticsearch 7.x 이하는
지원 대상으로 두지 않는다.

## 현재 계약

### Bulk

```yaml
steps:
  - id: seed_product_documents
    type: elasticsearch.command
    conn: elasticsearch_seed
    operation: bulk
    target:
      index: products
    source:
      file: elasticsearch/products.bulk.ndjson
      format: ndjson
    refresh: true
```

`operation: bulk` 는 project-local NDJSON file 을 Elasticsearch `_bulk` API 로 전송한다.

```ndjson
{"index":{"_id":"p-001"}}
{"product_id":"p-001","product_name":"keyboard","updated_at":"2026-01-01T00:00:00Z"}
{"index":{"_id":"p-002"}}
{"product_id":"p-002","product_name":"mouse","updated_at":"2026-01-01T00:01:00Z"}
```

### Reindex

```yaml
steps:
  - id: rebuild_product_search
    type: elasticsearch.command
    conn: elasticsearch_admin
    operation: reindex
    body:
      source:
        index: products-v1
        query:
          range:
            updated_at:
              gte: "2026-01-01T00:00:00Z"
      dest:
        index: products-v2
    refresh: true
```

`operation: reindex` 는 Elasticsearch `_reindex` API 를 실행한다. Query 는 새 document 를 생성하는
SQL 이 아니라 기존 source index 의 document selection 조건이다.

### Update By Query

```yaml
steps:
  - id: mark_discontinued_products
    type: elasticsearch.command
    conn: elasticsearch_admin
    operation: update_by_query
    target:
      index: products
    body:
      script:
        source: "ctx._source.status = params.status"
        params:
          status: discontinued
      query:
        term:
          discontinued: true
    refresh: true
```

### Delete By Query

```yaml
steps:
  - id: delete_expired_sessions
    type: elasticsearch.command
    conn: elasticsearch_admin
    operation: delete_by_query
    target:
      index: sessions
    body:
      query:
        range:
          expires_at:
            lt: "2026-01-01T00:00:00Z"
    refresh: true
```

### Raw Request

```yaml
steps:
  - id: put_product_pipeline
    type: elasticsearch.command
    conn: elasticsearch_admin
    operation: request
    request:
      method: PUT
      path: /_ingest/pipeline/product-normalize
      body: elasticsearch/product-normalize.pipeline.json
```

`operation: request` 는 explicit method/path/body 로 Elasticsearch API 를 실행한다. Built-in operation
으로 표현하기 어려운 관리성 API 에만 사용한다.

## Input

- 종류: 없음
- 개수: 0

`elasticsearch.command` 는 graph data input 을 받지 않는다. 실행 순서가 필요하면 `depends_on` 으로만
연결한다.

## Output

- 종류: 없음
- 개수: 0

`elasticsearch.command` 는 rowset/table/scalar output 을 공개하지 않는다. API response 의 count,
created/updated/deleted rows, bulk item errors 같은 값은 task result metrics/details 로 기록한다.

## 필수 field

- `conn`: Elasticsearch connection
- `operation`: 실행할 Elasticsearch command

Operation 별 추가 필수 field:

- `bulk`: `source.file`, `source.format: ndjson`
- `reindex`: `body.source`, `body.dest`
- `update_by_query`: `target.index`, `body.query`, `body.script`
- `delete_by_query`: `target.index`, `body.query`
- `request`: `request.method`, `request.path`

## 선택 field

- `target.index`: 단일 target index
- `target.index_template`: runtime context 로 render 할 target index template
- `target.index_timezone`: `index_template` render 기준 timezone
- `target.create_index_if_missing`: index 가 없으면 생성
- `target.settings`: project root 기준 index settings JSON file path
- `target.mappings`: project root 기준 index mappings JSON file path
- `source.file`: project root 기준 request body file path
- `source.format`: `json` 또는 `ndjson`
- `body`: inline JSON body
- `request.body`: project root 기준 JSON/NDJSON body file path 또는 inline mapping
- `refresh`: command 후 refresh 여부. Boolean 또는 Elasticsearch refresh option 문자열을 허용한다.

## Operation

지원 operation:

- `bulk`: `_bulk`
- `reindex`: `_reindex`
- `update_by_query`: `/{index}/_update_by_query`
- `delete_by_query`: `/{index}/_delete_by_query`
- `request`: explicit method/path request

`create_index`, `delete_index`, `put_mapping`, `put_settings`, `put_pipeline`, `refresh` 는
`operation: request` 로 표현한다. 반복 사용이 많아지면 별도 built-in operation 으로 승격한다.

## Body file 제약

- `source.file` 과 `request.body` 는 project root 기준 상대 경로다.
- `source.format: ndjson` 은 newline-delimited JSON 이며 마지막 line 은 newline 으로 끝나야 한다.
- `source.format: json` 은 단일 JSON object 여야 한다.
- body file 에 template rendering 을 적용하지 않는다.
- bulk item error 또는 Elasticsearch response failure 가 있으면 step 은 실패한다.

## Connection

`conn` 은 Elasticsearch connection 이다.

```yaml
connections:
  elasticsearch_admin:
    type: elasticsearch
    host: elasticsearch
    port: 9200
```

## Elasticsearch command showcase 기준

Elasticsearch extract showcase 는 Elasticsearch 본래 seed 방식인 `_bulk` NDJSON 을
`elasticsearch.command` 로 실행한다.

```yaml
steps:
  - id: delete_sales_index
    type: elasticsearch.command
    conn: elasticsearch_source
    operation: request
    request:
      method: DELETE
      path: /sales-documents

  - id: create_sales_index
    type: elasticsearch.command
    conn: elasticsearch_source
    operation: request
    depends_on:
      - delete_sales_index
    request:
      method: PUT
      path: /sales-documents
      body: elasticsearch/sales.index.json

  - id: seed_sales_documents
    type: elasticsearch.command
    conn: elasticsearch_source
    operation: bulk
    depends_on:
      - create_sales_index
    target:
      index: sales-documents
    source:
      file: elasticsearch/sales.bulk.ndjson
      format: ndjson
    refresh: true

  - id: fetch_sales_documents
    type: elasticsearch.extract
    conn: elasticsearch_source
    depends_on:
      - seed_sales_documents
    source:
      kind: search
      index: sales-documents
      query:
        match_all: {}
      fields:
        - column: sale_id
          path: sale_id
          type: str
        - column: updated_at
          path: updated_at
          type: timestamp
          datetime_precision: 6
    output:
      sales_rows:
        kind: rowset
```
