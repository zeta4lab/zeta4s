# Write Step Contract

## 대상 type

- `clickhouse.write`
- `oracle.write`
- `elasticsearch.write`

## 역할

`write` step 은 zeta4s runtime graph 안에서 만들어진 storage-neutral rowset을 target system 에
반영하는 egress boundary 다.

```text
upstream rowset output -> target system
```

`stage` 는 rowset 을 transform 용 runtime table snapshot 으로 고정하는 boundary 고, `write` 는 rowset
을 graph 밖 target system 상태로 반영하는 boundary 다. 두 step 모두 rowset 을 입력으로 받을 수 있지만
역할은 다르다.

- `clickhouse.write`: rowset 을 ClickHouse target table 에 쓴다.
- `oracle.write`: rowset 을 Oracle target table 에 쓴다.
- `elasticsearch.write`: rowset 을 Elasticsearch target index 에 쓴다.

`clickhouse.write` 를 write runtime 의 기준 구현으로 둔다. ClickHouse 는 SQL transform runtime 으로도
사용되지만, `clickhouse.write` 에서는 target system 중 하나로만 취급한다.

## 현재 계약

현재 계약은 `map` 으로 upstream rowset output 과 target spec 을 연결한다.

```yaml
steps:
  - id: write_sales_rows_to_clickhouse
    type: clickhouse.write
    conn: clickhouse_target
    depends_on:
      - fetch_sales_rows
    map:
      fetch_sales_rows.sales_rows:
        table: mart.sales
        mode: replace
        columns:
          - sale_id
          - store_id
          - amount
          - sold_at
```

## Source 참조

`map` key 는 다음 형식이다.

```yaml
map:
  <step_id>.<output_name>: <target_spec>
```

예:

```yaml
map:
  fetch_sales_rows.sales_rows:
    table: mart.sales
    mode: replace
    columns:
      - sale_id
```

`map` key 는 upstream step 의 named rowset output 을 가리킨다. `kind`, `step`, `output`, `backend`,
source table 이름을 반복해서 쓰지 않는다.
입력 rowset artifact 와 type metadata 는 [Rowset Contract](../../design/rowset.md)를 따른다.

`map` key 의 `<step_id>` 는 `depends_on` 에 포함되어야 한다. `map` 은 data reference 이고 실행 순서를
자동으로 만들지 않는다.

Upstream output 이름이 `schema.table` 형식이어도 `map` key 는 첫 번째 `.` 만 step id 와 output 이름의
구분자로 해석한다.

## Target

### ClickHouse target

필수 field:

- `table`
- `mode`
- `columns`

선택 field:

- `key`
- `engine`
- `order_by`
- `partition_by`
- `settings`
- `batch_size`

`table` 은 `database.table` 형식으로 쓴다. project-local database 를 암묵적으로 붙이지 않는다.
`mode: upsert` 는 `key` 가 필요하다.

### Oracle target

필수 field:

- `table`
- `mode`
- `columns`

선택 field:

- `key`
- `batch_size`

`table` 은 `table` 또는 `schema.table` 형식으로 쓴다. `mode: upsert` 는 `key` 가 필요하다. `key`
column 은 target table 생성 시 `NOT NULL` 로 만든다. 기존 target table 이 있으면 `columns` 존재 여부와
`key` column 의 `NOT NULL` contract 를 검증한다.

### Elasticsearch target

필수 field:

- `index` 또는 `index_template`
- `mode`
- `columns`

선택 field:

- `key`
- `document_id`
- `batch_size`
- `refresh`
- `create_index_if_missing`
- `index_timezone`

`mode: upsert` 는 `key` 또는 `document_id` columns 가 필요하다. `document_id` 는 string column 이름 또는
`{mode: columns, columns: [...]}` / `{mode: auto}` mapping 을 쓴다. `key` 는 `document_id.columns`
축약으로 취급한다. `{mode: auto}` 는 `replace`, `append` 에서만 쓸 수 있다. `mode: replace` 는 target
index mapping/settings 를 보존하고 기존 document 를 삭제한 뒤 rowset 을 bulk index 한다.

## Mode

지원 mode:

- `replace`: target 을 source rowset 기준으로 다시 쓴다.
- `upsert`: key 또는 document id 기준으로 있으면 갱신하고 없으면 추가한다.
- `append`: target 에 새 row/document 를 추가한다.

`mode` 는 step top-level 이 아니라 `map` entry 의 target spec 안에 둔다. `write.map` 은 정확히
하나의 entry 만 허용한다. 여러 target 에 써야 하면 write step 을 target 별로 분리한다.

`mode: merge` 는 허용하지 않는다. `upsert` 를 canonical 이름으로 사용한다.

## Retry semantics

`replace` 와 `upsert` 는 동일 rowset 재실행 시 target 상태가 중복 반영되지 않아야 한다.

`append` 는 at-least-once append semantics 이다. 동일 rowset 을 다시 실행하면 target 에 중복 row/document
가 추가될 수 있다. 중복 방지가 필요한 write 는 `append` 가 아니라 `upsert` 를 사용한다.

Elasticsearch `append` 는 bulk `create` action 을 사용한다. `_id` 가 명시된 append rowset 을 재실행하면
기존 document 를 조용히 덮어쓰지 않고 conflict 로 실패할 수 있다.

## Connection

`conn` 은 target system connection 이다.

- `clickhouse.write.conn`: ClickHouse target connection
- `oracle.write.conn`: Oracle target connection
- `elasticsearch.write.conn`: Elasticsearch target connection

Source rowset 을 만든 upstream step 의 `conn` 과 write step 의 `conn` 은 달라도 된다. Write step 은
runtime graph 안의 rowset artifact 를 외부 target system 으로 내보내는 step 이기 때문이다.

## 실행 종속

`map` 은 data reference 이고, 실행 순서를 만들지 않는다.

Source rowset 을 만드는 upstream step 은 `depends_on` 에 명시한다.

```yaml
steps:
  - id: fetch_sales_rows
    type: oracle.extract
    conn: oracle_source
    source:
      query: sql/oracle/fetch_sales.sql
    output:
      sales_rows:
        kind: rowset

  - id: write_sales_rows_to_clickhouse
    type: clickhouse.write
    conn: clickhouse_target
    depends_on:
      - fetch_sales_rows
    map:
      fetch_sales_rows.sales_rows:
        table: mart.sales
        mode: replace
        columns:
          - sale_id
          - amount
```

## Execution unit

write step 1개는 write execution unit 1개로 실행한다. Airflow task id 는 step id 를 그대로 사용한다.

```text
write_sales_rows
```

여러 target 에 써야 하면 write step 을 target 별로 분리한다. 각 write step 은 자신의 `depends_on` 이
완료된 뒤 독립적으로 실행된다.

Step 성공은 해당 write execution unit 성공을 뜻한다.

`pool`, `retry`, `timeout` 은 write step 의 execution unit 에 적용한다.

## Result/metrics

Runtime result stage 는 `write` 다.

최소 metrics:

- `input_rows`
- `output_rows`
- `success_rows`
- `failed_rows`
- `skipped_rows`
- `error_rows`

Result details 는 write execution manifest 형태여야 한다.

필수 manifest 항목:

- `source`: upstream rowset ref
- `target_type`: target adapter type
- `target`: target table
- `mode`
- `columns`
- `key`
- `batch_count`
- `input_rows`
- `output_rows`

`write` step 은 target system 상태를 변경한다. Graph output 은 만들지 않는다.

## Runtime boundary

Write runtime 은 source 를 DB table 로 가정하지 않는다. 입력은 rowset descriptor와 reader다.

공통 실행 흐름:

1. `map` key 로 upstream rowset descriptor를 resolve 한다.
2. rowset schema 와 target `columns` 를 검증한다.
3. target adapter 가 rowset batch 를 읽어 target system 에 반영한다.
4. write execution manifest 를 task result details 로 기록한다.

Target runtime 은 별도 adapter 로 분리한다.

Target table type 은 rowset 의 canonical logical type 을 기준으로 만든다. 같은 backend 에서 추출한
rowset 을 같은 backend 로 write 할 때만 source-native type hint 를 보존한다.

- `zeta4s.runtime.backends.clickhouse.write`
- `zeta4s.runtime.backends.oracle.write`
- `zeta4s.runtime.backends.elasticsearch.write`

공통 facade 는 target adapter 선택과 result contract 만 책임진다. Target 별 SQL 방식은 adapter 안에 둔다.

## 제약

- `write` step 은 graph output 을 만들지 않는다.
- `map` entry 는 1개 이상이어야 한다.
- `map` entry source 는 rowset output 이어야 한다.
- 한 `write` step 안의 모든 `map` entry 는 같은 target type 과 같은 `conn` 을 사용한다.
- `map` entry 별 target table 은 서로 달라야 한다.
- `columns` 는 source rowset schema 에 존재해야 한다.
- `mode` 는 각 `map` entry target spec 에 둔다.
- `mode: merge` 는 허용하지 않는다.
- ClickHouse `upsert` 는 `key` 가 필요하다.
- Oracle `upsert` 는 `key` 가 필요하다.
- Elasticsearch `upsert` 는 `key` 또는 `document_id` 가 필요하다.
- 현재 계약에서는 source DB table 을 직접 지정하는 긴 `source` block 을 쓰지 않는다.
