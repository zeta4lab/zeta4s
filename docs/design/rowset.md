# Rowset Contract

## 역할

Rowset 은 `extract` step 이 runtime graph 안으로 들여온 데이터를 표현하는 storage-neutral data contract다.
`stage` 와 `write` step 은 backend 종류와 무관하게 같은 rowset contract 를 입력으로 받는다.

```text
extract -> rowset -> stage/write
```

Rowset 은 runtime table 이 아니다. Table 생성, target 반영, transform 용 snapshot 고정은 후속
`stage` 또는 `write` step 의 책임이다.

## Runtime Representation

Authoring contract는 `kind: rowset`만 선언한다. runtime output binding은 물리 저장소를 가리키는
`RowsetDescriptor`와 column metadata로 구성된다.

- `kind`: `rowset`
- `storage`: runtime이 선택한 `parquet` 또는 `iceberg`
- `storage_uri`
- `table_identifier`와 `snapshot_id` (Iceberg인 경우)
- `rows`
- `bytes`
- `columns`
- `column_specs`

`column_specs`는 task result output manifest와 rowset descriptor에 기록한다. verification Runner는
ephemeral Parquet를, scheduler-projected 실행은 Iceberg snapshot을 사용하며 project/profile은 이를
선택하지 않는다.

## ColumnSpec

`column_specs[]` 항목은 다음 field 를 사용한다.

| field | 필수 | 의미 |
| --- | --- | --- |
| `name` | 예 | rowset column name |
| `type` | 예 | rowset type name |
| `nullable` | 예 | null 허용 여부 |
| `logical_type` | 예 | target-neutral logical type |
| `precision` | 아니오 | integer/float/decimal precision |
| `scale` | 아니오 | decimal scale |
| `datetime_precision` | 아니오 | timestamp fractional second precision |
| `source_backend` | 아니오 | source adapter hint |
| `source_type` | 아니오 | source-native type hint |

허용하는 `logical_type` 값은 다음이다.

- `boolean`
- `integer`
- `float`
- `decimal`
- `string`
- `date`
- `timestamp`

`source_backend` 과 `source_type` 은 rowset contract 의 분기 기준이 아니라 source-native 보존 hint 다.
Target adapter 는 같은 backend 로 되돌리는 경우에만 native type 을 보존할 수 있다. 다른 backend 로
stage/write 할 때는 `logical_type`, `precision`, `scale`, `datetime_precision`, `nullable` 을 기준으로
target type 을 결정한다.

## Producer

`oracle.extract`, `clickhouse.extract`, `elasticsearch.extract` 는 모두 같은 rowset output 을 만든다.

- Oracle/ClickHouse extract 는 source-native type 을 `source_type` 에 보존한다.
- Elasticsearch extract 는 `source.fields[].type` 에 canonical rowset type 을 사용한다.
- Producer 는 rowset schema 와 `column_specs` metadata 를 일관되게 기록한다.
- Producer 는 transform 용 runtime table 을 직접 만들지 않는다.

`elasticsearch.extract.source.fields[].type` 허용 값:

- `bool`
- `int`
- `float`
- `decimal`
- `str`
- `date`
- `timestamp`

`type: timestamp` 는 `datetime_precision` 을 생략하면 `6` 을 사용한다.

## Consumer

`clickhouse.stage`, `oracle.stage`, `clickhouse.write`, `oracle.write`, `elasticsearch.write` 는 upstream rowset
artifact 를 입력으로 받는다.

Consumer는 physical path나 Parquet API를 직접 열지 않고 `RowsetReader`에서 batch를 읽는다. Column
contract는 task result와 descriptor의 `column_specs`를 기준으로 해석하며 정상 extract producer가 만든
rowset은 이 metadata를 가진다.

Scheduler checkpoint는 Iceberg snapshot을 먼저 commit하고 그 snapshot reference를 metastore에 기록한다.
Metastore가 참조하지 않는 snapshot은 orphan으로 취급하며 resume selection에서 제외한다.

Target type mapping 은 target adapter 책임이다.

- ClickHouse target 은 ClickHouse source rowset 에서만 ClickHouse native type 을 보존한다.
- Oracle target 은 Oracle source rowset 에서만 Oracle native type 을 보존한다.
- 서로 다른 backend 사이에서는 canonical logical type 을 target type 으로 변환한다.
- Elasticsearch write 는 rowset column 값을 document field 로 반영하고 target index 정책은 write
  target spec 이 결정한다.

## Release Gate

Rowset 정합성 gate 는 source backend 와 주요 consumer 조합을 검증한다.

| source rowset | consumer |
| --- | --- |
| `oracle.extract` | `oracle.stage`, `clickhouse.write` |
| `clickhouse.extract` | `clickhouse.stage`, `oracle.write` |
| `elasticsearch.extract` | `clickhouse.stage`, `oracle.stage`, `elasticsearch.write` |

각 showcase 는 `z4s api deploy`, DAG run, reliability evidence 확인을 포함한다.
