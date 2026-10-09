# Extract/Write 성능 최적화

상태: backlog

`extract`/`write` runtime contract 검증과 별개로 backend 별 성능 병목을 추적 가능한
최적화 과제로 분리한다.

- `elasticsearch.write` bulk request, batch size, refresh/indexing, mapping 비용
- `elasticsearch.extract` pagination/search-after 경로와 rowset snapshot 변환 비용
- Oracle extract fetch path, arraysize, prefetchrows, type conversion 비용
- ClickHouse/Oracle target write 는 현재 주 병목으로 보지 않고 회귀 감시 대상으로 둔다

성능 benchmark 는 smoke DAG 와 분리된 isolated/fan-out DAG 로 반복 실행한다.
