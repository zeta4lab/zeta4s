# 용어

zeta4s 를 구현하고 논의할 때 쓰는 개념 정의다. 각 개념의 상세 계약은 이 디렉토리의
다른 문서와 `../usage/` 에 있다.

| 용어 | 의미 |
|------|------|
| Step | `jobs/*.yml` 의 `steps[]` 항목. 실행 가능한 최소 workflow node |
| Edge | step 간 실행 순서. `depends_on`, `when`, `join.rule` 을 정규화한 관계 |
| Terminal step | 같은 job graph 안에서 후행 step 이 없는 마지막 step. Airflow Asset 이 아님 |
| Job run result | terminal step 들의 상태를 종합해 만든 job 실행 결과. CLI/API summary 와 runtime report 에 기록 |
| StepOutput | step 이 만든 named data contract. table, scalar, storage-neutral rowset 을 구분한다 |
| Metastore | zeta4s 내부 run/report/step state/event/output binding metadata 를 저장하는 control-plane 저장소. 기본 구현체는 PostgreSQL 18, 선택 구현체는 ClickHouse |
| Runtime data backend | `stage`, `dbt.run`, `<db>.sql`, `write` 가 table 을 만들거나 읽고 쓰는 실행 DB/target boundary. Profile connection 의 `type` 에서 해석한다 |
| Runtime table adapter | StepOutput table 을 다루는 adapter. runtime data backend 별로 구현하며 현재 `clickhouse`, `oracle` 만 지원 |
| Transform provider | transform 을 수행하는 step adapter. `dbt.run`, `clickhouse.sql`, `oracle.sql` 등 |
| Step adapter | `steps[].type` 별 schema validation, 실행 매핑, runtime callable payload 를 책임지는 내장 adapter. scheduler task 바인딩은 type 과 무관한 generic 바인딩이다 |
| Flow control | `depends_on`, `when.*`, `join.rule`, retry, timeout, skip/failure propagation 같은 실행 제어 contract |
| Local runner | `zeta4s.core` 의 `LocalRunner`. `z4s run` 과 CI 계약 테스트용이며 의미론의 참조 구현. 위상 순서 병렬 실행과 graceful/hard stop 취소를 host process 안에서 제공하고, 재개/스케줄링은 비목표 |
| Scheduler engine | `ExecutionPlan` 을 projection 해 step 실행 조율 기반을 소유하는 외부 orchestration runtime. Airflow 와 Prefect |
| core-orchestrated / scheduler-projected | 실행 mode. 전자는 local runner 가 plan 전체를 host process 안에서 실행(로컬 실행), 후자는 scheduler engine 이 조율하고 step 별로 core step 실행 경계를 호출(운영 실행) |
| Airflow Asset/Dataset | Airflow 의 cross-DAG event trigger 기능. 기본 실행 모델에서는 사용하지 않음 |

`terminal step` 과 `Job run result` 는 다음 DAG 를 trigger 하기 위한 mechanism 이 아니다.
이 둘은 같은 job run 의 성공/실패를 판정하고 runtime report 로 남기기 위한 내부 실행 결과
개념이다.
