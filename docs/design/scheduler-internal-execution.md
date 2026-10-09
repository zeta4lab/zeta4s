# Scheduler Internal Execution Contract

Airflow와 Prefect는 외부 scheduler backend다. Canonical 실행 단위는 scheduler native object가
아니라 `project_id`/`job_id`/`run_id`로 식별되는 Step Graph run이다.

## Projection Boundary

`zeta4s-api`의 run service는 active deployment가 기록한 scheduler로 adapter를 선택한다.
Airflow adapter는 DAG/DAG run에, Prefect adapter는 deployment/flow run에 같은 canonical
run과 task 상태를 투영한다. Native identity와 원본 상태는 adapter 진단 metadata일 뿐 public
identity가 아니다.

Airflow용 standalone source는 Python 표준 라이브러리와 Airflow package만 import한다. Generated
task는 project, artifact, profile, job, step, run, attempt와 parameters를 common internal runtime
endpoint로 전달한다. SQL, connection metadata, password, runtime implementation은 generated
source에 넣지 않는다. Prefect worker는 같은 runtime facade를 process 안에서 호출한다.

## Internal API Boundary

Common internal route와 request model은 `src/zeta4s/api/app.py`가 정본이다.

- 모든 요청은 runtime internal token을 요구한다.
- 요청 identity는 metastore의 active deployment와 다시 대조한다.
- stale artifact/profile이나 미등록 job/step은 실행하지 않는다.
- `zeta4s-api`가 artifact에서 canonical plan을 다시 읽고 core runner를 호출한다.
- profile connection의 `password_ref`는 encrypted secret store에서 API process 안에서만 resolve한다.
- plaintext credential은 scheduler request, generated source, scheduler metastore에 저장하지 않는다.

## State Boundary

Metastore가 canonical run, step, attempt 상태의 source of truth다. Adapter는 native state, timestamp,
task와 log를 canonical snapshot으로 정규화한다. Scheduler capability가 없으면 빈 성공 응답 대신
명시적인 unsupported operation 오류를 반환한다.

## Deployment Boundary

Local compose는 scheduler 공식 image를 사용한다. Scheduler image version은 platform 설정이며
zeta4s version에서 파생하지 않는다. zeta4s image build는 `zeta4s-api` 하나이며 adapter 실행에
필요한 dependency만 포함한다.
