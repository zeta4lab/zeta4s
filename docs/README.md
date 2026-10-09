# zeta4s 문서 인덱스

이 디렉토리는 zeta4s 의 현재 기준 문서를 관리한다. zeta4s 의 궁극 목표는 AI Agent 가 생성한
계약(Contract)을 Step Graph 로 실행하는 범용 Runtime Engine 이 되는 것이다.

## 빠른 진입점

| 목적 | 시작 문서 |
|------|-----------|
| Quickstart, Docker volume reset | [../README.md](../README.md) |
| zeta4s 를 쓴다 — workspace, profile, project 작성과 `z4s` 실행 | [usage/README.md](./usage/README.md) |
| zeta4s 를 고친다 — 현재 코드의 설계 | [design/README.md](./design/README.md) |
| 검증 gate | [gate/README.md](./gate/README.md) |
| 목표와 완성 여부 | [roadmap/README.md](./roadmap/README.md) |
| 진행 중 구현 계획 | [plans/README.md](./plans/README.md) |

각 디렉토리는 파일 목록을 두지 않는다. 목록을 적으면 그 목록이 먼저 낡는다. 무엇이
있는지는 디렉토리를 보고, 규칙은 각 `README.md` 가 규정한다.

## 코드 위치 포인터

정본은 코드다. 아래 사실은 문서에 복제하지 않는다 — 복제하면 코드가 바뀔 때 조용히
틀린 문서가 된다. 필요하면 여기 좌표를 따라가 코드를 본다.

| 주제 | 정본 |
|------|------|
| version | `src/zeta4s/__init__.py` 의 `__version__` |
| CI gate 목록 | `.github/workflows/ci.yml` |
| release gate 절차와 판정 | `scripts/check_release_runtime_showcases.sh`, `scripts/check_runtime_reliability.sh` |
| static contract 검사 항목 | `scripts/check_static_cli_contract.sh` |
| lint/format 기준 | `pyproject.toml` 의 `[tool.ruff]` |
| 내장 step type 선언 정본(`_STEP_TYPE_SPECS`), pool stage 매핑, `when.expr` 문법 | `src/zeta4s/project/step_graph.py` |
| 외부 step type 등록(`StepTypeDescriptor`, entry-point discovery) | `src/zeta4s/project/step_types.py` |
| 내장 step type 실행 매핑 | `src/zeta4s/core/step_executors.py` |
| 배포되는 Airflow DAG source | `src/zeta4s/airflow/dag_source.py` |
| pool 이름과 slot 산출 | `src/zeta4s/project/pools.py` |
| Prefect deployment, task policy, pool projection | `src/zeta4s/prefect/prefect_engine.py` |
| step checkpoint contract, commit 경계와 input resume | `src/zeta4s/metastore/contracts.py`, `src/zeta4s/runtime/checkpoints.py`, `src/zeta4s/runtime/input_checkpoints.py` |
| `z4s` CLI 명령 목록 | `src/zeta4s/cli/main.py` |
| `zeta4s-api` endpoint 목록 | `src/zeta4s/api/app.py` |
| scheduler 중립 run service와 adapter dispatch | `src/zeta4s/api/services/scheduler_runs.py` |
| scheduler native run projection | `src/zeta4s/airflow/run_adapter.py`, `src/zeta4s/prefect/run_adapter.py` |
| Airflow REST 인증·재발급·오류 변환 | `src/zeta4s/airflow/rest_client.py` |
| Airflow DAG 축 조회/수렴 | `src/zeta4s/airflow/dags.py` |
| Airflow native run REST 조회/상태 변경 | `src/zeta4s/airflow/runs.py` |
| compose service, profile, 기본 포트 | `docker-compose.yml` 과 `.env.example` |
| 이미지 구성과 빌드 | `docker/zeta4s-api/Dockerfile`, `scripts/build_images.md` |

## 문서 기준

현재 canonical project contract 는 `project.yml`, `jobs/*.yml`, SQL/dbt 파일이다. 실행 connection 은
workspace `profiles/` 에서 관리한다.
문서는 이 contract 가 정적 검증, API deploy, scheduler execution, report/evidence 로 이어지는 현재
목표 구조만 설명한다.
