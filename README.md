# zeta4s

zeta4s 는 AI Agent 가 생성한 계약(Contract)을 Step Graph 로 실행하는 범용 Runtime Engine 이다.
1.0.0 은 이 Runtime Engine 의 contract authoring, validation, deploy, execution boundary 를
고정한 첫 내부 구현 기준선이다. Roadmap version tag 는 구현 묶음의 표식이며 정식 공개
릴리즈나 하위호환 보장을 뜻하지 않는다.

현재 canonical project contract 는 `project.yml`, `jobs/*.yml`, SQL/dbt 파일이다.
실행 connection 은 workspace `profiles/` 에서 관리한다.
기본 확장자는 `.yml` 이며, 수동 작성 파일의 `.yaml` 도 허용한다.
실행은 host 환경의 `z4s` CLI 와 Docker stack 안의 `zeta4s-api`/Airflow/Prefect 로
검증한다.
Airflow와 Prefect server는 공식 image를 사용하며 zeta4s가 빌드하는 runtime image는
`zeta4s-api` 하나다.
repository 표준 Python 도구는 `uv` 이며, `uv.lock` 을 dependency lock contract 로 추적한다.
기본 metastore는 PostgreSQL 18의 `zeta4s_metastore` database다. 같은 PostgreSQL service에서
Airflow와 Prefect metadata database를 분리하며, ClickHouse는 `asset` profile의 runtime data
backend 또는 명시적 `ZETA4S_METASTORE_TYPE=clickhouse` 선택 backend로만 사용한다.
`rowset`은 authoring 단계에서 물리 format을 선택하지 않는 step 간 data contract다. `z4s run` 검증은
ephemeral Parquet를 사용하고 Airflow/Prefect 실행은 Iceberg snapshot과 step-local checkpoint를
사용한다. Iceberg catalog/object storage 설정은 platform deployment가 소유하며 profile에 쓰지 않는다.

설치본은 두 개의 distribution 으로 구분한다.

- `zeta4s-cli`: host `z4s` CLI
- `zeta4s-api`: API server 와 Airflow/backend adapter layer

## Quickstart

workspace 를 만든다.

```bash
bash scripts/install_cli.sh
.venv/bin/z4s --help
.venv/bin/z4s work init
```

새 프로젝트 skeleton 을 만든다.

```bash
.venv/bin/z4s project init my_project
```

정적 contract 를 확인한다.

```bash
.venv/bin/z4s project check my_project --profile dev
```

로컬 Docker stack 을 기동한다.

```bash
bash scripts/configure_open_env.sh --force
docker compose --env-file .env --profile airflow --profile prefect --profile asset --profile checkpoint up -d
.venv/bin/z4s api connect local --url http://127.0.0.1:18088
.venv/bin/z4s api bootstrap --api local
.venv/bin/z4s api status --api local
```

프로젝트를 API 에 배포한다.

```bash
.venv/bin/z4s api deploy my_project --profile dev --api local
```

`jobs/*.yml` 의 schedule 배포도 이 명령에 포함되며 profile의 `scheduler`가 Airflow 또는
Prefect를 선택한다. 별도 `z4s schedule` 명령 그룹은 제공하지 않는다.

`z4s api` 명령은 장시간 작업의 주요 단계를 timestamp, step index, step name, status 로 출력한다.

## Adapter Contract

- AI Agent 또는 사용자는 project contract 를 생성하고, zeta4s 는 이를 Step Graph 로 정규화해 실행한다.
- zeta4s 의 핵심 책임은 contract 를 검증 가능한 execution contract 로 바꾸고 같은 contract 로
  배포, 실행, 관측하는 것이다.
- Metastore 는 `zeta4s-api` 가 소유한다.
- 운영 scheduler backend 는 Airflow와 Prefect이며, project 배포에 사용한 backend를 metastore에
  저장해 배포 해제에도 같은 backend를 사용한다.
- Data plane 은 project step graph 와 workspace profile connection 으로 결정한다.
- 지원 step adapter 는 `noop`, `oracle.extract`, `clickhouse.extract`, `elasticsearch.extract`,
  `clickhouse.stage`, `oracle.stage`, `http.lookup`, `dbt.run`, `dbt.test`, `sql.scalar`,
  `clickhouse.write`, `oracle.write`, `elasticsearch.write`, `elasticsearch.command`, `sql.check`,
  `oracle.sql`, `clickhouse.sql`, `oracle.call` 이다.
- 검증은 installed package Docker stack 기준으로 수행한다.

## Docker Volume Reset

adapter contract, Airflow DAG registration, project artifact, connection/pool 상태를 재검증하려면
Compose project volume 을 함께 제거한다.

```bash
docker compose --env-file .env --profile airflow --profile prefect --profile asset --profile checkpoint down -v --remove-orphans
docker compose --env-file .env --profile airflow --profile prefect --profile asset --profile checkpoint up -d
```

release gate 는 별도 Compose project name 과 port 를 사용한다. 검증 대상 project 와 job matrix 는
workspace showcase 계약으로 지정한다.

## Roadmap

현재 기준 계획 문서는 아래 하나다.

- [Step Graph Runtime Roadmap](docs/roadmap/00-step-graph-runtime-roadmap.md)

## License

zeta4s 는 [Apache License 2.0](LICENSE) 으로 배포한다. 저작권 고지는 [NOTICE](NOTICE) 에 있다.
