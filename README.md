# zeta4s

[![ci](https://github.com/zeta4lab/zeta4s/actions/workflows/ci.yml/badge.svg)](https://github.com/zeta4lab/zeta4s/actions/workflows/ci.yml)
[![release-gate](https://github.com/zeta4lab/zeta4s/actions/workflows/release-gate.yml/badge.svg)](https://github.com/zeta4lab/zeta4s/actions/workflows/release-gate.yml)

zeta4s 는 AI Agent 가 생성한 계약(Contract)을 Step Graph 로 실행하는 범용 Runtime Engine 이다.
version tag 는 구현 묶음의 표식이며 하위호환 보장을 뜻하지 않는다.

현재 canonical project contract 는 `project.yml`, `jobs/*.yml`, SQL/dbt 파일이다.
실행 connection 은 workspace `profiles/` 에서 관리한다.
project manifest 는 `project.yml` 이다. job 파일과 profile 은 `.yml` 이 기본이고 `.yaml` 도 허용한다.
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

배포물은 version tag 마다 공개된다. wheel 과 sdist 는
[GitHub Releases](https://github.com/zeta4lab/zeta4s/releases) 에, `zeta4s-api` image 는
`ghcr.io/zeta4lab/zeta4s-api:<version>` (linux/amd64, linux/arm64) 에 있다.

## Quickstart

workspace 를 만든다. `install_cli.sh` 는 기본으로 현재 checkout 을 설치하고, `--package` 로
Release 의 wheel 을 지정할 수 있다.

```bash
bash scripts/install_cli.sh
.venv/bin/z4s --help
.venv/bin/z4s work init
.venv/bin/z4s profile init dev
```

새 프로젝트 skeleton 을 만든다.

```bash
.venv/bin/z4s project init my_project
```

정적 contract 를 확인한다.

```bash
.venv/bin/z4s project check my_project --profile dev
```

로컬 Docker stack 을 기동한다. `configure_open_env.sh` 는 `ZETA4S_API_IMAGE` 를 local build tag 로
채우므로 image 를 먼저 빌드한다. 공개 image 를 쓰려면 `.env` 의 `ZETA4S_API_IMAGE` 를
`ghcr.io/zeta4lab/zeta4s-api:<version>` 으로 바꾸고 build 단계를 건너뛴다.

```bash
bash scripts/configure_open_env.sh --force
bash scripts/build_images.sh --load
docker compose --env-file .env --profile airflow --profile prefect --profile asset --profile checkpoint up -d
.venv/bin/z4s api connect local --url http://127.0.0.1:18088
.venv/bin/z4s api bootstrap --api local
.venv/bin/z4s api status --api local
```

프로젝트를 API 에 배포한다.

```bash
.venv/bin/z4s api deploy my_project --profile dev
```

`jobs/*.yml` 의 schedule 배포도 이 명령에 포함되며 profile의 `scheduler`가 Airflow 또는
Prefect를 선택한다. 별도 `z4s schedule` 명령 그룹은 제공하지 않는다. 배포 대상 API 는 profile 의
`api_endpoint` 가 있으면 그것을, 없으면 `z4s api connect` 로 저장한 기본 API 를 쓴다.

`z4s api` 명령은 장시간 작업의 주요 단계를 timestamp, step index, step name, status 로 출력한다.

## Adapter Contract

- AI Agent 또는 사용자는 project contract 를 생성하고, zeta4s 는 이를 Step Graph 로 정규화해 실행한다.
- zeta4s 의 핵심 책임은 contract 를 검증 가능한 execution contract 로 바꾸고 같은 contract 로
  배포, 실행, 관측하는 것이다.
- Metastore 는 `zeta4s-api` 가 소유한다.
- 운영 scheduler backend 는 Airflow와 Prefect이며, project 배포에 사용한 backend를 metastore에
  저장해 배포 해제에도 같은 backend를 사용한다.
- Data plane 은 project step graph 와 workspace profile connection 으로 결정한다.
- 내장 step type 목록의 정본은 `src/zeta4s/project/step_graph.py` 의 `_STEP_TYPE_SPECS` 다.
  step type 별 사용법은 [docs/usage/step-types](docs/usage/step-types) 에 있다.
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

## 문서

사용법, 설계, 검증 gate, roadmap 의 진입점은 [docs/README.md](docs/README.md) 다. 완성된 범위와 남은
목표는 [Step Graph Runtime Roadmap](docs/roadmap/00-step-graph-runtime-roadmap.md) 에 있다.

## 기여

변경은 `main` 으로 향하는 pull request 로 반영한다. pull request 는 `ci` 와 Docker stack
`release-gate` workflow 를 통과해야 한다. 작업 원칙은 [AGENTS.md](AGENTS.md) 에 있다.

## License

zeta4s 는 [Apache License 2.0](LICENSE) 으로 배포한다. 저작권 고지는 [NOTICE](NOTICE) 에 있다.
