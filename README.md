# zeta4s

[![ci](https://github.com/zeta4lab/zeta4s/actions/workflows/ci.yml/badge.svg)](https://github.com/zeta4lab/zeta4s/actions/workflows/ci.yml)
[![release-gate](https://github.com/zeta4lab/zeta4s/actions/workflows/release-gate.yml/badge.svg)](https://github.com/zeta4lab/zeta4s/actions/workflows/release-gate.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

zeta4s is a general-purpose runtime engine that executes contracts — typically written by an AI
agent — as a **Step Graph**. A project is a set of plain files (`project.yml`, `jobs/*.yml`,
SQL/dbt). zeta4s validates them statically, normalizes them into a scheduler-neutral execution plan,
deploys them to Airflow or Prefect, runs them, and records every run in its own metastore.

- **One contract, every stage** — the same Step Graph is checked, deployed, executed, and observed.
- **Scheduler-neutral** — Airflow and Prefect are interchangeable backends selected per profile.
  Step execution and credential resolution always happen inside `zeta4s-api`.
- **Built-in data steps** — extract, stage, write, SQL, dbt, Elasticsearch, and HTTP lookup steps
  for ClickHouse, Oracle, and Elasticsearch. External step types install as Python plugins.
- **Recoverable runs** — scheduler runs keep step-local checkpoints in Iceberg and resume inside
  the same run after a backend failure.

zeta4s ships two distributions: `zeta4s-cli` (the host `z4s` CLI) and `zeta4s-api` (the API server
and scheduler adapters, shipped as the `zeta4s-api` container image).

> Version tags mark release units; backward compatibility is not guaranteed yet.

## Requirements

| For | You need |
|---|---|
| CLI and local runs | Linux or macOS, `git`, [`uv`](https://docs.astral.sh/uv/) (it provisions Python 3.12) |
| Minimal Docker stack (Prefect) | Docker Engine with Compose v2.20 or later, 2 GB free RAM, 2 GB of disk for images |
| Full Docker stack (Airflow, Prefect, Oracle, Elasticsearch, ClickHouse) | 8 GB free RAM, 6 GB of disk for images |

## Quickstart

### 1. Install the CLI

```bash
git clone https://github.com/zeta4lab/zeta4s.git
cd zeta4s
bash scripts/install_cli.sh
export PATH="$PWD/.venv/bin:$PATH"
z4s --help
```

### 2. Create a workspace and your first job

A workspace holds projects and execution profiles. Create it outside the zeta4s checkout.

```bash
mkdir -p ~/zeta4s-demo && cd ~/zeta4s-demo
z4s work init
z4s profile init dev
z4s project init hello

cat > zeta4s-work/projects/hello/jobs/hello.yml <<'EOF'
job_id: hello
steps:
  - step_id: start
    type: noop
EOF

z4s project check hello --profile dev
```

### 3. Run it locally (no Docker)

```bash
z4s run hello hello --profile dev
```

`z4s run` executes the Step Graph on your machine and writes a report under `reports/hello/`.

### 4. Run it on a scheduler

Start the minimal stack from the zeta4s checkout: PostgreSQL, `zeta4s-api`, Prefect, and the Iceberg
checkpoint store.

```bash
cd /path/to/zeta4s
bash scripts/configure_open_env.sh --force
bash scripts/build_images.sh --load
docker compose --env-file .env --profile prefect --profile checkpoint up -d --wait zeta4s-api prefect-worker
```

`--wait` names the long-running services; one-shot setup containers (database migration, bucket and
warehouse creation) finish before `zeta4s-api` starts. To skip the local build, set `ZETA4S_API_IMAGE=ghcr.io/zeta4lab/zeta4s-api:<version>` in `.env`
before `docker compose up`.

Connect the CLI to the API, then deploy and run the job from the workspace:

```bash
z4s api connect local --url http://127.0.0.1:18088
z4s api bootstrap --api local

cd ~/zeta4s-demo
z4s api deploy hello --profile dev
z4s api run create hello hello
z4s api run status hello hello
```

The run reaches `state: succeeded` within a few seconds. `z4s api run tasks`, `logs`, and `summary`
show more detail. Once a job has more than one run, pass `--run-id <run_id>` (printed by
`run create`) to `run status`.

The `dev` profile uses Prefect. To use Airflow instead, set `scheduler: airflow` in the profile file
(`profiles/dev.yml` inside your workspace), start Airflow, and deploy again:

```bash
docker compose --env-file .env --profile airflow --profile prefect --profile checkpoint \
  up -d --wait zeta4s-api airflow-apiserver airflow-scheduler airflow-dag-processor
```

### 5. Clean up

```bash
cd /path/to/zeta4s
docker compose --env-file .env --profile airflow --profile prefect --profile asset --profile checkpoint down -v
```

## Full stack and showcase

The full local stack adds Airflow, Oracle, Elasticsearch, and ClickHouse for the data steps:

```bash
docker compose --env-file .env --profile airflow --profile prefect --profile asset --profile checkpoint \
  up -d --wait zeta4s-api prefect-worker airflow-apiserver airflow-scheduler airflow-dag-processor \
  oracle elasticsearch metastore
```

`zeta4s-work/projects/canonical_showcase` exercises every built-in step type against that stack.
The release gate runs it end to end on an isolated Compose project, including secret registration
and checkpoint recovery:

```bash
PROFILE_ID=prefect bash scripts/check_release_runtime_showcases.sh   # or PROFILE_ID=airflow
```

## Install from a release

Every version tag publishes wheels to [GitHub Releases](https://github.com/zeta4lab/zeta4s/releases)
and the `zeta4s-api` image to `ghcr.io/zeta4lab/zeta4s-api:<version>` (linux/amd64, linux/arm64).
The packages are not on PyPI; install the CLI from both release wheels:

```bash
V=1.0.22
R=https://github.com/zeta4lab/zeta4s/releases/download/$V
bash scripts/install_cli.sh --package "$R/zeta4s-$V-py3-none-any.whl" --package "$R/zeta4s_cli-$V-py3-none-any.whl"
```

## Documentation

The detailed documentation under [`docs/`](docs/README.md) is written in Korean.

| Topic | Start here |
|---|---|
| Writing projects, jobs, and profiles; the `z4s` CLI | [docs/usage](docs/usage/README.md) |
| Built-in step types | [docs/usage/step-types](docs/usage/step-types) |
| Architecture and design | [docs/design](docs/design/README.md) |
| Verification gates and CI | [docs/gate](docs/gate/README.md) |
| Single-node k3s deployment | [deploy/k3s](deploy/k3s/README.md) |
| What is done and what is next | [roadmap](docs/roadmap/00-step-graph-runtime-roadmap.md) |

## Contributing

Contributions are welcome. Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request.
Report security issues privately as described in [SECURITY.md](SECURITY.md).

## License

zeta4s is licensed under the [Apache License 2.0](LICENSE). See [NOTICE](NOTICE) for attribution.
