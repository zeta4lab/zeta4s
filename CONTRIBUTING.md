# Contributing to zeta4s

Thanks for your interest in zeta4s. This guide covers how to propose a change and what a pull
request needs before it can be merged.

## Before you start

- Search [existing issues](https://github.com/zeta4lab/zeta4s/issues) first. For a non-trivial
  change, open an issue to agree on the approach before writing code.
- zeta4s has not committed to backward compatibility yet. When a structural problem is found,
  the fix targets the intended design rather than preserving old behavior.
- [AGENTS.md](AGENTS.md) holds the working rules for this repository (they apply to humans and
  coding agents alike). The detailed design and usage documents under `docs/` are in Korean;
  issues and pull requests may be written in English or Korean.

## Development setup

```bash
uv sync --locked
uv run pytest -q
```

Before pushing, run the same checks as CI:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest -q
bash scripts/check_static_cli_contract.sh
bash scripts/check_version_consistency.sh
bash scripts/check_doc_contract.sh
bash scripts/check_k3s_manifests.sh
bash scripts/check_wheel_install.sh
```

The authoritative list of checks is [`.github/workflows/ci.yml`](.github/workflows/ci.yml).
Changes to the API, scheduler adapters, or Docker assets are also verified by the Docker-based
release gate ([`.github/workflows/release-gate.yml`](.github/workflows/release-gate.yml)), which you
can run locally with `PROFILE_ID=prefect bash scripts/check_release_runtime_showcases.sh`.

## Pull requests

- Branch from `main`: `feature/*`, `fix/*`, `docs/*`, or `chore/*`.
- Use [Conventional Commit](https://www.conventionalcommits.org/) prefixes (`feat:`, `fix:`,
  `docs:`, `test:`, `chore:`).
- Keep the change focused. Update the affected documents in `docs/` in the same pull request;
  documents describe the current contract only, and history lives in Git.
- A pull request is merged after `ci` and both `release-gate` jobs pass.

## License

By contributing, you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE).
