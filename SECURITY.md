# Security Policy

## Reporting a vulnerability

Please do not report security vulnerabilities through public issues, pull requests, or
discussions.

Report them privately through GitHub:
[Security → Report a vulnerability](https://github.com/zeta4lab/zeta4s/security/advisories/new).

Include what you can of the following:

- the affected component (`zeta4s-api`, `z4s` CLI, Docker or k3s deployment assets) and version
- steps to reproduce, or a proof of concept
- the impact you expect

You will receive an acknowledgement within 5 business days. We will keep you informed while we
investigate and credit you in the advisory unless you prefer otherwise.

## Supported versions

zeta4s has not committed to backward compatibility yet. Security fixes land on `main` and ship in
the next version tag; older tags are not patched.

## Deployment notes

- Set `ZETA4S_API_TOKEN` on any host reachable by others. When it is empty, the public API does not
  require authentication.
- The internal runtime endpoint (`/internal/v1/...`) is for scheduler workers only. Do not expose it
  outside the cluster or Compose network.
- The local Compose stack uses development credentials from `.env.example`. Do not use them in
  production.
