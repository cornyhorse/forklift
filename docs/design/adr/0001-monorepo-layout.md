# ADR 0001: Monorepo, with the engine staying at the repository root

- **Status**: Proposed
- **Date**: 2026-10-09
- **Context document**: [platform design](../platform.md), §11

## Context

Forklift is gaining a web service (gateway), workers, an MCP proxy and a client library around the
existing engine. They could live in a separate repository or in this one. Changes to the job
contract and to the engine seams the service needs (progress, cancellation, `run_job`) touch the
engine and the service at the same time.

The engine is published as `forklift-etl` from this repository's root (`pyproject.toml`,
`src/forklift/`), supports Python 3.8+ and pyarrow 16 and newer, and has a release workflow that
checks that a release tag (minus a leading `v`) equals the engine's version. The services will
need a newer Python (Django) and heavier dependencies that library users must not inherit.

## Decision

- One repository. The engine stays exactly where it is; new packages are siblings:
  `services/web`, `services/worker`, `services/mcp`, `clients/python`, plus `contracts/` and
  `deploy/{compose,helm}`.
- Every package builds, versions and publishes on its own. Tags carry a prefix per package
  (`web-v*`, `worker-v*`, `mcp-v*`, `client-v*`, `chart-v*`); the engine keeps `v*`.
- CI selects jobs with path filters. The engine keeps its version matrix; the services have their
  own Python and test jobs.
- A uv workspace (or an equivalent) installs everything for development.

## Consequences

- Contract and engine changes land atomically with the service changes that need them.
- `pip install forklift-etl` stays light; the service is `pip install forklift-web` or an image.
- The existing publish workflow fires on every published release, so it must ignore tags that do
  not start with `v` before the first non-engine release.
- Moving the engine into `packages/` later remains possible but is not needed; doing it now would
  churn every import path in the history and the release workflow for no gain.
