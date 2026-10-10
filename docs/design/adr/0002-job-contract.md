# ADR 0002: One versioned job contract for every interface

- **Status**: Proposed
- **Date**: 2026-10-09
- **Context document**: [platform design](../platform.md), §4

## Context

The same cleaning run should be expressible from Python, the CLI, the REST API, MCP tools and
Airflow. Without a shared definition each interface grows its own options, defaults and result
shape, and they drift.

## Decision

- `JobSpec` (kind, input, inline schema, output, options, limits) and `JobResult` (status, counts,
  findings by rule, warnings, artifacts, error code and message) are defined once in the engine as
  plain dataclasses (`forklift.jobs`), with `to_dict()` / `from_dict()`. No new runtime dependency.
- Their JSON Schemas are generated into `contracts/` and checked in. The gateway, the worker, the
  client and the MCP tools validate against them; contract tests fail CI when a model drifts.
- `forklift.run_job(spec)` and `forklift run-job spec.json` execute a spec locally. The worker runs
  exactly that entry point.
- `spec_version` is an integer. Additive changes keep it; anything else bumps it. Workers advertise
  the versions they accept; the gateway supports the current and the previous version.
- Locations in a spec are local to whoever executes it. The service stages remote inputs before the
  engine sees the spec; library users can still use `s3://` paths directly.
- Results never contain cell values; row samples are separate, permission-checked artifacts.

## Consequences

- Airflow users get a declarative way to run the engine with or without the service.
- The engine gains a small public module that must stay backward compatible.
- A breaking change to the contract is visible (a version bump) and can be rolled out gradually.
