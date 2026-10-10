# ADR 0003: Workers pull jobs through a lease API; no message broker to start with

- **Status**: Proposed
- **Date**: 2026-10-09
- **Context document**: [platform design](../platform.md), §5.3 and §6

## Context

Jobs (imports, previews, schema validation and generation) must run outside the gateway. The usual
Django answer is Celery or RQ with Redis or RabbitMQ. For a single organisation the volume is
modest, and every extra stateful service is something more to deploy, secure and back up, in both
Docker Compose and Helm.

## Decision

- Jobs are rows in a Postgres table owned by the gateway. A worker asks the gateway's internal API
  for a lease (`POST /internal/v1/leases` with its lanes and accepted spec versions); the gateway
  picks a job with `SELECT ... FOR UPDATE SKIP LOCKED` and returns the signed spec and the presigned
  URLs for that job.
- Leases are short and extended by heartbeats, which also carry progress and receive cancellation.
  An expired lease returns the job to the queue until its attempts are used up.
- Workers make outbound calls only. They have no inbound ports and no database access.
- Lanes (`interactive`, `batch`, later `sql`) let small, fast jobs and large ones use different
  worker pools and limits.

## Consequences

- No broker to run; Compose stays at gateway, worker, Postgres and an object store.
- Workers can run anywhere that can reach the gateway's internal port, including another network or
  cluster.
- Queue throughput is bounded by Postgres; that is ample for one organisation and can be revisited
  behind the same lease API (a broker could feed the lease endpoint) without changing workers.
- Scheduling recurring jobs needs a small scheduler loop in the gateway (with a database lock so
  only one replica fires a schedule).
