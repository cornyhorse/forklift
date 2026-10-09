# ADR 0004: The gateway never touches data; the engine runs only in sandboxed worker processes

- **Status**: Proposed
- **Date**: 2026-10-09
- **Context document**: [platform design](../platform.md), §3, §6 and §8

## Context

The service necessarily works with untrusted input: uploaded files of any shape, schemas written by
users (with expressions and regular expressions), and data from external sources. The same service
holds accounts, API tokens and connection secrets. A parser bug or resource exhaustion in data
handling must not be able to reach those.

## Decision

- The **gateway** (Django: login, UI, API, Postgres, secrets) never imports the engine or pyarrow
  and never reads object contents. Uploads go from the browser or client straight to the object
  store through presigned URLs. Even "validate this schema" is a short job on a worker.
- A **worker** is a supervisor plus a child engine process per job. The supervisor (trusted code)
  stages inputs into a scratch directory through presigned GETs, starts the engine with resource
  limits and the scratch directory as its only writable path, then uploads outputs through
  presigned PUTs. The engine process holds no credentials.
- Three isolation profiles: `standard` (non-root, read-only root file system, seccomp, rlimits,
  egress limited by NetworkPolicy), `no-network` (the engine in its own empty network namespace
  where user namespaces are available) and `sandboxed` (a sandboxed runtime class or one Kubernetes
  Job per run).
- Worker tokens can only lease, heartbeat and complete jobs. The internal API listens on its own
  port, which the ingress does not route.

## Consequences

- A compromised engine process can reach only its own job's scratch files.
- Interactive actions in the UI pay one worker round trip (a few hundred milliseconds).
- Workers need scratch space for inputs and outputs; streaming directly from the store is a later
  optimisation for very large files.
- Residual risk: the gateway's signing credentials can sign reads of stored data, so a full gateway
  compromise is still serious. Splitting the credential by purpose (upload, download, delete) where
  the store supports policies limits it.
