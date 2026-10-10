# Forklift platform: library, CLI, web, API and MCP

| | |
|---|---|
| **Status** | Proposed |
| **Date** | 2026-10-09 |
| **Decisions** | [ADR 0001](adr/0001-monorepo-layout.md) monorepo layout, [ADR 0002](adr/0002-job-contract.md) job contract, [ADR 0003](adr/0003-pull-lease-workers.md) pull-lease workers, [ADR 0004](adr/0004-trust-boundary.md) trust boundary, [ADR 0005](adr/0005-storage-and-destinations.md) storage and destinations, [ADR 0006](adr/0006-streaming-large-inputs.md) streaming large inputs, [ADR 0007](adr/0007-database-sources-and-targets.md) database sources and targets |

## 1. Summary

Forklift today is a Python library and a CLI that turn CSV, Excel, fixed-width and SQL sources into
validated Parquet. This document proposes how the same engine becomes reachable four ways, without
the engine depending on any of them:

1. **Python library** (`pip install forklift-etl`), as today.
2. **CLI** (`forklift ...`), as today, plus `forklift run-job spec.json`.
3. **Web service**: a browser UI where people drop files, build schemas and watch them get cleaned,
   and where admins connect buckets and other sources; a REST API for pipelines (Airflow, scripts).
4. **MCP**: tools an agent can call to inspect, validate and clean data, either locally (stdio, over
   the library) or remotely (over the web service's API).

The design rests on three ideas:

- **One contract.** A versioned `JobSpec` (what to read, which schema, where to write, options) and
  `JobResult` (counts, findings, warnings, artifacts, error) describe a cleaning run for every
  interface. The library runs it in-process; the service runs it on a worker.
- **One trust boundary.** The process that logs people in (the *gateway*) never reads uploaded data
  and never imports the engine. All engine code, which by its nature parses untrusted files and runs
  user-written schemas, runs in *workers* that have no inbound ports, no database access and no
  long-lived credentials.
- **Few moving parts.** Three deployables (gateway, worker, optional MCP proxy), Postgres and an
  S3-compatible object store. No message broker to start with: workers pull work from the gateway.

### Decisions already taken

| Topic | Decision |
|---|---|
| Repository | Monorepo; the web service is a separate pip package / image |
| Tenancy | Single organisation |
| Deployment | Docker Compose and a Helm chart |
| Processing isolation | Data processing runs in separate worker processes/services, away from the login-facing app |
| Storage | "S3" means *S3-compatible*: cloud-agnostic, cloud-friendly (RustFS, Ceph, R2, AWS S3, MinIO, ...) |
| Bundled object store | RustFS (Apache-2.0) in Docker Compose, the Helm chart's optional in-cluster store and the integration tests; it replaces MinIO |
| Sensitivity | Varies: anything from public data to PII; the design has to handle both |
| Authentication | Local accounts plus API tokens; SSO (OIDC) later |
| Destinations | Parquet on S3-compatible storage and local file systems, and tables in PostgreSQL, MySQL, SQL Server and Oracle ([ADR 0007](adr/0007-database-sources-and-targets.md)); Snowflake, Databricks and BigQuery next |
| First service release | The MVP of milestone M3 with workers: upload, schema, run, download; staged and streamed inputs; the four roles; purpose-built admin screens instead of the Django admin |
| Package names | `forklift-web`, `forklift-worker`, `forklift-client`, `forklift-mcp` (beside `forklift-etl`) |
| API framework | Django Ninja |
| Input sizes | Plan for single inputs larger than 10 GB: inputs above `stageMaxBytes` (default 2 GiB) are streamed to the engine through presigned URLs ([ADR 0006](adr/0006-streaming-large-inputs.md)) |
| Retention | Configured by admins (installation, classification and dataset level); no fixed defaults built in |
| Registry | GHCR for images; the Helm chart as an OCI artifact on GHCR |

### Non-goals (for now)

- Multi-tenancy (several organisations isolated from each other in one installation).
- A general workflow/orchestration engine. Forklift runs cleaning jobs and schedules simple
  recurring ones; Airflow and friends orchestrate.
- Cloud warehouses (Snowflake, Databricks, BigQuery) as sources or targets: next, after the
  relational databases ([ADR 0007](adr/0007-database-sources-and-targets.md)).
- Masking/anonymisation (`x-pii` stays documentation-only until it is designed separately).

## 2. Who uses it, and how

| Persona | Typical task | Interface |
|---|---|---|
| Analyst | Drop a CSV, see what is wrong with it, fix the schema, download clean Parquet and the rejected rows | Web UI |
| Data engineer | Define a dataset (source + schema + destination), run it on a schedule or from Airflow | Web UI, REST API, client library |
| Admin | Connect buckets and databases, manage users and tokens, set retention | Web UI (admin) |
| Agent | "Clean this file and put it in the bucket", "why did this load fail?", "draft a schema for this export" | MCP (local or remote) |
| Developer | Use the engine directly in Python or a notebook | Library, CLI |

The web UI's core loop: **upload → preview → schema (generate, edit, validate live) → run → results**
(counts, findings by rule, warnings, a sample of the output and of `bad_rows.parquet`, downloads).

## 3. Architecture

```
                     internet / office network
                                │
                           ┌────▼────┐
                           │ ingress │ TLS
                           └────┬────┘
                                │ public port: UI, /api/v1, (optional) remote MCP
   ┌────────────────────────────▼─────────────────┐       ┌────────────┐
   │ gateway (Django)                             │──────►│  Postgres  │
   │   accounts, tokens, roles, audit             │       └────────────┘
   │   schemas, connections, datasets, schedules  │
   │   job queue (Postgres), presigned URLs       │   never imports pyarrow or the engine
   │   internal port /internal/v1 (workers only)  │   never reads data bytes
   └────────────────────▲─────────────────────────┘
                        │ outbound only: lease, heartbeat, complete
   ┌────────────────────┴─────── worker (×N) ─────┐       ┌────────────────────────────┐
   │ supervisor (trusted code)                    │◄─────►│ S3-compatible store        │
   │   lease → stage small inputs to scratch, or  │ PUT/  │   uploads/  jobs/  outputs │
   │   pass large ones on as presigned URLs →     │ GET   │   (presigned URLs only)    │
   │   start engine → upload outputs → report     │       └─────────────▲──────────────┘
   │  ┌────────────────────────────────────────┐  │                     │
   │  │ engine process (forklift-etl)          │  │  streamed inputs:   │
   │  │   one job, rlimits, scratch dir,       │──┼─────────────────────┘
   │  │   no credentials; network: the store   │  │  range GETs on its own URLs
   │  │   only, none when inputs are staged    │  │       ┌────────────────────────────┐
   │  └────────────────────────────────────────┘◄─┼──────►│ mounted volumes (localfs)  │
   └──────────────────────────────────────────────┘       └────────────────────────────┘

   mcp (optional): a client of /api/v1 with a token; no database, no storage credentials
```

### 3.1 Components

| Component | Package / image | Responsibilities | Holds |
|---|---|---|---|
| **Engine** | `forklift-etl` (this repo's root) | Reading, typing, transformations, validation, constraints, row hash, Parquet output; `run_job(spec)` | Nothing; stateless library |
| **Gateway** | `forklift-web` / image `forklift-web` | UI, REST API, authentication and authorisation, schema registry, connections, datasets, schedules, job queue, presigning, retention, audit | Postgres, storage credentials (signing), connection secrets (encrypted) |
| **Worker** | `forklift-worker` / image `forklift-worker` | Lease jobs, stage inputs, run the engine in a child process, upload outputs, report progress and results | Its bootstrap token only |
| **MCP proxy** | image `forklift-mcp` (optional) | Remote MCP endpoint that translates tool calls into `/api/v1` calls | An API token per session |
| **Client** | `forklift-client` | Typed Python client for `/api/v1`; Airflow operator and sensor | Caller's token |

The worker image contains no Django; the gateway image contains no pyarrow and no engine. That split
is what makes the attack-surface argument real rather than a convention.

### 3.2 Job kinds and lanes

| Kind | What it does | Lane |
|---|---|---|
| `run` | A full import (`import_csv` today; Excel/fixed-width/SQL as they mature) | `batch` |
| `preview` | First *n* rows of a source or of an output, typed or raw, bounded in rows and bytes | `interactive` |
| `validate_schema` | Check a schema against a sample's header (and optionally rows): the engine's pre-write checks and warnings | `interactive` |
| `generate_schema` | Infer a schema from a sample | `interactive` |

Workers subscribe to lanes. The `interactive` lane has a small, always-warm pool with tight limits
(seconds, megabytes) so the UI's live validation stays responsive; `batch` workers have large limits
and scale with the queue. Because even "validate this schema" runs engine code, it goes through a
worker too. The cost is a round trip of a few hundred milliseconds; the benefit is that the gateway
never executes schema expressions, regular expressions or file parsers.

## 4. The job contract

`JobSpec` and `JobResult` are defined once, in the engine (`forklift.jobs`, plain dataclasses with
`to_dict()` / `from_dict()`), and published as JSON Schema under `contracts/`. Every other component
validates against those schemas; contract tests in CI fail if a model drifts.
See [ADR 0002](adr/0002-job-contract.md).

```jsonc
// JobSpec, spec_version 1 (illustrative; the JSON Schema is normative)
{
  "spec_version": 1,
  "job_id": "01JB2...",             // assigned by the gateway; any unique id for local runs
  "kind": "run",                    // run | preview | validate_schema | generate_schema
  "input": {
    "format": "csv",                // csv | excel | fwf | sql
    "location": {"type": "file", "path": "in/people.csv"},   // staged into scratch, or:
    // {"type": "presigned_url", "url": "https://store.example.org/...", "size": 53687091200,
    //  "etag": "..."}  for a streamed input (ADR 0006)
    "options": {"encoding": "utf-8", "delimiter": ",", "header_mode": "present"}
  },
  "schema": { "...": "inline JSON schema, never a path on the server" },
  "output": {"location": {"type": "file", "path": "out/"}, "compression": "snappy"},
  "options": {"apply_schema_extensions": true, "batch_size": 10000},
  "limits": {"max_input_bytes": 10737418240, "max_seconds": 3600}
}
```

```jsonc
// JobResult
{
  "spec_version": 1,
  "job_id": "01JB2...",
  "status": "succeeded",           // succeeded | failed | cancelled
  "counts": {"total_rows": 41, "valid_rows": 40, "invalid_rows": 1, "truncated_rows": 0},
  "schema_extensions": ["x-transformations", "x-primaryKey/x-uniqueConstraints/constraints"],
  "validation_summary": {"UNIQUE_VIOLATION:id": 1},
  "warnings": ["x-pii is documentation only: no masking is applied"],
  "artifacts": [
    {"kind": "data", "path": "out/data.parquet", "rows": 40, "bytes": 18211, "sha256": "..."},
    {"kind": "bad_rows", "path": "out/bad_rows.parquet", "rows": 1},
    {"kind": "manifest", "path": "out/manifest.json"}
  ],
  "error": null                    // or {"code": "BAD_ROWS_THRESHOLD_EXCEEDED", "message": "...", "retryable": false}
}
```

Rules:

- **Locations never carry credentials.** In the service a location is a local path (an input the
  supervisor staged into scratch, or an output the engine writes there) or a presigned URL issued
  for this job and this object only (a streamed input, §6.1). The gateway never hands a worker a
  bucket path to read with credentials of its own. A library user can still point a spec at
  `s3://` paths, which the engine reads with the caller's credentials.
- **No cell values in results.** Counts, codes and column names only, as the engine already does for
  `validation_summary`, `_rejection_reason` and error messages. Row samples (previews) are a
  separate, permission-checked artifact.
- **Stable error codes.** Every failure maps to a code (`SCHEMA_INVALID`, `INPUT_UNREADABLE`,
  `ENCODING_ERROR`, `COLUMN_MISSING`, `BAD_ROWS_THRESHOLD_EXCEEDED`, `CONSTRAINT_VIOLATION`,
  `LIMIT_EXCEEDED`, `PERMISSION_DENIED`, `TARGET_WRITE_FAILED`, `SPEC_INVALID`, `CANCELLED`,
  `INTERNAL`) plus the engine's verbose message. A threshold failure still lists the kept
  `bad_rows` artifact.
- **As built (v1):** `limits` also has `max_rows` (previews); a `sql_table` output has `mode`,
  `key_columns` and `staging` (`table` or `none`), and `output.artifacts` names the `file`
  directory that keeps the validated Parquet and `bad_rows` of a table load. The JSON Schemas in
  `contracts/` are normative.
- **Versioning.** `spec_version` is an integer. Workers advertise the versions they accept when they
  lease; the gateway only hands out jobs a worker understands. Additive changes keep the version;
  anything else bumps it, and the gateway supports N and N-1.

The same spec runs locally: `forklift run-job spec.json` and `forklift.run_job(spec)` return a
`JobResult`. That gives Airflow and scripts a declarative way to run the engine without the service,
and it is the exact code path the worker uses.

## 5. Gateway

### 5.1 Data model (first cut)

| Model | Key fields | Notes |
|---|---|---|
| `User`, `Group` | Django auth | Local accounts; OIDC later through the same models |
| `ApiToken` | owner (user or service account), prefix, hash, scopes, expires_at, last_used_at | Shown once at creation; stored as a hash |
| `Connection` | kind (`s3`, `localfs`, `sql`), name, config (endpoint, bucket, prefix / root path / DSN parts), `secret_ref`, allowed roles | Secrets live in the secret backend, never in `config` |
| `Schema`, `SchemaVersion` | name; version: JSON document, sha256, author, created_at, notes | Versions are immutable; editing creates a new version |
| `Dataset` | name, classification (`public`, `internal`, `sensitive`), source (connection + path or pattern), schema version, destination (connection + prefix), retention override | The unit people schedule and permission |
| `Upload` | object key, size, sha256, uploader, expires_at | Created before the presigned PUT, finalised after |
| `Job` | kind, lane, status, spec (JSON), result (JSON), dataset?, requested_by, attempt, lease (worker, expires_at), timestamps | State machine in §5.3 |
| `JobEvent` | job, time, type (`progress`, `log`, `state`), payload | Progress and logs without cell values |
| `Artifact` | job, kind (`data`, `bad_rows`, `manifest`, `metadata`, `preview`), object key, rows, bytes, sha256, expires_at | Downloads go through permission checks and presigned GETs |
| `RetentionPolicy` | scope (installation, classification, dataset), days per kind (uploads, data, bad_rows, previews, metadata, job_records; null = keep until deleted) | Set by admins (§5.7) |
| `Schedule` | dataset, cron expression, timezone, enabled, next_run_at | A small scheduler loop in the gateway enqueues jobs (not built yet) |
| `AuditLog` | actor, action, object, time, request id, ip | Downloads of sensitive artifacts, connection changes, token use |
| `Worker` | id, lanes, versions, last_seen | For the admin view and lease bookkeeping |
| `WorkerToken` | name, prefix, hash, expires_at, revoked_at | Created by admins; accepted only by `/internal/v1` |
| `InstallationSetting` | key, value | Admin-set values with defaults in code (stage_max_bytes, lease_seconds, max_attempts, lane limits, URL lifetimes) |

### 5.2 Roles

| Role | Can |
|---|---|
| Viewer | See datasets, jobs and results; download artifacts of `public` and `internal` datasets |
| Operator | Viewer + upload, run jobs, preview |
| Author | Operator + create and edit schemas and datasets |
| Admin | Everything, including connections, users, tokens, retention and audit |

Sensitive datasets add one permission, **view raw rows**, required to preview data or download
`bad_rows` / outputs. It applies to every role, admins included: an admin can grant it to
themselves, and that is audited. Tokens carry scopes that can only narrow their owner's role
(a token's effective scopes are its own scopes intersected with the role's):

| Role | Scopes (each role includes the one above) |
|---|---|
| Viewer | `schemas:read`, `datasets:read`, `connections:read`, `jobs:read`, `artifacts:read`, `tokens:read`, `tokens:write` |
| Operator | + `uploads:read`, `uploads:write`, `jobs:run` |
| Author | + `schemas:write`, `datasets:write` |
| Admin | + `admin:read`, `admin:write` |

Uploads are used by their uploader and jobs are cancelled by their requester (admins may do both);
connections are used by the roles they allow.

### 5.3 Job lifecycle

```
            enqueue                lease                 complete(succeeded)
  (API/UI/   ──────►  queued  ───────────►  running  ──────────────────────►  succeeded
  schedule)             ▲                    │  │  │
                        │ lease expired,     │  │  └── complete(failed) ─────►  failed
                        │ attempts left      │  └───── cancel requested ──────►  cancelled
                        └────────────────────┘
                     (heartbeat missing: lease returns to the queue; after max attempts → failed)
```

- The queue is a Postgres table; `SELECT ... FOR UPDATE SKIP LOCKED` hands each job to one worker.
- A lease lasts a short time (for example 60 s) and is extended by heartbeats, which also carry
  progress (rows read, rows rejected, bytes). Cancellation is answered on the next heartbeat.
- Jobs are idempotent per attempt: outputs go to `jobs/<job>/attempt-<n>/`, and only a successful
  attempt is published (§6.3). Retrying never mixes two attempts' files.
- A lease is (worker token, attempt); calls about a job the worker no longer holds answer `409`.
  Lease calls requeue expired leases; on the last attempt the job fails with `LEASE_EXPIRED`, or
  is cancelled if cancellation was requested.

### 5.4 APIs

**Public** (`/api/v1`, built with Django Ninja; session or token auth; OpenAPI generated and checked in under `contracts/`):

| Endpoint | Purpose |
|---|---|
| `POST /uploads` → presigned PUT (multipart for large files); `POST /uploads/{id}/complete` | Put data into the store without passing it through the gateway |
| `GET/POST /schemas`, `GET/POST /schemas/{id}/versions` | Schema registry |
| `POST /schemas/validate` | Enqueue an interactive `validate_schema` job and wait briefly for it |
| `GET/POST /connections` (admin), `POST /connections/{id}/test` | Sources and destinations |
| `GET/POST /datasets`, `POST /datasets/{id}/run` | Datasets |
| `POST /jobs` (with `Idempotency-Key`), `GET /jobs/{id}`, `GET /jobs/{id}/events`, `POST /jobs/{id}/cancel` | Jobs |
| `GET /jobs/{id}/artifacts`, `GET /artifacts/{id}/download` | Results; downloads are presigned and audited |
| `GET/POST /tokens` | API tokens |
| Webhooks (per dataset or token) | Job finished / failed, signed with HMAC |

**Internal** (`/internal/v1`, separate port, not routed by the ingress, worker tokens only):
`POST /leases` (lanes, accepted spec versions → a job or 204), `POST /jobs/{id}/heartbeat`,
`POST /jobs/{id}/complete`, `POST /jobs/{id}/presign` (more upload URLs for outputs),
`POST /jobs/{id}/input-url` (a fresh presigned URL for a streamed input). One gateway process
serves both ports and routes each request by the local port its connection arrived on, never by
a header. The admin endpoints (`/api/v1/admin/...`: users and roles, tokens, worker tokens,
workers, retention, audit log, settings) are part of the public API and need the Admin role.

### 5.5 UI

Server-rendered Django templates with HTMX for interactivity: few moving parts, nothing to build
for most pages, easy to keep accessible. The schema editor is the exception: a JSON editor
(CodeMirror or Monaco) with JSON-Schema-driven completion, beside a form view for the common cases
(types, required, nulls, transformations, keys, validation rules), live validation results and the
generated-from-sample starting point. Because every screen is backed by `/api/v1`, a richer front
end can replace pages later without touching the back end.

### 5.6 Secrets

Connection secrets are stored through a small backend interface: `env` (one encryption key from the
environment encrypts secrets in Postgres) to start, Kubernetes Secrets and Vault later. Secrets are
decrypted only to sign URLs (storage) or to hand a single job what it needs (SQL sources, §7.3), and
they are never logged (the engine already redacts connection strings).

### 5.7 Retention

Retention is set by admins, not built in. A policy says, per artifact kind (uploads, data, `bad_rows`,
previews, job records), either "keep *n* days" or "keep until deleted", and it can be set at three
levels: the installation, each classification, and each dataset (the most specific one wins). A new
installation keeps everything until an admin sets a policy; the admin screens show a warning while
`sensitive` datasets have no expiry. A sweeper in the gateway deletes expired objects and records
each deletion in the audit log. Job records can outlive their artifacts so history stays readable.

## 6. Workers

### 6.1 Supervisor and engine process

The worker is two processes with different trust:

1. The **supervisor** (small, trusted code) leases a job, validates the signed spec, prepares a
   per-job scratch directory, writes the inline schema to a file, and starts:
2. the **engine process**: `forklift run-job spec.json` (the same entry point library users have),
   with resource limits (CPU time, address space, open files, output size), a wall-clock timeout,
   the scratch directory as its only writable path, and no credentials of any kind. It reports
   progress on a pipe.

Inputs reach the engine one of two ways, chosen per input by size
([ADR 0006](adr/0006-streaming-large-inputs.md)):

| Mode | When | How | Engine network |
|---|---|---|---|
| **Staged** | Inputs up to `stageMaxBytes` (admin setting; default 2 GiB) | The supervisor downloads the object through a presigned GET into scratch and the spec points at the local copy | None needed: the `no-network` profile applies |
| **Streamed** | Larger inputs (planned for well beyond 10 GB) | The spec carries a presigned URL for that one object. The engine reads it as a forward-only stream (resuming with range requests after a dropped connection) plus small range reads for header detection. When a URL is about to expire, the engine asks the supervisor for a fresh one over the pipe | The object store endpoint only |

Outputs are written to scratch and uploaded by the supervisor with presigned multipart PUTs.
Parquet output is compressed and usually much smaller than the text it came from, so scratch is
sized for outputs, not inputs; streaming outputs directly is a later option if that stops being
true.

When the engine exits, the supervisor uploads the artifacts, reports the `JobResult`, deletes the
scratch directory and takes the next lease. One job per engine process: a crash, a leak or an
exploit does not outlive its job.

Either way, **the code that parses untrusted data never holds credentials.** A presigned URL opens
exactly one object, for a limited time, and the engine's network is limited to the store.

### 6.2 Isolation profiles

| Profile | Where | Engine process gets | Use for |
|---|---|---|---|
| **standard** (default) | Compose and Helm | Non-root, read-only root file system, default seccomp, rlimits, scratch dir; container egress limited to the gateway's internal port and the object store | Most installations |
| **no-network** | Where user namespaces are available | As standard, plus its own empty network namespace; only staged inputs (a larger input is refused, or routed to a `standard` lane if the admin allows it) | Untrusted files from outside the organisation |
| **sandboxed** | Helm | As standard (or no-network), plus a sandboxed runtime (`runtimeClassName`, for example gVisor or Kata) or one Kubernetes Job per run | Sensitive data from untrusted sources |

Limiting egress to "the object store" depends on where the store is. NetworkPolicies match IP
addresses, not host names: an in-cluster RustFS is selected by its pods, an external store by its
CIDR ranges where they are stable, and otherwise through an egress proxy that allows only the
store's host name. The chart supports all three (§10.2).

### 6.3 Publishing outputs

Engine outputs land in `jobs/<job>/attempt-<n>/`. For a dataset with a destination, the supervisor
then copies the files to the destination prefix with the data files first and `manifest.json` last,
so a reader that trusts only complete manifests never sees half a load. A threshold failure
publishes nothing to the destination but keeps `bad_rows.parquet` as a job artifact, matching the
engine's behaviour.

As built: for an `s3` destination the gateway publishes when the run completes, with server-side
copies (data first, `manifest.json` last), so the destination must be on the installation's own
store; a failed copy fails the job with `TARGET_WRITE_FAILED` and keeps the artifacts. Table
destinations are written by the engine itself (`sql_table`, ADR 0007).

## 7. Storage and sources

### 7.1 Object store layout (one bucket, prefixes per purpose)

| Prefix | Written by | Read by | Lifetime |
|---|---|---|---|
| `uploads/<upload>/` | Browser or client (presigned PUT) | Workers: the supervisor (staged) or the engine (streamed), both through presigned GETs | Admin-set retention (§5.7) |
| `jobs/<job>/attempt-<n>/` | Workers | Users (downloads), publish step | Admin-set retention (§5.7) |
| `previews/<job>/` | Workers | UI | Admin-set retention; short by nature |
| Destination prefixes (per connection) | Publish step | Downstream consumers | Owned by the destination |

The gateway's storage credential is split by purpose where the store supports policies: an upload
signer (`PutObject` on `uploads/`), a download signer (`GetObject` on `jobs/` and `previews/`), and
a retention sweeper (`DeleteObject`). The gateway never reads objects itself, but it can sign URLs
that do, so this is policy plus least privilege, not a hard guarantee; §8 lists it as a residual risk.

### 7.2 Local file systems

A `localfs` connection names a root directory that is mounted into the worker (Compose volume,
Kubernetes PVC). Inputs are bind-mounted read-only into the engine's sandbox; outputs are written
under the connection's root only. Paths are resolved and checked against the root before any job
is queued and again in the supervisor (the engine already rejects output names that escape their
directory).

As built: `localfs` connections can be created and tested, but are not dataset sources or
destinations yet, because job contract v1 has no location for a directory mounted into the
worker.

### 7.3 SQL sources and targets

Reading from or writing to a database needs network access to it, so those jobs run on a
separate `sql` lane whose workers' egress allows the configured database hosts. The job's
connection secret is delivered with the lease over TLS and lives only in memory and in the job's
scratch spec, which is deleted with the scratch directory. Reads use read-only sessions (the
engine's default, enforced by the database where it can be); a load goes through validated
Parquet and publishes all-or-nothing ([ADR 0007](adr/0007-database-sources-and-targets.md)).

## 8. Security model

| Asset | Threat | Mitigation |
|---|---|---|
| Accounts, tokens | Credential theft, brute force | Hashed tokens with prefixes and expiry, rate limiting, password policy, audit log; OIDC later |
| Gateway | Exploit through a malicious file or schema | The gateway never parses data or runs schema logic; uploads go straight to the store |
| Workers | Exploit through a malicious file, schema expression or regular expression | Engine hardening already in place (whitelist expression interpreter, ReDoS guard, Excel size and zip-bomb limits, validated SQL identifiers, output path checks); per-job process with rlimits; isolation profiles; no credentials in the engine process; engine network limited to the object store (none for staged inputs) |
| Other jobs' data | A compromised worker reaching beyond its job | Presigned URLs per job and object; worker tokens can only lease, heartbeat and complete; no database access |
| Data at rest | Disclosure of PII in outputs and `bad_rows` | Store-side encryption (SSE where available), admin-set retention per classification and dataset, view-raw-rows permission, audited downloads |
| Data in logs and results | PII in messages | The engine's rule: counts, codes and column names, never cell values; enforced by tests |
| Internal API | Abuse from inside the network | Separate port, not routed by the ingress; NetworkPolicy; worker tokens; signed specs |
| Supply chain | Vulnerable dependencies or images | Pinned lock files, Dependabot (already configured), image scanning in CI, minimal base images, SBOMs |

**Residual risks** we accept for a single organisation: the gateway's signing credentials could
read stored data if the gateway were fully compromised; SQL credentials reach the `sql` lane workers
for the duration of a job; the `standard` profile relies on container isolation rather than a
sandboxed runtime; an engine streaming a large input can open connections to the object store
(though only its own presigned URLs grant access to anything); a new installation deletes nothing
until an admin sets retention.

**Data classification** keeps "sometimes public, sometimes PII" manageable: a dataset's
classification sets defaults (retention, who may preview, whether downloads are audited) instead of
treating everything as maximally sensitive or nothing as sensitive.

## 9. Agents and pipelines

### 9.1 MCP

Two servers, one tool set:

| Server | Transport | Runs | For |
|---|---|---|---|
| `forklift-mcp` (extra of `forklift-etl`) | stdio | The engine in-process (`run_job`) | An agent on a machine that has the data |
| `forklift-mcp` image | Streamable HTTP | Calls `/api/v1` with a token | Agents that should use the service's connections, permissions and audit |

Tools: `list_connections`, `list_datasets`, `get_schema`, `generate_schema`, `validate_schema`,
`preview`, `run_job`, `get_job`, `explain_failure` (the error, findings by rule and the first rejected
rows, within the caller's permissions), `get_bad_rows` (bounded). Resources: the `docs/schemas` pages,
so an agent can read how an extension works before it writes one. Every tool has bounded output and
supports a dry run where it would change something.

### 9.2 Airflow and scripts

`forklift-client` provides a typed client and an Airflow operator and (deferrable) sensor that submit
a `JobSpec` or a dataset run and wait for the result. Teams whose Airflow workers have data access
can instead call `forklift.run_job` in a task; the result has the same shape.

## 10. Deployment

### 10.1 Docker Compose

`deploy/compose/docker-compose.yml` brings up `gateway`, `worker` (batch and interactive lanes in one
process for small installations), `postgres` and `rustfs`, with an optional `caddy` profile for TLS and
an optional `mcp` profile. One `.env` file holds the secrets. Data and database live in named volumes.
Workers sit on an `internal` Docker network that reaches only the gateway's internal port and
RustFS; with an external store, an optional allow-listing proxy service limits their egress to that
store's host. It is the development environment and a supported way to run a small installation.

### 10.2 Helm

`deploy/helm/forklift`:

```yaml
gateway:
  replicas: 2
  ingress: {enabled: true, host: forklift.example.org, tls: true}
  internalService: {port: 8081}            # workers only; not exposed by the ingress
workers:
  - name: interactive
    lanes: [interactive]
    replicas: 2
    resources: {limits: {cpu: "1", memory: 1Gi}}
    scratch: {sizeLimit: 2Gi}
  - name: batch
    lanes: [batch]
    replicas: 1
    autoscaling: {keda: {enabled: false, maxReplicas: 10}}   # scales on queue depth
    resources: {limits: {cpu: "4", memory: 16Gi}}
    scratch: {sizeLimit: 200Gi}             # outputs (and staged inputs up to stageMaxBytes)
    isolation: standard                     # standard | no-network | sandboxed
    runtimeClassName: ""                    # e.g. gvisor for the sandboxed profile
inputs:
  stageMaxBytes: 2Gi                        # larger inputs are streamed through presigned URLs
postgres: {external: true, existingSecret: forklift-db}
objectStore: {endpoint: https://s3.example.org, bucket: forklift, existingSecret: forklift-s3}
networkPolicies:
  enabled: true
  storeEgress:                              # how workers may reach the object store
    mode: cidr                              # podSelector (in-cluster RustFS) | cidr | proxy
    cidrs: [203.0.113.0/24]
    proxy: {enabled: false, allowHosts: [s3.example.org]}
image: {registry: ghcr.io/cornyhorse}       # forklift-web, forklift-worker, forklift-mcp
mcp: {enabled: false}
```

The chart is published as an OCI artifact (`oci://ghcr.io/cornyhorse/charts/forklift`). It ships
NetworkPolicies (deny all worker ingress; worker egress to the gateway's internal port and the store
only, by pod selector, CIDR or an allow-listing egress proxy), Pod Security "restricted" settings, a migration Job run before the gateway
rolls, and optional ServiceMonitors. It creates no cloud-specific resources: Postgres and the store are
external by default, and secrets can come from existing Secrets or an External Secrets operator.

### 10.3 Operations

- **Configuration**: environment variables with one documented settings module; the same names in
  Compose and Helm.
- **Observability**: structured JSON logs (never cell values), Prometheus metrics (queue depth, job
  durations, rows, rejections by code, lease expiries), optional OpenTelemetry traces across gateway,
  worker and engine.
- **Upgrades**: database migrations before the gateway rolls; workers drain (finish their lease,
  take no new one) on SIGTERM; the N/N-1 contract rule lets gateway and workers roll independently.
- **Backups**: Postgres (metadata, schemas, audit) and the store (data) are the only state.

## 11. Monorepo layout and releases

```
pyproject.toml, src/forklift/, tests/   engine + CLI          → PyPI forklift-etl (unchanged), tags v*
services/web/                           Django gateway (Ninja) → PyPI forklift-web,    image, tags web-v*
services/worker/                        supervisor             → PyPI forklift-worker, image, tags worker-v*
services/mcp/                           remote MCP proxy       → image,                      tags mcp-v*
clients/python/                         forklift-client        → PyPI forklift-client,       tags client-v*
contracts/                              JSON Schema (JobSpec, JobResult), OpenAPI; generated, checked in
deploy/compose/, deploy/helm/forklift/  deployment; chart published as an OCI artifact,  tags chart-v*
docs/                                   one documentation tree for everything
```

- The engine stays at the root ([ADR 0001](adr/0001-monorepo-layout.md)): no import or packaging
  churn, and the existing release workflow keeps working. It does need one guard: it runs on every
  published release and checks the tag against the engine's version, so it must skip tags that do
  not start with `v`.
- The engine keeps its wide support matrix (Python 3.8+, pyarrow 16 and newer). The services pin
  their own Python (3.12+, to confirm against the current Django LTS when work starts) and are
  tested in their own CI jobs, selected by path filters.
- A uv workspace (or an equivalent) installs everything for development; each package still builds
  and publishes on its own.
- Images are published to GHCR (`ghcr.io/cornyhorse/forklift-web`, `-worker`, `-mcp`), tagged with
  the package version and the commit; the chart goes to the same registry as an OCI artifact.

## 12. Engine changes needed first

These are small, useful on their own to library users, and the foundation for everything above.

| Change | Why |
|---|---|
| `forklift.jobs`: `JobSpec`, `JobResult`, `run_job(spec)`, JSON Schema export; `forklift run-job` | The contract (§4) |
| Progress callback and cancellation token for `import_*` | Heartbeats, progress bars, cancel |
| `ProcessingResults.to_dict()` and error codes on the engine's exceptions | Stable results for the API |
| Public `validate_schema(schema, columns, sample=None)` | The UI's live validation and the MCP tool; it is the pre-write check the engine already runs |
| Schema accepted as a dict as well as a file | Specs carry the schema inline |
| `import_csv(..., s3_client=)` / S3 endpoint option | Library users on S3-compatible stores (`import_sql` already takes `s3_client`; `import_csv` builds a default client) |
| `preview(source, schema=None, rows=n)` | The preview job |
| Arrow's streaming CSV reader for remote inputs: a `presigned_url` location read as a forward-only HTTP stream that resumes with range requests, small range reads for header detection, and a callback to refresh an expiring URL | Streamed inputs (§6.1). The same reader would speed up `s3://` inputs, which today go through Python's `csv` module row by row; only local files use Arrow's reader |
| `presigned_url` locations accepted only through `run_job`, and only for hosts the caller allows (the worker passes the store's endpoint) | Keeps the SSRF guard: schema generation and the public API still reject URLs from users |
| Footer detection without copying the input | It currently writes a filtered temporary copy, which for a streamed input would be a full local copy |
| Uniqueness checks with bounded memory | The constraint validator keeps one in-memory entry per distinct key, which for hundreds of millions of keys means many gigabytes; spill key sets to scratch (for example in hashed partitions) above a threshold |

## 13. Milestones

| # | Milestone | Done when |
|---|---|---|
| M0 | This design is reviewed and merged | — |
| M1 | Engine seams (§12) | `forklift run-job spec.json` produces a `JobResult` that validates against the published schema; contract tests in CI; a streamed CSV (presigned URL against RustFS) gives the same output as the local file |
| M2 | `forklift-mcp` (stdio) | An agent can generate, validate and apply a schema to a local file and explain a failed run |
| M3 | Service MVP | `docker compose up`; in the browser or through the API, a user uploads a CSV, picks or generates a schema, runs it and downloads Parquet and `bad_rows`; staged and streamed inputs both work; the four roles and the admin screens; database connections as sources and destinations on the `sql` lane; an end-to-end Compose test runs in CI and a run on an input larger than 10 GB runs on demand |
| M4 | Helm, hardening, authoring | Chart with NetworkPolicies and isolation profiles; schema editor; connections admin (S3-compatible, localfs); datasets, schedules, retention, audit |
| M5 | Integrations | Remote MCP, `forklift-client`, Airflow operator and sensor, webhooks, OIDC |
| M6 | Cloud warehouses | Snowflake, Databricks and BigQuery as sources and targets through their own bulk-load paths; tests that run in CI when an account's credentials are configured ([ADR 0007](adr/0007-database-sources-and-targets.md)) |

## 14. Alternatives considered

| Alternative | Why not (for now) |
|---|---|
| Separate repository for the web service | Rejected in favour of a monorepo: changes to the contract and the engine seams land together |
| Celery or RQ with Redis as the queue | A broker is one more stateful service to run and secure; Postgres row locking is enough for one organisation's volume, and the lease API hides the choice from workers |
| Gateway pushes jobs to workers | Workers would need inbound ports; pulling keeps them unreachable and lets them run in other networks |
| Workers read the store with their own credentials | Long-lived credentials in the process that parses untrusted data; not every S3-compatible store has short-lived, prefix-scoped credentials |
| Supervisor streams large inputs to the engine through a pipe | Would keep the engine network-less at any size, but the engine reads parts of a file more than once (header detection, the fallback reader for ragged rows, footer handling) and would first have to become single-pass ([ADR 0006](adr/0006-streaming-large-inputs.md)) |
| Stage every input, with large scratch volumes | Disk larger than the biggest input plus its outputs, and a full copy before work starts; kept for inputs up to `stageMaxBytes` |
| Single-page app front end | More build tooling and state for a small team; the API-first design keeps the option open |
| One Kubernetes Job per run by default | Strongest isolation, but slow start-up for interactive work and no Compose equivalent; offered as the `sandboxed` profile |

## 15. Questions resolved

| Question | Answer |
|---|---|
| Package and image names | `forklift-web`, `forklift-worker`, `forklift-client`, `forklift-mcp` |
| API framework | Django Ninja |
| Largest input to plan for | More than 10 GB: inputs above `stageMaxBytes` are streamed through presigned URLs, with the engine's network limited to the object store ([ADR 0006](adr/0006-streaming-large-inputs.md)) |
| Retention defaults | None built in: admins set retention per installation, classification and dataset (§5.7) |
| Registry | GHCR for the images; the Helm chart as an OCI artifact on GHCR |
| Staging threshold | `stageMaxBytes` defaults to 2 GiB (admins can change it): smaller inputs are copied to scratch and can use the `no-network` profile, larger ones are streamed |
