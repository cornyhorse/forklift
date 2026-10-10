# Design documents

| Document | Status | Summary |
|---|---|---|
| [Platform: library, CLI, web, API and MCP](platform.md) | Proposed | How the engine is offered as a library, a CLI, a web service with a REST API, and MCP tools, with data processing isolated in workers |

## Architecture decision records

| ADR | Status | Decision |
|---|---|---|
| [0001](adr/0001-monorepo-layout.md) | Proposed | Monorepo; the engine stays at the root, services are siblings with their own packages and tags |
| [0002](adr/0002-job-contract.md) | Proposed | One versioned `JobSpec` / `JobResult` contract for every interface |
| [0003](adr/0003-pull-lease-workers.md) | Proposed | Workers pull jobs through a lease API backed by Postgres; no message broker to start with |
| [0004](adr/0004-trust-boundary.md) | Proposed | The gateway never touches data; the engine runs only in sandboxed worker processes |
| [0005](adr/0005-storage-and-destinations.md) | Proposed | S3-compatible storage and local volumes; Parquet first, database tables later |
| [0006](adr/0006-streaming-large-inputs.md) | Proposed | Inputs above `stageMaxBytes` are streamed to the engine through presigned URLs; its network is limited to the object store |

New ADRs take the next number and the same headings (Status, Date, Context, Decision,
Consequences). An accepted ADR is not edited; a later ADR supersedes it.
