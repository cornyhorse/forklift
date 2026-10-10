# forklift on Docker Compose

The whole platform on one machine: the gateway (UI, `/api/v1`), a worker, PostgreSQL and RustFS
(the S3-compatible object store). It is the development environment and a supported way to run
a small installation (design: [docs/design/platform.md](../../docs/design/platform.md) §10.1).

## Start it

```bash
python deploy/compose/generate_env.py > deploy/compose/.env       # once: random secrets
docker compose -f deploy/compose/docker-compose.yml up -d --build --wait
```

Open <http://localhost:8080> and sign in as `admin` with the `FORKLIFT_ADMIN_PASSWORD` from
`deploy/compose/.env`. Keep that file private: it holds the admin and database passwords, the
store's root credentials and the keys that encrypt connection secrets.

`up` runs a one-shot `init` service first (safe to repeat): database migrations, the first admin,
the bucket with a CORS rule for the UI's origin (browsers upload and download straight to the
store), and a worker token, kept in a volume only the worker mounts.

## What runs where

| Service | Image | Networks | Published |
|---|---|---|---|
| `gateway` | `forklift-web` (services/web) | public, internal, db | 8080 (UI and `/api/v1`); 8081, the workers' port, is not published |
| `worker` | `forklift-worker` (services/worker) | internal | nothing |
| `sweeper` | `forklift-web`: `sweep_retention --every=3600` (healthy while its last sweep is under two hours old) | internal, db | nothing |
| `dispatcher` | `forklift-web`: `dispatch --every=10`, which queues the runs of [schedules](../../docs/platform/schedules.md) when they are due and sends webhook deliveries (healthy while its last good pass is under a minute old) | public, internal, db (webhooks need a route out) | nothing |
| `postgres` | `postgres:16` | db | nothing |
| `rustfs` | `rustfs/rustfs` | public, internal | 9000 (presigned URLs from browsers) |

The `internal` and `db` networks have no route out. The worker can reach only the gateway's
internal port and the store; it is not on the database's network. The worker runs as uid 10001
with a read-only root file system, `no-new-privileges`, a process and memory limit, and Landlock
where the kernel has it (`FORKLIFT_WORKER_LANDLOCK=required` refuses to start without it). See
[services/worker/README.md](../../services/worker/README.md) for what each isolation profile
enforces.

## Settings (`.env`)

| Variable | Default | What it is |
|---|---|---|
| `FORKLIFT_PUBLIC_URL` | `http://localhost:8080` | Where people reach the UI (CSRF origin and the bucket's CORS origin) |
| `FORKLIFT_PUBLIC_STORE_URL` | `http://localhost:9000` | Where browsers reach the store (presigned URLs are signed for it) |
| `FORKLIFT_ALLOWED_HOSTS` | `localhost,127.0.0.1,gateway` | Host names the gateway answers to |
| `FORKLIFT_PUBLIC_HOST_PORT`, `FORKLIFT_STORE_HOST_PORT` | `8080`, `9000` | Host ports to publish |
| `FORKLIFT_SECURE_COOKIES`, `FORKLIFT_BEHIND_TLS_PROXY` | `false` | Set both to `true` behind a TLS proxy |
| `FORKLIFT_TRUSTED_PROXIES` | `0` | Proxies in front of the gateway that add `X-Forwarded-For`; `1` behind a TLS proxy, so that the audit log and the [sign-in limits](../../docs/platform/sign-in-throttling.md) see each client's address, not the proxy's |
| `FORKLIFT_WEBHOOK_ALLOWED_HOSTS` | (none) | [Webhook](../../docs/platform/webhooks.md) receivers on an internal network: host names that may resolve to private addresses |
| `FORKLIFT_WEBHOOK_ALLOW_HTTP` | `false` | Also send webhooks to `http://` URLs (development only) |
| `FORKLIFT_WORKER_LANES`, `FORKLIFT_WORKER_CONCURRENCY` | `batch,interactive`, `2` | What the worker takes, and how many jobs at once |
| `FORKLIFT_WORKER_LANDLOCK` | `auto` | `required` in production |
| `FORKLIFT_WORKER_MEMORY` | `4g` | The worker container's memory limit |
| `FORKLIFT_LOG_LEVEL`, `FORKLIFT_WORKER_LOG_LEVEL` | `INFO`, `info` | Log levels (JSON logs) |
| `FORKLIFT_*_IMAGE`, `FORKLIFT_PYTHON_IMAGE` | | Other images or registries |

Anywhere but localhost: put a TLS proxy in front of ports 8080 and 9000, set the two public URLs
to their `https://` addresses, set the two TLS settings to `true` and `FORKLIFT_TRUSTED_PROXIES`
to `1`.

## SQL sources and targets

Database connections run on the `sql` lane. Add a second worker for it, on a network that reaches
your databases, with the ODBC drivers they need (the image has PostgreSQL and MariaDB/MySQL; build
`FROM forklift-worker` to add Microsoft's ODBC Driver 18 or Oracle Instant Client):

```yaml
  worker-sql:
    extends: {service: worker}
    command: [--gateway=http://gateway:8081, --lanes=sql, --store-host=rustfs:9000]
    networks: [internal, default]
```

## Test it

```bash
FORKLIFT_E2E_ENV_FILE=deploy/compose/.env python -m pytest deploy/compose/e2e --no-cov
```

The tests sign in, create an API token, upload a CSV straight to the store, run it on the worker
and download the Parquet; generate a schema; check that a threshold failure keeps `bad_rows`;
stream an input above `stage_max_bytes`; and check a viewer's limits and that the workers' API is
not on the public port. CI runs them on every change (the `platform-e2e` job).

The webhook test needs a receiver on the stack's network: start the stack with
`e2e/webhooks.yml` as well (it adds a `webhook-receiver` service, published on port 18099, and
lets the gateway and the dispatcher send to it over http), then point the test at it:

```bash
docker compose -f deploy/compose/docker-compose.yml -f deploy/compose/e2e/webhooks.yml up -d --build --wait
FORKLIFT_E2E_ENV_FILE=deploy/compose/.env FORKLIFT_E2E_RECEIVER_URL=http://localhost:18099 \
  python -m pytest deploy/compose/e2e --no-cov
```

Without `FORKLIFT_E2E_RECEIVER_URL` the webhook test is skipped.

## Stop it

```bash
docker compose -f deploy/compose/docker-compose.yml down        # keep the data
docker compose -f deploy/compose/docker-compose.yml down -v     # delete it too
```
