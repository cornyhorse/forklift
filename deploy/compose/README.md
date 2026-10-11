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
| `publisher` | `forklift-web`: `publish --every=5`, which [publishes](../../docs/platform/publishing.md) successful runs to their datasets' s3 destinations (data first, `manifest.json` last) and only then marks them succeeded; a run whose publisher stopped is resumed once its lease expires (healthy while its heartbeat is under a minute old) | public, internal, db (destinations may be outside) | nothing |
| `postgres` | `postgres:16` | db | nothing |
| `rustfs` | `rustfs/rustfs` | public, internal | 9000 (presigned URLs from browsers) |
| `caddy` (profile `caddy`) | `caddy:2.11.7` | public | 443 and 80 ([TLS with Caddy](#tls-with-caddy-the-caddy-profile)) |
| `egress-proxy` (profile `egress-proxy`) | `ubuntu/squid:6.6-24.04_edge` | internal, public | nothing ([An external store](#an-external-store-and-the-egress-proxy-the-egress-proxy-profile)) |

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
| `FORKLIFT_BIND_ADDRESS` | (every address) | Host address those two ports are published on; `127.0.0.1` behind Caddy, so that only Caddy is reachable from other hosts |
| `FORKLIFT_SECURE_COOKIES`, `FORKLIFT_BEHIND_TLS_PROXY` | `false` | Set both to `true` behind a TLS proxy |
| `FORKLIFT_TRUSTED_PROXIES` | `0` | Proxies in front of the gateway that add `X-Forwarded-For`; `1` behind a TLS proxy, so that the audit log and the [sign-in limits](../../docs/platform/sign-in-throttling.md) see each client's address, not the proxy's |
| `FORKLIFT_WEBHOOK_ALLOWED_HOSTS` | (none) | [Webhook](../../docs/platform/webhooks.md) receivers on an internal network: host names that may resolve to private addresses |
| `FORKLIFT_WEBHOOK_ALLOW_HTTP` | `false` | Also send webhooks to `http://` URLs (development only) |
| `FORKLIFT_WORKER_LANES`, `FORKLIFT_WORKER_CONCURRENCY` | `batch,interactive`, `2` | What the worker takes, and how many jobs at once |
| `FORKLIFT_WORKER_LANDLOCK` | `auto` | `required` in production |
| `FORKLIFT_WORKER_MEMORY` | `4g` | The worker container's memory limit |
| `FORKLIFT_LOG_LEVEL`, `FORKLIFT_WORKER_LOG_LEVEL` | `INFO`, `info` | Log levels (JSON logs) |
| `FORKLIFT_*_IMAGE`, `FORKLIFT_PYTHON_IMAGE` | | Other images or registries (`FORKLIFT_CADDY_IMAGE`, `FORKLIFT_SQUID_IMAGE` for the profiles) |
| `COMPOSE_PROFILES` | (none) | Profiles to start: `caddy` (`generate_env.py --domain` sets it) |

Anywhere but localhost, use the [caddy profile](#tls-with-caddy-the-caddy-profile), or put a TLS
proxy of your own in front of ports 8080 and 9000 with the same gateway settings it uses.

## TLS with Caddy (the caddy profile)

**When:** the stack is reached from other machines. Browsers send passwords and session cookies to
the gateway, and they upload and download straight to the store through presigned URLs: both
need https, and an https page cannot use an http store (mixed content). The `caddy` service
terminates TLS for two host names on ports 443 and 80 (which redirects to https):

| Site | Goes to |
|---|---|
| `FORKLIFT_DOMAIN`, for example `forklift.example.org` | the gateway's public port (UI, `/api/v1`); `/mcp` and `/mcp/*` go to the remote MCP server (`mcp:8000`, the `mcp` profile; 502 without it) |
| `FORKLIFT_STORE_DOMAIN`, for example `store.forklift.example.org` | RustFS: presigned URLs are signed for this name |

```bash
python deploy/compose/generate_env.py --domain forklift.example.org \
    --acme-email ops@example.org > deploy/compose/.env
docker compose -f deploy/compose/docker-compose.yml up -d --build --wait
```

`generate_env.py --domain` writes `COMPOSE_PROFILES=caddy` and the settings the gateway needs
behind a TLS proxy; to switch an existing installation, add the same lines to its `.env`:

| Variable | Value | Why |
|---|---|---|
| `FORKLIFT_DOMAIN`, `FORKLIFT_STORE_DOMAIN` | the two host names (`--store-domain`, default `store.<domain>`) | Caddy's sites and certificates |
| `FORKLIFT_TLS` | `acme` (`--acme-email`) or `internal` (`--internal-ca`) | where certificates come from (below) |
| `FORKLIFT_ACME_EMAIL` | the ACME account's email | `acme` only: Let's Encrypt, and ZeroSSL as a fallback, which needs it |
| `FORKLIFT_PUBLIC_URL`, `FORKLIFT_PUBLIC_STORE_URL` | `https://<domain>`, `https://<store domain>` (with `:<port>` for `--https-port`) | the CSRF origin and the bucket's CORS origin; the store URL presigned URLs are signed for and the page's `connect-src` |
| `FORKLIFT_ALLOWED_HOSTS` | `<domain>,localhost,127.0.0.1,gateway` | Caddy passes the client's `Host` on; `gateway` is how workers (and the MCP server) reach it |
| `FORKLIFT_BEHIND_TLS_PROXY`, `FORKLIFT_SECURE_COOKIES` | `true` | the gateway trusts `X-Forwarded-Proto` from Caddy and marks cookies `Secure` |
| `FORKLIFT_TRUSTED_PROXIES` | `1` | Caddy replaces `X-Forwarded-For` with the client's address, which the audit log and the [sign-in limits](../../docs/platform/sign-in-throttling.md) use |
| `FORKLIFT_BIND_ADDRESS` | `127.0.0.1` | the gateway's (8080) and the store's (9000) ports stay reachable from this host only: a client that bypassed Caddy could otherwise claim any address in `X-Forwarded-For` |
| `FORKLIFT_HTTPS_HOST_PORT`, `FORKLIFT_HTTP_HOST_PORT` | `443`, `80` (`--https-port`, `--http-port`) | Caddy's host ports; ACME needs 80 and 443 |
| `FORKLIFT_HSTS_SECONDS` | `31536000` (default) | `Strict-Transport-Security` on both sites; `0` tells browsers to forget it |

**DNS and certificates.** Both names must resolve to this host. With `FORKLIFT_TLS=acme`, Caddy
gets certificates from Let's Encrypt (ZeroSSL as a fallback) and renews them; ports 80 and 443
must be reachable from the internet for the challenges. With `FORKLIFT_TLS=internal`
(`--internal-ca`, for local and test installations, where names like `forklift.test` can live in
`/etc/hosts`), Caddy signs them with its own CA, and clients must trust its root:

```bash
docker compose -f deploy/compose/docker-compose.yml exec caddy \
    cat /data/caddy/pki/authorities/local/root.crt > caddy-root.crt
```

Certificates, keys and the CA live in the `caddy-data` volume: keep it (`down -v` deletes it, and
ACME rate-limits new certificates). Caddy checks its settings when it starts and exits with a
message naming the one to fix. `deploy/compose/caddy/Caddyfile` is the configuration.

**What else it changes.** Nothing limits or buffers request bodies (uploads of many GiB and
multipart parts stream through; checked with a 1 GiB upload in two parts, Caddy at 16 MiB of
memory), and there are no read or write timeouts. Caddy passes the client's `Host` header to
the store unchanged, which SigV4 presigned URLs need (they sign it). `X-Request-ID` comes back as
`X-Request-Id` (Caddy normalises header names). The gateway's internal port (8081) is never
routed.

**An external store with an `https://` URL** (below) needs no store site: leave
`FORKLIFT_STORE_DOMAIN` empty and Caddy serves only `FORKLIFT_DOMAIN` (`generate_env.py --domain
... --store-url https://...` does that).

**Verify it:**

```bash
curl -sI https://forklift.example.org/accounts/login/ | grep -i strict-transport-security
curl -s -o /dev/null -w '%{http_code} %{redirect_url}\n' http://forklift.example.org/
```

and run the end-to-end tests over https ([Test it](#test-it)).

## An external store and the egress proxy (the egress-proxy profile)

**When:** the object store is outside the stack (AWS S3, MinIO, Ceph RGW, ...). The worker stays
on the `internal` network, which has no route out, and reaches the store through the
`egress-proxy` service: Squid, on `internal` and `public`, which allows the store's URL and
nothing else (a `CONNECT` tunnel to an `https://` store's host and port, so TLS stays end to
end; plain requests to an `http://` one). The worker sends every store request through it
(`FORKLIFT_STORE_PROXY=http://egress-proxy:3128`: staging downloads, uploads and multipart parts,
and the engine's reads of streamed inputs) and gateway requests never; under Landlock a
streaming engine may connect to the proxy's port only
([services/worker/README.md](../../services/worker/README.md#reaching-the-store-through-a-proxy)).

```bash
python deploy/compose/generate_env.py --store-url https://s3.eu-west-1.amazonaws.com \
    --store-bucket my-forklift --store-region eu-west-1 > deploy/compose/.env
# fill in FORKLIFT_STORE_ACCESS_KEY and FORKLIFT_STORE_SECRET_KEY: an access key for the bucket
docker compose -f deploy/compose/docker-compose.yml -f deploy/compose/external-store.yml \
    up -d --build --wait
```

`external-store.yml` starts the proxy (it needs no `--profile`), points the worker at it, puts
`init` and the `sweeper` on `public` (they call the store) and leaves the bundled RustFS out. The
settings:

| Variable | Default | What it is |
|---|---|---|
| `FORKLIFT_STORE_URL` | `http://rustfs:9000` | the store's S3 endpoint, for the gateway's calls and the URLs it signs for workers |
| `FORKLIFT_STORE_HOST` | `rustfs:9000` | the host (and port) presigned URLs must name (the worker's `--store-host`) |
| `FORKLIFT_PUBLIC_STORE_URL` | | where browsers and API clients reach the store (`--public-store-url`; by default the store URL); behind Caddy it must be `https://` |
| `FORKLIFT_STORE_BUCKET`, `FORKLIFT_STORE_REGION` | `forklift`, `us-east-1` | the bucket and its region |
| `FORKLIFT_STORE_ACCESS_KEY`, `FORKLIFT_STORE_SECRET_KEY` | | the store's credentials (for the bundled RustFS, its root credentials) |
| `FORKLIFT_STORE_PROXY` | (none; `http://egress-proxy:3128` with `external-store.yml`) | the worker's store proxy |
| `FORKLIFT_EGRESS_PROXY_ALLOW` | `FORKLIFT_STORE_URL` | what the proxy allows: URLs (scheme, host, optional port) separated by spaces or commas |

`init` sets the bucket's CORS rule for the UI's origin and creates the bucket if it does not exist,
so the credential needs `s3:PutBucketCORS` (and `s3:CreateBucket` for a new bucket; on AWS outside
`us-east-1`, create the bucket first).

**The proxy's configuration** is `deploy/compose/egress-proxy/squid.conf` (the Helm chart's
`networkPolicies.storeEgress.mode: proxy` uses the same); `egress-proxy/start.sh` writes the
allowed destinations from `FORKLIFT_EGRESS_PROXY_ALLOW` into `/run/forklift-egress/allow.conf`,
which it includes, checks the result with `squid -k parse` and refuses to start on a value it
cannot use. Anything else gets 403. Nothing is cached, and the log (stdout, one line per request)
has no query strings: presigned URLs carry their signatures there. The `User-Agent` at the end of
each line tells the supervisor (`forklift-worker/...`) from the engine (`Python-urllib/...`). The
container runs as Squid's `proxy` user, without capabilities, on a read-only file system, and is
not published to the host.

**Verify it:**

```bash
docker compose -f deploy/compose/docker-compose.yml -f deploy/compose/external-store.yml \
    run --rm egress-proxy --check                          # the rules it would load
docker compose -f deploy/compose/docker-compose.yml -f deploy/compose/external-store.yml \
    logs egress-proxy                                      # what went through it
```

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
not on the public port. CI runs them on every change (the `platform-e2e` job), and against the
two profiles below (`.github/workflows/compose-profiles.yml`).

**Over https (the caddy profile)**, with Caddy's own CA and names in `/etc/hosts`; `SSL_CERT_FILE`
makes Python trust that CA, so every test runs over https, and `e2e/test_caddy.py` adds the
redirect, HSTS, secure cookies, a multipart upload by the client to the store's domain, a large
single PUT, client addresses through Caddy, the `/mcp` route (to a stand-in, `e2e/caddy.yml`) and
the ports published on localhost only:

```bash
python deploy/compose/generate_env.py --domain forklift-edge.test --internal-ca \
    --https-port 48443 --http-port 48480 > deploy/compose/.env
echo "127.0.0.1 forklift-edge.test store.forklift-edge.test" | sudo tee -a /etc/hosts
docker compose -f deploy/compose/docker-compose.yml -f deploy/compose/e2e/caddy.yml \
    up -d --build --wait
docker compose -f deploy/compose/docker-compose.yml exec caddy \
    cat /data/caddy/pki/authorities/local/root.crt > caddy-root.crt
SSL_CERT_FILE=caddy-root.crt FORKLIFT_E2E_ENV_FILE=deploy/compose/.env \
    python -m pytest deploy/compose/e2e --no-cov
```

**With an external store (the egress-proxy profile)**: `e2e/egress-proxy.yml` adds the
"external" store, a second RustFS on the `public` network only, and an `https://` destination the
proxy also allows. `e2e/test_egress_proxy.py` checks that staged and streamed inputs and the
uploads went through the proxy (from its log), that the worker cannot reach the store without it,
and that it refuses every other destination; it needs the docker CLI:

```bash
python deploy/compose/generate_env.py --store-url http://store-external:9000 \
    --public-store-url http://localhost:48290 > deploy/compose/.env
# the external RustFS's root credentials, any values (the last value of a variable wins)
printf 'FORKLIFT_STORE_ACCESS_KEY=e2e-key\nFORKLIFT_STORE_SECRET_KEY=e2e-secret-value\n' \
    >> deploy/compose/.env
docker compose -f deploy/compose/docker-compose.yml -f deploy/compose/external-store.yml \
    -f deploy/compose/e2e/egress-proxy.yml up -d --build --wait
FORKLIFT_E2E_ENV_FILE=deploy/compose/.env FORKLIFT_E2E_COMPOSE_PROJECT=forklift \
    python -m pytest deploy/compose/e2e --no-cov
```

`deploy/compose/tests` holds the tests of `generate_env.py`
(`python -m pytest deploy/compose/tests --cov=generate_env --cov-branch`).

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
