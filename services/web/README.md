# forklift-web

The gateway of the forklift service ([platform design](../../docs/design/platform.md) §5,
[ADR 0004](../../docs/design/adr/0004-trust-boundary.md)): the web interface, the public API
(`/api/v1`, [OpenAPI](../../contracts/openapi.json)), accounts, roles and API tokens, the
schema registry, datasets and schedules, the job queue that workers lease from (`/internal/v1`)
and the presigned URLs through which data moves straight between browsers, clients, workers and
the object store. It never imports the engine or pyarrow and never reads object contents
(`tests/test_trust_boundary.py` enforces both).

One process serves two ports and routes each request by the port its connection arrived on:

| Port | Serves | Who reaches it |
|---|---|---|
| `FORKLIFT_PUBLIC_PORT` (8080) | the UI, sign-in, `/api/v1`, `/healthz`, `/readyz` | people, clients, the ingress |
| `FORKLIFT_INTERNAL_PORT` (8081) | `/internal/v1` (worker tokens only), `/healthz`, `/readyz` | workers; never route it from the ingress |

## Running it

The image (`docker build -f services/web/Dockerfile -t forklift-web .` from the repository
root) runs gunicorn on both ports. Before the first start, and after each upgrade:

```bash
forklift-web migrate
FORKLIFT_ADMIN_PASSWORD='…' forklift-web bootstrap_admin --username admin
forklift-web create_worker_token --name batch-pool          # the workers' FORKLIFT_WORKER_TOKEN
forklift-web configure_cors --origin https://forklift.example.org   # the bucket's CORS rule
```

Beside the gateway, run `forklift-web sweep_retention --every=3600` (retention),
`forklift-web dispatch --every=10` ([schedules](../../docs/platform/schedules.md) and
[webhooks](../../docs/platform/webhooks.md)) and `forklift-web publish --every=5`
([publishing](../../docs/platform/publishing.md)), from the same image and settings. Each of
them stops on SIGTERM after the pass in progress, also as process 1 in a container.
[Docker Compose](../../deploy/compose/README.md) does all of this.

Other commands: `rotate_secrets` (after adding a key to `FORKLIFT_SECRETS_KEYS`),
`requeue_expired_leases`, `export_openapi contracts/openapi.json [--check]` and `check_oidc`
(tests [single sign-on](../../docs/platform/oidc.md) against the provider).

## Settings

Every deployment setting is an environment variable; Docker Compose and the Helm chart pass
them under the same names. A malformed value stops the gateway at start-up with a message that
names the variable. Settings admins change at run time (limits, lease length, URL lifetimes,
sign-in limits, retention) are installation settings in the database (Admin, Settings).

| Variable | Default | What it is |
|---|---|---|
| `FORKLIFT_SECRET_KEY` | (required) | Django's secret key (sessions, CSRF): long and random |
| `FORKLIFT_SECRETS_KEYS` | (required) | Fernet keys, comma-separated, that encrypt connection and webhook secrets; the first encrypts, all decrypt (`rotate_secrets`) |
| `FORKLIFT_SECRET_BACKEND` | `env` | Where secrets are kept (`env`: encrypted in PostgreSQL with the keys above) |
| `FORKLIFT_DB_HOST`, `FORKLIFT_DB_PORT` | `localhost`, `5432` | PostgreSQL |
| `FORKLIFT_DB_NAME`, `FORKLIFT_DB_USER` | `forklift`, `forklift` | |
| `FORKLIFT_DB_PASSWORD` | (required) | |
| `FORKLIFT_DB_SSLMODE` | `prefer` | libpq's `sslmode` (`verify-full` with a CA in production) |
| `FORKLIFT_DB_CONN_MAX_AGE` | `60` | Seconds a database connection is kept |
| `FORKLIFT_S3_BUCKET` | (required) | The installation's bucket (uploads, job outputs, previews) |
| `FORKLIFT_S3_ACCESS_KEY_ID`, `FORKLIFT_S3_SECRET_ACCESS_KEY` | (required) | The store credential; `FORKLIFT_S3_{UPLOAD,DOWNLOAD,DELETE}_ACCESS_KEY_ID` / `..._SECRET_ACCESS_KEY` split it by purpose |
| `FORKLIFT_S3_ENDPOINT_URL` | (AWS S3) | Where the gateway calls the store |
| `FORKLIFT_S3_PUBLIC_ENDPOINT_URL` | the endpoint | The store's address for browsers and API clients (their presigned URLs) |
| `FORKLIFT_S3_WORKER_ENDPOINT_URL` | the endpoint | The store's address for workers |
| `FORKLIFT_S3_REGION`, `FORKLIFT_S3_ADDRESSING_STYLE` | `us-east-1`, `path` | |
| `FORKLIFT_S3_CONNECT_TIMEOUT`, `FORKLIFT_S3_READ_TIMEOUT` | `5`, `30` | Seconds |
| `FORKLIFT_PUBLIC_URL` | | Where people reach the gateway (`https://forklift.example.org`): webhook payloads link under it, and the single sign-on redirect URI is built from it |
| `FORKLIFT_ALLOWED_HOSTS` | `localhost,127.0.0.1` | Host names the gateway answers to |
| `FORKLIFT_CSRF_TRUSTED_ORIGINS` | | Origins allowed to post forms (`https://forklift.example.org`) |
| `FORKLIFT_BEHIND_TLS_PROXY` | `false` | Trust the proxy's `X-Forwarded-Proto` |
| `FORKLIFT_SECURE_COOKIES` | as `FORKLIFT_BEHIND_TLS_PROXY` | Send cookies over https only |
| `FORKLIFT_TRUSTED_PROXIES` | `0` | Proxies whose `X-Forwarded-For` entry is the client's address (audit log, [sign-in limits](../../docs/platform/sign-in-throttling.md)) |
| `FORKLIFT_SESSION_SECONDS` | `28800` | How long a sign-in lasts |
| `FORKLIFT_PASSWORD_MIN_LENGTH` | `12` | At least 8 |
| `FORKLIFT_PASSWORD_SIGN_IN` | `true` | Keep the password form; `false` only with single sign-on |
| `FORKLIFT_OIDC_ISSUER`, `FORKLIFT_OIDC_*` | | [Single sign-on](../../docs/platform/oidc.md) through an OpenID Provider (below) |
| `FORKLIFT_LOGIN_REDIRECT_URL` | `/` | Where a sign-in without a `next` lands |
| `FORKLIFT_WEBHOOK_ALLOWED_HOSTS` | | Webhook receivers on an internal network: hosts that may resolve to private addresses |
| `FORKLIFT_WEBHOOK_ALLOW_HTTP` | `false` | Also send webhooks to `http://` URLs (development only) |
| `FORKLIFT_API_DOCS` | `true` | Serve the interactive API documentation at `/api/v1/docs` |
| `FORKLIFT_STATIC_ROOT` | | Where `collectstatic` copies the UI's static files for an ingress or CDN (the gateway serves them itself otherwise) |
| `FORKLIFT_CONTRACTS_DIR` | (packaged) | Another copy of the job contract's JSON Schemas |
| `FORKLIFT_PUBLIC_PORT`, `FORKLIFT_INTERNAL_PORT` | `8080`, `8081` | The two ports |
| `FORKLIFT_WEB_WORKERS`, `FORKLIFT_WEB_THREADS`, `FORKLIFT_WEB_TIMEOUT` | `2`, `8`, `120` | gunicorn (the image's command) |
| `FORKLIFT_LOG_LEVEL` | `INFO` | JSON logs on standard error |
| `FORKLIFT_DEBUG` | `false` | Django's debug mode: never in production |

### Single sign-on

The full guide, with Keycloak, Entra ID, Google and Dex examples, is
[docs/platform/oidc.md](../../docs/platform/oidc.md).

| Variable | Default | What it is |
|---|---|---|
| `FORKLIFT_OIDC_ISSUER` | (unset: off) | The provider's issuer, exactly as its discovery document states it |
| `FORKLIFT_OIDC_CLIENT_ID` | (required) | Forklift's client id at the provider |
| `FORKLIFT_OIDC_CLIENT_SECRET_FILE` / `FORKLIFT_OIDC_CLIENT_SECRET` | (one required) | The client secret, in a file (preferred) or the variable |
| `FORKLIFT_OIDC_TOKEN_AUTH_METHOD` | `client_secret_basic` | Or `client_secret_post` |
| `FORKLIFT_OIDC_SCOPES` | `openid email profile` | Must include `openid`; add `groups` where the provider needs it |
| `FORKLIFT_OIDC_PROVIDER_NAME` | `SSO` | "Sign in with *name*" |
| `FORKLIFT_OIDC_USERNAME_CLAIM` | `preferred_username` | Names new accounts (never finds one) |
| `FORKLIFT_OIDC_ROLE_CLAIM` | `groups` | The claim whose values map to roles (a dotted name reaches into objects) |
| `FORKLIFT_OIDC_ADMIN_GROUPS`, `..._AUTHOR_GROUPS`, `..._OPERATOR_GROUPS`, `..._VIEWER_GROUPS` | | Comma-separated values of the role claim for each role; the most powerful match wins |
| `FORKLIFT_OIDC_DEFAULT_ROLE` | `none` | The role without a match: `viewer`, `operator`, `author`, or `none` (refused) |
| `FORKLIFT_OIDC_CREATE_USERS` | `true` | Create accounts at the first sign-in |
| `FORKLIFT_OIDC_SYNC_ROLE` | `true` | Set the role from the claims at every sign-in |
| `FORKLIFT_OIDC_LINK_VERIFIED_EMAIL` | `false` | Link a first sign-in to the account with the same verified email address |
| `FORKLIFT_OIDC_USERINFO` | `false` | Also read claims from the UserInfo endpoint |
| `FORKLIFT_OIDC_PROVIDER_LOGOUT` | `false` | Sign out at the provider too |
| `FORKLIFT_OIDC_ALGORITHMS` | all asymmetric | Accepted ID token signature algorithms (never `none` or HMAC) |
| `FORKLIFT_OIDC_CA_FILE` | (system CAs) | CA certificates for a provider with a private CA |
| `FORKLIFT_OIDC_ALLOW_HTTP` | `false` | Development only: accept an `http://` provider |

## Tests

The tests need PostgreSQL and RustFS (`docker compose -f tests/integration-tests/services/compose.yaml
up -d --wait postgres rustfs`, then create the login `forklift_web_test` as CI does) and, for the
browser tests, Chromium (`python -m playwright install --with-deps chromium`):

```bash
pip install -e "services/web[test]"
cd services/web && python -m pytest --cov=forklift_web --cov-branch --cov-fail-under=100
```

They include role matrices over every endpoint and every page, the queue under concurrency,
the two-port server, single sign-on against a fake OpenID Provider (a real TLS server with real
RSA and EC keys) and a browser sign-in through it, and the check that `contracts/openapi.json`
is current.
