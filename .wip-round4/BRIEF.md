# Brief: integrations round (MCP, client + Airflow, OIDC, Helm, Compose profiles)

Repository `/home/user/forklift`, branch `claude/epic-johnson-hfwsxj` (freshly reset to `main`, which
has everything up to PR #19: platform MVP, schedules, webhooks, sign-in rate limit, schema editor,
multipart outputs, URL refresh, resumable publishing). Several agents work in this checkout at the
same time, each in its own area. The lead (me) integrates, writes the shared docs and commits.

Read first: `docs/design/platform.md` (the design; §9 agents and pipelines, §10 deployment, §5.4
APIs, §8 security), the ADRs in `docs/design/adr/`, `deploy/compose/README.md`,
`services/web/README.md`, `services/worker/README.md`, `docs/platform/*.md`.

The user asked for, in ONE pull request:
- **M2** `forklift-mcp` over stdio (design §9.1): an agent can generate, validate and apply a
  schema to a local file and explain a failed run.
- **M4** the Helm chart with NetworkPolicies and isolation profiles (design §10.2).
- **M5** remote MCP (the `forklift-mcp` image, Streamable HTTP over `/api/v1`), `forklift-client`
  (typed Python client) with an Airflow operator and deferrable sensor (§9.2), and OIDC sign-in.
- The Compose profiles `caddy` (TLS), `mcp`, and an egress proxy (§10.1).

Quality bar (same as every earlier round): production-grade, 100% line and branch coverage for
every Python package, real behaviour tests (real HTTP servers, real crypto, real Kubernetes where
it matters), verbose specific error messages, docs written for operators. Prefer verifying against
the real thing over trusting docs or this brief.

## Rules for every agent

- **Git**: do not commit, stash, checkout, reset, rebase or push. Edit files only; `git diff` /
  `git status` are fine. Another agent's uncommitted files are in the same tree: leave them alone.
- **Your area only** (listed in your prompt). Shared files belong to the lead: root `README.md`,
  `CHANGELOG.md`, `docs/design/**`, `docs/DOCUMENTATION_INDEX.md`, `.github/workflows/test.yml`,
  `.github/workflows/fast-test.yml`, `.github/workflows/publish.yaml`. Put what you need changed
  there in your report file (below) with the exact text. You MAY create a new workflow file of your
  own (`.github/workflows/<your-area>.yml`) for your area's CI jobs.
- **Report file**: write `$S/round4/<AGENT>.md` (your agent name from your prompt) as you go and
  finish it at the end: what you built and where; decisions and why; anything you could not do
  or verify (say so plainly); exact text for the lead's shared files (CHANGELOG entry in the
  CHANGELOG's style, design-doc changes, DOCUMENTATION_INDEX lines, test.yml job changes); how
  you verified (commands and results). Your final message to the lead can be short and point at it.
- **Quality**: code reads like the surrounding code (study the neighbours first); behaviour tests
  that assert outcomes; no wall-clock timing assertions; `tmp_path` for files; nothing written into
  the repository root. Specific error messages, but never cell values, passwords, tokens or secrets
  in messages, logs, results or audit entries.
- **Coverage**: 100% line and branch for what you add or change. No `# pragma: no cover` unless the
  code truly cannot run in CI, with the reason in the same comment.
- **Style**: `black --line-length 99`, `isort --profile black --line-length 99` (per tree: the root,
  `services/web`, `services/worker`, and your new tree), `flake8 --max-line-length=99
  --extend-ignore=E203,W503`. Tools: `$S/venv/bin/black`, `$S/venv/bin/isort`, `$S/venv/bin/flake8`.
- **Security invariants**: the gateway never imports `pyarrow` or `forklift` (the engine) and never
  reads object contents (a test enforces it). The engine process holds no credentials. Secrets are
  encrypted at rest, write-only in the UI/API, never logged. Every state-changing admin action is
  audited. Outbound requests from the gateway follow the SSRF rules in §8 where users control URLs.
- **Pin what you add** to versions that exist today (check PyPI / the registry; "latest" tags are
  not allowed in images). Current: MCP Python SDK `mcp` 2.3.0 (a 2.x API; read its docs/source in
  your venv, do not code from 1.x memory), `apache-airflow` 3.3.2 (Python >= 3.10), PyJWT 2.15.1.

## Environment

`S=/tmp/claude-0/-home-user-forklift/32052cf1-7a9c-57b5-940d-29385d406628/scratchpad`

- 4 CPUs, 15 GB RAM, ~15 GB free disk shared by all agents. Run targeted tests while iterating and
  full suites at the end. Delete what you no longer need (venvs, images, k3s clusters) before you
  finish. If writes fail with "no space left on device", delete your own large leftovers first.
- Network: PyPI, Docker Hub (`docker.io`), GHCR (`ghcr.io`) and `mirror.gcr.io` work. BLOCKED:
  `get.helm.sh`, GitHub release downloads, `quay.io`, `deb.debian.org` (so `apt` inside Dockerfiles
  fails locally; CI has it). Run CLI tools you cannot pip-install from their container images
  (e.g. `alpine/helm`, `ghcr.io/yannh/kubeconform`, `rancher/k3s`).
- Engine venv: `$S/venv` (Python 3.13, pyarrow 25, the engine installed editable). Engine tests:
  `$S/venv/bin/python $S/run_tree.py [pytest args]` from the repo root (runs `tests/`).
  Gateway venv: `$S/webvenv` (Python 3.12, forklift-web editable + test extras). Gateway tests:
  write your own runner like `$S/w_test.py` that sets `os.environ["FORKLIFT_DB_NAME"] =
  "forklift_web_<agent>"` (your own test database name, so agents do not collide) and
  `COVERAGE_FILE` to a file of your own, then calls `pytest.main`. Inline `VAR=value cmd` prefixes
  are blocked: put environment variables in a small runner script under `$S`.
  Create your own venvs under `$S/<agent>-...` for new packages (`python3.13 -m venv` or
  `python3.12 -m venv`); `pip install` works.
- Docker is running. If the daemon is gone: `docker info >/dev/null 2>&1 || (setsid nohup dockerd >
  /tmp/dockerd.log 2>&1 < /dev/null &)`. Never `pkill -f` a pattern that matches your own command.
  Shared test services (`docker compose -f tests/integration-tests/services/compose.yaml up -d
  --wait postgres rustfs`): PostgreSQL on 127.0.0.1:15432 (forklift_admin /
  forklift-admin-secret; the gateway tests' login `forklift_web_test` exists), RustFS on
  127.0.0.1:19000 (forklift-test / forklift-test-secret). Never stop or wipe them.
- Compose stacks: build the images with
  `docker build -f services/web/Dockerfile -t forklift-web:local .` and (apt is blocked locally)
  `docker build -f $S/localbase/worker.Dockerfile -t forklift-worker:local .` from the repo root;
  `$S/compose.local.yml` is an override that uses those prebuilt images. Generate an env file with
  `deploy/compose/generate_env.py`. Give your stack its OWN project name (`-p forklift-<agent>`)
  and OWN host ports (your prompt names a range), and `down -v` it when done. The e2e tests in
  `deploy/compose/e2e/` read `FORKLIFT_E2E_ENV_FILE`; see `$S/run_e2e.py` for a runner.
- The machine is shared: do not restart Docker or other agents' containers.

## Areas (who owns what)

| Agent | Owns |
|---|---|
| MCPL | `src/forklift/mcp/**` (new), its engine tests (`tests/unit-tests/test_mcp_*.py`), the root `pyproject.toml` (only: the `mcp` extra, `all`, the `forklift-mcp` script, wheel data for the docs resources), `docs/platform/mcp.md` (new) |
| CLIENT | `clients/python/**` (new: `forklift-client`, module `forklift_client`, Airflow operator/sensor/trigger), `deploy/compose/e2e/test_client.py` (new), `docs/platform/client.md` and `docs/platform/airflow.md` (new) |
| OIDC | `services/web/**` (the gateway: OIDC sign-in and anything else the gateway needs), `deploy/compose/e2e/oidc.yml` + `test_oidc.py` (new, optional Dex end-to-end), `docs/platform/oidc.md` (new). Other agents ask OIDC's area for gateway changes through the lead |
| HELM | `deploy/helm/**` (new), `docs/platform/helm.md` (new), `.github/workflows/helm.yml` (new); and ONLY `services/web/src/forklift_web/core/management/commands/create_worker_token.py` + its test if the chart needs to register a token it generated (tell OIDC's area nothing; the lead merges) |
| EDGE | `deploy/compose/**` except the new e2e files above (the `caddy` and `egress-proxy` profiles, their e2e), `services/worker/**` (egress-proxy support), `docs/platform/compose-profiles.md` (new, or extend `deploy/compose/README.md`) |
| MCPR | (second phase, after CLIENT) `services/mcp/**` (new: the remote MCP proxy, image `forklift-mcp`), `deploy/compose/e2e/test_mcp.py`; the `mcp` Compose service is added by the lead from MCPR's report |

## Contracts between areas (fixed now so parallel work fits)

### Remote MCP proxy (MCPR builds it; EDGE routes to it; HELM deploys it)
- Image `forklift-mcp`, built from `services/mcp/Dockerfile` with the repository root as context;
  runs as uid/gid 10001, works with a read-only root file system (writes nothing; `/tmp` may be an
  emptyDir/tmpfs).
- Listens on port `8000` (`FORKLIFT_MCP_PORT`). MCP Streamable HTTP at path `/mcp`. `GET /healthz`
  answers 200 `{"status": "ok"}` without authentication and without calling the gateway.
- Settings (environment): `FORKLIFT_MCP_API_URL` (base URL of the gateway's PUBLIC port, e.g.
  `http://gateway:8080`; required), `FORKLIFT_MCP_ALLOWED_HOSTS` (comma-separated `Host` header
  values it accepts, DNS-rebinding guard; required), `FORKLIFT_MCP_CA_FILE` (optional CA bundle for
  an https gateway), `FORKLIFT_MCP_LOG_LEVEL`.
- Authentication: every MCP request carries `Authorization: Bearer <Forklift API token>`; the proxy
  forwards that token to `/api/v1` and stores nothing. No token: 401 with `WWW-Authenticate: Bearer`.
- Compose: service `mcp`, profile `mcp`, network `public`. Behind the `caddy` profile, Caddy routes
  `https://{$FORKLIFT_DOMAIN}/mcp` (and `/mcp/*`) to `mcp:8000`. Helm: `mcp.enabled`, a Deployment
  + Service on 8000, ingress path `/mcp` on the gateway's host, NetworkPolicy egress only to the
  gateway's public port (and DNS).

### Worker egress through an allow-listing proxy (EDGE builds it; HELM uses it)
- Worker setting `FORKLIFT_STORE_PROXY=http://<host>:<port>` (and the matching CLI option, if the
  worker has CLI options for its settings): the supervisor sends store requests (staging GETs,
  output PUTs/parts) through it and gateway requests never; the engine process is given it for
  streamed inputs (`presigned_url`). When set, the sandbox's network rule (Landlock) allows the
  proxy's port instead of the store's. Ambient `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY` must not
  silently route the worker's or engine's traffic (verify what happens today and decide; document).
- The proxy: Squid, image `ubuntu/squid` with a pinned tag that exists on Docker Hub (verify with
  `docker pull`), configured to allow only the store's host name(s) (CONNECT to 443 for https
  stores, plain requests for http stores) and deny everything else. Compose service `egress-proxy`,
  profile `egress-proxy`, port 3128. HELM uses the same image and the same config shape (a
  ConfigMap) for `networkPolicies.storeEgress.mode: proxy`; HELM: copy the config EDGE writes in
  `deploy/compose/egress-proxy/squid.conf` once it exists, and say in your report whether they match.

### MCP tool set (MCPL and MCPR implement the same names and meanings)
Design §9.1: `list_connections`, `list_datasets`, `get_schema`, `generate_schema`,
`validate_schema`, `preview`, `run_job`, `get_job`, `explain_failure`, `get_bad_rows`; resources:
the `docs/schemas` pages (URI `forklift://docs/schemas/<file>.md`). The local server has no
service, so it implements the tools that make sense on local files (no `list_connections` /
`list_datasets`) with paths where the remote one takes upload/dataset/job ids. Every tool has
bounded output (rows, bytes, list lengths, all capped with the cap stated in the result), returns
structured JSON results, never puts secrets in results, and takes `dry_run` where it would change
something (write files, enqueue a job).

### Ports for your own Compose stacks / clusters (host side)
CLIENT 28000-28999, OIDC 38000-38999, EDGE 48000-48999, MCPR 58000-58999, HELM (k3s API and
port-forwards) 6440-6449 and 18400-18499.
