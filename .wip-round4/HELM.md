# HELM report: milestone M4's chart (`deploy/helm/forklift`)

Status: done. The chart, its tests (render tests, lint/kubeconform, a real k3s install), CI
(`.github/workflows/helm.yml`, including the OCI publish job) and the operator guide
(`docs/platform/helm.md`). No git operations. Outside my area I touched only what the prompt
allowed in `services/web`: `create_worker_token.py` and a new test file for it.

## What I built and where

| Path | What |
|---|---|
| `deploy/helm/forklift/Chart.yaml` | `version: 0.1.0`, `appVersion: "0.1.0"` (forklift-web and forklift-worker are both 0.1.0), `kubeVersion: ">=1.30.0-0"` |
| `deploy/helm/forklift/values.yaml` | Every value documented; safe defaults (NetworkPolicies on, Landlock `required`, secrets only from Secrets) |
| `deploy/helm/forklift/values.schema.json` | Strict (draft-07, every object `additionalProperties: false`): enums, URL/host/CIDR/lane/quantity patterns; `gateway.extraEnv` refuses chart-managed and secret-looking names; `workers[].settings` refuses the names the chart sets; `gateway.oidc.settings` takes exactly the gateway's other `FORKLIFT_OIDC_*` names |
| `templates/gateway.yaml` | ConfigMap of the non-secret settings; gateway Deployment (gunicorn on 8080 + 8081 as the image runs it; startup/readiness/liveness on `/healthz` `/readyz` with `Host: localhost`); Service `<rel>-gateway` (8080) and `<rel>-gateway-internal` (8081, ClusterIP); PDB when replicas > 1; Ingress (`/` to the gateway, `/mcp` to the MCP proxy when enabled; never the internal Service) |
| `templates/gateway-processes.yaml` | dispatcher (`dispatch --every=10`), publisher (`publish --every=5`, replicas configurable, PDB > 1), sweeper (`sweep_retention --every=3600`): Compose's commands and heartbeat health checks (startup + liveness probes) |
| `templates/workers.yaml` | One Deployment per `workers[]` entry (merged onto `workerDefaults`): lanes, concurrency, resources, scratch `emptyDir` with `sizeLimit`, token Secret file in `/run/secrets` (0440), every setting as `FORKLIFT_WORKER_*`; isolation `standard` / `no-network` (Localhost seccomp) / `sandboxed` (`runtimeClassName` required, `FORKLIFT_WORKER_MAX_JOBS=1`, concurrency 1, `sandboxedEngine`); `terminationGracePeriodSeconds` = drain + kill grace + 15; PDB; optional KEDA `ScaledObject` + `TriggerAuthentication` |
| `templates/bootstrap.yaml` | Hook Jobs: `<rel>-bootstrap` (pre-install/pre-upgrade: `migrate`, `admin`, `worker-token`, `cors` for an external store, one container per step), `<rel>-store-setup` (post-install/post-upgrade, bundled RustFS only: `bucket`, `cors`) |
| `templates/bootstrap-policy.yaml` | The bootstrap Job's NetworkPolicy as a pre-hook (weight -10), so an upgrade's Job already runs under the new rules |
| `templates/secrets.yaml` | Chart-generated Secrets as pre-hooks (weight -20), kept with `lookup`: `<rel>-secrets` (signing key + one Fernet key), `<rel>-admin`, `<rel>-worker-token` (`fkw_` + 43 chars), `<rel>-rustfs`. An **upgrade fails** rather than generate new `FORKLIFT_SECRETS_KEYS` if `<rel>-secrets` is missing |
| `templates/serviceaccount.yaml` | One SA (no token), plus a hook SA for the bootstrap Jobs |
| `templates/rustfs.yaml` | Optional bundled RustFS: StatefulSet + PVC, logs to stdout, console off, read-only root, Service, optional Ingress |
| `templates/mcp.yaml` | Optional MCP proxy per the brief's contract: Deployment (port 8000, `/healthz` probes), Service, PDB; env `FORKLIFT_MCP_PORT`, `FORKLIFT_MCP_API_URL` (`http://<rel>-gateway:8080`), `FORKLIFT_MCP_ALLOWED_HOSTS`, `FORKLIFT_MCP_LOG_LEVEL`, optional `FORKLIFT_MCP_CA_FILE` |
| `templates/egress-proxy.yaml` | Squid (`ubuntu/squid:6.6-24.04_edge`, uid 13, read-only root) for `storeEgress.mode: proxy`: ConfigMap with EDGE's `squid.conf` and an `allow.conf` rendered from `egressProxy.allow`, Deployment, Service, PDB |
| `templates/networkpolicies.yaml` | Default deny for every release pod + one policy per component (table in docs/platform/helm.md) |
| `templates/NOTES.txt` | How to reach the UI, read the admin password, what the policies do, the generated-secrets warning |
| `files/seccomp/worker-no-network.json` | The `no-network` Localhost profile: containerd 2's RuntimeDefault (taken from a running pod on k3s v1.36.4, amd64, caps dropped) plus `unshare` allowed only when its flags carry no `CLONE_NEW*` other than USER and NET (`SCMP_CMP_MASKED_EQ` 0x2E020000 == 0) |
| `files/create_bucket.py` | The store-setup Job's bucket step (waits up to 5 min for RustFS) |
| `ci/*-values.yaml` | `default` (external store, cidr), `rustfs` (bundled + podSelector + ingress), `cidr` (existing Secrets everywhere, split store credentials, sql group with DB egress, CA bundle), `proxy` (https store, virtual addressing), `no-network`, `sandboxed` (gVisor, no-network engine), `keda` (two scaled groups, own KEDA login), `mcp-oidc` (MCP + ingress + OIDC with an in-cluster provider) |
| `deploy/helm/tests/test_render.py`, `conftest.py`, `pytest.ini` | 145 render tests (pytest, pyyaml, jsonschema) |
| `deploy/helm/tests/lint.sh` | `helm lint --strict` (defaults + each ci file), `helm template`, `kubeconform -strict` for Kubernetes 1.30.0 and 1.36.0 (schemas pinned to commit `5f1fa4f7`; KEDA CRDs from datreeio/CRDs-catalog pinned to `63669a57`, so nothing is skipped) |
| `deploy/helm/tests/k3s_smoke.sh`, `k3s_e2e.py`, `k3s/*` | The real install on k3s in Docker (below) |
| `.github/workflows/helm.yml` | Jobs `lint`, `k3s`, `publish` (tag `chart-v*`, `packages: write` for that job only) |
| `docs/platform/helm.md` | Operator guide: install (OCI and repo), values, secrets, bootstrap/upgrades, isolation profiles, NetworkPolicies and the three store modes, webhooks/SSRF, OIDC, KEDA, MCP, testing, troubleshooting, releases |
| `services/web/src/forklift_web/core/management/commands/create_worker_token.py` | **Changed (allowed by my prompt):** `--from-file PATH` registers a token from a file |
| `services/web/tests/test_create_worker_token.py` | **New:** its tests (I left `tests/test_commands.py` alone; the old test there still passes) |

### `create_worker_token --from-file`

`forklift-web create_worker_token --name NAME [--expires-days N] --from-file PATH` reads at most
4 KiB, strips white space, requires exactly the generated format (`fkw_` + 43 URL-safe
characters), and stores it like a generated token (`prefix` = first 12 characters,
`tokens.hash_token`), audited as `worker_token.create` with `{"expires_at": ..., "from_file":
true}` by `system:create_worker_token`. Idempotent: the same token again prints "Worker token
fkw_xxxxxxxx... is already registered (<name>); nothing to do." and changes nothing. Refused,
with specific messages that never contain the file's contents: an unreadable file, a wrong format
(also non-ASCII, two tokens, over 4 KiB), an empty name, `--expires-days` below 1, a prefix
another token already has, a revoked or an expired token (never registered again). Nothing
secret is printed. Without `--from-file` the command behaves as before. Coverage of the module:
100% lines and branches. The registration logic lives in the command because that is the only
file I could change; the lead may want to move it into `services/workers.py` as
`register_worker_token` later.

## Decisions and why

- **Bootstrap split into a pre-hook and (bundled store only) a post-hook.** The brief asks for
  everything before the gateway rolls, but a bundled RustFS does not exist before the first
  install, so its bucket and CORS rule are set by a post-install/post-upgrade Job that waits for
  it. With an external store, CORS is in the pre-hook as asked.
- **Each bootstrap step is a container** (init containers in order): no shell script, exec arrays
  only, per-step logs (`-c migrate`), and the admin password reaches only the `admin` step, the
  token file only the `worker-token` step.
- **Generated Secrets are hooks** (they must exist before the pre-install Job) with
  `before-hook-creation`, values kept by `lookup`, so `helm uninstall` leaves them, which is what
  you want for the Fernet keys; documented with the cleanup command. An upgrade refuses to
  regenerate the Fernet keys (lost keys = unreadable connection secrets). GitOps/`helm template`
  users are told to use `existingSecret`s.
- **No secret checksums in annotations** (a hash of a weak admin password could be brute-forced);
  the settings ConfigMap's checksum is used (nothing secret in it). The OIDC client secret is a
  mounted file (`FORKLIFT_OIDC_CLIENT_SECRET_FILE`), not an env var.
- **Single sign-on only on the gateway's web pods.** The gateway refuses unknown `FORKLIFT_OIDC_*`
  names, and `FORKLIFT_OIDC_ISSUER` alone turns SSO on (there is no `..._ENABLED`); the
  dispatcher/publisher/sweeper/bootstrap never sign anyone in, so they get no OIDC settings and
  no client secret (OIDC's doc says the same).
- **`networkPolicies.storeEgress.mode` must be chosen for an external store** (no guessing; a
  clear error lists the three modes). With the bundled RustFS it defaults to `podSelector`.
- **No rule without peers.** Helm's `required` does not fail on empty lists, and an egress rule
  with ports but no `to` admits every address: every peer list is checked with an explicit
  `fail`, and a render test asserts that no policy in any example has an empty `from`/`to` or an
  unbounded `0.0.0.0/0`.
- **"The internet"** = `0.0.0.0/0` except private, CGNAT, loopback, link-local (cloud metadata),
  multicast, reserved ranges; used for webhooks (443, and 80 with `allowHttp`), the OIDC default
  (443), the egress proxy's upstream and, in proxy mode, the gateway processes' store traffic.
  The SSRF interplay is documented: an allow-listed internal webhook host also needs
  `networkPolicies.dispatcher.extraEgress`.
- **KEDA query** `SELECT COUNT(*) FROM core_job WHERE status IN ('queued', 'running') AND lane IN
  (...)` with `targetQueryValue` = concurrency: running jobs count, so scale-down does not take
  pods that still work. Table/columns checked against the gateway's migration and app label (a
  render test reads `0001_initial.py`, `choices.py`, `apps.py`); on k3s the table is `core_job`
  (migrations ran). Lane names are validated before they go into the SQL.
- **Bundled RustFS endpoint is the Service's FQDN** (`<rel>-rustfs.<ns>.svc.<clusterDomain>:9000`):
  Squid does not resolve single-label names (`dns_defnames off`), found on k3s in proxy mode.
- **Egress proxy = EDGE's `squid.conf`** (identical directives; a render test diffs them) plus
  an `allow.conf` in exactly the format EDGE's `start.sh` writes, rendered from
  `egressProxy.allow` (URLs, like `FORKLIFT_EGRESS_PROXY_ALLOW`), **except `dstdomain -n`**
  (see "Findings"). Health check = EDGE's (a request from 127.0.0.1, not logged).
- **`shareProcessNamespace: true` for dispatcher, publisher, sweeper**: their commands install no
  SIGTERM handler, and as PID 1 they ignored SIGTERM until the grace period (seen on k3s: 30 s
  in Terminating). With the pause process as PID 1 they stop at once; safe, since the next pass
  takes up whatever was cut off.
- **`enableServiceLinks: false` everywhere**: a Service named `<rel>-mcp` would otherwise inject
  `FORKLIFT_MCP_PORT=tcp://...`, colliding with the proxy's own setting.
- **Probes send `Host: localhost`** (always in `FORKLIFT_ALLOWED_HOSTS`, with the ingress host,
  the public URL's host and both Services' names).
- **No ServiceMonitors**: the services expose no metrics endpoint yet (design §10.2 mentions them
  as optional); left out rather than shipping a monitor for nothing.
- **sandboxed** follows the brief (`runtimeClassName`, `--max-jobs 1`). Documented caveats: the
  sandbox is per pod (a restarted container keeps the pod's gVisor Sentry / Kata VM), and the
  kubelet's restart back-off (10 s doubling to 5 min) also applies to exit 0, so a group with
  many short jobs needs more replicas. A KEDA `ScaledJob` (one Kubernetes Job per run) would be
  the stronger option later.
- Images: `image.registry` `ghcr.io/cornyhorse` + `web`/`worker`/`mcp` names, tag = appVersion,
  optional digests; third-party images pinned (`rustfs/rustfs:1.0.1`, `ubuntu/squid:6.6-24.04_edge`).
  CI tools pinned by digest: `alpine/helm:4.3.0@sha256:a6cf5459...`,
  `ghcr.io/yannh/kubeconform:v0.8.0@sha256:faffaf43...`, `rancher/k3s:v1.36.4-k3s1@sha256:edad48e1...`.

## OIDC settings the chart uses (please reconcile)

Read from `services/web/src/forklift_web/oidc/config.py` (OIDC's work in progress) and
`docs/platform/oidc.md`; a render test fails if `config.NAMES` and the chart drift apart.

| Value | Variable (gateway web pods only) |
|---|---|
| `gateway.oidc.issuer` | `FORKLIFT_OIDC_ISSUER` (turns SSO on; set only when `gateway.oidc.enabled`) |
| `gateway.oidc.clientId` | `FORKLIFT_OIDC_CLIENT_ID` |
| `gateway.oidc.clientSecret.existingSecret` / `.key` (default `client-secret`) | mounted at `/etc/forklift/oidc/client-secret`; `FORKLIFT_OIDC_CLIENT_SECRET_FILE=/etc/forklift/oidc/client-secret` (never `FORKLIFT_OIDC_CLIENT_SECRET`) |
| `gateway.oidc.scopes` (list) | `FORKLIFT_OIDC_SCOPES` (space-separated) |
| `gateway.oidc.caBundle.configMap` / `.key` | mounted; `FORKLIFT_OIDC_CA_FILE=/etc/forklift/oidc-ca/ca.crt` |
| `gateway.oidc.passwordSignIn` (default true) | `FORKLIFT_PASSWORD_SIGN_IN` |
| `gateway.oidc.settings` (map; schema enum) | `FORKLIFT_OIDC_TOKEN_AUTH_METHOD`, `_PROVIDER_NAME`, `_USERNAME_CLAIM`, `_ROLE_CLAIM`, `_ADMIN_GROUPS`, `_AUTHOR_GROUPS`, `_OPERATOR_GROUPS`, `_VIEWER_GROUPS`, `_DEFAULT_ROLE`, `_CREATE_USERS`, `_SYNC_ROLE`, `_LINK_VERIFIED_EMAIL`, `_USERINFO`, `_ALGORITHMS`, `_ALLOW_HTTP`, `_PROVIDER_LOGOUT` |

Redirect URI documented as `<publicUrl>/accounts/oidc/callback/` (from `forklift_web/urls.py`).
If OIDC renames anything: update the enum in `values.schema.json` (`OIDC_SETTINGS` list) and
`forklift.gatewayOidcEnv` in `templates/_helpers.tpl`.

## Findings for other areas (not changed by me)

1. **EDGE, `deploy/compose/egress-proxy/start.sh`**: it writes `acl ... dstdomain <host>` without
   `-n`. Squid's documentation for `dstdomain`: "a reverse lookup is tried if an IP-based URL is
   used and no match is found; `-n` disables it". So a request for `http://<any IP>:<store port>/`
   (or `CONNECT <IP>:443`) is allowed when that IP's PTR record names the store, and whoever owns
   the IP chooses its PTR record. A compromised engine (Landlock lets it reach the proxy's port)
   could use that to send data anywhere on the store's port. Fix: `dstdomain -n` (the chart does
   this; the render test pins it). Everything else in the chart's Squid configuration matches
   EDGE's `squid.conf` directive for directive (tested), including the image tag and uid 13.
2. **Lead (Compose) / gateway area**: `forklift-web dispatch`, `publish` and `sweep_retention`
   install no SIGTERM handler; in Compose (no `init: true` on those services) `docker compose
   stop` waits for its timeout and then kills them. Either `init: true` on those three services or
   a handler that ends the loop after the current pass. The chart works around it
   (`shareProcessNamespace`).
3. **Shared web venv**: `$S/webvenv` lacks PyJWT, so with OIDC's in-progress tree three existing
   tests (`test_commands.py::test_health_checks`, `::test_forklift_web_entry_points`,
   `test_workers.py::test_worker_tokens`) fail on `import jwt`. Not related to my change: the
   whole suite passes on a clean copy of HEAD plus my change (below).

## Exact text for the lead's shared files

### CHANGELOG.md (under `## [Unreleased]` → `### Added`, after the platform entries)

```markdown
- **Helm chart** (`deploy/helm/forklift`, milestone M4; guide in `docs/platform/helm.md`). The
  Compose stack on Kubernetes: the gateway (public port 8080 behind an optional Ingress, the
  workers' port 8081 on a Service of its own that only workers may reach), the dispatcher,
  publisher and sweeper with Compose's commands and heartbeat health checks, and one worker
  Deployment per `workers` entry (lanes, concurrency, resources, scratch `emptyDir` with a size
  limit, drain on SIGTERM) with the isolation profiles `standard`, `no-network` (a Localhost
  seccomp profile the chart ships: RuntimeDefault plus `unshare` for user and network namespaces
  only) and `sandboxed` (`runtimeClassName`, one job per process); optional KEDA autoscaling of a
  worker group on the queued and running jobs of its lanes, a bundled RustFS for small
  installations, the remote MCP proxy under `/mcp`, and single sign-on settings. Bootstrap runs as
  Helm hooks before every install and upgrade, before the gateway rolls: migrations, the first
  admin, the bucket's CORS rule, and the workers' token, which the chart generates into a Secret,
  keeps across upgrades and registers with the new `forklift-web create_worker_token --from-file`
  (idempotent; an admin-made token from an existing Secret works too). Secrets come only from
  Secrets; generated ones are kept, and an upgrade refuses to make new `FORKLIFT_SECRETS_KEYS`.
  Every pod meets Pod Security "restricted". NetworkPolicies deny everything by default, then:
  workers get no ingress and reach only DNS, the gateway's internal port and the object store,
  by pod selector, by CIDR, or through an allow-listing Squid egress proxy (the workers'
  `FORKLIFT_STORE_PROXY`); the dispatcher sends webhooks only to public addresses; the MCP proxy
  reaches only the gateway's public port. A strict values schema refuses values that cannot
  work, with the reason. Tested by `helm lint --strict`, `kubeconform -strict` (Kubernetes 1.30
  and 1.36, KEDA's CRDs included), render tests of these invariants for every example values
  file, and a real install on k3s whose network-policy controller enforces the policies: a CSV
  job end to end, a schema on a no-network worker, the policies refusing what they should, and
  an upgrade to the proxy mode. Published as `oci://ghcr.io/cornyhorse/charts/forklift` on
  `chart-v*` tags (`.github/workflows/helm.yml`).
```

### docs/design/platform.md §10.2 (replace the YAML sketch and the paragraph after it)

````markdown
### 10.2 Helm

`deploy/helm/forklift` (the main values; [docs/platform/helm.md](../platform/helm.md) has all of
them):

```yaml
publicUrl: https://forklift.example.org
gateway:
  replicas: 2
  ingress: {enabled: true, host: forklift.example.org, tls: true}
  internalService: {port: 8081}            # workers only; never routed by the ingress
  oidc: {enabled: false, issuer: "", clientId: "", clientSecret: {existingSecret: ""}}
workers:
  - name: interactive
    lanes: [interactive]
    replicas: 2
    concurrency: 2
    resources: {limits: {cpu: "1", memory: 1Gi}}
    scratch: {sizeLimit: 2Gi}
  - name: batch
    lanes: [batch]
    autoscaling: {keda: {enabled: false, minReplicas: 1, maxReplicas: 10}}   # queued + running jobs
    resources: {limits: {cpu: "4", memory: 16Gi}}
    scratch: {sizeLimit: 200Gi}             # outputs (and staged inputs up to stageMaxBytes)
    isolation: standard                     # standard | no-network | sandboxed
    runtimeClassName: ""                    # e.g. gvisor for the sandboxed profile
postgres: {host: postgres.example.org, existingSecret: forklift-db}
objectStore: {endpoint: https://s3.example.org, bucket: forklift, existingSecret: forklift-s3}
rustfs: {enabled: false}                    # a bundled RustFS (StatefulSet + PVC) for small installs
networkPolicies:
  enabled: true
  postgres: {to: [{ipBlock: {cidr: 10.0.12.5/32}}]}
  storeEgress:                              # how workers may reach the object store
    mode: cidr                              # podSelector (in-cluster store) | cidr | proxy
    cidrs: [203.0.113.0/24]
egressProxy: {allow: [https://s3.example.org]}   # proxy mode: Squid allows the store only
image: {registry: ghcr.io/cornyhorse}       # forklift-web, forklift-worker, forklift-mcp
mcp: {enabled: false}
```

The chart is published as an OCI artifact (`oci://ghcr.io/cornyhorse/charts/forklift`, on
`chart-v*` tags). It creates no cloud-specific resources: Postgres and the store are external
by default, and secrets come from existing Secrets (or an External Secrets operator) or are
generated once by the chart.

As built: every pod meets Pod Security "restricted" (uid 10001, read-only root file system, no
capabilities, no service-account token, seccomp `RuntimeDefault`, or for `no-network` workers a
Localhost profile the chart ships: `RuntimeDefault` plus `unshare` for user and network
namespaces only). A default-deny NetworkPolicy covers every pod of the release; on top of it the
workers get no ingress and reach only DNS, the gateway's internal port and the store (by pod
selector, by CIDR, or only through an allow-listing Squid that the workers use through
`FORKLIFT_STORE_PROXY`); the internal port admits only the workers; the dispatcher reaches
webhook receivers on public addresses only; the MCP proxy reaches only the gateway's public
port. Bootstrap is the Compose `init` as Helm hooks: before every install and upgrade, before
the gateway rolls, a Job migrates, creates the first admin, registers the workers' token
(generated into a Secret, kept across upgrades, `create_worker_token --from-file`) and sets the
bucket's CORS rule; with the bundled RustFS the bucket and its rule are set after the install.
KEDA scales a worker group on the queued and running jobs of its lanes. No ServiceMonitors yet:
the services have no metrics endpoint. CI checks it with `kubeconform` (Kubernetes 1.30 and
1.36), render tests of these invariants and a real install on k3s, whose network-policy
controller enforces the policies.
````

### docs/design/platform.md §13 (the M4 row)

```markdown
| M4 | Helm, hardening, authoring | Chart with NetworkPolicies and isolation profiles; schema editor; connections admin (S3-compatible, localfs); datasets, schedules, retention, audit. Built: all of it (the chart is `deploy/helm/forklift`), and a sign-in rate limit |
```

(If §10.1's "egress-proxy profile ... not built yet" changes with EDGE's work, nothing in §10.2
depends on that wording.)

### docs/DOCUMENTATION_INDEX.md (under "Platform (`platform/`)")

```markdown
- [helm.md](platform/helm.md): the Helm chart: install, values, secrets, bootstrap hooks and upgrades, isolation profiles, NetworkPolicies and the store-egress modes, KEDA, the MCP proxy, OIDC, tests, troubleshooting, releases
```

### Root README.md (optional, wherever Compose is mentioned)

```markdown
On Kubernetes, use the Helm chart in `deploy/helm/forklift` ([docs/platform/helm.md](docs/platform/helm.md)).
```

### test.yml / fast-test.yml

Nothing required: the chart's CI is `.github/workflows/helm.yml`. `fast-test.yml` already runs
`black`/`isort --check` over `deploy/`, which covers the chart's Python (clean). Note for the
release workflow: `publish.yaml` should keep ignoring `chart-v*` tags (it checks `v*` tags against
the engine's version); `helm.yml`'s `publish` job handles them.

## How I verified (commands and results)

All with my own names and ports (cluster `forklift-helm-smoke`, API 127.0.0.1:6440, forwards
18400/18401), helpers in `$S` (`helm-bin/helm`: the pinned alpine/helm image; `helm-bin/kubeconform`:
v0.8.0 built with `go install`, because GHCR blob downloads are blocked here).

1. `create_worker_token`: `$S/webvenv/bin/python $S/helm-w_test.py tests/test_create_worker_token.py
   tests/test_commands.py tests/test_workers.py tests/test_internal_api.py
   --cov=forklift_web.core.management.commands.create_worker_token --cov-branch
   --cov-fail-under=100` → my 16 tests (9 functions, one with 8 cases) pass, the module has 100%
   lines and branches (the 3 failures in that run are the PyJWT ones above). The **whole gateway
   suite** on a clean copy (`git archive HEAD` + my two files, `-m "not browser"`): **2027
   passed, 22 deselected (the Playwright browser tests), total coverage 100.00%** (7454
   statements, 1466 branches).
2. Render tests: `python -m pytest deploy/helm/tests` → **145 passed**.
3. `deploy/helm/tests/lint.sh` → `helm lint --strict` clean for the defaults and all 8 ci files;
   kubeconform `-strict`: **224 resources, 224 valid, 0 skipped** for Kubernetes 1.30.0 and 1.36.0.
4. Rendered Squid configuration parsed by `squid -k parse` in `ubuntu/squid:6.6-24.04_edge` as uid
   13 with a read-only root: no errors. Allow/deny decisions also checked against a running Squid
   (CONNECT to the allowed host passes, other hosts, ports and IPs get 403; query strings stripped
   from the log).
5. **k3s** (`deploy/helm/tests/k3s_smoke.sh`, three full runs, the last after every change):
   rancher/k3s v1.36.4-k3s1 in Docker, namespace with `pod-security.kubernetes.io/enforce=restricted`
   (every chart pod was admitted), images built from HEAD + my command + EDGE's worker
   (`--store-proxy`), the local no-apt worker variant.
   - Install: bootstrap Job logs `Applying core.0006_publishing... OK`, `Created the admin
     'admin'.`, `Registered worker token fkw_XXXXXXXX... (helm) from /run/secrets/worker-token.`;
     then every rollout, then the store-setup Job (bucket + CORS).
   - A CSV upload (presigned PUT through the store's forward) → `run` on the **batch** group →
     succeeded, `valid_rows == 2`, a Parquet artifact (`PAR1`) downloaded; `generate_schema` on
     the **no-network** group → succeeded. Its worker logged `"network_namespace": true,
     "landlock_abi": 7, "tcp_restricted": true` (the shipped seccomp profile works; Landlock works
     under containerd's RuntimeDefault).
   - NetworkPolicies biting (TCP connects from inside the pods): both worker groups reach
     `gateway-internal:8081` and `rustfs:9000` and are refused PostgreSQL 5432, the gateway's public
     8080 and the Kubernetes API 443; the gateway reaches PostgreSQL and RustFS; a pod in another
     namespace reaches none of 8080, 8081, 9000; a pod labelled like ingress-nginx (in namespace
     `ingress-nginx`) reaches 8080 but not 8081 or 9000.
   - Upgrade to proxy mode with changed log levels (every pod rolled; the bootstrap Job ran first
     while the old pods served): `Worker token ... is already registered (helm); nothing to do.`,
     the worker token and `FORKLIFT_SECRETS_KEYS` unchanged; both jobs succeeded again with the
     workers' staging GETs and output PUTs in Squid's log (query strings stripped); workers reach
     the proxy and 8081 but no longer RustFS; through Squid, `GET http://<rustfs FQDN>:9000/health`
     → 200, and `CONNECT <rustfs>:9000`, `GET http://<gateway>:8080/`, `CONNECT
     <gateway-internal>:8081`, `GET http://postgres...:5432/`, `CONNECT example.org:443` → 403.
   - The final run (after the last chart change) passed every stage above, and showed the old
     dispatcher/publisher/sweeper pods gone at once on the upgrade (`shareProcessNamespace`).
     After it I only added `-v` to the script's `docker rm -f` (the cluster's anonymous volumes
     go with it; shellcheck clean).
6. Style: `black --check`, `isort --check-only --profile black`, `flake8 --max-line-length=99
   --extend-ignore=E203,W503` clean for `deploy/helm` and my two `services/web` files;
   `shellcheck` clean for `lint.sh` and `k3s_smoke.sh`.

### What the local environment needed (the script handles it; harmless in CI)

- Kubernetes 1.35+ refuses cgroup v1: the script adds `--kubelet-arg=fail-cgroupv1=false` when
  Docker reports cgroup v1 (this host; CI runners are v2).
- This Docker does not let a privileged container lower `oom_score_adj` (every pod failed with
  "failed to update /proc/self/oom_score_adj"): `k3s/containerd-oom.toml` sets containerd's
  `restrict_oom_score_adj = true`.
- The shared disk is over 90% used: absolute eviction thresholds (`nodefs.available<1Gi`) and no
  image GC, or the node is tainted `disk-pressure`.
- `/var/lib/kubelet` is a volume of the k3s image, which hides a bind-mounted file: the seccomp
  profile is copied in after start.
- `FORKLIFT_K3S_CA_FILE` mounts the session proxy's CA so k3s can pull its system images.

## What only CI can verify (not run here)

- The real `forklift-worker` image (tini, ODBC drivers from apt; apt is blocked here) and images
  built from the merged tree (OIDC's gateway, all agents' changes) under the chart.
- `kubeconform` from its container image (here the same version built from source) and the k3s
  job on a cgroup v2 runner with AppArmor (the workflow sets
  `kernel.apparmor_restrict_unprivileged_userns=0` for the no-network group).
- The `publish` job (needs a `chart-v0.1.0` tag and GHCR write access).
- Not exercised on a real cluster anywhere (render-tested and schema-validated only): KEDA
  scaling (no KEDA installed), the `sandboxed` profile (no gVisor/Kata), the MCP proxy (its image
  does not exist yet), OIDC sign-in, a real ingress controller (impersonated by a labelled pod),
  the `cidr` mode against a real external store, IPv6 blocks.

## Cleanup

The k3s cluster is deleted by the script (and its anonymous volumes, which I also removed by
hand for the runs before that fix); I removed my images (`forklift-web:helm`,
`forklift-worker:helm`, `rancher/k3s`, `alpine/helm`, `ubuntu/squid`), my venv, the Go module
and build caches and the scratch copies. Left in `$S`: small runner scripts (`helm-*.py`,
`helm-*.sh`) and the run logs (`helm-smoke-final.log`, `helm-w-clean.log`). The shared test
services were not touched; the gateway tests used their own database name (`forklift_web_helm`).
