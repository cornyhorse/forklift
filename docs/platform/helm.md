# Kubernetes: the Helm chart

`deploy/helm/forklift` runs the platform on Kubernetes the way the [Docker Compose
stack](../../deploy/compose/README.md) runs it on one machine: the same images, processes,
commands, settings and bootstrap, with the trust boundary of the design
([platform.md](../design/platform.md) §6.2, §8, §10.2) enforced by NetworkPolicies and Pod Security
"restricted" instead of Docker networks.

| Workload | Image | What it does | Reachable on |
|---|---|---|---|
| `gateway` (Deployment, 2 replicas) | `forklift-web` | The UI and `/api/v1` on port 8080; the workers' `/internal/v1` on port 8081 (the gateway routes requests by the port their connection arrived on) | Service `<release>-gateway` (8080, from the ingress controller); Service `<release>-gateway-internal` (8081, from the workers only) |
| `dispatcher` | `forklift-web`: `dispatch --every=10` | Queues the runs of due [schedules](schedules.md), sends [webhooks](webhooks.md) | nothing |
| `publisher` | `forklift-web`: `publish --every=5` | [Publishes](publishing.md) runs to their s3 destinations | nothing |
| `sweeper` | `forklift-web`: `sweep_retention --every=3600` | Deletes what retention says has expired | nothing |
| `worker-<group>` (one Deployment per `workers` entry) | `forklift-worker` | Leases jobs of its lanes, runs the engine in a sandbox per job | nothing: workers have no ingress at all |
| `bootstrap` (hook Job) | `forklift-web` | Migrations, the first admin, the workers' token, the bucket's CORS rule | nothing |
| `rustfs` (StatefulSet, optional) | `rustfs/rustfs` | A bundled S3-compatible store for small installations | 9000, from the chart's pods (and browsers through its ingress) |
| `mcp` (optional) | `forklift-mcp` | The remote MCP endpoint at `/mcp` | 8000, from the ingress controller |
| `egress-proxy` (optional) | `ubuntu/squid` | The workers' only way out to an external store in proxy mode | 3128, from the workers only |

Resource names start with the release's name (`forklift-gateway` for a release named
`forklift`; a release whose name does not contain `forklift` gets `<release>-forklift-...`).
Like in Compose, the dispatcher, publisher and sweeper are healthy while the heartbeat file their
command touches after every good pass is younger than a minute (the sweeper: two of its
intervals). PostgreSQL and the object store are external by default: the chart creates no
cloud-specific resources.

## Requirements

- Kubernetes 1.30 or newer (tested on 1.36 with k3s), and a CNI that **enforces**
  NetworkPolicies (Calico, Cilium, the k3s network-policy controller, ...). Without one the
  policies are accepted and do nothing.
- PostgreSQL (16 is what the tests use) and an S3-compatible store with one bucket.
- An ingress controller for the UI (the default policy admits ingress-nginx; see
  [NetworkPolicies](#networkpolicies)).
- Kernels with Landlock (Linux 5.13 or newer with `landlock` in the LSM list; TCP rules from
  6.7): the workers run with `--landlock required` and refuse to start without it. Set
  `workerDefaults.landlock: auto` to run (with a warning) on nodes without it.
- For the `no-network` profile: the chart's seccomp profile on every node that runs those
  workers, and unprivileged user namespaces ([below](#no-network)).
- Optional: KEDA 2.x (autoscaling), a sandboxed runtime such as gVisor or Kata (`sandboxed`).

## Install

The secrets the chart reads and does not make (it never puts a secret into `values.yaml`):

```bash
kubectl create namespace forklift
kubectl label namespace forklift pod-security.kubernetes.io/enforce=restricted   # every pod passes
kubectl -n forklift create secret generic forklift-db --from-literal=password='...'
kubectl -n forklift create secret generic forklift-s3 \
    --from-literal=access-key-id='...' --from-literal=secret-access-key='...'
```

The least a production install needs (`values.yaml`):

```yaml
publicUrl: https://forklift.example.org
postgres:
  host: postgres.databases.svc.cluster.local
  existingSecret: forklift-db
objectStore:
  endpoint: https://s3.example.org
  existingSecret: forklift-s3
gateway:
  ingress: {enabled: true, className: nginx, host: forklift.example.org}
networkPolicies:
  postgres:
    to: [{namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: databases}}}]
  storeEgress: {mode: cidr, cidrs: [203.0.113.0/24]}
```

From the OCI registry (once a `chart-v*` tag has published it, see [Releases](#releases)):

```bash
helm install forklift oci://ghcr.io/cornyhorse/charts/forklift --version 0.1.0 \
    --namespace forklift --values values.yaml --wait --timeout 10m
```

From a checkout of this repository:

```bash
helm install forklift deploy/helm/forklift --namespace forklift --values values.yaml --wait --timeout 10m
```

`NOTES.txt` then says how to reach the UI and read the first admin's password. The chart refuses
values that cannot work, with the reason: no `postgres.host`, an external store without a
`networkPolicies.storeEgress.mode`, a `sandboxed` group without a runtime class, the `sql` lane
on a `no-network` group, unknown keys (the schema is strict), a secret-looking name in
`gateway.extraEnv`, and so on. `deploy/helm/forklift/ci/*-values.yaml` are examples of every
mode.

## Values

Every block is documented in [values.yaml](../../deploy/helm/forklift/values.yaml); the schema
([values.schema.json](../../deploy/helm/forklift/values.schema.json)) rejects anything else. The
settings reach the processes under the names Docker Compose uses (`FORKLIFT_*`, design §10.3).

| Block | What it sets |
|---|---|
| `image` | `registry` (default `ghcr.io/cornyhorse`), `tag` (default: the chart's `appVersion`), `pullPolicy`, `pullSecrets`; `web`, `worker`, `mcp` each with `name`, `tag`, `digest` |
| `publicUrl` | `FORKLIFT_PUBLIC_URL`, the CSRF origin and the bucket's CORS origin. Default: `https://<gateway.ingress.host>` |
| `secrets` | `existingSecret` with `secret-key` (`FORKLIFT_SECRET_KEY`) and `secrets-keys` (`FORKLIFT_SECRETS_KEYS`); default: generated once ([Secrets](#secrets)) |
| `admin` | `username`, `email`, `existingSecret` (key `password`) for the first admin; default: a generated password |
| `workerToken` | `existingSecret` (key `token`) for an admin-made token, `name`, `register`; default: generated and registered |
| `postgres` | `host`, `port`, `database`, `user` (or `userKey` in the Secret), `existingSecret`, `passwordKey`, `sslmode`, `connMaxAge` (`FORKLIFT_DB_*`) |
| `objectStore` | `endpoint`, `publicEndpoint`, `workerEndpoint`, `bucket`, `region`, `addressingStyle`, timeouts, `existingSecret` with `access-key-id` and `secret-access-key`, `purposeCredentials` (`upload`, `download`, `delete`: design §7.1), `workerCaBundle` (`FORKLIFT_S3_*`, `--ca-file`) |
| `gateway` | `replicas`, `allowedHosts`, `csrfTrustedOrigins`, `corsOrigins`, `behindTlsProxy`, `secureCookies`, `trustedProxies`, `sessionSeconds`, `passwordMinLength`, `loginRedirectUrl`, `apiDocs`, `logLevel`, `webhooks` (`allowedHosts`, `allowHttp`), `server` (gunicorn `workers`, `threads`, `timeoutSeconds`), `extraEnv` (other non-secret `FORKLIFT_*` settings), `extraSecretEnv` (`[{name, secretName, key}]`), `oidc`, `ingress`, `service`, `internalService`, `resources`, `pdb`, scheduling |
| `dispatcher`, `publisher`, `sweeper` | `everySeconds`, `resources`, scheduling; `publisher.replicas` (several may run) |
| `bootstrap` | `backoffLimit`, `activeDeadlineSeconds`, `keepSucceededJobs`, `resources` |
| `workers`, `workerDefaults` | One Deployment per entry: `name`, `lanes`, `replicas`, `concurrency`, `isolation`, `sandboxedEngine`, `runtimeClassName`, `seccompProfile`, `landlock`, `stageMaxBytes`, `maxJobSeconds`, `drainSeconds`, `killGraceSeconds`, `logLevel`, `resources`, `scratch` (`sizeLimit`, `medium`), `autoscaling.keda`, `pdb`, `settings` (other `FORKLIFT_WORKER_*`), `egress` (extra NetworkPolicy rules), scheduling. `workerDefaults` holds the defaults of every key |
| `keda` | The PostgreSQL login KEDA reads the queue with |
| `rustfs` | `enabled`, `image`, `existingSecret`, `persistence`, `ingress`, `resources` |
| `mcp` | `enabled`, `replicas`, `allowedHosts`, `caBundle`, `logLevel`, `resources` |
| `egressProxy` | `image`, `replicas`, `allow` (store URLs, as Compose's `FORKLIFT_EGRESS_PROXY_ALLOW`; default: the workers' store endpoint), `resources` |
| `networkPolicies` | `enabled`, `dns`, `ingressController`, `internet`, `postgres.to`, `storeEgress`, per-component `extraIngress` / `extraEgress`, `oidc` |

`gateway.allowedHosts` adds to what the chart always lets the gateway answer to: the ingress
host, the public URL's host, its two Services' names (short, `.<namespace>`, `.svc` and the
cluster domain) and `localhost` (the probes send `Host: localhost`).

## Secrets

Secrets reach the processes only from Kubernetes Secrets (`secretKeyRef`, or a mounted file);
nothing secret is ever in a ConfigMap, an annotation, a literal environment value or NOTES. The
render tests check that for every example values file.

| Secret | Made by | Keys | Read by |
|---|---|---|---|
| `postgres.existingSecret` | you | `password` (and `userKey`) | gateway processes, bootstrap, KEDA |
| `objectStore.existingSecret` | you (or the chart, for the bundled RustFS) | `access-key-id`, `secret-access-key` | gateway processes, bootstrap, RustFS |
| `<release>-secrets` or `secrets.existingSecret` | the chart once, or you | `secret-key`, `secrets-keys` | gateway processes, bootstrap |
| `<release>-admin` or `admin.existingSecret` | the chart once, or you | `password` | the bootstrap Job's `admin` step only |
| `<release>-worker-token` or `workerToken.existingSecret` | the chart once, or you | `token` | the workers (a file in `/run/secrets`, outside what the engine may read), the bootstrap Job's `worker-token` step |
| `gateway.oidc.clientSecret.existingSecret` | you | `client-secret` | the gateway's web pods only, as a file (`FORKLIFT_OIDC_CLIENT_SECRET_FILE`) |

Generated values are random: a 64-character signing key, a Fernet key from 32 random bytes, a
24-character admin password, a worker token in `create_worker_token`'s own format (`fkw_` and 43
URL-safe characters, 256 bits). They are kept across upgrades with `lookup`, and **an upgrade
refuses to make new `FORKLIFT_SECRETS_KEYS`** when `<release>-secrets` is missing or unreadable
(new keys would make every stored connection secret unreadable): restore it, or name one with
`secrets.existingSecret`. Back that Secret up with the database.

- `helm template`, client-side `--dry-run` and GitOps tools that render without a cluster cannot
  read Secrets, so they would generate new values on every render: with them, set every
  `existingSecret` (or use an External Secrets operator) and keep the chart from generating
  anything.
- The generated Secrets are hooks (they must exist before the pre-install Job), so
  `helm uninstall` leaves them, on purpose. Delete them when the installation goes for good:
  `kubectl -n forklift delete secret,serviceaccount,networkpolicy -l app.kubernetes.io/instance=<release>`.
- Rotating: change the Secret, then restart the pods that read it (`kubectl rollout restart`);
  the chart puts no checksum of a Secret into an annotation. The worker reads its token file for
  every request. `FORKLIFT_SECRETS_KEYS` rotates with `forklift-web rotate_secrets` (new key
  first, keep the old one until it has run).

## Bootstrap and upgrades

What Compose's one-shot `init` does, as Helm hooks; every step is safe to repeat and runs again on
every upgrade.

| Order | Hook | What |
|---|---|---|
| -30 | pre-install, pre-upgrade | ServiceAccount `<release>-bootstrap` (no token mounted) |
| -20 | pre-install, pre-upgrade | The generated Secrets (kept values) |
| -10 | pre-install, pre-upgrade | The bootstrap Job's NetworkPolicy: DNS, PostgreSQL and (external store) the store, so the Job of an upgrade already runs under the new rules |
| 0 | pre-install, pre-upgrade | Job `<release>-bootstrap`, one container per step, in order: `migrate` (`forklift-web migrate --no-input`), `admin` (`bootstrap_admin --username <admin.username>`: creates the account once, never changes it), `worker-token` (`create_worker_token --name <workerToken.name> --from-file /run/secrets/worker-token`: registers the chart's token, and says "already registered; nothing to do" afterwards), `cors` (external store: `configure_cors --origin <public URL's origin>` and `gateway.corsOrigins`) |
| | (the release) | Deployments, StatefulSet, Services, ... roll out after the Job succeeded: the gateway never runs against an unmigrated database |
| 0 | post-install, post-upgrade | With the bundled RustFS only: Job `<release>-store-setup`, `bucket` (waits up to 5 minutes for RustFS, creates the bucket if missing) and `cors` |

Logs of a step: `kubectl -n forklift logs job/<release>-bootstrap -c migrate`. A succeeded Job is
deleted (`bootstrap.keepSucceededJobs: true` keeps it until the next upgrade); a failed one stays
for its logs, and the release fails: nothing is rolled out. Long migrations need a longer
`--timeout` (Helm's default is 5 minutes). The worker token is registered with the same format
and hashing as a generated one: a revoked or expired token is never registered again (the step
fails and says so), and a token whose prefix another token has is refused.

Upgrades: the hooks run first, then the gateway rolls (one new pod before an old one stops); the
workers drain on SIGTERM: they take no new lease and running jobs may finish for
`drainSeconds` (`terminationGracePeriodSeconds` is that plus `killGraceSeconds` plus 15 s), then
their jobs are handed back and queued again. Gateway and workers may roll in any order (the N/N-1
contract rule, design §10.3).

## Isolation profiles

`isolation` per worker group (design §6.2; what each enforces in the engine is in
[services/worker/README.md](../../services/worker/README.md)):

| Profile | Pod | Worker |
|---|---|---|
| `standard` | seccomp `RuntimeDefault` | `FORKLIFT_WORKER_ISOLATION=standard`, Landlock (`required` by default) |
| `no-network` | seccomp `Localhost`, `seccompProfile.localhostProfile` (default `forklift/worker-no-network.json`) | `FORKLIFT_WORKER_ISOLATION=no-network`: the engine in its own empty network namespace; only staged inputs (`stageMaxBytes` caps them); the `sql` lane is refused |
| `sandboxed` | `runtimeClassName` (required, e.g. `gvisor` or `kata`); seccomp as `sandboxedEngine` needs | `FORKLIFT_WORKER_MAX_JOBS=1`, concurrency 1: one job per process; `sandboxedEngine` (`standard` or `no-network`) is the profile inside the sandbox |

Every worker pod also gets: uid/gid 10001, a read-only root file system, no capabilities, no
privilege escalation, `automountServiceAccountToken: false`, a memory limit, scratch as an
`emptyDir` with `scratch.sizeLimit` (outputs plus staged inputs), `/tmp` as a small memory
`emptyDir`, and the token as a read-only file. `sql`-lane groups need `egress` rules for their
databases (and stay `standard`).

### no-network

Kubernetes' `RuntimeDefault` seccomp profile forbids `unshare`, which the engine's network
namespace needs, so `no-network` groups use a Localhost profile: the chart ships
[files/seccomp/worker-no-network.json](../../deploy/helm/forklift/files/seccomp/worker-no-network.json),
which is containerd 2's `RuntimeDefault` for a pod without capabilities (taken from a running pod
on k3s v1.36.4, amd64) plus one rule: `unshare` allowed only for new user and network namespaces
(its flags may contain no other `CLONE_NEW*` flag). Put it on every node that runs such workers,
at `<kubelet root>/seccomp/forklift/worker-no-network.json` (`/var/lib/kubelet/seccomp/...`): with
your node image, a DaemonSet that copies it, or the Security Profiles Operator (a
`SeccompProfile` resource, then point `seccompProfile.localhostProfile` at the path it reports).
A pod whose profile is missing stays in `CreateContainerError` ("cannot load seccomp profile").
For arm64 nodes, or another runtime, make the profile the same way: run a pod with
`RuntimeDefault` and `capabilities: {drop: [ALL]}`, copy `linux.seccomp` from its container's
`config.json` (under `/run/containerd/io.containerd.runtime.v2.task/k8s.io/<id>/`), and add the
`unshare` rule.

The nodes must also allow unprivileged user namespaces (`user.max_user_namespaces` above 0; on
Ubuntu 23.10 and later `kernel.apparmor_restrict_unprivileged_userns=0`). The worker checks at
start and exits with code 2 when it cannot create the namespace; its first log line says what it
enforces (`"network_namespace": true`, `"landlock_abi": 7`, ...).

### sandboxed

The image runs under the runtime class (gVisor or Kata, installed on the nodes by you) with
`--max-jobs 1`: the worker exits after one job and Kubernetes starts the container again for the
next one. Two things to know: a restarted container keeps its pod, so the sandbox kernel (gVisor's
Sentry, Kata's VM) is per pod, not per job; and the kubelet delays restarts of a container that
keeps exiting (10 s, doubling to 5 minutes, reset after 10 minutes of running), so a group that
runs many short jobs needs more replicas than its concurrency suggests.

## NetworkPolicies

With `networkPolicies.enabled` (the default) the chart makes a **default deny** for every pod of
the release (ingress and egress), then one policy per component that allows only this:

| Pods | Ingress | Egress |
|---|---|---|
| gateway | 8080 from the ingress controller (`networkPolicies.ingressController`, when `gateway.ingress.enabled`) and the MCP proxy; **8081 from the workers only**; `gateway.extraIngress` | DNS; PostgreSQL (`networkPolicies.postgres.to`, required); the store; the OIDC provider (with `gateway.oidc`); `gateway.extraEgress` |
| dispatcher | none | DNS; PostgreSQL; the store; **webhooks: 443 (and 80 with `allowHttp`) to the internet only**; `dispatcher.extraEgress` |
| publisher, sweeper | none | DNS; PostgreSQL; the store (`publisher.extraEgress` for destinations elsewhere) |
| workers | **none at all** | DNS; the gateway's 8081; the store as `storeEgress` says; the group's `egress` |
| bootstrap | none | DNS; PostgreSQL; the store (external store) |
| store-setup | none | DNS; RustFS |
| rustfs | 9000 from the gateway processes, store-setup, the workers (podSelector mode) or the egress proxy (proxy mode), the ingress controller (`rustfs.ingress`); `rustfs.extraIngress` | none |
| mcp | 8000 from the ingress controller; `mcp.extraIngress` | DNS; the gateway's 8080 only |
| egress-proxy | 3128 from the workers only | DNS; the store's port (the internet, or RustFS) |

"DNS" is `networkPolicies.dns` (default: kube-dns pods in `kube-system`, UDP and TCP 53).
"The internet" is `networkPolicies.internet`: `0.0.0.0/0` except the private (10/8, 172.16/12,
192.168/16), shared (100.64/10), loopback, link-local (169.254/16: cloud metadata), multicast and
reserved ranges. Pod and Service networks are normally inside those ranges; if yours are public
addresses, add them to `except`. Add an IPv6 block on dual-stack clusters. No rule of the chart
admits or reaches "everything": a rule without peers is refused at render time.

### The store: three modes

NetworkPolicies match addresses, not host names (design §6.2). `networkPolicies.storeEgress.mode`
says how the workers reach the store:

| Mode | Workers may reach | Enforces | Leaves open |
|---|---|---|---|
| `podSelector` (default with `rustfs.enabled`) | the store's pods (the bundled RustFS, or `podSelector`/`namespaceSelector`) on its port | exactly the store's pods | nothing else |
| `cidr` | `cidrs` on `ports` (default: the endpoints' ports, 443 for https) | the store's address ranges | anything else in those ranges on that port (a shared cloud range is shared with other tenants) |
| `proxy` | only the egress proxy (3128); the proxy only the store's host names and ports (`egressProxy.allow`: CONNECT to the TLS port of an https store, plain requests to the port of an http one), and its pod only the internet (or RustFS) on those ports | the store by name | the proxy's own pod reaches any public address on the store's port, but Squid refuses every other name |

In proxy mode the workers get `FORKLIFT_STORE_PROXY=http://<release>-egress-proxy:3128`: the
supervisor's downloads and uploads and the engine's streamed reads go through it, gateway calls
never, and Landlock lets a streaming engine connect only to the proxy's port. The proxy's
`squid.conf` is the Compose profile's (`deploy/compose/egress-proxy/squid.conf`: nothing cached,
no body limits, query strings stripped from the log, so presigned URLs' signatures are never
logged); its allow file is rendered from `egressProxy.allow` in the format the profile's
`start.sh` writes, with one difference: host names are matched with `dstdomain -n`, so a URL
with an IP address is never allowed because of its reverse DNS name (which whoever controls
that address chooses). A store on a private address also needs `networkPolicies.egressProxy.to`
(the default "internet" excludes private ranges). The gateway processes hold the store's
credentials and call the store themselves in every mode; in proxy mode they may reach
`storeEgress.gatewayTo` (default: the internet on the store's ports).

With an external store and NetworkPolicies on, you must choose the mode: the chart refuses to
guess, because the safe answers depend on where the store is.

### Webhooks and SSRF

The gateway already refuses webhook URLs whose host resolves to a private, loopback, link-local,
shared or reserved address, pins the connection to the address it checked, and follows no
redirects ([webhooks.md](webhooks.md#where-webhooks-may-point)). The dispatcher's policy enforces the
same in the network: even a bug in that check could not reach the cluster, the nodes or the cloud
metadata service. The two have to agree for internal receivers: a host in
`gateway.webhooks.allowedHosts` also needs a rule in `networkPolicies.dispatcher.extraEgress`
(its address and port), or its deliveries fail with a connection error.

### OIDC

With `gateway.oidc.enabled`, the gateway's pods (and only they: the other processes have no
single sign-on settings) may reach `networkPolicies.oidc.to` on `oidc.ports`, by default the
internet on 443. The default suits a provider outside the cluster (Entra ID, Okta, Google,
Keycloak behind its own ingress) without knowing its addresses; a provider in the cluster needs
its pods as `to` (and its port), since the internet excludes the cluster's ranges. The provider's
discovery document, keys and token endpoint are all fetched from the gateway.

### Kubernetes details

- Probes come from the kubelet on the node, which network-policy implementations admit.
  `kubectl port-forward` reaches a pod through the kubelet as well, so it works under the
  policies (the smoke test uses it).
- Exposing the gateway without an ingress controller (`gateway.service.type: LoadBalancer` or
  `NodePort`) needs `networkPolicies.gateway.extraIngress` for the clients' address ranges on
  port 8080: the policy admits only the ingress controller and the MCP proxy otherwise. The same
  holds for in-cluster API clients (an Airflow namespace, say).
- KEDA's operator connects to PostgreSQL from its own namespace: the chart's policies do not
  cover it (they select only the release's pods), but a policy on your database may need to
  admit it.
- `enableServiceLinks: false` on every pod: Kubernetes would otherwise set variables such as
  `FORKLIFT_MCP_PORT=tcp://...` for Services named `forklift-*`, which collide with settings.
- The dispatcher, publisher and sweeper pods share their process namespace: their commands
  install no SIGTERM handler, and as PID 1 they would ignore SIGTERM until the grace period ends;
  with the pod's pause process as PID 1 they stop at once (what they were doing is taken up again
  by the next pass).

## Autoscaling with KEDA

`workers[].autoscaling.keda.enabled` makes a `ScaledObject` (and its `TriggerAuthentication`) for
the group; the Deployment then has no `replicas` of its own. The trigger is KEDA's PostgreSQL
scaler with

```sql
SELECT COUNT(*) FROM core_job WHERE status IN ('queued', 'running') AND lane IN ('batch')
```

(the gateway's job table; the group's lanes) and `targetQueryValue` = `jobsPerReplica`
(default: the group's `concurrency`), so KEDA aims for one pod per `concurrency` jobs in flight,
between `minReplicas` (0 scales to zero when nothing is queued or running) and `maxReplicas`.
Running jobs count too, so that a pod is not taken away while it works on a job the queue no
longer shows; scale-down is one pod a minute after `cooldownPeriod`. A pod that is taken away
anyway drains (`drainSeconds`): give batch groups a `drainSeconds` as long as their jobs.

KEDA needs a PostgreSQL login: `keda.postgres.existingSecret` and `keda.postgres.user`, by default
the gateway's. Better a login that can only count jobs:

```sql
CREATE ROLE forklift_keda LOGIN PASSWORD '...';
GRANT CONNECT ON DATABASE forklift TO forklift_keda;
GRANT SELECT ON core_job TO forklift_keda;
```

## The remote MCP proxy

`mcp.enabled` runs `forklift-mcp` (2 replicas, port 8000, `GET /healthz` for the probes) and
routes `/mcp` on the gateway's ingress host to it. It is a client of the gateway's public port
(`FORKLIFT_MCP_API_URL=http://<release>-gateway:8080`), with each caller's own API token; it holds
no credentials. `FORKLIFT_MCP_ALLOWED_HOSTS` (the DNS-rebinding guard) is the ingress host, its
Service's names and `mcp.allowedHosts`. Its policy admits the ingress controller and lets it reach
the gateway's public port and nothing else. Clients connect to
`https://<gateway.ingress.host>/mcp` with `Authorization: Bearer <API token>`
([mcp.md](mcp.md)).

## Single sign-on (OIDC)

```yaml
gateway:
  oidc:
    enabled: true
    issuer: https://login.example.org/realms/forklift     # FORKLIFT_OIDC_ISSUER
    clientId: forklift                                    # FORKLIFT_OIDC_CLIENT_ID
    clientSecret: {existingSecret: forklift-oidc, key: client-secret}
    scopes: [openid, email, profile]                      # FORKLIFT_OIDC_SCOPES
    caBundle: {configMap: "", key: ca.crt}                # FORKLIFT_OIDC_CA_FILE
    passwordSignIn: true                                  # FORKLIFT_PASSWORD_SIGN_IN
    settings:                                             # other FORKLIFT_OIDC_* settings
      FORKLIFT_OIDC_ROLE_CLAIM: groups
      FORKLIFT_OIDC_ADMIN_GROUPS: forklift-admins
```

The client secret is mounted as a file (`FORKLIFT_OIDC_CLIENT_SECRET_FILE`), in the gateway's web
pods only. `settings` takes exactly the names the gateway knows (it refuses unknown
`FORKLIFT_OIDC_*` names at start-up, and so does the schema). The provider's redirect URI is
`<publicUrl>/accounts/oidc/callback/` ([oidc.md](oidc.md)).

## Testing the chart

```bash
deploy/helm/tests/lint.sh                     # helm lint --strict, helm template, kubeconform
python -m pytest deploy/helm/tests            # the render tests (pytest, pyyaml, jsonschema)
docker build -f services/web/Dockerfile -t forklift-web:local .
docker build -f services/worker/Dockerfile -t forklift-worker:local .
deploy/helm/tests/k3s_smoke.sh                # a real install on k3s in Docker
```

- `lint.sh` lints with the defaults and with every `ci/*-values.yaml`, renders each, and
  validates the result with `kubeconform -strict` against Kubernetes 1.30 and 1.36 (schemas
  pinned to a commit; KEDA's CRDs from the CRD catalog, also pinned).
- The render tests check the invariants for every example: Pod Security "restricted" for every
  pod; every pod under the default deny and one policy of its own; workers without ingress; the
  internal port open to the workers only; no rule without peers; nothing secret in ConfigMaps,
  annotations or literal environment values; the hook order and that the pre-install Job uses
  only what exists before it; each store-egress mode; the isolation profiles; KEDA's query
  against the gateway's real table; every gateway and worker setting either set by the chart or
  accepted through it; and the values that must fail.
- `k3s_smoke.sh` starts `rancher/k3s` (pinned) in Docker, imports the images, starts a test-only
  PostgreSQL, labels the namespace `pod-security.kubernetes.io/enforce=restricted`, installs the
  chart with the bundled RustFS, a standard batch group and a no-network interactive group (the
  seccomp profile copied onto the node), and then: runs a CSV through the public API to Parquet
  and a schema generation on the no-network group; checks from inside the cluster that the
  workers reach the internal port, the store and DNS but neither PostgreSQL, nor the public port,
  nor the Kubernetes API, that a pod outside the release reaches neither port nor the store, and
  that a pod like the ingress controller reaches the public port but not the internal one;
  upgrades the release to the proxy mode with changed settings, so that every pod rolls (the
  hooks again: the token "already registered", the secrets unchanged), runs both jobs again with
  the workers' store requests going through Squid, and checks that the workers now reach the
  proxy but not RustFS, and that Squid passes the store's requests and refuses CONNECT to the
  store's plain port, the gateway, PostgreSQL and any other host. `FORKLIFT_K3S_KEEP=1` keeps the cluster; `FORKLIFT_WEB_IMAGE` and
  `FORKLIFT_WORKER_IMAGE` pick other images.

CI runs all three (`.github/workflows/helm.yml`).

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `helm install` fails with "networkPolicies.storeEgress.mode is required with an external store" | Choose `cidr`, `proxy` or `podSelector` ([above](#the-store-three-modes)) |
| The install times out in the bootstrap Job | `kubectl -n forklift logs job/<release>-bootstrap --all-containers`. A `migrate` that cannot connect: `postgres.host`, the Secret, or `networkPolicies.postgres.to` (the Job's policy admits only that). A `cors` that fails: the store's credential needs `s3:PutBucketCORS` on the bucket (the message says so) |
| A worker pod restarts with exit code 2 | It cannot provide its profile: its log says why (no Landlock with `landlock: required`; no user namespace for `no-network`) |
| A `no-network` worker stays in `CreateContainerError` | The seccomp profile is not on that node ([no-network](#no-network)) |
| Workers log "the gateway ... could not be reached" | The internal Service, or a CNI that has not applied the gateway's policy yet (it says so for a few seconds after an install) |
| Uploads fail in the browser (CORS, or connection refused) | The browser reaches the store at `objectStore.publicEndpoint`: it must be reachable from browsers and listed in the bucket's CORS rule (`publicUrl`, `gateway.corsOrigins`); with the bundled RustFS give it `rustfs.ingress` |
| The gateway answers 400 Bad Request | The Host header is not in `FORKLIFT_ALLOWED_HOSTS`: add it to `gateway.allowedHosts` |
| Webhooks to an internal receiver fail with a connection error | Allow-list the host (`gateway.webhooks.allowedHosts`) **and** add `networkPolicies.dispatcher.extraEgress` for it |
| Sign-in with SSO fails to reach the provider | `networkPolicies.oidc.to` and `ports`: a provider in the cluster is not "the internet" |
| An upgrade fails: "The Secret ... is missing or cannot be read" | `<release>-secrets` was deleted; restore it from a backup (or `secrets.existingSecret`): new keys would make the stored connection secrets unreadable |
| Pods are rejected by Pod Security admission | They are not, as rendered; check what else mutates them (a webhook adding a sidecar that runs as root, say) |

## Releases

The chart is published as an OCI artifact on GHCR when a tag `chart-v<version>` is pushed whose
version is `Chart.yaml`'s `version`: the `publish` job of `.github/workflows/helm.yml` (after the
lint and k3s jobs pass, with `packages: write` for that job only) packages it and pushes it to
`oci://ghcr.io/cornyhorse/charts/forklift`. Raise `version` for every change to the chart, and
`appVersion` when it should default to newer `forklift-web` and `forklift-worker` images (which
are published with the package version as their tag).
