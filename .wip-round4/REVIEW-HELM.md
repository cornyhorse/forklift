# REVIEW-HELM: independent review of the Helm chart (M4)

Reviewer: REVIEW-HELM. Scope: `deploy/helm/forklift/**`, `deploy/helm/tests/**`,
`.github/workflows/helm.yml`, `docs/platform/helm.md`, and `create_worker_token --from-file`
(+ `services/web/tests/test_create_worker_token.py`). Reviewed against design §6.2, §7, §8 and §10.2,
the Compose stack, `services/web` settings and `services/worker` settings and README.

The lead has since committed the chart as it was (`e77ca04 Add forklift-mcp ... and the Helm
chart`). `git status` shows no changes under `deploy/helm` after that commit, so the file:line
references below are for HEAD. I edited nothing in the repository. My scratch files are in
`$S/review-helm/`: renders `r-*.yaml`, override values `x-*.yaml`, probe scripts `scripts/`,
`helm.sh` (pinned `alpine/helm:4.3.0@sha256:a6cf…`), and `render_tests.log`.

Baseline: the chart's own render tests pass (`145 passed`, `$S/review-helm/render_tests.log`), and
every `ci/*-values.yaml` renders.

## Summary

| # | Severity | Finding |
|---|---|---|
| B1 | **blocking** | With `objectStore.addressingStyle: virtual` (as in `ci/proxy-values.yaml`), workers refuse every input: `FORKLIFT_WORKER_STORE_HOSTS` lacks `<bucket>.<host>` |
| S1 | should-fix | User-supplied NetworkPolicy rules are not validated. A rule with no `ports` opens every port, including the internal port 8081, and a rule with no `to`/`from` reaches everything. The docs say this "is refused at render time" |
| S2 | should-fix | Store ports in podSelector mode are **Service** ports, but NetworkPolicies match the **pod** port after DNAT. Setting `rustfs.service.port` to anything but 9000 cuts every pod off the store |
| S3 | should-fix | The gateway processes and the bootstrap Job have no way to trust a private-CA store. `workerCaBundle` reaches the workers only, so `ci/cidr-values.yaml` cannot bootstrap |
| S4 | should-fix | `postgres.sslmode: verify-full`/`verify-ca` can never connect (no root cert can be given; libpq looks in `/nonexistent/.postgresql/root.crt`). `ci/cidr-values.yaml` uses verify-full. The default `prefer` does not authenticate the server |
| S5 | should-fix | Generated Secrets are `pre-upgrade` hooks with `before-hook-creation`: every upgrade deletes and re-creates `FORKLIFT_SECRETS_KEYS`. Under Argo CD every sync generates new keys, silently |
| S6 | should-fix | The bootstrap NetworkPolicy is a hook, so nothing ever removes it. After `networkPolicies.enabled=false`, or an uninstall and reinstall, the bootstrap Job still runs under the stale rules |
| S7 | should-fix | `sandboxed` on gVisor has no Landlock (gVisor has no syscalls 444–446). The default `landlock: required` makes the workers exit 2. The example silently uses `auto`, which lets the engine read the worker token and reach 8081 |
| S8 | should-fix | One ServiceAccount serves every pod. Cloud-identity annotations meant for the gateway (IRSA, Workload Identity) also go to workers, RustFS, Squid and MCP |
| S9 | should-fix | KEDA ignores `postgres.userKey`: it signs in as `postgres.user` ("forklift") while the gateway uses the Secret's user |
| S10 | should-fix | Worker→gateway (token, presigned URLs, SQL connection strings) and MCP→gateway traffic is plaintext HTTP. Design §7.3 says "over TLS", and the chart docs don't mention it |
| S11 | should-fix | Squid pods do not roll when the rendered `squid.conf`/`allow.conf` changes: the checksum covers only the parsed allow entries |
| S12 | should-fix (docs) + nit | The no-network seccomp profile also allows `CLONE_NEWTIME`, against what the docs say, and it opens nested user+net namespaces (nf_tables) to the engine. The trade-off is not documented |
| S13 | should-fix (docs/defaults) | On cgroup v2 with Kubernetes ≥ 1.28, one job that exceeds memory kills the whole worker container and every concurrent job in it. The engine's `RLIMIT_AS` "auto" is the node's RAM |
| S14 | should-fix (low) | The liveness probes restart a process where Compose only marks it unhealthy. One step that keeps failing stops the dispatcher's other step through restart back-off; the sweeper's first sweep must finish within 120 s |
| N1–N14 | nit | IPv6 literals, mutable Squid tag, KEDA overlap and perf, uninstall leftovers, webhook ports, docs overclaims, CI pins, NOTES, MCP CA, label collisions, `FORKLIFT_WORKER_ID`, revoked-token remedy, helm-diff |

---

## B1 (blocking): virtual-host addressing makes workers refuse every input

**Where:** `templates/workers.yaml:77` sets
`FORKLIFT_WORKER_STORE_HOSTS = hostPort(workerEndpoint)`, which is the endpoint host only.
`_helpers.tpl:575-577` knows that with `addressingStyle: virtual` URLs go to `<bucket>.<host>`,
because it adds that host to Squid's allow list, but the workers' list does not get it.

**Reproduction.** The rendered `ci/proxy-values.yaml` has:
```
FORKLIFT_S3_ADDRESSING_STYLE: "virtual"
- {name: FORKLIFT_WORKER_STORE_HOSTS, value: "storage.example.net"}
acl forklift_store_2 dstdomain -n forklift.storage.example.net      # Squid knows
```
I signed a URL with the gateway's own client construction (`forklift_web/storage.py`: boto3 1.43,
`endpoint_url=https://storage.example.net`, `addressing_style=virtual`), using
`scripts/presign_virtual.py`:
```
https://forklift.storage.example.net/uploads/u1/in.csv?X-Amz-Algorithm=...
```
Then I fed it to the worker's own settings loader and planner with the rendered env
(`scripts/worker_plan.py`, `$S/kvenv`):
```
REFUSED: input.location points at forklift.storage.example.net, which is not an object store
this worker may read (--store-host: storage.example.net).
```
**Why it matters:** every job with an input fails, in every store mode, as soon as virtual
addressing is used. That includes the chart's own proxy example, and AWS S3 and most hosted
stores favour virtual-host style. The k3s test uses path-style RustFS, so it cannot catch this.

**Fix:** with `addressingStyle: virtual`, render
`FORKLIFT_WORKER_STORE_HOSTS=<host>,<bucket>.<host>` (with the port when there is one), the same
list `forklift.egressProxyAllow` builds. Add a render test that runs a URL from botocore through
`forklift_worker.spec.plan_job` for every example.

---

## S1 (should-fix): user-supplied NetworkPolicy rules are not validated

**Where:** `values.schema.json:208-240` (`egressRule`/`ingressRule` have no `required`, no
`minItems`; `ports` items do not require `port`); the rules are passed through verbatim at
`templates/networkpolicies.yaml:53-55` (`gateway.extraIngress`), `72-74`, `102-104`, `124-126`,
`162-164` (`workers[].egress`), `236-238`, `282-284`. `docs/platform/helm.md:253` says: "No rule of
the chart admits or reaches 'everything': a rule without peers is refused at render time."
`test_no_rule_admits_everyone` checks only the `ci/` examples.

**Reproduction** (`x-rules.yaml` on top of `ci/default-values.yaml`; helm exits 0):
```yaml
networkPolicies:
  gateway: {extraIngress: [{from: [{namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: airflow}}}]}]}
  postgres: {to: [{namespaceSelector: {}}]}
workers: [{name: interactive, lanes: [interactive]},
          {name: sql, lanes: [sql], egress: [{ports: [{protocol: TCP, port: 5432}]}, {to: [{ipBlock: {cidr: 0.0.0.0/0}}]}]}]
```
It renders:
```
# fk-forklift-gateway ingress
- from: [{namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: airflow}}}]   # no ports: 8081 too
# fk-forklift-worker-sql egress
- ports: [{port: 5432, protocol: TCP}]          # no `to`: any address on 5432
- to: [{ipBlock: {cidr: 0.0.0.0/0}}]            # no ports: everything, incl. 169.254.169.254 and the API server
```
**Why it matters:** the docs recommend exactly this `gateway.extraIngress` for "in-cluster API
clients (an Airflow namespace, say)" (`helm.md:309`). Written the natural way, without `ports`, it
opens the workers-only internal port 8081 to that namespace. That breaks the property the design
calls the trust boundary (§8, "Internal API").

**Fix:** in the schema, require `from`/`to` (`minItems: 1`) and `ports` (`minItems: 1`, each with
`port`) for every extra rule; refuse `{}` selectors in peers (`minProperties: 1` on
`labelSelector`, or refuse `namespaceSelector: {}`); and for `gateway.extraIngress`, refuse port
8081 (or `endPort` ranges covering it) unless a value explicitly allows it. Alternatively `fail`
in the templates with a message. Extend the render test to a values file of hostile extra rules.

---

## S2 (should-fix): store ports are Service ports, but NetworkPolicies match pod ports

**Where:** `_helpers.tpl:476-478` (`list .Values.rustfs.service.port`), `:505-506` (gateway in
proxy mode), `:200-207` (`storePorts` from the endpoint URL for an in-cluster store in podSelector
mode), and `templates/networkpolicies.yaml:196-203` (the egress proxy's ports, from the URL). The
schema accepts any `rustfs.service.port` (`values.schema.json` → `rustfs.service.port` 1–65535).
The RustFS container always listens on 9000 (`rustfs.yaml:215`), and the RustFS ingress policy
hard-codes 9000 (`networkpolicies.yaml:235`).

**Reproduction:** `helm template ... -f ci/rustfs-values.yaml --set rustfs.service.port=80`
renders:
```
fk-forklift-gateway        egress to rustfs ports [{'port': 80}]
fk-forklift-worker-batch   egress to rustfs ports [{'port': 80}]
fk-forklift-rustfs         ingress ports [{'port': 9000}]
Service fk-forklift-rustfs [{port: 80, targetPort: s3}]   StatefulSet containerPort 9000
FORKLIFT_S3_ENDPOINT_URL   http://fk-forklift-rustfs.forklift.svc.cluster.local:80
```
NetworkPolicy ports refer to the destination pod's port: CNIs evaluate egress after the Service
DNAT (Kubernetes docs: policies apply to pod IPs and ports). Traffic to `:80` becomes pod `:9000`,
which no egress rule allows.

**Why it matters:** a value the schema accepts silently cuts the gateway, dispatcher, publisher,
sweeper, workers and (in proxy mode) Squid off the store. The same happens with any in-cluster
store in `podSelector` mode whose Service maps ports (MinIO's chart: Service 9000 or 80 → 9000; a
TLS Service 443 → 9000). The chart derives the port from the endpoint URL, which names the Service
port.

**Fix:** for the bundled RustFS use 9000 (or the named port `s3`) in every egress rule. For
`podSelector` mode with an external in-cluster store, document that `storeEgress.ports` are the
store pods' ports and require it, rather than deriving them from the URL. The same note applies to
`postgres.port` when PostgreSQL sits behind a port-mapping Service.

---

## S3 (should-fix): the gateway processes cannot trust a private-CA store

**Where:** `values.yaml:104-105` ("A CA bundle ... the workers trust"); only `workers.yaml:89-90`
and `:118-123` use it. The gateway builds its boto3 clients with the default `verify`
(`forklift_web/storage.py:123-137`), so only the image's bundle or `AWS_CA_BUNDLE` would work.
`gateway.extraEnv` and `extraSecretEnv` accept `FORKLIFT_*` names only, and the gateway pods have
no extra volumes.

**Reproduction** (rendered `ci/cidr-values.yaml`; it uses `workerCaBundle: corporate-ca` for
`https://s3.eu-west-1.example.org`):
```
Deployment fk-forklift-gateway      volumes= ['tmp']   CA env= []
Deployment fk-forklift-publisher    volumes= ['tmp']   CA env= []
Job        fk-forklift-bootstrap    volumes= ['tmp']   CA env= []
Deployment fk-forklift-worker-batch volumes= ['scratch','tmp','token','ca']  CA env= ['FORKLIFT_WORKER_CA_FILE']
```
**Why it matters:** the workers' CA is needed only for the store, because the chart's gateway URL
is plain `http://…-gateway-internal`. A store that needs it is just as untrusted for the gateway.
Its HEADs, multipart bookkeeping, publisher copies and sweeper deletes fail TLS verification, and
the pre-install `cors` step (`configure_cors`) fails, so the install fails. The cidr example
cannot work as written.

**Fix:** rename or extend to `objectStore.caBundle`. Mount it in the gateway processes, the
bootstrap and store-setup Jobs and the workers, set `AWS_CA_BUNDLE` for boto3 (or add a
`FORKLIFT_S3_CA_FILE` setting in the gateway area) and `FORKLIFT_WORKER_CA_FILE` for the workers.

---

## S4 (should-fix): `verify-full`/`verify-ca` cannot connect; the default `prefer` does not authenticate

**Where:** `values.yaml:83` (`sslmode: prefer  # ... verify-full ...`), `ci/cidr-values.yaml`
(`sslmode: verify-full`). The gateway passes only `sslmode` (`settings.py:81`). There is no
`sslrootcert` setting, and `PGSSLROOTCERT` cannot be set (extraEnv is `FORKLIFT_*` only). The image's
`HOME` is `/nonexistent`.

**Reproduction:** I started a throw-away `postgres:16 -c ssl=on` (snakeoil certificate) and
connected from `forklift-web:local` as uid 10001, read-only root, with psycopg 3 as the gateway
does:
```
== sslmode=require      connected
== sslmode=verify-ca    OperationalError ... root certificate file "/nonexistent/.postgresql/root.crt" does not exist
                        Either provide the file, use the system's trusted roots with sslrootcert=system, ...
== sslmode=verify-full  (same)
```
The certificate was not even checked: libpq refuses before verification for want of a root file,
so a publicly trusted database certificate fails the same way. I removed the container and network
afterwards.

**Why it matters:** the only modes that authenticate the database server cannot be used, and the
cidr example's bootstrap `migrate` fails. The default `prefer` also accepts a downgrade to clear
text, so the DB password and data can be intercepted. That falls short of "the defaults are the
safe ones" (`values.yaml:6`). KEDA inherits `sslmode`.

**Fix (with the gateway area):** add `postgres.caBundle` (ConfigMap or Secret), mounted into every
gateway process and the bootstrap Job. Set `FORKLIFT_DB_SSLROOTCERT`, a new gateway setting passed
as `OPTIONS.sslrootcert`, or at least `PGSSLROOTCERT`, and allow `sslrootcert=system`. Then make
`require` (or `verify-full` with a CA) the default for an external database.

---

## S5 (should-fix): generated Secrets are re-created on every upgrade, and regenerated under Argo CD

**Where:** `templates/secrets.yaml:8` (`pre-install,pre-upgrade`, weight -20, default delete policy
`before-hook-creation`), `:28-29` (`keptSecretValue` via `lookup`), and the guard at `:11-14`
(`IsUpgrade` only). `docs/platform/helm.md:143-146` mentions GitOps.

**What happens:**
1. **On every `helm upgrade`** Helm deletes `<rel>-secrets` (`FORKLIFT_SECRETS_KEYS`,
   `FORKLIFT_SECRET_KEY`), the admin, worker-token and RustFS Secrets, and then creates them again
   with the looked-up values. That is the documented behaviour of a hook with
   `before-hook-creation`, and it happens even when nothing changed. If the create fails after
   the delete (API timeout, quota, an admission policy that now rejects the Secret, an interrupted
   `helm`), the Fernet keys are gone. The next upgrade then fails by design ("restore it from a
   backup"). The only copy left is in the environment of the pods that are still running. Pods
   that start in the window get `CreateContainerConfigError`.
2. **Argo CD** renders with `helm template`, so `lookup` returns nothing and `.Release.IsUpgrade`
   is false. It maps `pre-install`/`pre-upgrade` to `PreSync` and `before-hook-creation` to
   `BeforeHookCreation`. Every sync therefore deletes the four Secrets and creates them with **new
   random values**, and the guard cannot fire. Pods do not restart (there is no checksum, on
   purpose), so old pods keep the old Fernet key and any new or rescheduled pod gets a new one.
   Pods then encrypt connection secrets with different keys, and once the old pods are gone every
   connection secret is unreadable. The admin Secret no longer matches the admin. A new worker
   token is registered on each sync and the old ones stay valid. `helm template | kubectl apply`
   does the same.

The docs tell GitOps users to set `existingSecret`, but the chart's default is to generate, and
the failure is silent and cannot be undone.

**Fix:** make the generated Secrets `pre-install` only, so they are made once and never touched by
upgrades; `lookup` still keeps them across an uninstall and reinstall. Also add
`helm.sh/resource-policy: keep`. Make generation fail closed when Helm cannot see the cluster, for
example `secrets.generate: true|false` required to be explicit, or refuse to generate
`FORKLIFT_SECRETS_KEYS` unless `lookup` of the release namespace succeeds. Say in NOTES that the
keys must be backed up together with the database.

---

## S6 (should-fix): the bootstrap NetworkPolicy outlives its rules

**Where:** `templates/bootstrap-policy.yaml:1` (rendered only `if networkPolicies.enabled`; it is a
pre-install/pre-upgrade hook).

**Reproduction:** with `--set networkPolicies.enabled=true` the render contains
`fk-forklift-bootstrap` (a hook) among 8 policies. With `false` it contains `[]`. Helm does not
track hook resources ("not managed as part of the release"; the chart's own cleanup line,
`helm.md:150`, lists `networkpolicy` for that reason). After an upgrade with
`networkPolicies.enabled=false`, the old `fk-forklift-bootstrap` therefore stays, still selects the
bootstrap Job's pods (`component=bootstrap`, `instance`), and keeps them isolated to DNS + the
**old** `postgres.to` + the old store rule. The same happens after `helm uninstall`, or for a new
install with policies off in a namespace that kept it.

**Why it matters:** the most common first-install failure is a wrong
`networkPolicies.postgres.to`: the `migrate` step cannot connect (the troubleshooting table says
so). The obvious diagnostic step, turning policies off, then fails in exactly the same way,
because the stale hook policy still applies. The operator concludes that policies are not the
cause.

**Fix:** when `networkPolicies.enabled=false`, still render a hook policy with the same name that
allows all traffic (`ingress: [{}]`, `egress: [{}]`), or delete it with a pre-upgrade hook. Also add
it to the troubleshooting table.

---

## S7 (should-fix): `sandboxed` on gVisor runs without Landlock

**Evidence:** gVisor's syscall table (`pkg/sentry/syscalls/linux/linux64.go`, master, fetched to
`$S/review-helm/dl/linux64.go`) ends at 441 for amd64 and arm64. Syscalls 444–446
(`landlock_*`) are absent and `Missing` returns `ENOSYS`. The chart's default is `landlock:
required` (`values.yaml:264`). On gVisor the worker therefore exits with code 2. The example
`ci/sandboxed-values.yaml` silently sets `landlock: auto`, and `docs/platform/helm.md`
("sandboxed", lines 222-229) says nothing about it.

**Why it matters:** this is the profile for "sensitive data from untrusted sources". Without
Landlock, per the worker README ("What is not enforced"), the engine runs as the same uid as the
supervisor:
- it can read `/run/secrets/worker-token` (mode 0440, group = fsGroup 10001, `workers.yaml:113-117`);
- with `sandboxedEngine: standard` (the default) it has unrestricted TCP inside the pod, and the
  pod's policy allows `gateway-internal:8081`.

A compromised engine can therefore lease other jobs with the stolen token: presigned URLs to other
users' inputs, and the SQL connection strings that leases of `sql` jobs carry if the group serves
`sql`. It can then pass their data out through its own outputs. The gVisor sandbox protects the
node, not the other jobs.

**Fix:** document that gVisor provides no Landlock. For `isolation: sandboxed`, either `fail` at
render time when `landlock: required` and the runtime is gVisor (unknowable, so instead require an
explicit `landlock` value for sandboxed groups), or default `sandboxedEngine: no-network` for
sandboxed groups, which takes away the engine's network. Consider a separate uid for the token
file, readable by the supervisor only, if the worker ever drops privileges for the engine.

---

## S8 (should-fix): one ServiceAccount for every pod

**Where:** `values.yaml:29-34`; every pod uses `forklift.serviceAccountName` (`gateway.yaml:42`,
`workers.yaml:41`, `rustfs.yaml:205`, `egress-proxy.yaml:97`, `mcp.yaml:33`), and the bootstrap SA
copies the same annotations (`serviceaccount.yaml:106-108`).

**Why it matters:** the only place for workload-identity annotations is
`serviceAccount.annotations`, for example `eks.amazonaws.com/role-arn` (IRSA, whose webhook injects
a projected token and `AWS_ROLE_ARN` whatever `automountServiceAccountToken` says), or Azure
Workload Identity, or GKE `iam.gke.io/gcp-service-account`. An operator who gives the gateway an
identity for its store gives the same identity to the workers, which parse untrusted files, and to
Squid, RustFS and MCP. The design's invariant is that a worker holds "its bootstrap token only"
(§3.1, §8). Without Landlock (S7, or `landlock: auto`) the engine can read the injected token file.

**Fix:** one ServiceAccount per component (at least gateway processes, workers, others), with
annotations per SA. Document that workers must never get a cloud identity.

---

## S9 (should-fix): KEDA ignores `postgres.userKey`

**Where:** `templates/workers.yaml:184` (`userName: default postgres.user keda.postgres.user`) and
`:199-202` (only `password` from the Secret).

**Reproduction:** `-f ci/keda-values.yaml` with `postgres: {userKey: username}` and
`keda.postgres: {existingSecret: "", user: ""}` renders:
```
gateway env: FORKLIFT_DB_USER from secretKeyRef {name: forklift-db, key: username}
ScaledObject: userName: forklift          # postgres.user's default, which userKey is meant to replace
TriggerAuthentication: [{parameter: password, name: forklift-db, key: password}]
```
**Why it matters:** KEDA signs in with a user name that is probably wrong, so the scaler errors
and the group never scales (with `minReplicas: 0` it never starts at all).

**Fix:** when `userKey` is set and `keda.postgres.user` is empty, add
`{parameter: userName, name: <postgres.existingSecret>, key: <userKey>}` to the
TriggerAuthentication. KEDA's postgresql scaler reads `userName` from `authParams`
(`postgresql_scaler.go:54`: `order=authParams;triggerMetadata`). Add a render test.

---

## S10 (should-fix, docs/design): the internal port is plaintext

**Where:** `workers.yaml:66` (`http://<rel>-gateway-internal:8081`), `mcp.yaml:48`
(`http://<rel>-gateway:8080`). The gateway image serves plain HTTP only. Design §7.3 (line 485):
"The job's connection secret is delivered with the lease **over TLS**." `docs/platform/helm.md`
does not mention it.

**Why it matters:** the worker token, the presigned URLs and the SQL connection strings, with their
passwords, cross the pod network in clear text. So do users' API tokens on the MCP→gateway hop.
Anyone who can capture pod traffic gets them: a compromised node, or a CNI without encryption,
which is the default for Calico, Flannel and kube-router.

**Fix:** state it in helm.md (Secrets / NetworkPolicies) and recommend CNI encryption (Cilium or
Calico WireGuard) or a mesh with mTLS; correct §7.3 (lead). Longer term, serve TLS on 8081
(gunicorn `--certfile`) with `workerCaBundle` and an `https://` gateway URL. The worker already
supports one.

---

## S11 (should-fix): Squid does not restart when its configuration changes

**Where:** `templates/egress-proxy.yaml:95`:
`checksum/config: {{ include "forklift.egressProxyAllow" . | sha256sum }}`. That hashes the parsed
`{scheme, host, port}` list, not the ConfigMap. Squid reads `squid.conf`/`allow.conf` once at start
(`-NYC`, no reload).

**Why it matters:** a chart upgrade that changes the *rendering*, keeps the hosts the same and
leaves the Deployment spec unchanged leaves the Squid pods running the old configuration. Examples:
this round's `dstdomain -n` fix, a new `http_access` rule, or a log-format change that stops
logging query strings. Security fixes to the proxy would not take effect.

**Fix:** checksum the whole ConfigMap data (render both files in a helper and hash that).

---

## S12 (should-fix for the docs; nit for the mask): what the no-network seccomp profile opens

**Where:** `files/seccomp/worker-no-network.json`, rule 13: `unshare` allowed when
`flags & 0x2E020000 == 0`, which masks NEWNS, NEWCGROUP, NEWUTS, NEWIPC and NEWPID. `clone`
keeps containerd's `0x7E020000` mask, `clone3` is `ENOSYS`, and `mount`, `setns`, `bpf`, `keyctl`,
`io_uring*`, `fsopen` etc. stay denied (checked against the 363-name allow list). The
docs (`helm.md:207`) say: "its flags may contain no other `CLONE_NEW*` flag".

**Reproduction** (`scripts/unshare_probe.py`, `docker run -u 10001 --cap-drop ALL --security-opt
no-new-privileges --security-opt seccomp=<the profile> python:3.13-slim`):
```
NEWUSER|NEWNET: ALLOWED        NEWUSER|NEWTIME: ALLOWED        (CLONE_NEWTIME = 0x80 is not in the mask)
NEWUSER|NEWNS / NEWPID / NEWUTS / NEWIPC / NEWCGROUP: Operation not permitted
in the new user+net namespace: NETLINK_NETFILTER socket opened (nf_tables reachable)
in the new user namespace: unshare(CLONE_NEWTIME) ALLOWED
```
**Why it matters:** the time namespace is minor, but the docs' statement is false. The larger
point is that the profile applies to the whole container. `forklift_worker/sandbox.py:62` gives the
engine its namespaces and then `exec`s it with no seccomp filter of its own, so a compromised
engine can `unshare(NEWUSER|NEWNET)` again and gain `CAP_NET_ADMIN` there. That exposes nf_tables
and other netlink families, the kernel surface behind the well-known unprivileged-userns escapes
(CVE-2022-0185, CVE-2023-32233, CVE-2024-1086), which `RuntimeDefault` closes by forbidding
`unshare`. "no-network" trades network isolation for a larger kernel attack surface, and
`helm.md` presents it as strictly stronger.

**Fix:** add `0x80` to the unshare mask (`0x2E0200A0`). Document the trade-off in `helm.md` and
the worker README. In the worker area, after `enter_network_namespace()`, install a seccomp filter
in the sandbox step (`prctl(PR_SET_SECCOMP)`; `NO_NEW_PRIVS` is already set) that denies
`unshare`/`clone` with namespace flags and `setns` to the engine.

---

## S13 (should-fix, docs/defaults; not reproduced here, the host is cgroup v1): one OOM kills the pod's other jobs

`RLIMIT_AS` "auto" is `SC_PHYS_PAGES` of the **node** (`forklift_worker/isolation.py:226`). The
README says to let the container's memory limit bound resident memory, and the chart sets
`resources.limits.memory` but not `FORKLIFT_WORKER_LIMIT_ADDRESS_SPACE`. Since Kubernetes 1.28 on
cgroup v2, the kubelet sets `memory.oom.group=1`; `singleProcessOOMKill` (1.32) defaults to false
there. An engine that exceeds the limit therefore kills the **whole container**: the supervisor
and every concurrent job. The default `interactive` group has `concurrency: 2` on 1 GiB, so one
malicious upload can kill another user's preview. In Compose, runc kills only the engine.

**Fix:** document it in "Isolation profiles"; for untrusted inputs prefer `concurrency: 1` with
more replicas, or mention the kubelet's `singleProcessOOMKill: true`. Optionally offer a per-group
`limitAddressSpace` (with a warning about the virtual-size gap the README describes).

---

## S14 (should-fix, low): liveness probes restart where Compose only marks unhealthy

**Where:** `templates/gateway-processes.yaml:216-226`, `:271-282`. The dispatcher touches its
heartbeat only when **every** step succeeded (`dispatch.py:68-69`), but its design is "a step that
fails ... stops neither the other steps nor the loop" (`dispatch.py:37-38`). Under Kubernetes, one
step that keeps failing (say webhooks) means no heartbeat; after about 105 s the liveness probe
kills the dispatcher, and CrashLoopBackOff (up to 5 min) also stops **schedules** from firing. The
sweeper's startup probe allows 120 s (`start: 120`), but `retention.sweep` is unbounded
(`services/retention.py:448-461`). A first sweep after an outage or a newly set policy that takes
longer is killed and starts over, repeatedly.

**Fix:** use the heartbeat only to tell whether the loop is alive. Have the commands touch a
"pass finished" file, with a readiness or metrics signal for "a step failed", and give the
sweeper's startup probe its own generous limit (for example `2 × everySeconds`).

---

## Nits

- **N1 IPv6 literals** (`_helpers.tpl:185-197`, `:585-586`, `egress-proxy.yaml:65`):
  `egressProxy.allow: [https://[2001:db8::10]:9443]` renders `acl forklift_store_1 dstdomain -n
  [2001`. `forklift.urlPort` of `https://[2001:db8::10]` returns `10]`, and the NetworkPolicy then
  gets port 0. EDGE's `start.sh` handles brackets (`dst ${host%]}`); copy its logic.
- **N2 Squid tag** `ubuntu/squid:6.6-24.04_edge` is a moving risk-channel tag: Docker Hub shows it
  last updated 2026-05-07, and no `_stable` exists. With `digest: ""` and `IfNotPresent`, nodes can
  run different builds of the proxy that guards egress. Default `egressProxy.image.digest` to a
  pinned digest.
- **N3 KEDA:** groups with overlapping lanes (`ci/keda-values.yaml`: `batch` [batch, sql] and `big`
  [batch]) each scale to the *whole* demand of their lanes, not "its share of the queue"
  (`workers.yaml:156`, helm.md). `minReplicas > maxReplicas` is not refused. The
  `status IN ('queued','running')` count cannot use the partial indexes `job_queue`
  (`status='queued'`) and the running one; consider the sum of two counts.
- **N4 uninstall leftovers:** the cleanup line (`helm.md:150`) misses failed or kept `job`s and
  the RustFS PVC (`data-<rel>-rustfs-0`). The PVC carries the instance label, so `pvc` and `job`
  can be added to the command.
- **N5 webhook ports:** the gateway accepts any port (`webhook_client.py:136-140`); the dispatcher
  policy allows 443 (and 80) only. A receiver on `:8443` fails with a connection error. Add a value
  (`networkPolicies.dispatcher.webhookPorts`) or a troubleshooting line.
- **N6 "could not reach the cluster"** (`helm.md:287`): on managed clusters with a public API
  endpoint (GKE, EKS, AKS public), the "internet" rule of the dispatcher reaches the API server on
  443. So do the gateway with the OIDC default and every gateway process in proxy mode. Nodes with
  public IPs on 80/443 are reachable too. There is no token, so the risk is low, but the claim
  should be softened.
- **N7 DNS** (`helm.md:241`): workers "reach only DNS, the gateway's internal port and the store",
  but cluster DNS forwards to the internet. In the standard profile UDP is not Landlock-restricted
  (worker README), so DNS tunnelling is an exfiltration channel even for staged inputs. Say so and
  point to `no-network`.
- **N8 CI:** `postgres:16` is a floating tag (`helm.yml:97`, `k3s/postgres.yaml:26`). `pip install
  pytest pyyaml jsonschema black isort flake8` is unpinned (`helm.yml:59`), so a new black style can
  break the job; the repo elsewhere uses `black>=23.12` ranges. A GHCR package pushed by
  `GITHUB_TOKEN` is private on first publish: say in "Releases" that it must be made public once,
  or `helm install oci://…` fails for users.
- **N9 NOTES.txt:121** says "registered the workers' token" based on `workerToken.existingSecret`,
  not on the actual `register` decision (`bootstrap.yaml:121`).
- **N10 MCP:** `mcp.caBundle`/`FORKLIFT_MCP_CA_FILE` can never apply, because the chart always uses
  `http://<rel>-gateway` (`mcp.yaml:48`). `FORKLIFT_MCP_ALLOWED_HOSTS` has `<svc>` and
  `<svc>.<ns>.svc` but not `<svc>.<ns>` or `.svc.<clusterDomain>` (`mcp.yaml:9`), whereas the docs
  say "its Service's names".
- **N11 labels:** `commonLabels` and `podLabels` accept any key (`values.schema.json`). One that
  sets `app.kubernetes.io/component`, `instance` or `name` duplicates a selector key in the
  rendered YAML (an install error, or broken selectors and policies). Refuse
  `app.kubernetes.io/*` and `forklift.cornyhorse.dev/*` there.
- **N12** `workers[].settings` accepts `FORKLIFT_WORKER_ID`, which gives every replica the same id
  (`Worker.worker_id` is unique); add it to the refused names.
- **N13 create_worker_token:** if an admin revokes the chart's token, every later upgrade fails at
  `worker-token` with "Put a newly generated token in the file". For the chart the remedy is
  "delete `<rel>-worker-token` and upgrade" (a new one is generated, registered, and picked up by
  the workers, which re-read the file per request); document it. The command does not warn about
  a group- or world-readable token file. That is harmless in the chart (0440), but a one-line
  warning fits the command's purpose.
- **N14 helm-diff/helmfile:** a client-side render with `IsUpgrade` and no lookup (helm-diff's
  default unless `--dry-run=server`) hits the S5 guard and fails. A line in the docs saves a
  support question. (I did not run helm-diff here.)

---

## What I checked and found sound

- **Default deny covers every chart pod**: hook Jobs, store-setup, RustFS, Squid, MCP. Each also
  has exactly one policy of its own, and every chart policy has `policyTypes: [Ingress, Egress]`.
  Peers are same-namespace `podSelector`s with name, instance and component labels, so they match
  no other release's pods. No chart-made rule has empty peers or `0.0.0.0/0` without `except`, and
  `dns.to` and `internet` have `minItems: 1`.
- **Gateway**: 8081 admits only `component=worker` pods of the release (all groups), and 8080 only
  the ingress controller and the MCP proxy. The internal Service is ClusterIP and never an Ingress
  backend. Probes send `Host: localhost`; `/readyz` touches only the DB, so a missing bucket cannot
  deadlock `--wait`.
- **Workers**: no ingress; egress to kube-dns, the gateway's 8081 and the store (or Squid 3128)
  only. They cannot reach PostgreSQL, the API server, 169.254.169.254 or other namespaces unless
  `cidrs` or `egress` say so. IPv6 is denied by omission. In proxy mode Squid allows only the store
  names: `dstdomain -n`, CONNECT only for https, `!CONNECT plain_http` for http. Its policy admits
  workers only, and its egress is the internet without private and link-local ranges (or RustFS).
  The squid.conf directives match EDGE's (diff: only the include path differs), and EDGE now uses
  `-n` too.
- **Pod Security**: every pod and container (init containers included) is restricted: non-root
  10001 (Squid 13), RO root, `drop: [ALL]`, no privilege escalation, `RuntimeDefault` or
  `Localhost`, no host namespaces or ports, `automountServiceAccountToken: false` on SAs and pods,
  `enableServiceLinks: false`. Writable paths are size-limited emptyDirs.
  `shareProcessNamespace` is set only on the single-container dispatcher, publisher and sweeper
  pods, so the only other process they share with is pause; nothing in another container is
  exposed. The seccomp profile denies mount, setns, bpf, keyctl, io_uring, the new mount API and
  clone namespace flags (apart from S12). The supervisor removes stale `job-*` scratch at start,
  so a restarted sandboxed container cannot see the previous job's leftovers.
- **Secrets**: no secret in a ConfigMap, annotation, NOTES or literal env. The `extraEnv` schema
  refuses secret-looking names and `FORKLIFT_OIDC_*`. The OIDC client secret is a file on the web
  pods only, and the admin password reaches only the `admin` step. KEDA and RustFS use
  `secretKeyRef`. Generated values use crypto randomness (Sprig) in formats the gateway accepts:
  Fernet 32 bytes URL-safe, `fkw_` plus 43 alphanumerics, about 256 bits. The upgrade guard for a
  missing `<rel>-secrets` works as described. Hook Job logs show only the 12-character token
  prefix.
- **Hooks**: the weights (SA -30, Secrets -20, policy -10, Job 0) are right, and the pre-install
  Job uses inline env (not the ConfigMap) and only things made before it. A failed Job is kept and
  replaced on the next upgrade. Migrations run before anything rolls; rollback runs no hooks and
  changes no Secrets. The store-setup post-hook waits for RustFS.
- **Compose parity**: commands, intervals and heartbeat thresholds are the same. Every `FORKLIFT_*`
  name the chart sets exists in `settings.py`, the Dockerfile (`FORKLIFT_WEB_*`) or the worker's
  `OPTIONS`, and the required ones are set. Lanes, isolation, landlock and max-jobs are mapped as
  the worker README defines them, and the `no-network`+`sql` combination is refused. The KEDA table
  and columns (`core_job.status`, `lane`, statuses `queued`/`running`) and the lane-name validation
  before SQL are correct. The MCP contract (port 8000, `/healthz`, `/mcp` on the gateway's host,
  egress to 8080 + DNS only, `enableServiceLinks: false`) is followed.
- **Schema vs templates**: all 101 `.Values.*` paths the templates read exist in the schema
  (`scripts/schema_paths.py`). The worker keys read via `$w` are all in `definitions.worker`.
- **`create_worker_token --from-file`**: same prefix and SHA-256 hashing as generated tokens, and
  the regex matches `token_urlsafe(32)`. The 4 KiB read cap and ASCII check are there, and messages
  never contain the file's contents. It is idempotent (same hash: "already registered"; another
  hash with the same prefix, a revoked token or an expired token is refused) and audited with
  `from_file`. The tests assert the outcomes, including a real `/internal/v1/leases` call with the
  registered token.
- **CI**: top-level `contents: read`, and `packages: write` only on the tag-gated publish job,
  which checks the tag against `Chart.yaml`. helm, kubeconform and k3s images are pinned by digest,
  and kubeconform schemas by commit. Path filters do not apply to tag pushes, so `chart-v*`
  triggers the publish job. The k3s job looks runnable on `ubuntu-latest` (privileged Docker,
  cgroup v2, the userns sysctl); I did not run it.

## Not verified

No k3s install (none of the findings needed a live cluster; the Helm and Argo CD hook semantics
cited are documented behaviour). I did not reproduce S13, because this host is cgroup v1. No
gVisor runtime was available (S7 rests on gVisor's syscall table). I did not run helm-diff (N14).

## Cleanup

Removed: the temporary `rh-pg-ssl` container and `rh-pg` network, and the `alpine/helm` image I
pulled. Left in `$S/review-helm/`: renders, override values, probe scripts, and two downloaded
source files (`dl/`, plain data).
