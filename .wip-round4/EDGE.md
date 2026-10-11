# EDGE report: the Compose profiles `caddy` and `egress-proxy`, and the worker's store proxy

Everything is uncommitted in the working tree. My area is `deploy/compose/**`, `services/worker/**` and the new `.github/workflows/compose-profiles.yml`. I did not change `services/web` or the engine (`src/forklift`).

## What I built, and where

### Worker: `FORKLIFT_STORE_PROXY` / `--store-proxy` (services/worker)

- `settings.py`: a new option, `--store-proxy` (variable `FORKLIFT_STORE_PROXY`, the name the contract fixes; it is not `FORKLIFT_WORKER_*`). It must be `http://host:port`. It is refused, with a specific message, when it has credentials (the engine is given the URL, and the engine holds no credentials), a path, a query or a fragment, a bad port or no port, or any scheme but http (urllib cannot talk TLS to a proxy; https stores still get end-to-end TLS through CONNECT). The value is normalised (host lower-cased, IPv6 in brackets). `--engine-env` now also refuses `*_PROXY` names (`NO_PROXY` included) and says to use `--store-proxy`.
- `transport.py`: `HttpClient(proxy=...)`.
  - Without a proxy, `ProxyHandler({})` means the client never takes a proxy from the environment.
  - With one, a `ProxyHandler` subclass sends every request through it: absolute-form requests for http, CONNECT for https. It overrides `proxy_open` because urllib's own handler still consults `NO_PROXY` from the environment even when proxies are given explicitly. I checked this in the stdlib source on 3.12, 3.13 and 3.14.
  - Errors name the proxy. A transport error ends with `, through the store proxy egress-proxy:3128`. A 403 from Squid (it has an `X-Squid-Error` header) is reported as `refused by the store proxy egress-proxy:3128 (ERR_ACCESS_DENIED)` instead of being taken for the store refusing the URL.
- `supervisor.py`: two clients. `http` for the gateway never uses a proxy; `store_http` (the proxy, if set) goes to `JobRunner` for staging, PUTs and multipart parts. The startup log line has `store_proxy`.
- `isolation.py`:
  - For streamed inputs with a store proxy, the engine's environment gets `http_proxy`, `https_proxy`, `HTTP_PROXY` and `HTTPS_PROXY` set to that proxy, and nothing else proxy-related.
  - Proxy variables from the worker's own environment are never passed. This replaces the old pass-through of ambient `HTTPS_PROXY`/`HTTP_PROXY`/`NO_PROXY`.
  - Under Landlock with a store proxy, the TCP rule allows the proxy's port only, not the store's.
  - `sandbox_config()` lost its unused `env` parameter.
- Tests:
  - `tests/forward_proxy.py`: a real HTTP forward proxy (absolute form and CONNECT, an allow list, Squid-like 403s) that records what it forwards.
  - `tests/unit/test_store_proxy.py`, end to end with the fake gateway and fake engine:
    - staging and uploads (including multipart parts) go through the proxy, and gateway calls never do;
    - a streaming engine reads through the proxy (its User-Agent is `Python-urllib`) with exactly the four variables;
    - under Landlock the engine can connect to the proxy's port and not to the store's;
    - a staged engine gets no proxy variables;
    - ambient `HTTP(S)_PROXY`/`ALL_PROXY` change nothing;
    - a refusing proxy fails the job with a clear message.
  - New cases in `test_transport.py`, `test_settings.py` and `test_isolation.py`.
  - `tests/integration/test_store_proxy.py` (with `FORKLIFT_TEST_SERVICES=1`): the real engine and RustFS-signed SigV4 URLs through the forward proxy. It covers a staged input with its artifacts, a streamed input read by the real engine, an 11 MiB output uploaded in 3 parts, and a proxy that refuses the store.
  - `fake_engine.py` now reads presigned URLs with the default opener, so it honours the proxy environment it is given, as the real engine does.
- `README.md`: a settings row, the engine-environment paragraph, the Landlock row, the "Container egress" bullet, and a new section "Reaching the store through a proxy" with the before/after of ambient proxies.

**Engine:** no change was needed. The engine's `PresignedUrlSource` uses `build_opener()`, which reads `http(s)_proxy` from its environment, and the worker now controls that environment exactly: the store proxy or nothing, and never `NO_PROXY`. I did not run `run_tree.py` because no engine file changed.

### What ambient proxy variables did before (checked, not assumed)

- **Supervisor.** `build_opener()` adds urllib's environment `ProxyHandler`. I ran the HEAD version of `transport.py` with `HTTP_PROXY` pointing at a recording proxy: the proxy received `POST http://…/internal/v1/leases` with `Authorization: Bearer <worker token>`. So an ambient `HTTP_PROXY` (or `HTTPS_PROXY` for an https gateway) got the worker token. Store requests went through it too, unless `NO_PROXY` matched.
- **Engine.** For streamed inputs the worker passed the ambient `HTTPS_PROXY`/`HTTP_PROXY`/`NO_PROXY` (without user info) and opened the proxy's port in Landlock as well as the store's.

Now the gateway is always direct, the store is direct or reached only through `--store-proxy` (whatever `NO_PROXY` says), and the engine gets only the store proxy. This is documented in the worker README.

### The `caddy` profile (deploy/compose)

- **`caddy/Caddyfile`**
  - Two sites: `{$FORKLIFT_DOMAIN}` goes to `gateway:8080`, with `/mcp` and `/mcp/*` going to `mcp:8000` (`flush_interval -1` for SSE); `{$FORKLIFT_STORE_DOMAIN}` goes to `rustfs:9000`.
  - Host passthrough: there is deliberately no `header_up Host`.
  - No body limit, no request buffering (Caddy's defaults), and both are commented.
  - HSTS on both sites (`FORKLIFT_HSTS_SECONDS`, default 31536000; 0 turns it off).
  - Certificates come from snippets: `FORKLIFT_TLS=acme` (`tls {$FORKLIFT_ACME_EMAIL}`) or `internal`.
  - Global options: `skip_install_trust` (the root file system is read-only) and a log filter that drops the health check's admin-API requests.
  - The store site is a snippet that is left out when `FORKLIFT_STORE_DOMAIN` is empty. That case is an external store with an https URL of its own; Caddy then serves the UI only.
- **`caddy/start.sh`** checks the settings before Caddy starts and exits 2 with the variable to fix. It catches: no domain, the two domains being the same, `acme` without an email, an unknown `FORKLIFT_TLS`, and a non-numeric HSTS value. It also chooses the store-site snippet.
- **`docker-compose.yml`**
  - New service `caddy` (`caddy:2.11.7`, profile `caddy`): ports `${FORKLIFT_HTTPS_HOST_PORT:-443}` (tcp and udp) and `${FORKLIFT_HTTP_HOST_PORT:-80}`, volumes `caddy-data` and `caddy-config`, read-only, `cap_drop ALL` plus `NET_BIND_SERVICE`, `no-new-privileges`, a health check on the admin API, and it depends on a healthy gateway.
  - New service `egress-proxy`, described in the next section.
  - The gateway's and the store's published ports take `FORKLIFT_BIND_ADDRESS` through `${FORKLIFT_BIND_ADDRESS:+${FORKLIFT_BIND_ADDRESS}:}`. When it is unset the port spec is exactly today's (`8080:8080`, every address). `generate_env.py --domain` sets it to `127.0.0.1`.
  - Store variables: `FORKLIFT_STORE_URL`, `FORKLIFT_STORE_BUCKET` and `FORKLIFT_STORE_REGION` for the gateway processes and init, and `FORKLIFT_STORE_HOST` for the worker's `--store-host`. Their defaults are today's literal values.
  - The worker gets `FORKLIFT_STORE_PROXY: ${FORKLIFT_STORE_PROXY:-}`.
  - I diffed `docker compose config` of the default stack at HEAD against mine: the only difference is the worker's empty `FORKLIFT_STORE_PROXY` (the worker ignores an empty value).
- **`generate_env.py`** is now importable, with `main(argv)`.
  - Options: `--domain`, `--store-domain` (default `store.<domain>`), `--acme-email` or `--internal-ca` (exactly one with `--domain`), `--https-port` and `--http-port` (URLs carry the port when it is not 443).
  - With `--domain` it writes `COMPOSE_PROFILES=caddy`, the https public URLs, `ALLOWED_HOSTS`, the TLS-proxy settings, `FORKLIFT_TRUSTED_PROXIES=1` and `FORKLIFT_BIND_ADDRESS=127.0.0.1`.
  - For an external store: `--store-url`, `--public-store-url`, `--store-bucket`, `--store-region`. The store credentials are written empty, so Compose refuses to start until they are filled in.
  - Contradictions are explained. For example, https UI plus an http store is refused as mixed content, and `--store-domain` with `--store-url` is refused.
  - With no options the output is the same as before: the same keys, and only a comment changed.
- **`deploy/compose/tests/test_generate_env.py`**: 34 tests, 100% line and branch coverage.

### The `egress-proxy` profile and external stores (deploy/compose)

- **`egress-proxy/squid.conf`**
  - Port 3128.
  - `include /run/forklift-egress/allow.conf`, then `http_access deny all`.
  - `cache deny all` and `cache_mem 0 MB`.
  - No body limits.
  - `forwarded_for delete` and `httpd_suppress_version_string on`.
  - `strip_query_terms on`, with a log format that ends with the User-Agent, written to stdout; requests from 127.0.0.1 (the health check) are not logged.
  - `pid_filename none`, `pinger_enable off`, `shutdown_lifetime 3 seconds`.
  - I removed `via off`: it logs "WARNING: HTTP requires the use of Via" at every start.
- **`egress-proxy/start.sh`** (bash, shellcheck clean)
  - It renders `allow.conf` from `FORKLIFT_EGRESS_PROXY_ALLOW`, which defaults to `FORKLIFT_STORE_URL`. Entries are URLs separated by spaces or commas.
  - For each entry it writes an `acl … dstdomain -n <host>`, or `dst <ip>` for IP literals, one `acl …_port port N`, and one rule: `allow CONNECT` for https, `allow !CONNECT plain_http` for http.
  - It validates every entry with a specific message, runs `squid -k parse`, then `exec squid -NYC`.
  - `start.sh --check` prints the rules and exits.
- **The `egress-proxy` service**: `ubuntu/squid:6.6-24.04_edge`, which reports Squid 6.14; it is the 24.04 LTS track, the newest-patched tag, and the one HELM uses. It runs as `user 13:13` (the image's `proxy` user), read-only, with `cap_drop ALL` and `no-new-privileges`. `/run/forklift-egress`, the spool and the log directory are tmpfs owned by uid 13. Networks are `internal` and `public`, nothing is published, and the health check is a real request from 127.0.0.1 (answered 403, not logged).
- **`external-store.yml`** (operator override)
  - `rustfs` moves to a profile `bundled-store`, so it is not started.
  - `init` gets `depends_on: !override {postgres}` and the `public` network.
  - `sweeper` gets the `public` network; both call the store.
  - The worker gets `FORKLIFT_STORE_PROXY=http://egress-proxy:3128` and depends on a healthy proxy.
  - `egress-proxy` gets `profiles: !reset []`, so it always starts with an external store and nobody can forget the profile. The worker stays on `internal`, which has no route out.
- **`-n` on `dstdomain`** (the lead's/HELM's finding). `start.sh` generates `dstdomain -n <host>`. Without `-n`, Squid matches a request to a bare IP by that IP's reverse-DNS name, and whoever controls the IP chooses the PTR record. The e2e `test_the_proxy_allows_exactly_the_store` now sends `http://<store-external's IP>:9000/health` through the proxy and asserts 403 `ERR_ACCESS_DENIED`; it passed against real Squid, which logged `TCP_DENIED/403 … GET http://172.25.0.2:9000/health`. The CI `compose-files` job also greps for `dstdomain -n`.
- **HELM parity.** HELM's ConfigMap squid.conf has the same directives as mine apart from the include path (`/etc/forklift-squid/allow.conf`); I compared the directive lists and they match. HELM's `test_the_egress_proxy_matches_the_compose_profile` enforces this. Its `allow.conf` has the same per-entry shape as what my `start.sh` writes.

### End-to-end tests (deploy/compose/e2e) and CI

- **`e2e/caddy.yml`**: an MCP stand-in. It is `receiver.py` on `public` with the network alias `mcp`, so it must not be started together with the real `mcp` profile.
- **`e2e/test_caddy.py`**: 9 tests, skipped unless the `.env` has `FORKLIFT_DOMAIN`. They cover:
  - https URLs on both domains;
  - the 308 redirect from http (path and query kept);
  - HSTS on both sites;
  - `Secure` session and CSRF cookies, and the session API working;
  - a client multipart upload: 3 presigned part PUTs to `https://store.<domain>`, then a run and a download from the store domain;
  - a 64 MiB single PUT;
  - client addresses: the audit log records the address the gateway sees when the client connects directly, never the forged `X-Forwarded-For: 203.0.113.9`;
  - `/mcp`, `/mcp/` and `/mcp/messages` reach the stand-in, and `/mcpx` reaches the gateway;
  - the gateway's and the store's ports are refused on the host's outgoing address and accepted on 127.0.0.1.
- **`e2e/egress-proxy.yml`**: the "external" store (a second RustFS on `public` only, published on 127.0.0.1), and `tls-stand-in` (Caddy with its internal CA), an https destination the proxy also allows, so a CONNECT that must succeed is tested against real Squid.
- **`e2e/test_egress_proxy.py`**: needs the docker CLI; skipped unless the `.env` has `FORKLIFT_STORE_URL`.
  - The proxy allows exactly the store (one http host:port) and the https stand-in through CONNECT. It refuses with 403: the store by its IP address, the store on another port, CONNECT to the store, CONNECT to the stand-in on another port, the stand-in over plain http, the gateway, `example.org`, and CONNECT to `example.org:443`.
  - The worker cannot reach the store directly, neither by name (DNS) nor by address, nor reach the internet.
  - A staged and a streamed input both succeed, and the proxy's log shows the supervisor's GET (`forklift-worker`), the engine's GET (`Python-urllib`), the artifact PUTs, and no `X-Amz-` anywhere.
- **Shared e2e changes**:
  - `test_stack.Client` sends `Origin: <base_url>` with session (CSRF) requests, as browsers do. Over https Django also checks Origin/Referer, so without it every session POST got 403.
  - New helper `store_config(settings)`, used by `test_schedules.py` and `test_publishing.py` instead of a hard-coded `http://rustfs:9000` and bucket `forklift`, so the whole suite runs against an external store too.
- **`.github/workflows/compose-profiles.yml`**: three jobs.
  - `compose-files`: lint `deploy/compose`, `generate_env.py` at 100%, `caddy fmt` and `caddy validate` with and without the store site, a `start.sh` refusal, and Squid `--check` for every kind of entry plus refusals.
  - `caddy`: generate a TLS env with `--internal-ca` on ports 48443/48480, add `/etc/hosts` entries, `up --build`, take the root CA, run test_caddy, test_stack, test_sign_in, test_schedules and test_publishing with `SSL_CERT_FILE`, then `down -v` and remove the hosts entries.
  - `egress-proxy`: an external-store env plus random credentials, `up --build` with `external-store.yml` and `e2e/egress-proxy.yml`, then the same test files with `test_egress_proxy.py`.
  - The test files are listed explicitly, so other agents' e2e files (test_client, test_oidc, test_mcp) do not run in my jobs.
- **`deploy/compose/README.md`**: rows in "What runs where", new settings rows, "TLS with Caddy (the caddy profile)" (when, variables, DNS and certificates, what it changes, verifying), "An external store and the egress proxy (the egress-proxy profile)" (when, the command, variables, what `external-store.yml` changes, the proxy's configuration and log, verifying, including `run --rm egress-proxy --check`, which I ran), and "Test it" for both profiles.

## Decisions and why

- **Contract variable name `FORKLIFT_STORE_PROXY`** (not `FORKLIFT_WORKER_STORE_PROXY`). The contract fixes it and HELM's chart already sets it (`deploy/helm/forklift/templates/workers.yaml`).
- **Ambient proxies are ignored entirely**, not merely given lower priority. It is the only way to guarantee that "the worker uses exactly the proxy it is configured with, for exactly the store". `NO_PROXY` can otherwise silently bypass even an explicit proxy in urllib.
- **Engine proxying through its environment, not a new CLI flag.** The worker builds the engine's whole environment from an allow-list, so this is exact and needs no engine change.
- **Store proxy errors name the proxy.** A misconfigured allow-list is the likeliest operator mistake.
- **Not publishing the gateway and store to everyone behind Caddy: a bind-address variable**, not an override file. Profiles cannot change other services' ports. `:+` interpolation keeps the default exactly as today, and `generate_env.py --domain` writes `127.0.0.1`. This matters for security: with `FORKLIFT_TRUSTED_PROXIES=1`, anyone who reaches the gateway's port directly can claim any client address. The audit test showed it, and the README says so.
- **HSTS from Caddy**, so no gateway change was needed.
- **Squid rules generated by `start.sh` from URLs, not Compose `configs.content`.** It needs parsing (scheme, port defaults, IP literals) and validation with clear errors, and Squid has no environment-variable substitution. The include-file shape lets Helm render the same `allow.conf` itself.
- **`external-store.yml` as an override, instead of `.env` alone.** `init` and the `sweeper` need a route to an external store, and RustFS should not run. Profiles cannot change networks or depends_on.
- **No method restriction on plain-http stores.** Squid could restrict methods only for http, not inside CONNECT, so it would be inconsistent, and it would break silently if workers ever needed another method.

## Findings worth knowing

1. **Ambient proxies received the worker token** (above). Now fixed, and proposed for the CHANGELOG's Security section.
2. **Django's https CSRF check.** Session (cookie) API requests over https need an `Origin` or `Referer` header besides `X-CSRFToken`; browsers send it, scripts may not. The gateway's 403 message ("send the value of the csrftoken cookie in an X-CSRFToken header (or use an API token)") is misleading in that case. A suggested gateway change for OIDC's area is below; it is optional.
3. **Caddy canonicalises header names.** The gateway's `X-Request-ID` reaches clients as `X-Request-Id`. HTTP headers are case-insensitive; the README mentions it.
4. **Brief-wide: host ports 48000-48999 are inside Linux's ephemeral port range** (32768-60999 here). Once, `up` failed with "address already in use" for 48480 because an outgoing connection briefly had that local port; a retry worked. CI is unaffected.
5. **The MCP service behind Caddy (for the lead / MCPR).** Caddy passes `Host: <FORKLIFT_DOMAIN>` (plus `:<port>` when `FORKLIFT_HTTPS_HOST_PORT` is not 443), and sets `X-Forwarded-Proto: https` and `X-Forwarded-For`. So `FORKLIFT_MCP_ALLOWED_HOSTS` must include that exact host value. Without the `mcp` service, `/mcp` answers 502. Do not run `e2e/caddy.yml`'s stand-in (alias `mcp`) together with the `mcp` profile.

## What I could not do or verify

- **Real ACME certificates.** There is no public DNS here. ACME mode is checked only for the configuration: `caddy adapt` shows Let's Encrypt and ZeroSSL issuers with the email, and start.sh refuses a missing email.
- **Multi-GiB uploads through Caddy are tested to 1 GiB** (a manual run, not in CI): two 512 MiB parts took 7.7 s, with Caddy at about 16 MiB of memory. CI tests 3 × 5 MiB parts and a 64 MiB PUT.
- **A real https external store with the real worker through Squid** is not tested end to end. Real Squid's CONNECT allow rule is tested with the stand-in, and the worker's CONNECT path with the forward proxy in unit tests.
- **Worker integration tests use the Python forward proxy, not Squid.** Squid is exercised by the Compose e2e with the real worker and engine. If you want Squid in the worker job too, a `squid` service in `tests/integration-tests/services/compose.yaml` (not my area) would do it.
- **CI has not run on GitHub.** I ran its steps locally: the compose-files checks and both stacks with the same commands, but locally with my own project names and ports and with the scratchpad's no-apt worker image (`$S/localbase/worker.Dockerfile`, built from `git archive HEAD` plus my `services/worker`). The gateway image was built from HEAD.
- **HTTP/3 (UDP 443)** is published but not tested.
- **`compose-files` lints all of `deploy/compose`.** At the time of writing CLIENT's `deploy/compose/e2e/test_client.py` fails black and flake8 (E501), which would fail that step until CLIENT formats it.
- **Images.** I removed my `forklift-web:edge` and `forklift-worker:edge`. I kept `caddy:2.11.7`, which I pulled: Docker Hub answered 429 to another pull during this session, and MCPR or the lead may need it for `/mcp` through Caddy. `ubuntu/squid:6.6-24.04_edge` was HELM's and I left it.

## Verification (commands and results)

| What | Result |
|---|---|
| Worker unit tests with coverage (`$S/edge/wt.py tests/unit --cov=forklift_worker --cov-branch --cov-fail-under=100`) on `$S/kvenv312`, `$S/kvenv` and `$S/kvenv314` | 388 passed each, 100% |
| Worker integration tests with `FORKLIFT_TEST_SERVICES=1` (`$S/edge/wts.py tests/integration`) on 3.12, 3.13 and 3.14 | 20 passed each, the 4 store-proxy tests included |
| black, isort and flake8 on `services/worker`, and on my `deploy/compose` files | clean |
| shellcheck (shellcheck-py) on both start scripts | clean |
| `generate_env.py` tests | 34 passed, 100% line and branch |
| Default stack with `e2e/webhooks.yml` (project `forklift-edge`, ports 48180/48190/48199): test_stack, sign_in, schedules, publishing, webhooks, with caddy and egress tests skipping | 14 passed, 12 skipped, at the end and once earlier |
| Caddy stack (project `forklift-edge-tls`, 48443/48480, gateway and store on 127.0.0.1:48181/48191, `/etc/hosts` entries for `forklift-edge.test` and `store.forklift-edge.test`, `SSL_CERT_FILE` = Caddy's root): test_caddy, test_stack, sign_in, schedules, publishing | 22 passed (egress tests skipped); 0 error lines in Caddy's log |
| External-store stack (project `forklift-edge-egress`, external RustFS on 127.0.0.1:48290): test_egress_proxy, test_stack, sign_in, schedules, publishing | 16 passed, after the `-n` change |
| compose-files CI steps run locally | all pass |
| Clean-up | all stacks `down -v`; `/etc/hosts` restored (diffed against my backup); my images and scratch leftovers removed |

## Exact text for the lead's shared files

### CHANGELOG.md, under `### Security`

```markdown
- **Workers ignore proxy variables in their environment.** `forklift-worker` used urllib's
  default proxy handling: an ambient `HTTP_PROXY` (or `HTTPS_PROXY` for an https gateway)
  received the worker's gateway requests, the worker token in their `Authorization` header
  included, and streaming engines were given the worker's `HTTPS_PROXY`/`HTTP_PROXY`/`NO_PROXY`.
  Now the gateway is always reached directly, the store directly or only through the new
  `--store-proxy` (even for hosts `NO_PROXY` names), the engine gets no proxy variable from the
  worker's environment, and `--engine-env` refuses `*_PROXY` names.
```

### CHANGELOG.md, under `### Added` (inside the platform bullet list, after "Fresh URLs for streamed inputs")

```markdown
  - **TLS and an egress proxy for Docker Compose.** The `caddy` profile puts Caddy in front of
    the gateway (`FORKLIFT_DOMAIN`; `/mcp` goes to the remote MCP server) and of the store
    (`FORKLIFT_STORE_DOMAIN`: browsers PUT and GET presigned URLs there), with certificates from
    ACME (`FORKLIFT_ACME_EMAIL`) or Caddy's own CA (`FORKLIFT_TLS=internal`), HSTS, no body
    limits or buffering, and the client's `Host` passed on unchanged (SigV4 signs it).
    `generate_env.py --domain ... (--acme-email ... | --internal-ca)` writes the settings the
    gateway needs behind it, and `FORKLIFT_BIND_ADDRESS=127.0.0.1` so that only Caddy is reachable
    from other hosts. An external store (`external-store.yml`, `generate_env.py --store-url`)
    replaces the bundled RustFS; workers stay on a network with no route out and reach it through
    the `egress-proxy` profile: Squid allowing only the store's URL (`CONNECT` to an https store's
    host and port, plain requests to an http one; `FORKLIFT_EGRESS_PROXY_ALLOW`), caching
    nothing and logging no query strings. New worker setting `--store-proxy`
    (`FORKLIFT_STORE_PROXY`): staging downloads, uploads and multipart parts go through it and
    gateway requests never; a streaming engine is given that proxy only and, under Landlock, may
    connect to its port only. End-to-end tests run every test over https behind Caddy and
    against an external store behind the proxy (`.github/workflows/compose-profiles.yml`).
```

### docs/design/platform.md

**§6.1**, the Streamed row of the table: change the last cell "The object store endpoint only" to:

```markdown
The object store endpoint only (or the allow-listing proxy in front of it, §6.2)
```

**§6.1**, replace the last paragraph ("Either way, **the code that parses untrusted data never holds credentials.** …") with:

```markdown
Either way, **the code that parses untrusted data never holds credentials.** A presigned URL opens
exactly one object, for a limited time, and the engine's network is limited to the store. As
built, a worker with `FORKLIFT_STORE_PROXY` sends every store request (staging downloads, uploads,
multipart parts) through that allow-listing proxy and gateway requests never; a streaming engine
is given that proxy only and, under Landlock, may connect to its port only. Proxy variables in the
worker's own environment are ignored by the supervisor and never reach the engine.
```

**§6.2**, the last paragraph: append after "The chart supports all three (§10.2).":

```markdown
Compose has the third as the `egress-proxy` profile (§10.1).
```

**§8**:

- Security table, Workers row: change "engine network limited to the object store (none for staged inputs)" to:

  ```markdown
  engine network limited to the object store or its allow-listing proxy (none for staged inputs)
  ```

- Residual risks: change "behind proxies the per-address limit depends on `FORKLIFT_TRUSTED_PROXIES`" to:

  ```markdown
  behind proxies the per-address limit depends on `FORKLIFT_TRUSTED_PROXIES` and on the gateway's port being reachable only through the proxy (Compose: `FORKLIFT_BIND_ADDRESS=127.0.0.1`)
  ```

**§10.1**, replace "The `caddy`, `mcp` and egress-proxy profiles are not built yet." with the following (add the lead's own `mcp` sentence):

```markdown
The `caddy` profile terminates TLS for two names: the UI and API (`FORKLIFT_DOMAIN`, with `/mcp`
routed to the remote MCP server) and the store (`FORKLIFT_STORE_DOMAIN`), because browsers PUT and
GET presigned URLs there and an https page cannot use an http store; certificates come from ACME
or Caddy's own CA, Caddy passes the client's `Host` on unchanged (SigV4 signs it) and neither
limits nor buffers bodies, and the gateway's and store's ports are then published on localhost
only (`FORKLIFT_BIND_ADDRESS`). With an external store (`external-store.yml`), the bundled RustFS
is not started, `init` and the sweeper join `public`, and the worker, still on `internal` only,
reaches the store through the `egress-proxy` profile's Squid, which allows the store's URL and
nothing else (`CONNECT` for https, plain requests for http; no caching, no query strings in its
log).
```

**§13**, the M4 row: append to "Built so far: …":

```markdown
; the Compose profiles for TLS (Caddy) and for an external store behind an allow-listing egress proxy
```

### docs/DOCUMENTATION_INDEX.md

There are no index lines for the Compose or worker READMEs yet. Under `## Platform (platform/)`:

```markdown
- [deploy/compose/README.md](../deploy/compose/README.md): the Docker Compose stack: services, settings, TLS with Caddy (the `caddy` profile), an external store behind the egress proxy (the `egress-proxy` profile), tests
- [services/worker/README.md](../services/worker/README.md): the worker: how a job runs, settings, isolation profiles, reaching the store through a proxy
```

### .github/workflows/test.yml

No change is required. My jobs are in `compose-profiles.yml`, and `platform-e2e` keeps working: my new e2e files skip there, and I re-ran the default stack's suite. Optional: `platform-e2e` could also run `deploy/compose/tests`, but `compose-profiles.yml` already does.

### Gateway (OIDC's area): optional

Not needed for anything I built. It is a clearer message for finding 2. In `services/web/src/forklift_web/api/auth.py`, `SessionCookieAuth.__call__`, change the 403 detail to:

```python
                "CSRF check failed: send the value of the csrftoken cookie in an X-CSRFToken "
                "header and, over https, an Origin (or Referer) header of this site (or use an "
                "API token).",
```

The gateway tests that assert the old text would need the same update. No other gateway setting was missing: HSTS comes from Caddy, and `FORKLIFT_BEHIND_TLS_PROXY`, `FORKLIFT_SECURE_COOKIES`, `FORKLIFT_TRUSTED_PROXIES`, `FORKLIFT_PUBLIC_URL` (CSRF and CORS origins) and the public store URL (CSP `connect-src`) were all enough.
