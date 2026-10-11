# REVIEW-EDGE: review of commit 4f3cb7c (worker store proxy, Compose `caddy` and `egress-proxy` profiles)

Reviewer: REVIEW-EDGE. I read every changed line (`git show 4f3cb7c`, 31 files) and checked the risky parts against the real software: the worker's code, real Squid (`ubuntu/squid:6.6-24.04_edge`, Squid 6.14), real Caddy (`caddy:2.11.7`), RustFS 1.0.1 and the real engine. I changed no repository file. Probes and fix prototypes are in `$S/review-edge/`. All my containers, networks and volumes are removed.

`S=/tmp/claude-0/-home-user-forklift/32052cf1-7a9c-57b5-940d-29385d406628/scratchpad`

## Summary

| # | Severity | Where | What |
|---|---|---|---|
| B1 | **blocking** | `deploy/compose/egress-proxy/start.sh:49-51` (and Helm `templates/egress-proxy.yaml:66`) | An IP-literal allow entry (`dst`) also matches any host name that resolves to that IP, even partly. Squid then connects to the name's *other* addresses, so the allow-list can be bypassed. |
| S1 | should-fix | `deploy/compose/caddy/Caddyfile:30-32` | Caddy's error log records the full presigned URL, credential and signature included. It does so for every failed proxied request, an aborted browser upload among them. |
| S2 | should-fix | `deploy/compose/egress-proxy/squid.conf` | Squid adds `Cache-Control: max-age=259200` to every request it forwards. For an http store, the worker's PUTs save it as object metadata, so every later download of an artifact (`bad_rows` included) is cacheable for 3 days. |
| S3 | should-fix | `services/worker/src/forklift_worker/transport.py:194-204` | With an https store (the main case), a proxy refusal is a *transient* `TransportError` (`OSError: Tunnel connection failed: 403 Forbidden`). It is retried, the job is marked retryable, and the message is not the one the README promises. Squid's 503 errors are also called "refused". |
| S4 | should-fix (docs) | `services/worker/README.md:243-245` | For a streamed input, the engine reports the proxy's refusal as "the object store refused the presigned URL … may have expired". The proxy is not named, and the engine asks the gateway for a fresh URL first. |
| S5 | should-fix | `Caddyfile:47`, `docker-compose.yml:253-262`, `e2e/test_caddy.py:92-98` | The http→https redirect drops a non-default https port (`--https-port 48443` redirects to `https://<domain>/…`, port 443). The test leaves the port out of its assertion, which hides this. |
| S6 | should-fix (docs, residual risk) | `deploy/compose/README.md:139-141`, worker README, design §8 | Allowing a store by host name cannot stop a compromised engine from writing to *other* buckets on a shared host such as AWS S3 (a CONNECT tunnel is opaque to Squid). This residual risk is not documented. |
| S7 | should-fix (low) | `docker-compose.yml:291`, `compose-profiles.yml:39`, Helm `values.yaml:349` | `ubuntu/squid:6.6-24.04_edge` is a mutable risk-channel tag. Pin it by digest. |
| N1-N7 | nit | various | Proxy password echoed in a settings error; port 0 accepted; newline in the allow list silently drops entries; validation mismatches; ACME port warning; Caddy path matching; startup warning for ignored ambient proxies. |

The worker's main security claim holds. Gateway requests (the worker token) never go through a proxy or anywhere but `--gateway`, and with `--store-proxy` no store request bypasses the proxy. Details are under "Checked and sound".

---

## B1 (blocking): IP-literal allow entries let a compromised engine reach any address through the proxy

**Where:** `deploy/compose/egress-proxy/start.sh:49-51`

```bash
if [[ "$host" =~ ^[0-9.]+$ || "$host" == \[* ]]; then
    host=${host#[}
    echo "acl ${acl} dst ${host%]}"
```

The same rendering is in HELM's `deploy/helm/forklift/templates/egress-proxy.yaml:66` (`acl … dst {{ $entry.host }}`).

**Why it is wrong.** Squid's `dst` ACL resolves the requested host name and matches when *any* of its addresses is in the list. Squid then forwards to the name's addresses in its own order, which need not start with the matching one. A name such as `x.attacker.example` with A records `[the store's IP, the attacker's IP]` therefore passes the rule, and the connection goes to the attacker. The threat is an engine compromised through a malicious file: under Landlock it may connect only to the proxy's port, but through the proxy it can send any request. With the fix below the proxy is the only path out, and it then exfiltrates nothing. Without the fix it can send data to any IP on the store's port. If the store is on port 80, that includes `169.254.169.254` (cloud instance metadata).

`start.sh:11` documents "an IP address matches that address", and CI (`compose-profiles.yml:99,103`) renders and accepts `http://10.0.0.5`, so this is a supported configuration.

**Reproduction** (real Squid with the repository's `squid.conf` and `start.sh`). Two upstream echo servers, `good` (172.19.0.2) and `evil` (172.19.0.3), share a Docker network alias `multi`, so Docker's DNS answers `multi` with both addresses. The allow list is the IP literal of `good`:

```bash
docker run -d --name rev-edge-squid --network rev-edge-net --user 13:13 --read-only --cap-drop ALL \
  --tmpfs /run/forklift-egress:uid=13,gid=13,mode=0700 --tmpfs /var/spool/squid:uid=13,gid=13,mode=0700 \
  --tmpfs /var/log/squid:uid=13,gid=13,mode=0700 -e "FORKLIFT_EGRESS_PROXY_ALLOW=http://172.19.0.2:8000" \
  -v "$PWD/squid.conf:/etc/squid/squid.conf:ro" -v "$PWD/start.sh:/etc/squid/forklift/start.sh:ro" \
  --entrypoint bash ubuntu/squid:6.6-24.04_edge /etc/squid/forklift/start.sh
# client.py: N GETs through http://rev-edge-squid:3128, counting who answered
```

```
http://172.19.0.2:8000/x {'200 good': 1}
http://multi:8000/x {'200 evil': 20}                 <- allowed, and all 20 went to the other address
http://goodname:8000/x {'200 good': 3}               <- any name resolving to the IP is allowed too
http://rev-edge-evil:8000/x {'403 ERR_ACCESS_DENIED 0': 3}
```

Scripts: `$S/review-edge/srv/upstream.py`, `$S/review-edge/srv/client.py`.

**Fix (verified).**

1. Render `dst -n <ip>`. With `-n`, Squid does no DNS lookup for this ACL, so it matches only URLs whose host *is* that IP literal.
2. As defence in depth, add these two lines before the `include` in `squid.conf`:

   ```
   http_access deny to_localhost
   http_access deny to_linklocal
   ```

With both changes (`$S/review-edge/sqfix3/`), the same scenario gives:

```
http://172.19.0.2:8000/x {'200 good': 2}
http://multi:8000/x {'403 ERR_ACCESS_DENIED 0': 10}
http://goodname:8000/x {'200 good': 2}          <- hostname entry, still allowed
http://rev-edge-evil:8000/x {'403 ERR_ACCESS_DENIED 0': 2}
```

`start.sh --check` with `http://10.0.0.5 https://s3.eu-west-1.amazonaws.com https://[fd00::5]:8443` then parses, and renders `acl forklift_store_1 dst -n 10.0.0.5` and `acl forklift_store_3 dst -n fd00::5`.

Follow-ups for the fix:

- Update the CI grep at `compose-profiles.yml:103` (`dst -n 10.0.0.5`).
- Mirror the change in HELM's template.
- If feasible, add an e2e case: the external store's IP as an allow entry, plus a name that resolves to it, for example through a Compose network alias shared with another container.

---

## S1 (should-fix): Caddy logs presigned URLs, signatures included

**Where:** `deploy/compose/caddy/Caddyfile:30-32` (`log default { exclude admin.api }`).

**Why.** Caddy writes no access log here, but its `http.log.error` logger records every failed proxied request with `request.uri`, query string and all. Failures happen in normal operation: a browser tab closed during an upload, a client aborting a download, RustFS restarting. A presigned URL is a bearer credential until it expires. Download URLs open job artifacts, `bad_rows` (PII) among them. Squid's config takes care to strip query terms (`strip_query_terms`); Caddy's does not. Caddy redacts `Authorization` and `Cookie` by default (I checked: `"Authorization":["REDACTED"],"Cookie":["REDACTED"]`), but not URIs. The engineer's "0 error lines in Caddy's log" was a run with no failures.

**Reproduction** (real Caddy with the repository's Caddyfile and `start.sh`, `FORKLIFT_TLS=internal`, stand-in upstreams).

A browser-like client aborts a 100 MiB PUT after 1 MiB (`$S/review-edge/abort_upload.py`):

```
{"level":"error","logger":"http.log.error","msg":"readfrom tcp …->…:9000: unexpected EOF","request":{…,"method":"PUT","host":"store.rev-edge.test:47443","uri":"/forklift/uploads/u2/big.csv?X-Amz-Credential=forklift-abc%2F20261011&X-Amz-Signature=ABORTEDSIGNATURE",…},"status":502,…}
```

The store is down (`docker stop` the stand-in), and a GET with a presigned query arrives:

```
{"level":"error","logger":"http.log.error","msg":"dial tcp: lookup rustfs …","request":{…,"uri":"/forklift/uploads/u1/people.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=forklift-abc%2F20261011%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=deadbeefSIGNATURE",…}}
```

**Fix (verified with caddy:2.11.7, `$S/review-edge/caddyfix/Caddyfile`).** Filter the query in the default logger:

```
log default {
	exclude admin.api
	format filter {
		request>uri query {
			replace X-Amz-Signature REDACTED
			replace X-Amz-Credential REDACTED
			replace X-Amz-Security-Token REDACTED
		}
	}
}
```

The same failing request then logs `"uri":"/forklift/x?X-Amz-Credential=REDACTED&X-Amz-Expires=900&X-Amz-Signature=REDACTED"`, still as JSON. A test could cover this: in test_caddy, PUT a presigned part URL and close the connection mid-body, then assert that `docker logs caddy` contains no `X-Amz-Signature=` followed by anything other than `REDACTED`.

---

## S2 (should-fix): Squid stamps `Cache-Control: max-age=259200` on objects the worker uploads

**Where:** `deploy/compose/egress-proxy/squid.conf`: there is no `request_header_access` rule. Squid adds `Cache-Control: max-age=<refresh_pattern max>` (default 4320 min = 259200 s) to every request it forwards.

**Why.** For an http store, the worker's artifact PUTs and multipart parts reach the store with that header. S3-compatible stores save `Cache-Control` from a PUT as object metadata and return it on every GET. So every artifact uploaded through the proxy is served to browsers and downstream readers with `Cache-Control: max-age=259200` and no `private`, which any shared cache may honour. That includes `bad_rows.parquet`, which holds PII. Server-side copies keep metadata by default (CopyObject's `MetadataDirective` is `COPY`), so published copies can carry it too. That conflicts with the classification and retention model (§5.7, §8): a cached copy outlives the object's deletion. An https store is not affected, because Squid cannot change headers inside a CONNECT tunnel.

**Reproduction.** Real Squid (repository config, allow `http://rev-edge-rustfs:9000`) in front of my own RustFS 1.0.1. A presigned PUT goes through Squid (signed with boto3 the way the gateway signs), then a direct GET follows, as a browser would make it (`$S/review-edge/p4_presign.py`):

```
PUT through Squid: 200
GET (as a browser would, direct): Cache-Control = max-age=259200
```

The headers Squid forwarded to an echo upstream for a plain urllib PUT:

```
{'Accept-Encoding': 'identity', 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': '3', 'Host': 'goodname:8000', 'User-Agent': 'Python-urllib/3.13', 'Content-MD5': '…', 'Via': '1.1 988cdeab6369 (squid)', 'Cache-Control': 'max-age=259200', 'Connection': 'keep-alive'}
```

**Fix (verified, `$S/review-edge/sqfix/squid.conf`).** Add this line to `squid.conf`. Workers never send `Cache-Control` on store requests, so nothing legitimate is lost.

```
request_header_access Cache-Control deny all
```

The upstream then receives no `Cache-Control`. The e2e could assert that a job artifact in the external store has no `Cache-Control` (HEAD it with the store credentials). HELM's copy needs the same line, to keep parity.

---

## S3 (should-fix): an https store's refusal by the proxy is "transient", retried, and worded differently from the docs

**Where:** `services/worker/src/forklift_worker/transport.py:194-204`. The README's claim is at `services/worker/README.md:243-245`.

**Why.** Squid answers a denied CONNECT with `403`. urllib's tunnel code (`http.client._tunnel`) raises `OSError("Tunnel connection failed: 403 Forbidden")`; it is not an `HTTPError`, so the `X-Squid-Error` branch never runs. The worker turns it into a `TransportError`, which `is_transient()` treats as worth retrying. The effects:

- Staging retries 3 times and uploads 5 times, with backoff.
- The job result is `retryable: True`, and the UI then says "Running the same job again may succeed." (`ui/templates/ui/jobs/_live.html:53`).
- The message is not the documented "`refused by the store proxy egress-proxy:3128 (ERR_ACCESS_DENIED)`".

An https store (S3, for example) is the main use of the proxy, and it is the case the unit, integration and e2e tests do not cover: every "refused" test uses an http store.

Related wording problems:

- Any `X-Squid-Error` answer is called "refused". A 503 `ERR_CONNECT_FAIL` (the store is down) or `ERR_DNS_FAIL` becomes "`refused by the store proxy … (ERR_CONNECT_FAIL)`". I saw both headers from real Squid.
- A staging 403 through Squid reads "The object store at X refused the input's presigned URL (HTTP 403: refused by the store proxy …); it may have expired before the job started" (`staging.py` `_status_error`). That sentence blames the store and the URL's age, then the proxy.
- In multipart uploads a proxy 403 also triggers fresh part URLs and retries (`uploads.py:187`).

**Reproduction** (`$S/review-edge/p1_https_refused.py`, `p3_https_stage.py`, using the tests' `ForwardProxy`, which answers like Squid):

```
TransportError | store.example.org: OSError: Tunnel connection failed: 403 Forbidden, through the store proxy 127.0.0.1:35015 | transient: True
HttpStatusError | store.example.org answered HTTP 403: refused by the store proxy 127.0.0.1:35015 (ERR_ACCESS_DENIED) | transient: False

code: INPUT_UNREADABLE | retryable: True
message: Downloading the input from s3.eu-west-1.amazonaws.com failed (OSError: Tunnel connection failed: 403 Forbidden, through the store proxy 127.0.0.1:44645).
CONNECTs the proxy refused: ['s3.eu-west-1.amazonaws.com:443', 's3.eu-west-1.amazonaws.com:443', 's3.eu-west-1.amazonaws.com:443'] waits between them: [0.79, 1.4]
```

The first two lines contrast the https and http cases. The last three are a real `stage_input` of an https URL.

**Suggested fix.**

- In `HttpClient._open`, when `self.proxy` is set and the `OSError` text matches `^Tunnel connection failed: (\d{3})`, raise `HttpStatusError(host, code, f"the store proxy {self.proxy} refused the tunnel (CONNECT answered {code})")`. That makes 403 and 407 non-transient, and keeps 502, 503 and 504 transient.
- Word non-403 `X-Squid-Error` answers as "the store proxy … answered HTTP 503 (ERR_CONNECT_FAIL)" rather than "refused".
- Give a proxy refusal its own non-retryable message in staging and multipart, without the "may have expired" sentence.
- Add tests: an https URL through `ForwardProxy(allow=[])` in `test_transport.py`, and a job in `test_store_proxy.py`.

---

## S4 (should-fix, docs): for a streamed input, a proxy refusal is reported as the store refusing the URL

**Where:** `services/worker/README.md:243-245`: "A store request the proxy refuses fails the job with the proxy named (`... refused by the store proxy egress-proxy:3128 (ERR_ACCESS_DENIED)`), and a proxy that cannot be reached is named in the error too."

**Why.** For a streamed input the supervisor never touches the store before the engine runs, so the first refusal reaches the engine, which knows nothing about proxies. It treats the 403 as a refused URL, asks the supervisor for a fresh one (a gateway round trip), gets 403 again, and fails with a misleading message. The README's claim covers the supervisor's requests only.

**Reproduction.** The real engine, RustFS on 19000 and the tests' `ForwardProxy` with a refusing allow list. The scratch test is `$S/review-edge/ittest/test_probe_streamed_refused.py`; it reuses the worker's integration fixtures and runs with `$S/review-edge/itrun.py`.

```
PROBE code: PERMISSION_DENIED | retryable: False
PROBE message: The object store at '127.0.0.1' refused the presigned URL (HTTP 403) although it had just been refreshed; it may have expired or have been signed for another object or with credentials that have ended
PROBE requests the proxy refused: 2
```

**Fix.** At least correct the README: name the cases in which the proxy is identified, and say that a streamed input's refusal shows up as the store's. A real fix is an engine change (not EDGE's area): `PresignedUrlSource` could recognise an `X-Squid-Error` header on a 403 and say "refused by the HTTP proxy" without refreshing the URL. The worker could also run a cheap startup or per-job check that the proxy allows the store host, a CONNECT or HEAD to `--store-host`, and log a clear error.

---

## S5 (should-fix): the http→https redirect loses a non-default https port

**Where:** `deploy/compose/caddy/Caddyfile:47` relies on Caddy's automatic redirects. `docker-compose.yml:260-262` maps host ports to the container's 443/80, and the test is `deploy/compose/e2e/test_caddy.py:92-98`.

**Why.** Caddy listens on 443 inside the container, so its automatic redirect goes to `https://{host}{uri}` without a port. With `--https-port 48443 --http-port 48480` (the documented test setup, and the case where an operator uses other ports because 443 is taken), `http://<domain>:48480/…` redirects to `https://<domain>/…`. Nothing on port 443 there serves forklift, or worse, another service on the host does. `test_plain_http_is_redirected_to_https` checks scheme, host, path and query but not the port, so CI passes.

**Reproduction** (real Caddy, the repository's Caddyfile, host ports 47443/47480):

```
$ curl -sS -o /dev/null -D - --resolve rev-edge.test:47480:127.0.0.1 "http://rev-edge.test:47480/jobs/?page=2"
HTTP/1.1 308 Permanent Redirect
Location: https://rev-edge.test/jobs/?page=2
```

**Fix (verified, `$S/review-edge/caddyfix2/Caddyfile`).**

1. Pass `FORKLIFT_HTTPS_HOST_PORT` to the caddy service.
2. In `start.sh`, export `FORKLIFT_HTTPS_PORT_SUFFIX` (`""` for 443, `:<port>` otherwise).
3. Add explicit http sites, one before the UI site and one inside the `(store-site)` snippet. An explicit `http://<name>` site replaces Caddy's automatic redirect for that name; a catch-all `http://` does not, which I tried.

```
http://{$FORKLIFT_DOMAIN} {
	redir https://{host}{$FORKLIFT_HTTPS_PORT_SUFFIX}{uri} 308
}
```

Result, with `caddy fmt` clean:

```
Location: https://rev-edge.test:47443/jobs/?page=2
Location: https://store.rev-edge.test:47443/jobs/?page=2
```

Caddy answers ACME HTTP-01 challenges before site routes, so ACME should be unaffected. Confirm that in an ACME setup, since I could only check it with the internal CA. Also make the test assert `location.port` against `FORKLIFT_HTTPS_HOST_PORT`.

---

## S6 (should-fix, docs / residual risk): a host allow-list does not confine a shared store such as AWS S3

**Where:**

- `deploy/compose/README.md:139-141`: "allows the store's URL and nothing else".
- `services/worker/README.md`, "Reaching the store through a proxy".
- Design §8, residual risks: "(though only its own presigned URLs grant access to anything)".
- The proposed §10.1 text in EDGE.md.

**Why.** For an https store, Squid sees only `CONNECT host:443`. Whatever request goes inside the tunnel is opaque to it: any bucket, key or method, signed with any credentials, including an attacker's own presigned PUT or a public-write bucket. On a multi-tenant endpoint like `s3.<region>.amazonaws.com`, which the docs use as the example, a compromised engine can therefore exfiltrate to an attacker's bucket through the allowed host. For an http store, every method and path on the host is allowed, which is the engineer's documented choice. This is inherent to host-based allow-listing; the problem is that operators are told otherwise.

**Proof** (real Squid allowing only `https://store.rev-edge.test`, a Caddy TLS site in front of an echo upstream):

```
proxy: HTTP/1.1 200 Connection established
store got: PUT /someone-elses-bucket/exfiltrated.bin?X-Amz-Signature=signed-with-other-credentials HTTP/1.1
Squid log: TCP_TUNNEL/200 3227 CONNECT store.rev-edge.test:443 …
```

**Fix (docs).**

- Say "the store's host (and port)", not "the store's URL and nothing else".
- Add the residual risk to the README and §8: on a shared store host the proxy limits *where* the engine connects, not *which bucket* it writes to.
- Point to the real controls: on AWS, a VPC gateway endpoint with an endpoint policy that limits access to the installation's bucket (or `aws:ResourceAccount`); on MinIO or Ceph, a dedicated endpoint or no anonymous or public-write buckets.

---

## S7 (should-fix, low): `ubuntu/squid:6.6-24.04_edge` is a mutable, least-tested channel tag

**Where:**

- `deploy/compose/docker-compose.yml:291`
- `.github/workflows/compose-profiles.yml:39`
- HELM `deploy/helm/forklift/values.yaml:349`, which has an empty `digest: ""` field.

**Facts** (Docker Hub API, `hub.docker.com/v2/repositories/ubuntu/squid/tags`, checked today, plus `squid -v` in the images):

| Tag | Updated | Digest | Squid |
|---|---|---|---|
| `6.6-24.04_edge` | 2026-05-07 | `sha256:8a3baed477e2…` | 6.14 (`6.14-0ubuntu0.24.04.2`) |
| `6.6-24.04_beta` (= `latest` = `edge`) | 2025-11-24 | `sha256:6a097f68bae7…` | 6.13 (`6.13-0ubuntu0.24.04.3`) |
| `7.2-26.04_edge` | 2026-10-09 | `sha256:5e112df07ad5…` | (26.04 LTS track; 2 days old) |

There is no `_candidate` or `_stable` tag for any track. `_beta` lags behind on patches, so `6.6-24.04_edge` is a defensible choice. But Canonical rebuilds `_edge` tags in place (this one last on 2026-05-07), so a tag pin does not pin the image.

**Fix.** Pin the digest:

```
ubuntu/squid:6.6-24.04_edge@sha256:8a3baed477e2c282ab8aa5edad442f69873246964f225c5c2ae8364b6610963c
```

That is the repo digest `docker pull` reported, and it matches the Hub API. Apply it in Compose, in the CI `SQUID_IMAGE` and in HELM's `digest`. Also document why the edge channel was chosen and how to move it (Dependabot's `docker` ecosystem tracks digests).

---

## Nits

**N1. A `--store-proxy` password is echoed to stderr.**

Where: `settings.py:208`, printed by `cli.py:26`, so it ends up in the container log.

```
'http://user:s3cr3t-pass@egress-proxy:3128' REFUSED: 'http://user:s3cr3t-pass@egress-proxy:3128' carries credentials: …
```

The refusal is right, but the message repeats the secret. The same pattern already exists for `--gateway` (`settings.py:148`). Suggestion: print the URL without userinfo, for example `http://***@egress-proxy:3128`, in both places.

**N2. Port 0 is accepted and silently becomes port 80.**

`http://egress-proxy:0` normalises to `http://egress-proxy`, because `host_of` drops a falsy port. The worker then connects to port 80, and Landlock opens 80 (`p2_proxy_url.py`: `'http://egress-proxy:0' -> 'http://egress-proxy' port_of: 80`). Use `if not port:` in `_proxy_url`. A space in the host (`http://egress proxy:3128`) is also accepted.

**N3. A newline in `FORKLIFT_EGRESS_PROXY_ALLOW` silently drops the following entries.**

`start.sh:29` uses `read -r -a`, which reads only the first line. `printf 'http://a.example:9000\nhttp://b.example:9000'` renders only `a.example`. That fails closed, but without a word, and a YAML block scalar in Helm or Compose produces exactly this. Use `"${FORKLIFT_EGRESS_PROXY_ALLOW//[$'\n\t,']/ }"`, or refuse newlines with a message.

**N4. The two scripts validate store URLs differently.**

- `start.sh:49` treats any run of digits and dots as an IP. `123`, `1.2.3` and `010.0.0.5` render `dst 123` and so on, and Squid accepts them with getaddrinfo/inet_aton rules (`1.2.3` is 1.2.0.3, `010` is octal).
- `generate_env.py` accepts store URLs that `start.sh` (and the worker's `--store-host`) then refuse: a trailing dot (`https://s3.example.org.`) or an underscore (`https://s3_x.example.org`).

Both fail at start with a clear message, but validating the same way in `generate_env.py`, and accepting only dotted-quad IPv4 in `start.sh`, would be cleaner.

**N5. `generate_env.py` has two small gaps.**

- `--acme-email` together with `--https-port`/`--http-port` other than 443/80 is accepted without a warning, although ACME's HTTP-01 and TLS-ALPN-01 challenges need the public ports 80 and 443.
- `_EMAIL` allows `$`, which Compose then interpolates when it reads `.env`.

**N6. Caddy's `path` matcher is case-insensitive and matches the cleaned, decoded path.**

`/MCP`, `/Mcp/x`, `/api/../mcp`, `/%6dcp` and `//mcp` go to the MCP server. `/mcp/../api/v1/me` goes to the gateway, with the raw path. `/mcpx` and `/mcp%00` go to the gateway. This is harmless, since both are public with their own authentication, but the README and test describe exactly `/mcp` and `/mcp/*`. It is worth one sentence for MCPR, whose server sees the raw path.

**N7. Ignored ambient proxy variables go unannounced (optional).**

The worker now ignores `HTTP(S)_PROXY`. That is right, and it is in the CHANGELOG. A one-time startup warning when such variables are set ("…_PROXY is ignored; use --store-proxy") would save operators who relied on them, for example Docker's client-side proxy injection, from a confusing network error.

---

## Checked and sound

### Worker (`services/worker`)

- **Gateway requests never use a proxy.** `GatewayClient` gets `Supervisor.http` (`supervisor.py:240`), built with `ProxyHandler({})`. Because an instance of `ProxyHandler` is passed in, `build_opener` drops its default environment handler.
- **Store requests with a proxy.** `_Proxy.proxy_open` sets the proxy for http (absolute form) and https (a tunnel), and never consults `NO_PROXY`. The tests cover this with `NO_PROXY=*`, and I re-read the stdlib's `ProxyHandler.proxy_open`, `Request.set_proxy` and `AbstractHTTPHandler.do_open`.
- **Every store path uses the store client.** Staging, single PUTs and multipart parts all use `JobRunner.http`, which is `store_http`. No other urllib or `http.client` use exists in the worker.
- **Redirects** are still refused (`_NoRedirect`).
- **Fresh input URLs.** The refresh comes from the gateway, directly, and the engine fetches the new URL through the proxy.
- **The engine's environment.**
  - It is still an allow-list. A streaming engine gets exactly `http_proxy`/`https_proxy`/`HTTP_PROXY`/`HTTPS_PROXY` with the proxy value, which cannot carry credentials, and no `NO_PROXY`.
  - A staged engine gets none of them.
  - `--engine-env` refuses `*_PROXY` (`NO_PROXY` and `no_proxy` included).
  - The worker token and the gateway address never reach the engine.
  - The engine's own `build_opener()` honours its environment. The real engine went through the proxy in the integration test, with User-Agent `Python-urllib`.
- **Landlock.**
  - With a proxy, a streaming engine may connect to the proxy's port only. Without one, exactly the store ports (`stream_ports`), with no ambient proxy ports any more.
  - Staged engines get no TCP at all, and SQL jobs stay unrestricted (documented).
  - The Landlock unit test ran here (ABI 7), not skipped.
- **Proxy URLs.**
  - An https proxy URL is refused with a clear message.
  - An IPv6 proxy host is normalised to brackets and works in `HTTPConnection` and Landlock.
  - A proxy that never answers times out at `http_timeout`, transient and named (`timed out, through the store proxy …`).
  - A proxy that answers CONNECT 200 and then garbage gives a TLS error, named (covered by `test_https_goes_through_a_tunnel_and_errors_name_the_proxy`).
- **Error messages** carry the host only, never the query.
- **Tests.** Worker unit tests: 388 passed on Python 3.12, 3.13 and 3.14. The store-proxy integration tests (`FORKLIFT_TEST_SERVICES=1`, RustFS): 4 passed.

### Squid config and `start.sh`

- Host names use `dstdomain -n` (a bare IP is refused). They match exactly, with no subdomains.
- CONNECT is allowed only to an https entry's host and port. A plain request to an https entry's host is refused, and so is a CONNECT to an http entry's host (I checked `CONNECT goodname:8000` → 403).
- These were all refused with 403:
  - the cache manager (`/squid-internal-mgr/*`, `cache_object://`) and internal static pages;
  - `ftp://` URLs;
  - the userinfo trick `http://allowed:8000@evil:8000/`;
  - the gateway;
  - `example.org`.
- Uppercase and trailing-dot request hosts reach the same allowed host.
- **What reaches the store.** `X-Forwarded-For` is deleted and `Proxy-Authorization` is stripped. `Via` has no version.
- **Logs.**
  - The access log has no query strings, for allowed requests, 503s or denials alike.
  - `cache_log` printed no URLs for `ERR_CONNECT_FAIL` or `ERR_DNS_FAIL`.
  - The health check is not logged.
- **Caching and limits.** Nothing is cached (`cache deny all`, `cache_mem 0`, no `cache_dir`), and there are no body limits.
- **The container** runs non-root, read-only, with all capabilities dropped.
- **`start.sh`.** The anchored regex prevents injection into `allow.conf`, there is no `eval`, `--check` works, and `squid -k parse` rejects invalid IPs with the line.

### Caddy

- **Host passthrough.** `Host: store.rev-edge.test:47443` reached the upstream unchanged.
- **Forwarded headers.** `X-Forwarded-For`, `X-Forwarded-Proto` and `X-Forwarded-Host` are replaced, not taken from the client. A forged `X-Forwarded-For: 203.0.113.9` reached the upstream as `172.19.0.1`. The gateway reads only `X-Forwarded-For` (`middleware.py:48-62`), so the client-supplied `Forwarded` and `X-Real-IP` that pass through do not matter.
- **Body limits and timeouts.** There are no body limits and no buffering. There are no read or write timeouts (Caddy's defaults: a 1-minute header read and a 5-minute idle timeout only).
- **No `encode` directive**, so no compression that would break the ETag and Content-Length checks.
- **HSTS** is on both sites and configurable; `0` sends `max-age=0`.
- **`/mcp` routing**: `/mcp`, `/mcp/` and `/mcp/x` go to MCP, and `/mcpx` goes to the gateway (see N6).
- **Default log redaction.** `Authorization` and `Cookie` are redacted by default.
- **ACME and internal CA.** The issuer is chosen by snippet, and certificates are stored per issuer, so switching issuers gets new certificates. `start.sh` refuses bad settings.
- **CI step.** `caddy fmt --diff` exits 1 on an unformatted file, so the step works.

### The default stack

- `docker compose config` of HEAD~1's file against the new one, with the old generator's `.env`, differs only in `FORKLIFT_STORE_PROXY: ""`. The worker ignores an empty variable (`settings.py:620`).
- An unset `FORKLIFT_BIND_ADDRESS` keeps `8080:8080` and `9000:9000`.
- `generate_env.py` without options: same keys, one comment changed. Secrets have the same strength as before (`token(48)`, a 32-byte Fernet key, `token(24)`, `token(32)`, `token(18)`, a 6-byte hex access key).

### `generate_env.py`

- 34 tests passed, 100% line and branch coverage.
- With `--domain`: https public URLs (with the port when not 443), `ALLOWED_HOSTS`, `BEHIND_TLS_PROXY`, `SECURE_COOKIES`, `TRUSTED_PROXIES=1` and `BIND_ADDRESS=127.0.0.1` are consistent.
- The CSRF origin works over https because the Host and port are passed through.
- Mixed content and the other contradictions are refused.

### `external-store.yml`

`config` with an S3 env resolves as described:

- `rustfs` is not started;
- `init` and `sweeper` are on `public`;
- the worker stays on `internal` only, with `FORKLIFT_STORE_PROXY=http://egress-proxy:3128` and `--store-host=s3.eu-west-1.amazonaws.com`;
- the proxy allows `FORKLIFT_STORE_URL`.

### E2E and CI

- **The e2e test does prove the worker cannot reach the store without the proxy**, at the network level. A probe inside the worker container fails on DNS for the store's name and gets no route for its IP or for `1.1.1.1:443`. The job tests' traffic appears in Squid's log with the right User-Agents.
- **Lint.** black, isort and flake8 over `deploy/compose` are clean now, including the other agents' e2e files.
- **Workflow.**
  - Permissions are `contents: read`.
  - `/etc/hosts` is added and removed in an `always()` step.
  - Actions are pinned like the rest of the repository (`@v4`/`@v5`).
  - The compose features used (`!override`, `!reset`, `start_interval`) are available on ubuntu-latest's Docker and Compose.
  - The new e2e files skip in `platform-e2e`.
- **Gaps** (folded into B1, S3 and S5 above): no e2e for an IP-literal allow entry, none for an https store through the real worker, and the redirect test does not check the port.

### Docs

The two READMEs match the code, except for:

- the S3/S4 claim about refusals naming the proxy;
- the S6 "nothing else";
- the S5 redirect.

---

## For the lead (other areas)

- **HELM** renders the same `dst <ip>` (B1) at `deploy/helm/forklift/templates/egress-proxy.yaml:66`.
- **HELM's squid.conf** must also get `request_header_access Cache-Control deny all` (S2) and the `to_localhost`/`to_linklocal` denies to stay in parity, and its `squid.image.digest` should be set (S7). HELM's parity test will need updating together with EDGE's files.
- **MCPR** should know about N6 (Caddy routes case-insensitively, on the cleaned path), and about EDGE's own note that `FORKLIFT_MCP_ALLOWED_HOSTS` must include `<domain>[:port]` exactly as Caddy passes it.
- **Engine (S4):** recognising `X-Squid-Error` in `PresignedUrlSource` would give streamed inputs a correct error. It is optional, but it is the only real fix there.

## What I ran

- **Worker tests:** `$S/review-edge/wt.py` (unit) on kvenv, kvenv312 and kvenv314; integration via `wt.py --services tests/integration/test_store_proxy.py`; the S4 probe via `$S/review-edge/itrun.py`.
- **`generate_env.py`:** `$S/review-edge/ge.py`, 34 passed at 100%.
- **Probes:** `$S/review-edge/p1_https_refused.py`, `p2_proxy_url.py`, `p3_https_stage.py`, `p4_presign.py`, `p5_slow_proxy.py`, `tls_raw.py`, `abort_upload.py`, `srv/*.py`.
- **Containers:** real Squid and Caddy on a private network `rev-edge-net` with host ports 47443, 47444, 47480, 47481 and 47900. Everything is removed: containers, the network, the volumes `rev-edge-caddy-data` and `rev-edge-caddy-config`, and my RustFS's anonymous volume.
- **Images:** I pulled `ubuntu/squid:6.6-24.04_beta` for the version comparison and removed it. I kept `ubuntu/squid:6.6-24.04_edge`, which I pulled, because it is the project's pinned image and HELM or the lead may need it.
- **Fix prototypes**, not applied to the repository:
  - `$S/review-edge/sqfix3/` (B1 + S2 + the defence-in-depth denies)
  - `$S/review-edge/caddyfix/` (S1)
  - `$S/review-edge/caddyfix2/` (S5)
