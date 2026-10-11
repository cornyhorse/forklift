# REVIEW-CLIENT: review of `forklift-client` + its Airflow provider (commit ca466a4)

Reviewer: REVIEW-CLIENT. I changed no repository files: scratch work, probes and logs are in
`$S/review-client/` (`S=/tmp/claude-0/-home-user-forklift/32052cf1-7a9c-57b5-940d-29385d406628/scratchpad`).
I read every line of `clients/python/src/**`, the fake (`tests/fake_api.py`), the operator/sensor/trigger tests,
the e2e test, both guides and `client.yml`, and compared them with the gateway's real code
(`api/routes_*.py`, `api/payloads.py`, `services/{jobs,uploads,artifacts,queue,retention,webhooks}.py`,
`storage.py`, `middleware.py`, `services/installation.py`) and with Airflow 3.3.2's own source in `$S/client/af312`.

Sanity: the suites pass as reported (`tests/unit` 265 passed with `$S/client/core313`; `tests/with_airflow`
56 passed with `$S/client/af312`); `generate_models.py --check` is clean against the current
`contracts/openapi.json` (including the OIDC agent's changes).

Reproductions: `R=$S/review-client`. Core probes run with `$S/client/core313/bin/python $R/<probe>.py`.
The Airflow probes are pytest files in `$R/aftests/`. They have their own conftest, a copy of the client's
fixtures with their own AIRFLOW_HOME. Run them with
`$S/client/af312/bin/python $R/run_af.py $R -q -s -p no:cacheprovider -W ignore $R/aftests/<file>.py`.

Summary: **1 blocking, 8 should-fix, 11 nits.** Everything security-critical I checked holds: the token
reaches only the gateway, redirects are not followed, TLS is never disabled, and webhook verification is
correct. Most findings are about the guarantees the docs make: retries re-attach, clearing starts a new
job, and long uploads.

---

## Blocking

### B1. `download_artifact(a, "out/")` writes every artifact to one FILE named `out` when `out/` does not exist yet
- **Where:** `clients/python/src/forklift_client/_downloads.py:125-128` (`target.is_dir()` is the only directory
  test, and `Path("out/")` is `Path("out")`). The documented headline examples do exactly this:
  `docs/platform/client.md:20` and `:201` (`# out/<name>`), `clients/python/README.md:31`, and
  `src/forklift_client/__init__.py:10`.
- **Repro:** `$S/client/core313/bin/python $R/probe_download_dir.py $R`. This is the quick start's loop over
  a job's two artifacts, in an empty working directory:
  ```
  returned out
  returned out
  left in .../tmpob3609cy : ['out'] | out is a file: True | content: b'PAR1 bad rows'
  ```
- **Why it matters:** silent data loss in the first example a user copies. `data.parquet` is overwritten by
  `bad_rows.parquet`, both calls "succeed", both return `out`, and nothing says the directory was missing.
- **Fix:** treat a destination that ends in a path separator (or `os.sep`/`/`) as a directory: create it, or
  raise `FileNotFoundError`. Alternatively, make the directory-vs-file choice explicit (`directory=` /
  `path=`). Add a test with a missing `out/`.

---

## Should-fix

### S1. Operator: the re-attach design breaks on Airflow 3's real kill and clear paths
Code: `airflow/operators.py:37-42` (key), `:162-179` (`_submit` re-attaches to any non-failed/cancelled
job), `:241-249` (`on_kill`). Docs: `docs/platform/airflow.md:76`, `:97-98`, `:111-119`.

The guide promises: "a retry gets the job its earlier try submitted", "clearing the task starts a new series
of tries, so its key changes and a new job is submitted", and "cancelled → fails without retries". On Airflow
3.3, four things go wrong.

**(a) A retry re-attaches to a job that the previous try's `on_kill` is cancelling, then fails without
retries.** `_submit` treats a job that is `running` with `cancel_requested: true` as alive. The gateway keeps
it `running` until the worker's next heartbeat (lease 60 s, so about 20 s, or up to the lease expiry when the
worker is gone; `services/queue.py:113`). The retry then waits for it, gets `cancelled`, and `job_outcome`
raises `AirflowFailException`.
- Repro, with real Airflow (dag.test): `execution_timeout` makes the task runner call `task.on_kill()`
  (`airflow/sdk/execution_time/task_runner.py:2186-2190`). The operator cancels, and the retry
  (retry_delay 0) re-attaches:
  `$S/client/af312/bin/python $R/run_af.py $R -q -s -p no:cacheprovider -W ignore $R/aftests/test_review_cancel_reattach.py`
  ```
  STATE {'load': 'failed'} TRIES {'load': 2} JOBS 1 POST /jobs 2 cancels 1
  re-attached: True | AirflowFailException: True
  1 passed
  ```
  The task allowed 3 retries but used 1, and no new job was ever submitted.

**(b) Clearing a RUNNING task keeps the key.** Airflow sets `max_tries = try_number + retries` only for task
instances that are not running. A running one becomes `RESTARTING` with `max_tries` unchanged and is "always
eligible for retry" (`airflow/models/taskinstance.py:389-418`, `:1975-1979`). So `first_try`, and with it
the key, stay the same.
- Repro (real `clear_task_instances` on Airflow 3.3.2's SQLite DB): `$S/client/af312/bin/python $R/probe_clear.py $R`
  ```
  clear of a success  TI: ... before=(1, 2, 'airflow:cd44baf74c35') after=(1, 3, None, 'airflow:943fe21e7b73') -> new key
  clear of a running  TI: ... before=(1, 3, 'airflow:943fe21e7b73') after=(1, 3, RESTARTING, 'airflow:943fe21e7b73') -> SAME KEY
  ```
  The outcome depends on what state the old job is in:
  - `cancel_on_kill=False`, or a job that is `publishing` or has already `succeeded` (it cannot be cancelled):
    the "clear" re-attaches and no new job runs.
  - `cancel_on_kill=True` and the job is still running: case (a), and the task fails.
  - The job was queued: it is cancelled at once and resubmitted under `:1`, so it works by accident.

**(c) `on_kill` also fires when nobody acted.** The Airflow 3.3 supervisor forwards SIGTERM on graceful
shutdown ("K8s pod SIGTERM", `supervisor.py:1496-1512`). It also SIGTERMs the task after
`MAX_FAILED_HEARTBEATS` (the Airflow API server unreachable, `supervisor.py:1745-1754`), and on execution
timeout. With the default `cancel_on_kill=True`, a pod eviction, a deploy or a lost network to the API server
cancels the forklift job. The retry then restarts it from scratch, or hits (a). That is the opposite of
`airflow.md:97` ("a worker that crashed, a lost network ... must not start the job a second time").

**(d) Inconsistent meaning of `cancelled`.** A job cancelled in forklift while the task waits fails the task
for good (`AirflowFailException`, "someone stopped it on purpose"). The same deliberate cancel made while the
task is `up_for_retry` is silently undone: the next try resubmits it under `:1`.

**Fix (suggested):**
- In `_submit`, do not re-attach to a job with `cancel_requested`. Wait for it to end, then resubmit under
  the next key.
- Decide whether `cancelled` is final in both paths.
- Make the clear case real: include something that changes on a clear of a running task, or document that
  clearing a running task re-attaches.
- Document that `cancel_on_kill` also fires on worker shutdown, heartbeat loss and `execution_timeout`.
  Consider defaulting it to cancel only on user action, which is detectable on 3.3 by the TI's state, or
  leave the default as is but say so.
- Add dag.test tests for `execution_timeout` + retries, and a real clear.

### S2. The default Idempotency-Key collides across re-created DAG runs and across Airflow installations
- **Where:** `airflow/operators.py:37-42`. The key is `sha256([dag_id, task_id, run_id, map_index, first_try])`
  and the gateway scopes keys per forklift *user* (`services/jobs.py:275-277`). Scheduled `run_id`s are
  deterministic (`scheduled__<run_after>`, `airflow/models/dagrun.py:858-860`). Docs: `airflow.md:118-119`
  ("A new DAG run always has new keys").
- **Repro (re-created run):**
  `$S/client/af312/bin/python $R/run_af.py $R -q -s -p no:cacheprovider -W ignore $R/aftests/test_review_recreated_run.py`
  ```
  run ids: manual__2026-10-10T00:00:00+00:00 | manual__2026-10-10T00:00:00+00:00 | states: {'load': 'success'} {'load': 'success'}
  jobs in the gateway: 1 | POST /jobs: 2 | 'Re-attached' logged: True
  ```
  A run deleted and created again (the usual "start this day over" in Airflow, or a catch-up that re-creates
  it) "succeeds" at once with the old run's job and result. Nothing runs, for as long as the old job record
  is retained.
- **Same mechanism across installations:** staging and prod, or blue/green Airflows, with the same DAG and
  the same forklift service account produce identical keys. The second one either gets the first one's job
  (same spec) or fails with `AirflowFailException` "the task's job changed since an earlier try" (different
  spec), which is a misleading message.
- **Fix:** add a namespace to the key. One option is a per-connection or per-installation component (an
  operator/connection setting `idempotency_namespace`, defaulting to e.g. the Airflow `[api] base_url` or
  the connection id plus a random value stored in the connection's extra). Another is something that differs
  for a re-created run. Otherwise document both limitations plainly and recommend one forklift service
  account per Airflow installation.

### S3. `ca_file` replaces the system CAs, so a private-CA gateway with a public store (e.g. AWS S3) cannot upload or download
- **Where:** `_transport.py:140-149` (`ssl.create_default_context(cafile=ca_file)` loads *only* that bundle,
  and the same context is used for presigned store URLs). Docs: `client.md:38` ("for installations with a
  private CA"), `airflow.md:27`, the example `airflow.md:36` (`"/etc/ssl/private-ca.pem"`), and the
  troubleshooting row `airflow.md:257` ("set the CA bundle").
- **Repro:** `$S/client/core313/bin/python $R/probe_cafile.py $R/privca.pem`. A public host stands in for the
  store (pypi.org through the agent proxy):
  ```
  default -> {'x509': 127, 'crl': 0, 'x509_ca': 127}
     HTTP 404
  ca_file=private CA -> {'x509': 1, 'crl': 0, 'x509_ca': 1}
     TransportError: GET public store: no answer from pypi.org (TLS error: CERTIFICATE_VERIFY_FAILED) retryable= False
  ```
- **Why it matters:** this is the typical enterprise setup (internal CA for the gateway, cloud object store).
  Following the guide, every upload and download fails with the error the troubleshooting table says to fix
  with exactly that setting.
- **Fix:** load the default certs as well (`ctx.load_default_certs()` and then `ctx.load_verify_locations(ca_file)`).
  Or add a separate `store_ca_file`. At minimum, document that the bundle must also contain the store's CAs.

### S4. Long multipart uploads: the ticket's URLs (up to 1000) all expire with the upload record, and every later part is first PUT to a dead URL; the sweeper can abort the upload
- **Where:** `_uploads.py:218-237`. URLs come from the ticket until part 1000, and are refreshed only after a
  failure, one part at a time.
  - Gateway: `services/uploads.py:124-153` (ticket URLs and `upload.expires_at` both last
    `upload_url_seconds`, 1 h by default) and `:178-199` (only `POST /parts` moves `expires_at`).
  - Sweeper: `services/retention.py:269-283` (aborts PENDING uploads past `expires_at`; Compose runs it
    every 3600 s).
  - Docs: `client.md:141-142` ("part URLs are asked for in batches as they are needed").
- **Repro:** `$S/client/core313/bin/python $R/probe_part_expiry.py` (16 parts, URLs expire after part 2,
  1000 URLs in the ticket like the real gateway):
  ```
  parts: 16 | PUT requests: 31 | POST /parts calls: 15 | retry sleeps: 15 | status: complete
  ```
- **Why it matters:** after the first hour, every remaining part costs an extra request and a backoff sleep.
  That request can be a full 64 MiB transfer: urllib sends the whole body, since there is no
  `Expect: 100-continue`. It also uses one of the part's 4 attempts. Between `expires_at` and the client's
  first 403, the hourly sweeper may abort the multipart upload. Every part after that gets `NoSuchUpload`
  (404, not retried) and the whole upload fails. The fake hides all of this: `first_parts = 2`, no URL
  lifetime, no sweeper.
- **Fix:** refresh proactively, using `ticket.expires_at` (known to the client). When less than a margin is
  left (say 10 min or 1/4 of the lifetime), ask for the next batch with `POST /parts`. That also keeps the
  upload alive for the sweeper. Ask for batches of the parts still to come, not one part. Make the fake give
  ~1000 URLs and expire them by time.

### S5. The trigger's polls block shared executor threads: the client's full RetryPolicy (with `Retry-After` sleeps) runs inside `asyncio.to_thread`
- **Where:**
  - `airflow/triggers.py:76,82`: `client.get_job` runs in asyncio's default executor.
  - `hooks.py:162-165`: `aget_conn` gives the default `RetryPolicy(attempts=4, max_retry_after=120)`.
  - `_transport.py:216-256`: the retry loop sleeps in that thread.
- **Repro:** `$S/client/af312/bin/python $R/run_af.py $R -q -s -p no:cacheprovider -W ignore $R/aftests/test_review_trigger_threads.py`
  (4 triggers, a 4-thread default executor, the gateway answers 503 + `Retry-After: 2`):
  ```
  an unrelated to_thread call waited 5.8 s for a free executor thread
  first event after 5.8 s (end_time was 1 s): {'status': 'timeout', ...}
  ```
- **Why it matters:** the default executor has `min(32, cpu+4)` threads and is shared by every trigger in the
  triggerer that uses `to_thread`/`run_in_executor`. With the defaults, a 503/429 with `Retry-After: 120`
  holds a thread for up to 3 × 120 s. A blackholed gateway holds it for 4 × 30 s + backoff. Meanwhile
  `end_time` cannot fire, and other providers' triggers stall.
- **Fix:** give the trigger a client with `NO_RETRIES` (the trigger loop already polls through transient
  errors, with its own `asyncio.sleep`). Honour `ApiError.retry_after` in the loop, and use a shorter socket
  timeout for polls.

### S6. `ForkliftJobSensor` in poke/reschedule mode fails on a short gateway outage (the operator and the trigger poll through it)
- **Where:** `airflow/sensors.py:170-175`. `poke` lets the client's errors propagate once its 4 attempts
  (about 3.5 s) are used up. Compare `airflow.md:86-87` (operator: "polling goes on") and `client.py:652-659`.
- **Repro:** `$S/client/af312/bin/python $R/run_af.py $R -q -s -p no:cacheprovider -W ignore $R/aftests/test_review_sensor_transient.py`
  ```
  STATE {'wait': 'failed'} | ServerError in log: True       # sensor, 4 x 502
  OPERATOR STATE {'load': 'success'}                         # operator, the same 4 x 502
  ```
- **Why it matters:** a gateway restart or rolling deploy (10-30 s of 502s) fails every waiting sensor that
  has no retries. With retries, it burns them.
- **Fix:** in `poke`, catch transient errors (`is_transient`), log them, and return `is_done=False`. Let
  `timeout` bound the wait, as for the trigger.

### S7. `validate_schema(wait_seconds=…)`: the gateway may hold the request longer than the client's socket timeout, and the call fails without a retry
- **Where:** `client.py:370-399` (the default 30 s timeout is kept, and this POST is not replay-safe).
  Gateway: `services/installation.py:147-151` (`validate_wait_seconds` may be up to 60) and
  `services/jobs.py:358-401` (the job is created first, then the request is held up to
  `min(wait_seconds, validate_wait_seconds)` without sending a byte).
- **Repro (the fake holds the request; a 1 s timeout stands in for 30 s):** `$S/client/core313/bin/python $R/probe_validate_wait.py`
  ```
  TransportError: POST /api/v1/schemas/validate: no answer from 127.0.0.1:43603 (timed out) | sent: True
  POST /schemas/validate sent 1 time(s)
  ```
- **Why it matters:** on an installation with `validate_wait_seconds` > 30, or any `wait_seconds` > timeout,
  every slow validation raises. The job runs on, but its id is lost to the caller. The e2e test runs with
  `timeout=60`, so it does not see this.
- **Fix:** for this request, use a read timeout of at least `wait_seconds` (or 60) plus a margin, or clamp
  `wait_seconds` below the timeout. Document the interaction.

### S8. TLS failures are classified inconsistently
- **Where:** `client.py:652-659` (`wait_for_job` treats *every* `TransportError` as transient, including
  `retryable=False` TLS failures) versus `airflow/triggers.py:27-31` (honours `retryable`). Separately,
  `_transport.py:121-124` makes *every* `ssl.SSLError` non-retryable, including transient ones such as
  `SSLEOFError` from a load balancer, not only `SSLCertVerificationError`.
- **Repro:** `$S/client/core313/bin/python $R/probe_wait_tls.py $R` (a gateway whose certificate does not
  verify; simulated clock):
  ```
  WARNING Waiting for job ...: ... (TLS error: CERTIFICATE_VERIFY_FAILED); polling on   (x11)
  wait_for_job raised only after 11 polls / 120 s of simulated time: ...
  ```
- **Why it matters:** with `timeout=None`, which is the sync operator's default (`wait_timeout=None`), a
  gateway certificate rotated to an untrusted CA makes the task poll forever instead of failing. Conversely,
  one handshake EOF ends a deferred wait with `status: "error"`.
- **Fix:** in `wait_for_job`, re-raise `TransportError` with `retryable=False`. Mark only certificate
  verification errors (`ssl.SSLCertVerificationError`) as permanent.

---

## Nits

1. **`Retry-After` values that `str.isdigit()` accepts but `float()` refuses escape as a bare `ValueError`.**
   http.client decodes header bytes as latin-1, so `¹²³` are possible. `_transport.py:97-98`.
   `probe_retry_after.py`: `'²' -> raises ValueError` and `wait_for_job -> builtins ValueError`. Fix: use
   `value.isascii() and value.isdigit()`, or try/except.
2. **`paginate(client.list_job_events, job)` raises `TypeError: ... unexpected keyword argument 'offset'`**
   (`probe_paginate_events.py`), while `client.md:234-236` says `paginate()` works for any list method that
   returns a `Page`. Either exclude it in the docs or make `paginate` use the cursor for events.
3. **`wait_for_job(timeout=T)` overshoots `T` by up to attempts × socket timeout** (2 min with the defaults),
   because the deadline is checked only between polls and each poll carries its own retries. The same
   applies to the sync operator's `wait_timeout`. `probe_wait_overshoot.py`: `TransportError after 4.0 s
   (timeout was 0.5 s)` with timeout=1, 4 attempts. There is also no whole-exchange deadline (unlike the
   gateway's own `deadline.py`), so a trickling server holds a call indefinitely. Consider capping each
   poll's retries by the time left.
4. **The trigger builds a `ForkliftClient` on the event loop.** `ssl.create_default_context()` costs about
   21 ms each (measured), on every `run()` and `on_kill()` (`hooks.py:162-165`). A triggerer restart with
   hundreds of forklift triggers blocks the loop for seconds. `airflow.md:177` says the connection is read
   without blocking. Build the client in `to_thread`, or cache SSL contexts per `ca_file`.
5. **Operator `on_kill` runs from the task runner's SIGTERM handler, and makes a new hook and connection
   lookup there** (`operators.py:241-249`). In Airflow 3 that goes through `SUPERVISOR_COMMS`, whose
   `threading.Lock` is not re-entrant (`comms.py:234`). A SIGTERM during an `xcom_push` or connection fetch
   can deadlock until SIGKILL, and then the job is not cancelled. Any `ForkliftError` other than `Conflict`
   also propagates out of the signal handler into the interrupted frame. Keep the client from `execute` on
   `self`, and catch `ForkliftError`.
6. **The package ships `py.typed`, but mypy (default settings) reports 2 errors and CI does not type-check:**
   `_transport.py:128` (`error.reason` may be an exception) and `_uploads.py:175` (`_Window` is not
   `IO[bytes]`). User-facing types are fine (checked with a snippet: `paginate` yields `JobSummary`,
   `status` is the `Literal`).
7. **The e2e cancel test assumes no worker serves the `sql` lane** (`deploy/compose/e2e/test_client.py:193-196`),
   but Compose takes `--lanes=${FORKLIFT_WORKER_LANES:-batch,interactive}` (`docker-compose.yml:221`). Skip
   when the env file's lanes include `sql`, or cancel a job that is otherwise certain to stay queued.
8. **The XCom `return_value` is the whole `JobDetailOut` including `spec`** (`operators.py:49`), which
   embeds an inline schema document, on top of a result of up to 1 MiB. That is bounded only by the
   gateway's limits. A slimmer document without `spec` (or with it as an opt-in) is kinder to XCom backends
   and proxies in front of the Airflow API server.
9. **`wait_timeout` with retries:** a timed-out wait raises `AirflowException`, so each retry re-attaches and
   waits again, and the effective wait is (retries+1) × `wait_timeout`. `airflow.md:75` says "the task
   fails". Document this, or raise `AirflowFailException` for the timeout.
10. **The webhook `t` is parsed with `int()`,** which also accepts `" 1760100000"`, `"+1760100000"`,
    `"1_760_100_000"` and non-ASCII digits. This is harmless (the HMAC is over the canonical value, and the
    documented snippet does the same), but `t.isascii() and t.isdigit()` would be stricter.
11. **Doc inaccuracies besides those above:**
    - `client.md:141-142`: URLs come from the ticket (up to 1000), not "in batches as needed".
    - `airflow.md:76`: `cancel_on_kill` also fires on worker shutdown and heartbeat loss (S1c).
    - `airflow.md:117-119`: see S1b and S2.

---

## Fidelity: where the fake diverges from the real gateway

| Real gateway | Fake | Consequence |
|---|---|---|
| Ticket has up to 1000 part URLs; URLs and the upload expire after `upload_url_seconds`; the sweeper aborts pending uploads | 2 URLs; no lifetime; no sweeper | S4 is untestable as written |
| `POST /schemas/validate` holds the request up to `validate_wait_seconds` (≤ 60) | answers at once | S7 |
| Idempotency keys are per user; fingerprint over parsed values (UUIDs normalised) | global; raw JSON | fine for the tests' single user |
| `POST /parts` refuses part numbers > count and > 1000 per request (400) | accepts anything | the client stays within the limits (min(count, n+99)) |
| `DELETE /uploads/{id}` refuses an upload that is the input of queued/running jobs (409) | always deletes | the client's `_discard` only runs before a job exists: OK |
| Invalid UUID in a path: 422 (Ninja validation) | 404 | error classes still right (`BadRequest`/`NotFound`) |
| Expired presigned PUT on S3 may reset the connection mid-body instead of a clean 403 | clean 403 | the client handles both (`fresh_url` on any failure): OK |

Outside the client (for the gateway owner, **not verified**): `complete_upload` calls
`complete_multipart` again on a retry when an earlier call completed the store side but the gateway did not
commit (`services/uploads.py:226-229`). On RustFS/MinIO that answers `NoSuchUpload` → 502 on every retry,
and the client's failure path then deletes an object that is in fact complete. Tolerating
`NoSuchUpload` when the HEAD shows the declared size would make completion idempotent at the store level too.

---

## Checked and found sound

- **Token placement:** `Authorization: Bearer` goes only to `base_url` (`client.py:241-247`). Presigned
  store requests carry none: `stream(..., headers={})` and the `_put` headers. The fake refuses
  Authorization on the store, as S3 does.
- **Redirects:** not followed. `_NoRedirect` replaces urllib's default handler (`build_opener` skips the
  default class), and a 3xx becomes an `ApiError`/`StoreError`.
- **Schemes:** only http/https URLs are opened (`host_of`).
- **TLS:** `create_default_context` (CERT_REQUIRED and hostname checks). It is never disabled, and
  `PYTHONHTTPSVERIFY` does not affect it.
- **No leaks:** the token and presigned URLs do not appear in `repr`, exceptions or logs.
  - Messages name the host only, and 422 details drop pydantic's `input`.
  - Base URLs with credentials are refused without being echoed.
  - The only theoretical leak is `http.client.InvalidURL` quoting a gateway-made URL that contains a space
    or control character. Gateway keys are sanitised, so it is unreachable.
- **Retry rules:**
  - GET/PUT/DELETE and keyed POSTs (submit, run, complete, parts) retry on transport errors and
    429/502/503/504.
  - Unkeyed POSTs (create upload/schema/version, validate, cancel) retry only on refused/DNS, 429, or 503
    with `Retry-After`. A reset after the body was sent (`getresponse()` errors are raw, so `sent=True`) is
    not retried.
  - `Retry-After` handles seconds and HTTP dates, with a cap; the `-0000` (naive) form is ignored.
  - One `X-Request-ID` is kept across retries, and it matches the gateway's `_REQUEST_ID` pattern.
- **Idempotency** matches `services/jobs.py`: a key per user, a fingerprint check, 200 vs 201,
  `409 idempotency_key_reused`, and the concurrent-insert path (`IntegrityError`, then replay). Retries of
  `submit_job`/`run_dataset` cannot duplicate a job.
- **Uploads:**
  - `Content-Length` and `Content-MD5` on every PUT. urllib's default form `Content-type` on part PUTs is
    harmless.
  - Part ETags are kept. The completion body has the shape `UploadCompleteIn` expects, and `{}` for single
    uploads.
  - A 403 gets a fresh part URL, and the rewind/MD5 are correct on a retry.
  - Short, long and grown files are detected. Failed uploads are deleted, and 410 counts as success.
- **Downloads:**
  - The file goes to a temp file next to the target, then `os.replace` after the size and sha256 check. A
    symlink at the target is replaced, not followed, and the partial file is removed on every error,
    `BaseException` included.
  - `Content-Length` truncation is detected; sha256 is checked when it is reported (the worker always
    reports it).
  - A retry gets a fresh, audited URL. 410 → `Gone`.
- **Events and job states:**
  - The events cursor matches the gateway (`id__gt`, `ordering = ["id"]`, the count is of the filtered
    set).
  - Cancel semantics match: queued → cancelled; running → `cancel_requested`; publishing or finished → 409.
  - `publishing` is non-terminal in the client, operator, sensor and trigger.
- **Models:**
  - They match `payloads.py` and the OpenAPI document: nullability, defaults (`Artifact.expires_at`), unknown
    fields ignored.
  - Enum values pass through at run time (`Literal` is typing only).
  - Dates use `Z` or offsets, and a bad date becomes `UnexpectedResponse`.
  - The generator check passes. Core code has no 3.10-only syntax (3.9 runs in CI).
- **Webhooks:** matches `services/webhooks.py:signature()` and `webhooks.md`. It is constant-time and
  compares every secret, enforces the tolerance in both directions, and returns False for malformed,
  missing, non-ASCII or multi-`=` headers. Body type is checked, and rotation works with a list of secrets.
- **Airflow APIs:**
  - Only `airflow.sdk` (3.2+) interfaces are used, plus `airflow.triggers.base`, which is still the public
    place in 3.3.
  - Declarative `conn-fields`: Airflow's UI stores them under the unprefixed `field_name`
    (`api_fastapi/core_api/services/ui/connections.py:231`), which is what `client_for` reads.
  - The `first_try` formula equals the task runner's (`task_runner.py:682-684`) for retries and
    non-running clears. Deferral resumption does not recompute the key.
  - Trigger: kwargs serialise to JSON only.
  - `execute_complete` refetches the job instead of trusting the event. Sensor `soft_fail`/`never_fail` are
    applied by `resume_execution`.
  - Trigger `on_kill` follows 3.3's semantics (user action only; not called on triggerer restart or
    redistribution).
- **CI (`client.yml`):**
  - Permissions are `contents: read` and checkout uses `persist-credentials: false`.
  - The test matrix is 3.9-3.14, and Airflow runs as 3.2.2/py3.10 and 3.3.2/py3.13 with constraints.
  - Path filters include `contracts/openapi.json`, and there is a wheel clean-install check.
  - Tool versions use `>=` floors, as in the repository's other workflows.

## What I ran
- `cd clients/python && $S/client/core313/bin/python -m pytest tests/unit -q -p no:cacheprovider` → 265 passed.
- `$S/client/af312/bin/python -m pytest tests/with_airflow -q -p no:cacheprovider` → 56 passed.
- `$S/client/core313/bin/python clients/python/tools/generate_models.py --check` → exit 0.
- The probes and scratch tests named above, all under `$R`, with outputs as quoted.
- mypy 1.18.2 in my own venv (`$R/venv`, deleted afterwards: `python3.13 -m venv $R/venv && $R/venv/bin/pip install mypy==1.18.2` to re-run).
- `git status`: nothing in the repository changed by this review.
- No Compose stack was started. The real-gateway facts above come from reading its code; the e2e results
  the CLIENT agent reported cover the request and response shapes.
