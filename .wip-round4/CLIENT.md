# CLIENT: `forklift-client` (typed Python client of /api/v1) with an Airflow operator, sensor and trigger

All files are new and in my area; I changed nothing else. No git operations beyond `git status`/`git diff`
(and a read-only `git archive HEAD deploy/compose` to run main's Compose files from my scratch dir).

## What I built, and where

| Path | What |
|---|---|
| `clients/python/pyproject.toml` | Package `forklift-client` 0.1.0 (hatchling, like the others), import name `forklift_client`, Python >= 3.9, **no run-time dependencies**; extras `airflow` (`apache-airflow>=3.2`, see Decisions) and `dev`; entry point `apache_airflow_provider` → `forklift_client.airflow.provider:get_provider_info`; black/isort config (py39 target); pytest `filterwarnings` that turns any `DeprecationWarning`/`FutureWarning` raised from `forklift_client.*` into an error (Airflow's moved imports are FutureWarnings: a deprecated import fails the tests - checked by planting one) |
| `clients/python/src/forklift_client/client.py` | `ForkliftClient(base_url, token, timeout=30, ca_file=None, retry=RetryPolicy(), user_agent=None)`, `from_env()`; methods below |
| `.../_transport.py` | urllib transport (no redirects, http(s) only, env proxies, CA bundle), `RetryPolicy`, `send_with_retries` (the retry rules), `Retry-After` parsing |
| `.../_uploads.py` | `upload_file`: one presigned PUT or multipart (part URLs fetched in batches of 100, ETags kept), streamed (seekable files read in place; non-seekable streams spooled per part into a `SpooledTemporaryFile`, 8 MiB in memory), `Content-Length` + `Content-MD5` on every PUT, retries per PUT (a part gets a fresh URL for every retry, so a 403/expired URL is retried), size verification (short/long/grown files), failed uploads deleted (`DELETE /uploads/{id}` aborts multipart) |
| `.../_downloads.py` | `download_artifact` (to a file or into a directory; temp file + `os.replace` only after size and sha256 matched; retries with a fresh presigned URL on transport errors, 5xx/429/408/403) and `open_artifact` (a stream checked at its end) |
| `.../_model.py`, `.../models.py` | `Model` base (`from_dict`/`to_dict`, ISO-8601 parsing that works on 3.9, unknown fields ignored, missing required ones → `UnexpectedResponse`), `Page`; **`models.py` is generated** from `contracts/openapi.json` by `clients/python/tools/generate_models.py` (19 resources: `Me`, `User`, `Token`, `Schema`, `SchemaVersion`, `Connection`, `Dataset`, `Upload`, `UploadTicket`, `PartUrl`, `Job` (=JobDetailOut), `JobSummary` (=JobOut), `JobError`, `JobEvent`, `Artifact`, `Download`, `Schedule`, `Webhook`, `WebhookDelivery`; enums as `Literal` aliases `JobStatus`, `JobKind`, `Classification`, ...) |
| `.../errors.py` | `ForkliftError` → `ApiError` (`status`, `code`, `detail`, `method`, `path`, `request_id`, `retry_after`, `transient`) with `BadRequest` (400/422), `Unauthorized`, `Forbidden`, `NotFound`, `Conflict`, `Gone`, `TooManyRequests`, `ServerError`; `TransportError` (`sent`, `retryable`), `StoreError`, `VerificationError`, `UnexpectedResponse`, `JobFailed`, `JobTimeout` (also a `TimeoutError`) |
| `.../webhooks.py` | `verify_webhook_signature(body, header, secret, *, tolerance=300, now=None)`: exactly the algorithm of docs/platform/webhooks.md (same header parsing, `str(int(t)) + "." + body`, HMAC-SHA256 hex, `hmac.compare_digest`, `abs(now - t) <= tolerance`); also accepts several secrets (rotation); returns False for malformed headers incl. non-ASCII signatures (the doc's snippet would raise TypeError there) |
| `.../airflow/` | `provider.py` (provider info: connection type `forklift` with **declarative** `conn-fields` `ca_file`/`timeout` and `ui-field-behaviour`, as Airflow 3.2+ wants; imports nothing from Airflow), `hooks.py` (`ForkliftHook`, `client_for(connection)`), `operators.py` (`ForkliftJobOperator`), `sensors.py` (`ForkliftJobSensor`), `triggers.py` (`ForkliftJobTrigger`) |
| `clients/python/tests/` | `fake_api.py` (fake gateway + object store on a real `ThreadingHTTPServer`, http or https), `openapi_check.py` (validates **every request** - query params declared, bodies strictly against the request schema - **and every JSON answer** of the fake against `contracts/openapi.json`), `unit/` (265 tests), `with_airflow/` (56 tests, `dag.test()`), `with_airflow/coverage.ini` |
| `clients/python/tools/generate_models.py` | The generator (stdlib); `--check` exits 1 when models.py is stale |
| `clients/python/README.md` | Package readme |
| `deploy/compose/e2e/test_client.py` | E2E against a real stack (9 tests, below) |
| `docs/platform/client.md`, `docs/platform/airflow.md` | Guides |
| `.github/workflows/client.yml` | CI (lint + model check; unit tests 3.9-3.14 at 100%; build/twine/clean-install; Airflow 3.2.2/py3.10 and 3.3.2/py3.13) |

### Client API (for MCPR: small and predictable)

`me()`; `list_schemas()`, `get_schema()`, `create_schema()`, `list_schema_versions()`, `create_schema_version()`,
`get_schema_version()`, `validate_schema()`; `list_connections(kind=)`, `get_connection()`; `list_datasets()`,
`get_dataset()`, `run_dataset(dataset, upload_id=, idempotency_key=)`; `create_upload()`, `list_uploads()`,
`get_upload()`, `upload_part_urls()`, `complete_upload()`, `delete_upload()`, `upload_file()`;
`submit_job(kind, **JobIn fields, idempotency_key=)`, `get_job()`, `list_jobs(status=, kind=, dataset_id=, mine=)`,
`list_job_events(job, after=, limit=)` / `iter_job_events()` (cursor), `cancel_job()`,
`wait_for_job(job, timeout=3600, poll_interval=1, max_poll_interval=30, raise_on_failure=False, on_poll=None)`;
`list_artifacts()`, `get_artifact()`, `get_download_url()`, `download_artifact()`, `open_artifact()`;
`list_schedules(enabled=, dataset_id=)`, `get_schedule()`; `list_webhooks()`, `get_webhook()`;
`paginate(list_method, *args, page_size=100, **filters)`. List methods return `Page` (`items`, `count`, `offset`,
`limit`, `has_more`, `next_offset`; limit 1-500). Every id parameter also takes the resource. Models' `to_dict()`
is the API's JSON (good for MCP tool results). `TERMINAL_STATUSES` / `ACTIVE_STATUSES` exported. Logger:
`forklift_client`. A client is thread-safe and holds no connections (MCPR can make one per request token; it
builds an `ssl` context in the constructor, so caching one per token is cheaper if that matters).

## Decisions and why

1. **`airflow` extra is `apache-airflow>=3.2`, not `>=3.0`** (deviation from the brief, on purpose). I checked the
   wheels: Airflow 3.0 (task-sdk 1.0.x) has no `airflow.sdk.BaseHook`; 3.1 (task-sdk 1.1.0) has no
   `airflow.sdk.exceptions.AirflowException`/`AirflowFailException`/`TaskDeferred` and no `airflow.sdk.conf`, while
   in 3.3.2 the old `airflow.exceptions.AirflowFailException` etc. emit `DeprecatedImportWarning`, and
   `get_ui_field_behaviour()`/`get_connection_form_widgets()` on hooks are deprecated since 3.2 in favour of
   provider-info `ui-field-behaviour`/`conn-fields`. 3.2.0 is the first release with all public interfaces used,
   so `>=3.0` would have been a false promise or required deprecated imports. Tested on 3.2.2 and 3.3.2.
2. **Default Idempotency-Key of the operator**: `airflow:` + sha256 of `[dag_id, task_id, run_id, map_index, first_try]`
   with `first_try = max(1, ti.max_tries - task.retries + 1)` - NOT the try number. `first_try` is constant across the
   retries of a task instance (so a retry re-attaches, as asked) but changes when the task is **cleared** (Airflow sets
   `max_tries = try_number + retries` on clear; the task runner itself uses `max_tries - retries + 1` as "first try" for
   sensor reschedules). Without it, clearing a task whose job succeeded would silently return the old job forever
   for that DAG run. Documented. A replayed job that **failed or was cancelled** is not re-attached: the operator
   submits again under `<key>:1`, `<key>:2` ... (deterministic, so a crash right after resubmitting re-attaches to the
   resubmission; bounded by `MAX_RESUBMISSIONS = 100`) - the same semantics Airflow 3.3's `ResumableJobMixin`
   documents (active → reconnect, succeeded → return, failed → resubmit), but working on 3.2 and without the
   window that mixin admits between submit and persisting the id. A `409 idempotency_key_reused` (the job spec
   changed between tries) fails the task with `AirflowFailException` and says to clear it.
3. **Failure semantics**: job `failed` → `AirflowException` (retries apply; they resubmit), `cancelled` →
   `AirflowFailException` (someone stopped it on purpose). Sensor: failed/cancelled → `AirflowFailException`
   (`soft_fail` skips). `publishing` is never terminal anywhere (client, operator, sensor, trigger).
4. **Retry rules** (client): replay-safe = GET/PUT/DELETE + POSTs with Idempotency-Key (submit, dataset run) +
   complete upload + part URLs → retried on transport errors and 429/502/503/504. Other POSTs only when the server
   certainly did not act: connection refused / DNS failure (`TransportError.sent == False`), 429, 503 with
   `Retry-After`. TLS failures never. `Retry-After` honoured up to `max_retry_after` (else raised with `retry_after`).
   One `X-Request-ID` per logical call across retries (the gateway echoes it; errors carry it). `wait_for_job` and
   the trigger poll through transient errors until their deadline.
5. **Token hygiene**: the bearer token goes only to `base_url`; presigned store requests never carry it (the fake
   store refuses requests with an Authorization header, as S3 does, so a leak would fail tests); `repr(client)`,
   exceptions and logs never contain it or presigned URLs (host only); 422 details list `loc: msg`, never pydantic's
   echoed `input`. Base URLs with credentials are refused without echoing them.
6. **Models generated, not hand-written**; the contract test (`tests/unit/test_contract.py`) fails when (a)
   `models.py` != generator output for the current `contracts/openapi.json`, (b) any API call in `client.py` (found by
   AST) is not an operation of the document, or returns a model other than that operation's documented 2xx
   schema, (c) `submit_job`/`validate_schema`/`create_upload`/`create_schema`/`create_schema_version`/`run_dataset`
   parameters != the request schema's properties, (d) list methods' filters != the operation's query parameters.
   Plus the fake checks every request/answer at run time.
7. Uploads: Content-MD5 on every PUT (the worker does the same; the store rejects corrupted bodies). `sha256=` is
   only recorded as `declared_sha256` (the API does not check it; computing it would cost a second full read).
8. `delete_upload()` treats 410 (already deleted/expired) as success, because a retried DELETE whose first answer
   was lost gets 410.

## What the lead must change in shared files (exact text)

### CHANGELOG.md - under `## [Unreleased]` → `### Added`, after the "Platform: schedules, webhooks, ..." item

```markdown
- **`forklift-client`: a typed Python client of `/api/v1`, with an Airflow operator, deferrable
  sensor and trigger** (`clients/python`, its own package; guides `docs/platform/client.md` and
  `docs/platform/airflow.md`):
  - **The client** uses the standard library only (Python 3.9 to 3.14) and returns the API's
    resources as frozen dataclasses generated from `contracts/openapi.json` by
    `clients/python/tools/generate_models.py`; a contract test fails when the document and the
    models, the calls or the methods' parameters drift apart. It covers who-am-I, schemas and
    their versions and validation, connections, datasets and their runs, uploads, jobs (submit,
    get, list, events with a cursor, cancel, `wait_for_job()`: `publishing` is not the end),
    artifacts, schedules and webhooks, with `Page` results and `paginate()`. Errors are one
    hierarchy (`ApiError` and its subclasses by status with the API's `code`, `detail` and
    request id; `TransportError`, `StoreError`, `VerificationError`, `JobFailed`, `JobTimeout`)
    and never contain the token or presigned URLs; the token is sent to the gateway only.
    Requests are retried with backoff and `Retry-After` only where that is safe, and job
    submission and dataset runs carry an `Idempotency-Key`, so a request sent again cannot
    start a second job. `upload_file()` uploads a path or a file object straight to the store in
    one PUT or in parts (streamed, `Content-MD5` on every PUT, a failed part retried with a fresh
    URL, the size checked, a failed upload deleted); `download_artifact()` and
    `open_artifact()` check the bytes against the artifact's size and sha256 and put a file into
    place only once it is complete. `verify_webhook_signature()` checks a delivery as
    `docs/platform/webhooks.md` describes (constant-time, timestamp tolerance, two secrets during
    a rotation).
  - **Airflow** (extra `airflow`, Airflow 3.2 or newer through its public `airflow.sdk`
    interfaces): a `forklift` connection type (Base URL, API token, CA bundle, timeout) found
    through the `apache_airflow_provider` entry point, `ForkliftHook`, `ForkliftJobOperator`
    (runs a dataset or submits a job with the fields of `JobIn`, pushes `job_id`, waits in the
    worker or with `deferrable=True` in the triggerer, returns the job as JSON, fails with the
    job's error code and message, cancels the job when the task is killed) and
    `ForkliftJobSensor` (poke, reschedule or deferrable), both through `ForkliftJobTrigger`. The
    operator's default `Idempotency-Key` comes from the DAG, the task, the DAG run, the map index
    and the first try of the task instance's current tries, so a retry re-attaches to the job
    its earlier try submitted, a job that failed or was cancelled is submitted again, and
    clearing the task starts a new job.
  - Tested against a fake gateway and object store on a real HTTP server that checks every
    request and answer against the OpenAPI document (100% of lines and branches on every
    supported Python), with Airflow 3.2.2 and 3.3.2 by running DAGs with `dag.test()`
    (operator, sensor, deferrals and trigger really executed), and against the Compose stack
    (`deploy/compose/e2e/test_client.py`). CI: `.github/workflows/client.yml`.
```

### docs/design/platform.md

§9.2 - replace the paragraph with:

```markdown
`forklift-client` (`clients/python`; standard library only, Python 3.9+) is a typed client of
`/api/v1`: resources as dataclasses generated from `contracts/openapi.json` (a contract test fails
when they drift), one exception hierarchy, retries only where they are safe, job submission with an
`Idempotency-Key`, uploads straight to the store (one PUT or multipart, streamed and checked) and
downloads checked against the artifact's size and sha256, and a webhook signature check
([client](../platform/client.md)). Its `airflow` extra (Airflow 3.2+) registers a `forklift`
connection type, a hook, an operator that submits a `JobSpec` (the fields of `JobIn`) or a dataset
run and waits for the result, in the worker or deferred to the triggerer, and a deferrable sensor
([Airflow](../platform/airflow.md)). The operator derives its `Idempotency-Key` from the DAG, task,
run, map index and the first try of the current series of tries, so a task retry re-attaches to the
job it already submitted; `publishing` is not terminal. Teams whose Airflow workers have data access
can instead call `forklift.run_job` in a task; the result has the same shape.
```

§11 - after the bullet list's "The services pin their own Python ..." bullet, add:

```markdown
- `forklift-client` supports Python 3.9+ (its Airflow extra: Airflow 3.2+, so Python 3.10+) and is
  tested in `.github/workflows/client.yml` (path filter `clients/python/**`,
  `contracts/openapi.json`); its models are generated from `contracts/openapi.json`, so a change of
  the API is followed by `python clients/python/tools/generate_models.py`.
```

§13 - M5 row, "Built so far:" → `Built so far: webhooks; forklift-client with the Airflow operator, deferrable sensor and trigger` (plus whatever the other agents add).

### docs/DOCUMENTATION_INDEX.md - under `## Platform (platform/)`

```markdown
- [client.md](platform/client.md): the Python client `forklift-client`: install, tokens, every method group, pagination, errors, retries and idempotency, uploads and downloads, webhook signatures
- [airflow.md](platform/airflow.md): the Airflow provider of `forklift-client`: the `forklift` connection, `ForkliftJobOperator` and `ForkliftJobSensor`, deferrable waits, retries that re-attach to their job, XCom, a complete DAG
```

and in the package readmes table (or a new line where `services/web/README.md` / `services/worker/README.md` are listed):

```markdown
| `forklift-client` (Python client, Airflow provider) | [clients/python/README.md](../clients/python/README.md) |
```

### .github/workflows/test.yml - job `platform-e2e`

The e2e job must install the client, or `test_client.py` skips (it skips cleanly: verified, `1 skipped` without it):

```yaml
    - name: Install the test client's dependencies
      run: |
        python -m pip install --upgrade pip
        pip install pytest pyarrow ./clients/python
```

It also needs the webhook receiver it already starts (`e2e/webhooks.yml`, `FORKLIFT_E2E_RECEIVER_URL`) for the
signature test; nothing else. The job's path filters (if any are added) should include `clients/python/**`.

### .github/workflows/fast-test.yml - `format-check` (and `autoformat`)

`clients/` is not in the format check yet:

```yaml
    - name: Check formatting with Black
      run: black --check --diff --line-length 99 src/ tests/ services/ deploy/ clients/

    - name: Check import order with isort
      run: |
        isort --check-only --diff --profile black --line-length 99 src/ tests/ deploy/
        isort --check-only --diff --profile black --line-length 99 services/web
        isort --check-only --diff --profile black --line-length 99 services/worker
        isort --check-only --diff --profile black --line-length 99 clients/python
```

(and the same paths in the autoformat job). `client.yml` checks black/isort/flake8 on `clients/python` itself.

### Releasing to PyPI (lead's call) - proposed `.github/workflows/publish-client.yml`

Not created. Needs a PyPI trusted publisher for project `forklift-client` (environment `pypi-client`), and
`publish.yaml` must skip tags that do not start with `v` (design §11), e.g. `if: startsWith(github.event.release.tag_name, 'v')`
on its jobs - today a `client-v0.1.0` release would make it fail its tag/version check.

```yaml
name: Publish forklift-client to PyPI

on:
  release:
    types: [published]

permissions:
  contents: read

jobs:
  build:
    if: startsWith(github.event.release.tag_name, 'client-v')
    runs-on: ubuntu-latest
    timeout-minutes: 15
    steps:
    - uses: actions/checkout@v4
      with:
        persist-credentials: false
    - uses: actions/setup-python@v5
      with:
        python-version: '3.12'
    # The tag (without "client-v") must be the version in clients/python/pyproject.toml
    - name: Verify the release tag matches the package version
      env:
        TAG_NAME: ${{ github.event.release.tag_name }}
      run: |
        python - <<'PY'
        import os, sys, tomllib
        tag = os.environ["TAG_NAME"]
        with open("clients/python/pyproject.toml", "rb") as handle:
            version = tomllib.load(handle)["project"]["version"]
        if tag != f"client-v{version}":
            sys.exit(f"Release tag {tag!r} does not match forklift-client {version!r}: expected client-v{version}")
        PY
    - name: The models match contracts/openapi.json
      run: python clients/python/tools/generate_models.py --check
    - name: Build and check
      run: |
        python -m pip install --upgrade pip build twine
        python -m build clients/python --outdir dist/
        twine check dist/*
    - uses: actions/upload-artifact@v4
      with:
        name: client-dist
        path: dist/

  test:
    needs: build
    runs-on: ubuntu-latest
    timeout-minutes: 15
    strategy:
      fail-fast: true
      matrix:
        python-version: ['3.9', '3.12', '3.14']
    steps:
    - uses: actions/checkout@v4
      with:
        persist-credentials: false
    - uses: actions/setup-python@v5
      with:
        python-version: ${{ matrix.python-version }}
    - uses: actions/download-artifact@v4
      with:
        name: client-dist
        path: dist/
    - name: Install the wheel and run the unit tests against it
      run: |
        python -m pip install --upgrade pip
        pip install dist/*.whl pytest pytest-cov
        cd clients/python && python -m pytest tests/unit -p no:cacheprovider

  publish-pypi:
    needs: [build, test]
    runs-on: ubuntu-latest
    environment:
      name: pypi-client
      url: https://pypi.org/p/forklift-client
    permissions:
      id-token: write  # trusted publishing
    steps:
    - uses: actions/download-artifact@v4
      with:
        name: client-dist
        path: dist/
    - uses: pypa/gh-action-pypi-publish@release/v1
```

(The workflow itself is untested, as it only runs on a release; its test step was checked locally: the unit tests
pass, 265, against the built wheel installed in a clean venv, importing `forklift_client` from site-packages.)

### Root README.md (optional, lead's call)

A line where the platform packages are listed: "`clients/python` - `forklift-client`, a typed Python client of the
API with an Airflow operator and deferrable sensor ([guide](docs/platform/client.md))."

## How I verified it (commands and results)

All venvs under `$S/client/`.

- Unit/contract tests, 100% line+branch of `forklift_client` (airflow subpackage omitted there, it has its own job):
  `cd clients/python && python -m pytest tests/unit --cov=forklift_client --cov-branch --cov-fail-under=100`
  → **265 passed, 100%** on **Python 3.9, 3.10, 3.11, 3.12, 3.13, 3.14** (3.13 in a local venv; all six in
  `mirror.gcr.io/library/python:<v>-slim` containers with the checkout mounted read-only and offline wheels:
  `$S/client/run_docker_py.sh <v>`). The TLS tests (a real https fake with an openssl-made self-signed cert;
  `ca_file` accepted, an unverifiable cert → non-retryable `TransportError`) ran in all of them.
  (Python 3.9 needed one change: a bare `continue` that CPython 3.9's optimizer hides from coverage.)
- Airflow tests: `python -m pytest tests/with_airflow --cov --cov-config=tests/with_airflow/coverage.ini`
  → **56 passed, 100% of `forklift_client.airflow`** on **Airflow 3.3.2** (Python 3.12, constraints-3.3.2) and on
  **Airflow 3.2.2** (Python 3.12, constraints-3.2.2). All suites together in the 3.3.2 venv: 321 passed.
  The tests run real DAG files with `dag.test()` (SQLite metadata DB, `airflow db migrate`): operator success with
  XCom `return_value`/`job_id`, templated JobIn fields, failed → task failed with code/message, cancelled → no
  retries, `wait_for_completion=False`, sync and deferred `wait_timeout`, **retry re-attach** (try 1 loses the gateway
  after submitting, try 2 sends the same key and waits for the same job: one job in the gateway), failed job →
  resubmission under `<key>:1`, already-succeeded job returned, changed spec (409) → fail without retries, other 409s,
  bounded resubmissions, deferred success/failure/404/timeout, transient errors polled through in the trigger,
  sensor poke/deferred/soft_fail/timeout/missing job, operator → sensor via the `job_id` XCom; plus provider
  discovery (`ProvidersManager` field behaviours and form widgets, `ProvidersManagerTaskRuntime().hooks["forklift"]`),
  the provider info validated against Airflow's `provider_info.schema.json`, hook/connection parsing, `test_connection`,
  `aget_conn`, operator and trigger `on_kill`.
- The complete example DAG in `docs/platform/airflow.md` was extracted from the page and run with `dag.test()`
  against the fake (paths and ids substituted): upload → deferred load → report → fetch, all `success`, file downloaded.
- E2E against a real Compose stack (my own project `forklift-client`, ports 28080/28090/28099, images
  `forklift-web:local`/`forklift-worker:local` (main) re-tagged `:client`, main's `deploy/compose` files from
  `git archive HEAD` so other agents' in-progress Compose edits could not interfere, plus `e2e/webhooks.yml`):
  `deploy/compose/e2e/test_client.py` → **9 passed** (twice): single-PUT upload, schema + dataset (reads uploads) run,
  wait, download `data`/`bad_rows` and read them with pyarrow, `open_artifact` stream, events; a 12.5 MB file uploaded
  **in 3 parts to RustFS** (thresholds lowered via admin settings, restored after) and a run over it with every row
  valid; generate + validate schema; one Idempotency-Key → one job, another request with it → `Conflict
  idempotency_key_reused`; **cancel** of a job that is certain to be queued (a run to a sql table: the sql lane, which
  the stack's worker does not serve) → `cancelled`/`CANCELLED`, a second cancel → `Conflict`; 404 with request id;
  wrong token → 401 without echo; a webhook test event delivered by the dispatcher to the receiver verifies with
  `verify_webhook_signature` (and fails with a wrong secret, an altered body, a late clock). Main's whole e2e suite plus
  mine against the same stack: **23 passed**. Without `forklift_client` installed the file reports `1 skipped`.
- Build: `python -m build clients/python` + `twine check` pass; the wheel contains `py.typed` and the
  `apache_airflow_provider` entry point; it installs in a clean venv and imports (`forklift_client`,
  `forklift_client.airflow.provider`) without Airflow.
- Lint: `black --check --line-length 99`, `isort --check-only --profile black --line-length 99`, `flake8
  --max-line-length=99 --extend-ignore=E203,W503` clean for `clients/python` and `deploy/compose/e2e/test_client.py`.
  `generate_models.py --check` clean against the current `contracts/openapi.json` (which OIDC has already extended:
  the client's models are unaffected so far).

## Not done / not verified (plainly)

- `.github/workflows/client.yml` has not run on GitHub; I ran the same commands locally. The publish workflow is only
  proposed (above).
- Airflow ran through `dag.test()` (tasks and triggers in-process, the in-process execution API). Not tested: a
  separate triggerer process, a Celery/Kubernetes executor, the React UI's connection form (I checked the provider
  metadata Airflow builds it from, not the rendered page), a real user "mark failed" reaching the trigger's `on_kill`
  (Airflow 3.3 only; I called `on_kill` directly). Airflow 3.2 does not call trigger `on_kill` at all; the docs say so.
- Not covered by the client (by design, admin/UI work): creating datasets, connections, schedules and webhooks,
  tokens, admin endpoints. MCPR can use `client._request(...)` internally if it needs one of those, but a public
  method is cleaner - tell me/the next agent if needed.
- After the OIDC agent's (or anyone's) change to `contracts/openapi.json`, if a response schema the client uses
  changes (`UserOut`, `JobOut`, ...), the contract test fails until `python clients/python/tools/generate_models.py`
  is run (and the diff looked at). Today it passes with OIDC's current document.

## Clean-up

Compose stack `forklift-client` stopped with `down -v`; image tags `forklift-web:client`/`forklift-worker:client`
removed; the Python slim images I pulled for the version matrix removed; scratch venvs for Airflow 3.2.2 and the e2e
run, probes and wheelhouses deleted. Kept (small, handy for re-runs): `$S/client/core313` (client + dev tools,
47 MB) and `$S/client/af312` (Airflow 3.3.2 + client editable, 275 MB) - delete them when done.
