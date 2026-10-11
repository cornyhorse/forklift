# Python client (`forklift-client`)

`forklift-client` is a typed Python client of the gateway's public API (`/api/v1`): uploads,
schemas, datasets, jobs, artifacts, schedules and webhooks, with retries that are safe, job
submission that cannot create a job twice, uploads and downloads that go straight to the object
store and are checked on the way, and a helper that verifies webhook signatures. It has no
dependencies beyond the Python standard library (3.9 or newer). Its Airflow operator, sensor
and trigger are described in [airflow.md](airflow.md).

```python
from forklift_client import ForkliftClient

client = ForkliftClient("https://forklift.example.org", token)

upload = client.upload_file("exports/people.csv")
job = client.run_dataset(dataset_id, upload_id=upload.id)
job = client.wait_for_job(job, timeout=3600, raise_on_failure=True)
print(job.result["counts"])                     # {"total_rows": ..., "valid_rows": ..., ...}
for artifact in client.list_artifacts(job):
    client.download_artifact(artifact, "out/")  # data.parquet, bad_rows.parquet, ...
```

## Install

```bash
pip install forklift-client                 # the client
pip install "forklift-client[airflow]"      # and the Airflow provider (Airflow 3.2+)
pip install ./clients/python                # from a checkout of this repository
```

## Connecting

| Argument | Default | What it is |
|---|---|---|
| `base_url` | (required) | The gateway's public URL, as people open it: `https://forklift.example.org` (a trailing `/api/v1` is accepted too). `http://` works, but sends the token in the clear: use it on `localhost` only |
| `token` | (required) | An API token (`fkl_...`): **API tokens** in the UI, or `POST /api/v1/tokens`. Leading and trailing whitespace (a newline from a file) is dropped |
| `timeout` | `30` | Socket timeout of each request, in seconds (a slow upload is fine: it bounds each wait for the network, not the whole transfer) |
| `ca_file` | the system's | A PEM bundle to verify the gateway's and the object store's certificates with, for installations with a private CA |
| `retry` | `RetryPolicy()` | When and how often to send a request again (below); `NO_RETRIES` turns retries off |
| `user_agent` | | Put in front of the client's own `User-Agent` (`forklift-client/0.1.0 Python/3.12.3`) |

`ForkliftClient.from_env()` reads `FORKLIFT_URL`, `FORKLIFT_TOKEN` and (optionally)
`FORKLIFT_CA_FILE`. A client is thread-safe and holds no open connections, so one per process
(or per token) is enough. Proxies come from the environment (`HTTPS_PROXY`, `NO_PROXY`), as for
everything built on `urllib`.

**Tokens.** A token can do what its scopes allow within its owner's role. A pipeline usually
needs `jobs:read` and `jobs:run` (submit, follow and cancel jobs; validate schemas),
`artifacts:read` (downloads), `uploads:read` and `uploads:write` (uploads) and
`datasets:read`; creating schemas needs `schemas:write` (the Author role). A service account
(Admin → Users) with the Operator role and a token scoped to just that is the usual setup.

**Where the token goes.** Only to the gateway: every request to `/api/v1` carries
`Authorization: Bearer <token>`. Uploads and downloads go straight to the object store through
presigned URLs, which carry their own short-lived signature and never get the token. Redirects
are not followed. The token never appears in `repr(client)`, in exceptions or in log messages;
neither do presigned URLs (messages name the store's host only).

## Resources

Methods return the API's resources as frozen dataclasses (`forklift_client.models`), generated
from the OpenAPI document `contracts/openapi.json`:

| Model | Is | Returned by |
|---|---|---|
| `Me` (`User`, `Token`) | The caller | `me()` |
| `Schema`, `SchemaVersion` | A schema and its immutable versions | `list_schemas()`, `get_schema()`, `create_schema()`, `create_schema_version()`, `get_schema_version()`, `list_schema_versions()` |
| `Connection` | A source or destination (secrets are never returned: `secret_fields` names those that are set) | `list_connections()`, `get_connection()` |
| `Dataset` | A source, a schema version and a destination | `list_datasets()`, `get_dataset()` |
| `UploadTicket`, `PartUrl`, `Upload` | An upload being made, and the upload | `create_upload()`, `upload_part_urls()`, `complete_upload()`, `get_upload()`, `list_uploads()`, `upload_file()` |
| `Job` | A job, with its spec | `submit_job()`, `run_dataset()`, `validate_schema()`, `get_job()`, `cancel_job()`, `wait_for_job()` |
| `JobSummary` | A job without its spec | `list_jobs()` |
| `JobEvent` | A state change, progress report or log line | `list_job_events()`, `iter_job_events()` |
| `Artifact`, `Download` | A file a job wrote, and its presigned URL | `list_artifacts()`, `get_artifact()`, `get_download_url()` |
| `Schedule` | A schedule of a dataset | `list_schedules()`, `get_schedule()` |
| `Webhook` | A webhook of the caller | `list_webhooks()`, `get_webhook()` |

Ids are strings; times are timezone-aware `datetime` objects; lists are tuples; JSON objects (a
job's `result` and `spec`, a schema's `document`) stay dicts. `to_dict()` gives the resource
back as the JSON the API sent (times in ISO 8601), which is what to store or pass on;
`Model.from_dict()` builds one from it. Fields a newer gateway adds are ignored, values of
enumerations it adds are passed through, and a field this client needs that the gateway does
not send is an `UnexpectedResponse`. Methods that take an id also take the resource:
`client.wait_for_job(job)` and `client.wait_for_job(job.id)` are the same.

## Method groups

### Who am I

```python
me = client.me()
me.user.username, me.user.role        # "etl-bot", "operator"
me.scopes                             # effective scopes of this token
```

### Schemas

```python
schema = client.create_schema("people", {"properties": {"id": {"type": "integer"}}},
                              description="HR export", notes="first draft")
version = client.create_schema_version(schema, new_document, notes="age is optional")
client.get_schema_version(schema, 2).document
for version in client.paginate(client.list_schema_versions, schema):   # newest first
    ...
job = client.validate_schema(upload_id=upload.id, schema=draft, format="csv")
job = client.wait_for_job(job)        # validate_schema waits briefly; it may come back queued
```

`validate_schema()` enqueues a `validate_schema` job on the interactive lane and the gateway
waits for it up to `wait_seconds` (at most its `validate_wait_seconds`, 10 by default): the job
comes back finished, or queued or running. Each call is a new job.

### Connections and datasets

```python
for connection in client.paginate(client.list_connections, kind="s3"):
    print(connection.name, connection.config, connection.secret_fields)
dataset = client.get_dataset(dataset_id)
job = client.run_dataset(dataset, upload_id=upload.id)   # upload_id: datasets that read uploads
```

Creating and changing connections and datasets is an administrative task for the UI (or the
raw API); the client reads them and runs datasets.

### Uploads

```python
upload = client.upload_file("big.csv")                        # a path
upload = client.upload_file(open("big.csv", "rb"))            # an open binary file
upload = client.upload_file(stream, filename="x.csv", size=n, # a stream that cannot seek
                            classification="sensitive")
```

`upload_file()` does the whole upload and returns the complete `Upload`, ready to be the input
of a job:

1. `POST /uploads` with the name and size answers with one presigned PUT URL, or, for files
   above the gateway's `multipart_threshold_bytes` (256 MiB by default), the part size
   (`multipart_part_bytes`, 64 MiB, more for files that would need more than 10,000 parts) and
   the first part URLs.
2. The bytes go up in one PUT, or part by part: part URLs are asked for in batches as they are
   needed (`POST /uploads/{id}/parts`), and each part's ETag is kept.
3. `POST /uploads/{id}/complete` (with the parts) makes the gateway check, with a HEAD of the
   object, that it has the declared size.

Every PUT sends `Content-Length` and `Content-MD5`, so the store itself refuses a body that was
corrupted on the way. Bodies are streamed: from the file itself when it can seek (memory stays
at about a megabyte whatever the size), otherwise one part at a time through a temporary file
(in memory up to 8 MiB) so that a failed part can be sent again. A PUT that fails with a
connection error, a timeout, 408, 429 or 5xx is sent again after a backoff; a part gets a fresh
URL for each retry, so a 403 (an expired URL) is retried too, while a single PUT whose URL has
expired (an hour by default: `upload_url_seconds`) is not. The file must not change while it is
uploaded: reading fewer or more bytes than its size is a `VerificationError`. An upload that
fails is deleted (`DELETE /uploads/{id}`, which aborts a multipart upload), so no half-uploaded
object is left behind. `sha256=` records a checksum you already have as `declared_sha256`; the
gateway does not check it.

The lower-level calls are there too: `create_upload()`, `upload_part_urls()`,
`complete_upload()`, `get_upload()`, `list_uploads()` and `delete_upload()` (an upload that is
already deleted or expired is not an error).

### Jobs

```python
job = client.submit_job(
    "run",                                   # run, preview, validate_schema, generate_schema
    upload_id=upload.id, format="csv",       # or dataset_id=...
    schema_version_id=version.id,            # or schema={...} (an inline draft)
    input_options={"delimiter": ";"},
    options={"bad_rows_threshold": 0.05},
    limits={"max_seconds": 600},
    idempotency_key="people-2026-10-11",     # optional, see below
)
job = client.get_job(job.id)
page = client.list_jobs(status="failed", dataset_id=dataset.id, mine=True, limit=20)
for event in client.iter_job_events(job):   # state changes, progress, log lines
    print(event.id, event.type, event.payload)
client.cancel_job(job)
job = client.wait_for_job(job, timeout=3600, poll_interval=1, max_poll_interval=30,
                          raise_on_failure=True, on_poll=lambda j: print(j.status))
```

The fields of `submit_job()` are those of `JobIn` in the OpenAPI document. A job is `queued`,
`running`, `publishing` (a run whose outputs are being copied to its dataset's s3
destination, see [publishing.md](publishing.md)) and then `succeeded`, `failed` or `cancelled`.
**`publishing` is not the end**: `wait_for_job()` waits through it, so `succeeded` means the
outputs are published. `wait_for_job()` polls with growing intervals, logs connection errors
and transient answers (429, 502-504) and polls on, and raises `JobTimeout` (a `TimeoutError`;
the job goes on running) after `timeout` seconds (`None`: no limit). With
`raise_on_failure=True` a job that failed or was cancelled raises `JobFailed`, which has the
job and its error's `code` and `message`; without it, check `job.status` and `job.error`.

`cancel_job()` cancels a queued job at once (it comes back `cancelled`); a running one stops at
its worker's next heartbeat (`cancel_requested` is true until then). A job that has finished or
is publishing cannot be cancelled (`Conflict`).

### Artifacts

```python
for artifact in client.list_artifacts(job):              # data, bad_rows, manifest, ...
    path = client.download_artifact(artifact, "out/")    # out/<name>; returns the path
client.download_artifact(artifact.id, "people.parquet")  # an id, to a file name
with client.open_artifact(artifact) as stream:           # a stream, checked at its end
    for chunk in stream:
        sink.write(chunk)
link = client.get_download_url(artifact)                 # the presigned URL itself
```

Each download asks the gateway for a presigned URL (`GET /artifacts/{id}/download`, audited,
valid for `download_url_seconds`, 5 minutes by default) and reads the bytes from the store.
They are checked against the artifact: their count must be its `bytes` and, when the worker
reported one, their sha256 its `sha256`; a mismatch is a `VerificationError`.
`download_artifact()` writes to a temporary file next to the destination and renames it into
place only when the check passed, so a file under the destination's name is always complete;
a connection error or a store error worth retrying (5xx, 429, 408, and 403: an expired URL)
starts the download again with a fresh URL. `open_artifact()` is checked when the end is read
and is not retried once reading began. An artifact deleted by retention is `Gone`.

### Schedules and webhooks

```python
for schedule in client.paginate(client.list_schedules, enabled=True):
    print(schedule.dataset_name, schedule.cron, schedule.timezone, schedule.next_run_at)
client.get_schedule(schedule_id).last_outcome
for webhook in client.paginate(client.list_webhooks):
    print(webhook.name, webhook.active, webhook.consecutive_failures)
```

Schedules and webhooks are set up in the UI (or the raw API: [schedules.md](schedules.md),
[webhooks.md](webhooks.md)); the client reads them.

## Pagination

List methods return one `Page`: `items` (a tuple), `count` (all items), `offset`, `limit`,
`has_more` and `next_offset`; a page holds at most 500 items (the API's limit). `paginate()`
reads every page of any of them:

```python
for job in client.paginate(client.list_jobs, status="failed", page_size=200):
    ...
```

Pages are limit/offset pages of lists ordered by the API (jobs newest first, schemas and
datasets by name, schedules by their next run): a list that changes while it is read can
repeat or miss items. A job's events are read with a cursor instead: `list_job_events(job,
after=last_id)` returns only events after that id, and `iter_job_events()` follows it, so
nothing is repeated or missed. `list_artifacts()` and `upload_part_urls()` return plain lists.

## Errors

Every error is a `ForkliftError`:

| Exception | When |
|---|---|
| `ApiError` | The API answered with an error status: `status`, `code` and `detail` (the API's), `method` and `path`, `request_id` (the gateway's `X-Request-ID`, which its logs and audit log carry), `retry_after` |
| `BadRequest` | 400 (malformed, or names something that cannot be used) and 422 (the body has not the API's shape: `detail` lists the fields) |
| `Unauthorized`, `Forbidden` | 401 (no, unknown, revoked or expired token), 403 (the role or the token's scopes do not allow it) |
| `NotFound`, `Conflict`, `Gone` | 404, 409 (for example a finished job cancelled, or `code == "idempotency_key_reused"`), 410 (deleted by retention) |
| `TooManyRequests`, `ServerError` | 429, 5xx (502: the object store behind the gateway did not answer) |
| `TransportError` | No HTTP answer: refused, reset, DNS, TLS, timeout (`sent` says whether the request may have reached the server) |
| `StoreError` | The object store refused a presigned upload or download (`status`, and its error `code` such as `AccessDenied`) |
| `VerificationError` | Bytes are not what they should be (a download's size or sha256, an upload's size) |
| `UnexpectedResponse` | The answer is not what the API sends (not JSON: is `base_url` the gateway's?), or misses a field |
| `JobFailed`, `JobTimeout` | `wait_for_job()`: the job failed or was cancelled (`raise_on_failure`), or did not end in time |

```python
try:
    client.cancel_job(job)
except Conflict as error:
    log.info("not cancelled: %s", error.detail)
except ApiError as error:
    log.error("%s (quote request id %s to the operators)", error, error.request_id)
```

Messages say what was asked, what came back and the request id, for example
`POST /api/v1/jobs failed with HTTP 403 (permission_denied): ... [request id 3f2c...]`. They
never contain the token, presigned URLs or data; for a 422 the field names are listed but not
the values pydantic echoes.

## Retries and idempotency

| Request | Sent again after |
|---|---|
| `GET`, `PUT`, `DELETE`, and the POSTs the API makes idempotent: job submission and dataset runs (with their `Idempotency-Key`), completing an upload, asking for part URLs | a connection error or timeout; 429, 502, 503, 504 |
| Other POSTs (creating a schema or an upload, `validate_schema()`, `cancel_job()`) | only when the gateway certainly did not act: the connection was refused or the name did not resolve; 429; 503 with `Retry-After` |
| Anything | never after a TLS failure (a certificate that does not verify), never after other 4xx/5xx |

`RetryPolicy(attempts=4, initial_delay=0.5, max_delay=30, max_retry_after=120)` is the
default: four tries in all, waits doubling from half a second up to 30 seconds with jitter, a
`Retry-After` the gateway sends waited instead (unless it asks for more than `max_retry_after`:
then the `TooManyRequests` or `ServerError` is raised, with `retry_after` set). One request keeps
its `X-Request-ID` across its retries. Retries are logged at INFO on the `forklift_client`
logger.

**Job submission cannot create a job twice.** `submit_job()` and `run_dataset()` send an
`Idempotency-Key` (a random one unless you give `idempotency_key=`): the gateway answers a key
it has already seen from the same user with the job it created then (HTTP 200 instead of 201),
so a submission sent again after a lost answer, by the client's retries or by yours, gets the
same job. Give your own key (at most 255 printable ASCII characters) to make that hold across
processes and restarts, for example `f"{pipeline}:{partition}"`. A key used before for a
*different* request is a `Conflict` with `code == "idempotency_key_reused"`: a new request
needs a new key. Keys are per user and kept as long as the job record.

## Webhook signatures

`verify_webhook_signature(body, header, secret, tolerance=300)` checks a delivery exactly as
[webhooks.md](webhooks.md#checking-a-delivery) describes: the HMAC-SHA256 of
`<timestamp>.<raw body>` keyed with the signing secret, compared in constant time, with the
timestamp at most `tolerance` seconds from the receiver's clock. Give it the raw body, before
any parsing; it returns `False` for a missing, malformed, forged, altered or replayed delivery.
During a secret rotation, pass both secrets: `[old_secret, new_secret]`.

```python
import json
from http.server import BaseHTTPRequestHandler

from forklift_client import verify_webhook_signature

class Receiver(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers["Content-Length"]))
        if not verify_webhook_signature(body, self.headers.get("Forklift-Signature"), SECRET):
            self.send_response(401)
            self.end_headers()
            return
        self.send_response(204)            # answer first, then do the slow work
        self.end_headers()
        handle(json.loads(body))           # "id" is the delivery: the same on every retry
```

## Compatibility and development

The client speaks version 1 of the API. Its models are generated from `contracts/openapi.json`
by `clients/python/tools/generate_models.py`; a contract test fails when the document and the
models (or the methods' parameters) drift apart, and the tests run every request and answer
through a check against the document. After the API changes:

```bash
python clients/python/tools/generate_models.py           # rewrite models.py
python clients/python/tools/generate_models.py --check   # what CI runs
cd clients/python && python -m pytest tests/unit --cov=forklift_client --cov-branch --cov-fail-under=100
```

The tests use a fake gateway and object store on a real local HTTP server
(`tests/fake_api.py`); `deploy/compose/e2e/test_client.py` uses the client against a running
Compose stack (uploads in one PUT and in parts, a dataset run and its downloads, idempotent
submission, cancellation, a webhook signature).
