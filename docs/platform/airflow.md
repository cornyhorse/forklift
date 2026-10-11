# Airflow: operator, sensor and trigger

`forklift-client[airflow]` lets an Airflow DAG run forklift jobs and wait for them: a
connection type `forklift`, a hook that gives the [Python client](client.md), an operator that
runs a dataset or submits a job and waits for its result, in the worker or deferred to the
triggerer, and a sensor that waits for a job to end. It needs **Apache Airflow 3.2 or newer**
(3.2 is the first release with every public interface it uses: `airflow.sdk.BaseHook`,
`airflow.sdk.conf`, `airflow.sdk.exceptions`, declarative connection fields) and so Python
3.10 or newer. It is tested with Airflow 3.2.2 and 3.3.2.

```bash
pip install "forklift-client[airflow]"
```

Airflow finds the provider through its `apache_airflow_provider` entry point: after the install,
`airflow providers list` shows `forklift-client` and the connection form offers the type
**Forklift**. Install it wherever tasks and triggers run (workers, triggerer) and where the UI
runs (api-server), like any provider.

## The connection

| Field | Value |
|---|---|
| Connection type | `forklift` (**Forklift**) |
| Base URL (host) | The gateway's public URL, `https://forklift.example.org`. A host name alone means https (with the connection's port, if it has one) |
| API token (password) | A forklift API token (`fkl_...`), from **API tokens** in forklift's UI. Airflow keeps it encrypted and masks it in task logs |
| CA bundle (extra `ca_file`) | Optional: a PEM file on the workers and the triggerer to verify the gateway's and the object store's certificates with |
| Timeout (extra `timeout`) | Optional: seconds per request (default 30) |

The default connection id is `forklift_default`. From the environment or a secrets backend, as
JSON:

```bash
export AIRFLOW_CONN_FORKLIFT_DEFAULT='{"conn_type": "forklift",
  "host": "https://forklift.example.org", "password": "fkl_...",
  "extra": {"ca_file": "/etc/ssl/private-ca.pem", "timeout": 60}}'
airflow connections test forklift_default     # "Connected to https://... as etl-bot (operator)"
```

(`airflow connections test` and the form's **Test** button need `[core] test_connection =
Enabled`; they call `GET /api/v1/me` with the token.)

Give the token only what the DAGs need: `jobs:read` and `jobs:run` to run and follow jobs, and
`artifacts:read` and `uploads:read`/`uploads:write` for tasks that download outputs or upload
files with the hook. A forklift service account with the Operator role is the usual owner.

## ForkliftJobOperator

```python
from forklift_client.airflow.operators import ForkliftJobOperator

load = ForkliftJobOperator(
    task_id="load_people",
    dataset_id="91f2c0de-...",          # a dataset run: its source, schema and destination
    deferrable=True,                     # wait in the triggerer, not in a worker slot
    poll_interval=30,
)
```

It submits a job (`POST /api/v1/jobs`), pushes its id as the XCom `job_id`, and by default waits
for it to end. The job's fields are those of the API's `JobIn` (see [client.md](client.md)) and
are templated:

| Parameter | Default | What it does |
|---|---|---|
| `kind` | `run` | `run`, `preview`, `validate_schema` or `generate_schema` |
| `dataset_id` | | A dataset run (with `upload_id` for datasets that read uploads) |
| `upload_id`, `format`, `input_options` | | A job on an upload: its id, its format (`csv`, `excel`, `fwf`) and reader options |
| `schema_version_id`, `schema` | | A stored schema version, or an inline schema (a dict) |
| `compression`, `options`, `limits`, `classification` | | As the API takes them |
| `idempotency_key` | derived (below) | The job's `Idempotency-Key` |
| `wait_for_completion` | `True` | Wait for the job; `False` returns it once queued (follow it with the sensor) |
| `deferrable` | `[operators] default_deferrable` | Wait in the triggerer, which frees the worker slot (needs a running triggerer) |
| `poll_interval` | `10` | Seconds between polls of the job |
| `wait_timeout` | `None` | Seconds to wait at most; the task fails, the job is **not** cancelled |
| `cancel_on_kill` | `True` | Cancel the job when the task is killed (marked failed or success, cleared, timed out by `execution_timeout`) while it waits |
| `forklift_conn_id` | `forklift_default` | The connection |

**How it ends.** The task succeeds when the job succeeded. A job is `publishing` while its
outputs are copied to its dataset's s3 destination ([publishing.md](publishing.md)): that is not
the end, the operator waits through it, so a successful task means published outputs. A job
that **failed** fails the task with the job's error code and message (`Forklift job 0a6d...
(run) failed: BAD_ROWS_THRESHOLD_EXCEEDED: 11 of 41 rows were rejected ...`); the task's
`retries` apply. A job that was **cancelled** fails the task without retries
(`AirflowFailException`): someone stopped it on purpose. Connection errors and transient
answers (429, 502-504) while waiting are logged and the polling goes on; other API errors
(401, 403, 404) fail the task.

**Waiting.** Without `deferrable`, the task polls in its worker. With `deferrable=True` it hands
the wait to `ForkliftJobTrigger` in the triggerer: one coroutine per job, polling
`GET /api/v1/jobs/{id}` every `poll_interval` seconds in a thread, which fires once when the job
ends; the task then resumes in a worker only to read the job and finish. `wait_timeout` is
measured from the submission and survives a triggerer restart.

### Retries re-attach to the job

A task that fails after it submitted its job (a worker that crashed, a lost network, a gateway
that restarted) must not start the job a second time when Airflow retries it. The operator
therefore submits with an `Idempotency-Key` derived from the task instance:

```
airflow:<sha256 of [dag_id, task_id, run_id, map_index, first try]>
```

where *first try* is the try that started the task instance's current series of tries
(`max_tries - retries + 1`, which Airflow itself uses for sensors' reschedules): it stays the
same across the retries of a run of the task, and **not** the try number. The gateway answers a
key it has seen from the same user with the job it created then, so a retry gets the job its
earlier try submitted:

| The job submitted under the key is | The retry |
|---|---|
| queued, running or publishing | waits for it again ("Re-attached to job ...") |
| succeeded | returns it at once, without running it again |
| failed or cancelled | submits the job again under the key with `:1` appended (`:2` the time after, ...), so that a retry of that try finds the new job in turn |

**Clearing** the task (to run it again on purpose) starts a new series of tries, so its key
changes and a new job is submitted, also when the earlier one succeeded. A new DAG run always
has new keys.

Give `idempotency_key` to choose the key yourself, for example per data partition so that two
DAG runs for the same day run the job once: `idempotency_key="people-{{ ds }}"`. The same
rules apply, except that clearing the task keeps that key (and so re-attaches). If the job's
fields change between tries (the DAG file was edited before a retry), the gateway refuses the
old key for the new job (`idempotency_key_reused`): the task fails without retries and says so;
clear it to submit the changed job.

### XCom

| Key | Value |
|---|---|
| `job_id` | The job's id, pushed as soon as it was submitted |
| `return_value` | The job as `GET /api/v1/jobs/{id}` returns it, as JSON |

`return_value` is the job's whole document: `id`, `kind`, `status`, `dataset_id`, `upload_id`,
`attempt`, `error` (`code`, `message`) and `result` (the engine's JobResult,
`contracts/jobresult.schema.json`: `counts` with `total_rows`, `valid_rows`, `invalid_rows`,
`truncated_rows` and, for table loads, `rows_written`; `validation_summary`, the findings by
rule; `warnings`; `artifacts`), `created_at`/`started_at`/`finished_at` (ISO 8601) and `spec`.
It never holds data: download artifacts with the hook when a task needs them.

```python
@task
def report(job: dict):
    counts = job["result"]["counts"]
    if counts["invalid_rows"]:
        print(f"{counts['invalid_rows']} of {counts['total_rows']} rows were rejected")

report(load.output)
```

## ForkliftJobSensor

```python
from forklift_client.airflow.sensors import ForkliftJobSensor

wait = ForkliftJobSensor(
    task_id="wait_for_people",
    job_id="{{ ti.xcom_pull(task_ids='submit_people', key='job_id') }}",
    deferrable=True,
    poke_interval=60,
    timeout=6 * 3600,
)
```

It waits for a job that something else submitted (an operator with
`wait_for_completion=False`, a schedule, another system) and succeeds with the job as its XCom
once it succeeded (`publishing` is not the end). A job that failed or was cancelled fails the
sensor without retries (the job will not change), or skips it with `soft_fail=True`. Everything
of `BaseSensorOperator` applies: `mode="poke"` or `"reschedule"`, `timeout`, `soft_fail`,
`exponential_backoff`; `deferrable=True` waits in the triggerer (with `poke_interval` between
polls and `timeout` as its deadline). The sensor never cancels the job.

## The trigger

`forklift_client.airflow.triggers.ForkliftJobTrigger(job_id, forklift_conn_id, poll_interval,
end_time, cancel_on_kill)` is what both defer to. It reads the connection without blocking the
triggerer's event loop, polls in a thread, and fires one event:

```json
{"status": "succeeded" | "failed" | "cancelled" | "timeout" | "error",
 "job_id": "0a6d...", "message": "Job 0a6d... failed: BAD_ROWS_THRESHOLD_EXCEEDED: ..."}
```

`end_time` is a Unix time (absolute, so a triggerer that restarts does not start the wait over).
When a user kills a deferred operator task (Airflow 3.3 calls the trigger's `on_kill`), the
trigger cancels the job if the operator's `cancel_on_kill` is set.

## A complete DAG

A file lands on the Airflow worker every night; the DAG uploads it, runs a dataset on it with a
deferred wait, reports the counts and downloads the clean Parquet for the next system:

```python
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG, task

from forklift_client.airflow.hooks import ForkliftHook
from forklift_client.airflow.operators import ForkliftJobOperator

DATASET_ID = "91f2c0de-6c55-4a8e-9a0b-3c1d2e4f5a6b"   # people: reads uploads

with DAG(
    "people_nightly",
    schedule="0 2 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:

    @task
    def upload(ds=None) -> str:
        client = ForkliftHook().get_conn()
        path = Path(f"/data/incoming/people-{ds}.csv")
        return client.upload_file(path, classification="sensitive").id

    load = ForkliftJobOperator(
        task_id="load",
        dataset_id=DATASET_ID,
        upload_id="{{ ti.xcom_pull(task_ids='upload') }}",
        deferrable=True,
        poll_interval=30,
        wait_timeout=4 * 3600,
    )

    @task
    def report(job: dict) -> None:
        counts = job["result"]["counts"]
        print(f"{counts['valid_rows']} valid, {counts['invalid_rows']} rejected")

    @task
    def fetch(job: dict, ds=None) -> str:
        client = ForkliftHook().get_conn()
        (data,) = [a for a in client.list_artifacts(job["id"]) if a.kind == "data"]
        target = Path(f"/data/clean/{ds}")
        target.mkdir(parents=True, exist_ok=True)
        return str(client.download_artifact(data, target))   # checked: size and sha256

    upload() >> load
    report(load.output)
    fetch(load.output)
```

A retry of `upload` uploads the file again (an upload that failed half-way was deleted), a
retry of `load` re-attaches to its job, a retry of `fetch` downloads again (the file is put into
place only once it is complete and checked).

## Troubleshooting

| Symptom | Cause |
|---|---|
| `Connection 'forklift_default' has no host` / `has no password` | Fill in the Base URL and the API token |
| `... failed with HTTP 401 (not_authenticated)` | The token is wrong, revoked or expired |
| `... failed with HTTP 403 (permission_denied)` | The token's scopes or its owner's role do not allow it (see above) |
| `no answer from ... (TLS error: CERTIFICATE_VERIFY_FAILED ...)` | The gateway's certificate is not trusted: set the CA bundle |
| A deferred task stays deferred | No triggerer runs, or it cannot reach the gateway (it needs the provider installed too) |
| `The job's Idempotency-Key ... was used for a different job` | The task's job changed between tries: clear the task |
