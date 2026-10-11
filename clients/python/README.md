# forklift-client

A typed Python client for the forklift gateway's API (`/api/v1`), with an Airflow operator,
deferrable sensor and trigger ([platform design](../../docs/design/platform.md) §9.2).

- Standard library only (Python 3.9 or newer): `urllib`, `ssl`, `hashlib`, `json`.
- The API's resources as frozen dataclasses generated from
  [contracts/openapi.json](../../contracts/openapi.json); one exception hierarchy with the
  status, the API's error code and detail and the request id (never the token).
- Retries only where they are safe, and job submission with an `Idempotency-Key`, so a request
  sent again cannot start a second job.
- Uploads straight to the object store through presigned URLs, in one PUT or in parts, streamed,
  with `Content-MD5` and size checks; downloads checked against the artifact's size and sha256.
- `verify_webhook_signature()` for receivers of the gateway's webhooks.
- `forklift_client.airflow` (extra `airflow`, Airflow 3.2+): a `forklift` connection type, a
  hook, `ForkliftJobOperator` (waits in the worker or deferred; retries re-attach to the job
  they submitted) and `ForkliftJobSensor`.

```bash
pip install forklift-client                 # or: pip install "forklift-client[airflow]"
```

```python
from forklift_client import ForkliftClient

client = ForkliftClient("https://forklift.example.org", token)   # or ForkliftClient.from_env()
upload = client.upload_file("people.csv")
job = client.submit_job("run", upload_id=upload.id, format="csv", schema_version_id=version_id)
job = client.wait_for_job(job, raise_on_failure=True)
for artifact in client.list_artifacts(job):
    client.download_artifact(artifact, "out/")
```

Documentation: [docs/platform/client.md](../../docs/platform/client.md) (every method group,
errors, retries and idempotency, uploads and downloads, webhook signatures) and
[docs/platform/airflow.md](../../docs/platform/airflow.md) (the connection, the operator and
sensor, deferrable waits, retries, XCom, an example DAG).

## Development

```bash
pip install -e "clients/python[dev]"
cd clients/python
python -m pytest tests/unit --cov=forklift_client --cov-branch --cov-fail-under=100
python tools/generate_models.py --check          # the models match contracts/openapi.json

# The Airflow provider, in an environment with Airflow (its constraints file pins the rest):
pip install "apache-airflow==3.3.2" pytest pytest-cov jsonschema \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt
pip install --no-deps -e clients/python
python -m pytest tests/with_airflow --cov --cov-config=tests/with_airflow/coverage.ini
```

`tests/fake_api.py` is a fake gateway and object store on a real local HTTP server that checks
every request and answer against the OpenAPI document; `tests/with_airflow` runs DAGs with
`dag.test()` against it (tasks, deferrals and triggers really executed). The end-to-end test
against a Compose stack is `deploy/compose/e2e/test_client.py`.

After a change of the API, regenerate the models with `python tools/generate_models.py` and
look at what changed.
