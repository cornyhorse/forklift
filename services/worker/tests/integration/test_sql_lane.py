"""An sql-lane job through the worker and the real engine, against PostgreSQL with a restricted
login (FORKLIFT_TEST_SERVICES=1): the connection string reaches the engine in spec.json only."""

from __future__ import annotations

import json
import logging

import pytest
import service_helpers
from it_helpers import REAL_ENGINE, engine_has_run_job

from forklift_worker.supervisor import Supervisor

pytestmark = [
    pytest.mark.services,
    pytest.mark.skipif(not engine_has_run_job(), reason="this engine has no `forklift run-job`"),
]


@pytest.fixture
def postgres(services_enabled):
    database = service_helpers.Postgres.from_environment()
    if database.driver is None:
        pytest.fail("No PostgreSQL ODBC driver is installed.", pytrace=False)
    try:
        database.admin("SELECT 1")
    except Exception as error:  # any failure means the server is not usable
        pytest.fail(f"PostgreSQL is not reachable ({type(error).__name__}).", pytrace=False)
    database.create_namespace()
    try:
        yield database
    finally:
        database.drop_everything()


def test_a_sql_job_keeps_its_connection_string_to_itself(
    gateway, make_settings, postgres, caplog, tmp_path
):
    postgres.admin(
        f"CREATE TABLE {postgres.table('people')} (id integer, name text)",
        f"INSERT INTO {postgres.table('people')} VALUES (1, 'alice'), (2, 'bob')",
    )
    login = postgres.create_user()
    postgres.grant_select(login, "people")
    connection = postgres.login_connection_string(login)
    spec = {
        "spec_version": 1,
        "job_id": "sql",
        "kind": "run",
        "input": {"format": "sql", "location": {"type": "sql", "connection_string": connection}},
        "schema": json.loads(
            service_helpers.sql_schema_file(tmp_path, postgres.namespace, ["people"]).read_text()
        ),
        "output": {"location": {"type": "file", "path": "out/"}},
    }
    job = gateway.enqueue(spec)
    caplog.set_level(logging.DEBUG, logger="forklift_worker")
    supervisor = Supervisor(
        make_settings(engine_command=REAL_ENGINE, engine_read_path=[], lanes=["sql"])
    )
    supervisor.run()

    report = job.completed
    assert report["result"]["status"] == "succeeded", report["result"]
    assert any(artifact["kind"] == "data" for artifact in report["artifacts"])
    everything = json.dumps(gateway.requests) + caplog.text
    assert login.password not in everything
    assert connection not in everything
    for stored in gateway.objects.values():
        assert login.password.encode() not in stored
    assert [p.name for p in supervisor.settings.scratch.iterdir()] == [".forklift-worker.lock"]
