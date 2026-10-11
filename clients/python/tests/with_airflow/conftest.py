"""An Airflow installation for the tests: its home, a SQLite metadata database and a DAGs folder,
and a helper that writes a DAG file there and runs it with ``dag.test()`` (tasks, deferrals and
triggers really executed, in this process, against the fake gateway).

Airflow reads its configuration when it is first imported, so AIRFLOW_HOME and the rest are set
here, before any test module imports it; the directory outlives single tests (tmp_path cannot
be had this early) and is removed when the session ends.
"""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path

import pytest

HOME = Path(tempfile.mkdtemp(prefix="forklift-airflow-"))
DAGS = HOME / "dags"
DAGS.mkdir()
os.environ.update(
    {
        "AIRFLOW_HOME": str(HOME),
        "AIRFLOW__CORE__DAGS_FOLDER": str(DAGS),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{HOME}/airflow.db",
    }
)

DAG_HEADER = """\
from datetime import datetime, timedelta

from airflow.sdk import DAG

from forklift_client.airflow.operators import ForkliftJobOperator
from forklift_client.airflow.sensors import ForkliftJobSensor

with DAG(
    {dag_id!r},
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={{"retry_delay": timedelta(seconds=0)}},
) as dag:
"""


@pytest.fixture(scope="session", autouse=True)
def metadata_database():
    """Airflow's metadata database, made once (``airflow db migrate``)."""
    migrated = subprocess.run(
        [sys.executable, "-m", "airflow", "db", "migrate"], capture_output=True, text=True
    )
    assert migrated.returncode == 0, migrated.stderr[-3000:]
    yield
    shutil.rmtree(HOME, ignore_errors=True)


@pytest.fixture
def connection(fake, monkeypatch):
    """The ``forklift_default`` connection, pointing at the fake gateway."""
    from fake_api import TOKEN

    value = {"conn_type": "forklift", "host": fake.url, "password": TOKEN}
    monkeypatch.setenv("AIRFLOW_CONN_FORKLIFT_DEFAULT", json.dumps(value))
    return value


@pytest.fixture
def run_dag(connection, request):
    """``run_dag(tasks)``: a DAG of ``tasks`` (Python source, the body of ``with DAG``), named
    after the test, run once with ``dag.test()``; returns the DAG run."""

    def run(tasks: str):
        dag_id = request.node.name.replace("[", "_").replace("]", "").replace("-", "_")
        path = DAGS / f"{dag_id}.py"
        path.write_text(DAG_HEADER.format(dag_id=dag_id) + textwrap.indent(tasks, "    "))
        spec = importlib.util.spec_from_file_location(dag_id, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.dag.test()

    return run
