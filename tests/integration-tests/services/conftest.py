"""Fixtures for the integration tests that run against real services.

The services are RustFS (S3-compatible object store), PostgreSQL, MySQL, SQL Server and Oracle
Database Free, defined in ``compose.yaml`` next to this file. The tests run only when
``FORKLIFT_TEST_SERVICES=1`` is set:

    scripts/test-services.sh up
    FORKLIFT_TEST_SERVICES=1 python -m pytest tests/integration-tests/services

With the variable set, a service that cannot be reached fails its tests instead of skipping
them, so a CI job cannot pass by testing nothing. Every test works in its own bucket, logins and
namespace (a schema on PostgreSQL and SQL Server, a database on MySQL, a user on Oracle), all
removed afterwards, so the tests can run against long-lived services and in any order. Settings
are described in ``service_helpers.py``.

Database fixtures: ``database`` runs a test once on each of the four databases;
``column_grant_database`` on those that grant SELECT on single columns (not Oracle);
``postgres``, ``mysql``, ``mssql`` and ``oracle`` give one of them. Each yields a
``service_helpers.Database`` whose namespace exists and is empty (see README.md for its calls).
"""

from __future__ import annotations

from typing import Callable, Dict, Iterator, Optional

import pytest
from service_helpers import (
    DATABASES,
    ENABLED,
    Database,
    MsSql,
    MySql,
    ObjectStore,
    Oracle,
    Postgres,
)


@pytest.fixture(autouse=True)
def _require_opt_in():
    if not ENABLED:
        pytest.skip(
            "service tests need FORKLIFT_TEST_SERVICES=1 and the services in "
            "tests/integration-tests/services/compose.yaml (scripts/test-services.sh up)"
        )


def _unreachable(service: str, error: Exception) -> None:
    pytest.fail(
        f"{service} is not reachable ({type(error).__name__}: {error}). Start the services "
        "with scripts/test-services.sh up, or unset FORKLIFT_TEST_SERVICES to skip these tests.",
        pytrace=False,
    )


# --------------------------------------------------------------------------- object store


@pytest.fixture(scope="session")
def object_store() -> ObjectStore:
    store = ObjectStore.from_environment()
    if ENABLED:
        try:
            store.client().list_buckets()
        except Exception as error:  # any failure means the store is not usable
            _unreachable(f"The object store at {store.endpoint}", error)
    return store


@pytest.fixture
def bucket(object_store: ObjectStore) -> Iterator[str]:
    """A new, empty bucket, deleted with everything in it after the test."""
    import uuid

    name = f"forklift-it-{uuid.uuid4().hex[:12]}"
    object_store.client().create_bucket(Bucket=name)
    yield name
    object_store.empty_and_delete(name)


@pytest.fixture
def s3_environment(
    monkeypatch, tmp_path, object_store: ObjectStore
) -> Callable[[Optional[Dict[str, str]]], None]:
    """Point boto3's default credential chain at the store, as a pipeline's environment would.

    ``import_csv`` and friends build their client from the environment. Call the returned
    function with the credentials to use (root credentials are set to begin with). Shared AWS
    config and credential files are hidden so a developer's own AWS setup cannot leak in.
    """
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "no-aws-config"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "no-aws-credentials"))
    for name in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_SESSION_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_ENDPOINT_URL", object_store.endpoint)
    monkeypatch.setenv("AWS_DEFAULT_REGION", object_store.region)

    def use(credentials: Optional[Dict[str, str]] = None) -> None:
        credentials = credentials or object_store.root_credentials
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", credentials["aws_access_key_id"])
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", credentials["aws_secret_access_key"])
        if credentials.get("aws_session_token"):
            monkeypatch.setenv("AWS_SESSION_TOKEN", credentials["aws_session_token"])
        else:
            monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)

    use()
    return use


# --------------------------------------------------------------------------- databases


def _open(factory) -> Iterator[Database]:
    database = factory()
    if database.driver is None:
        pytest.fail(
            f"No ODBC driver for {database.kind} is installed; install one or set "
            f"FORKLIFT_TEST_{database.setting_prefix}_DRIVER (see "
            "tests/integration-tests/services/README.md).",
            pytrace=False,
        )
    try:
        database.ping()
    except Exception as error:  # any failure means the server is not usable
        _unreachable(f"{database.kind} at {database.host}:{database.port}", error)
    database.create_namespace()
    try:
        yield database
    finally:
        database.drop_everything()


@pytest.fixture(params=list(DATABASES))
def database(request) -> Iterator[Database]:
    """Each test using this runs on PostgreSQL, MySQL, SQL Server and Oracle, in a fresh
    namespace."""
    yield from _open(DATABASES[request.param].from_environment)


@pytest.fixture(params=[kind for kind, cls in DATABASES.items() if cls.supports_column_grants])
def column_grant_database(request) -> Iterator[Database]:
    """Like ``database``, on the databases that grant SELECT on single columns.

    Oracle grants SELECT on whole tables only; tests that need column grants show that with
    their own Oracle test (the grant is refused) instead of being skipped there.
    """
    yield from _open(DATABASES[request.param].from_environment)


@pytest.fixture
def postgres() -> Iterator[Postgres]:
    yield from _open(Postgres.from_environment)


@pytest.fixture
def mysql() -> Iterator[MySql]:
    yield from _open(MySql.from_environment)


@pytest.fixture
def mssql() -> Iterator[MsSql]:
    yield from _open(MsSql.from_environment)


@pytest.fixture
def oracle() -> Iterator[Oracle]:
    yield from _open(Oracle.from_environment)
