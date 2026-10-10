"""Connections: validation per kind, write-only encrypted secrets, tests and connection strings."""

from __future__ import annotations

import socket

import pytest
from django.conf import settings
from world import World, make_user, s3_connection

from forklift_web import secret_backend, storage
from forklift_web.core.choices import Role
from forklift_web.core.models import AuditLog, Connection, Dataset
from forklift_web.errors import Conflict, InvalidRequest, NotFound, PermissionDenied
from forklift_web.policy import Actor
from forklift_web.services import connections

pytestmark = pytest.mark.django_db

STORE = settings.FORKLIFT_STORE


def s3_config(**changes):
    return {"bucket": "exports", "endpoint_url": "https://s3.example.org", **changes}


S3_SECRETS = {"access_key_id": "AKIA-TEST", "secret_access_key": "very-secret-value"}
SQL_CONFIG = {
    "dialect": "postgresql",
    "host": "db.example.org",
    "database": "sales",
    "username": "reader",
}


def test_secrets_are_encrypted_write_only_and_never_audited(admin_actor, as_user):
    connection = connections.create_connection(
        admin_actor, name="exports", kind="s3", config=s3_config(), secrets=S3_SECRETS
    )
    stored = Connection.objects.get(pk=connection.pk)
    assert "very-secret-value" not in stored.secret_ciphertext
    assert connections.secrets_of(stored) == S3_SECRETS
    assert stored.secret_fields == ["access_key_id", "secret_access_key"]
    response = as_user(admin_actor.user).get(f"/api/v1/connections/{connection.pk}")
    assert response.status_code == 200
    assert "very-secret-value" not in response.content.decode()
    assert response.json()["secret_fields"] == ["access_key_id", "secret_access_key"]
    for entry in AuditLog.objects.all():
        assert "very-secret-value" not in str(entry.details)
    assert stored.config == {
        "bucket": "exports",
        "endpoint_url": "https://s3.example.org",
        "prefix": "",
        "region": "us-east-1",
        "addressing_style": "path",
    }


@pytest.mark.parametrize(
    "config,message",
    [
        ({}, "s3 connections need config.bucket"),
        ({"bucket": 5}, "config.bucket must be a string"),
        ({"bucket": "Bad_Name"}, "not a valid bucket name"),
        (s3_config(endpoint_url="ftp://x"), "http\\(s\\) URL of the store without a path"),
        (s3_config(endpoint_url="https://x/path"), "without a path"),
        (s3_config(addressing_style="sideways"), "addressing_style must be"),
        (s3_config(prefix="/abs"), "relative key prefix"),
        (s3_config(prefix="a/../b"), "relative key prefix"),
        (s3_config(prefix="a//b"), "relative key prefix"),
    ],
)
def test_s3_config_validation(admin_actor, config, message):
    with pytest.raises(InvalidRequest, match=message):
        connections.create_connection(
            admin_actor, name="x", kind="s3", config=config, secrets=S3_SECRETS
        )


def test_s3_prefix_and_secret_validation(admin_actor):
    connection = connections.create_connection(
        admin_actor,
        name="x",
        kind="s3",
        config=s3_config(prefix="team/out/"),
        secrets={**S3_SECRETS, "session_token": "tok"},
    )
    assert connection.config["prefix"] == "team/out"
    with pytest.raises(InvalidRequest, match="need the secrets: secret_access_key"):
        connections.create_connection(
            admin_actor, name="y", kind="s3", config=s3_config(), secrets={"access_key_id": "a"}
        )
    with pytest.raises(InvalidRequest, match="no secret named password"):
        connections.create_connection(
            admin_actor,
            name="y",
            kind="s3",
            config=s3_config(),
            secrets={**S3_SECRETS, "password": "p"},
        )
    with pytest.raises(InvalidRequest, match="must be a non-empty string"):
        connections.create_connection(
            admin_actor,
            name="y",
            kind="s3",
            config=s3_config(),
            secrets={**S3_SECRETS, "session_token": ""},
        )


@pytest.mark.parametrize(
    "root,ok",
    [
        ("/data/exports", True),
        ("relative", False),
        ("/data/", False),
        ("/data/../etc", False),
        ("//data", False),
    ],
)
def test_localfs_root_validation(admin_actor, root, ok):
    def create():
        return connections.create_connection(
            admin_actor, name="files", kind="localfs", config={"root_path": root}
        )

    if ok:
        assert create().config == {"root_path": root}
    else:
        with pytest.raises(InvalidRequest, match="absolute, normalised path"):
            create()


@pytest.mark.parametrize(
    "config,message",
    [
        ({**SQL_CONFIG, "dialect": "db2"}, "is not supported"),
        ({**SQL_CONFIG, "port": "5432"}, "config.port must be a TCP port"),
        ({**SQL_CONFIG, "port": 70000}, "config.port must be a TCP port"),
        ({**SQL_CONFIG, "port": True}, "config.port must be a TCP port"),
        ({**SQL_CONFIG, "options": []}, "config.options must be an object"),
        ({**SQL_CONFIG, "options": {"PWD": "x"}}, "may not set 'PWD'"),
        ({**SQL_CONFIG, "options": {"bad;key": "x"}}, "may not set 'bad;key'"),
        ({**SQL_CONFIG, "options": {"sslmode": 1.5}}, "must be a string or an integer"),
        ({**SQL_CONFIG, "options": {"sslmode": False}}, "must be a string or an integer"),
        ({k: v for k, v in SQL_CONFIG.items() if k != "host"}, "sql connections need config.host"),
    ],
)
def test_sql_config_validation(admin_actor, config, message):
    with pytest.raises(InvalidRequest, match=message):
        connections.create_connection(
            admin_actor, name="db", kind="sql", config=config, secrets={"password": "p"}
        )


def test_kind_name_roles_and_uniqueness(admin_actor):
    with pytest.raises(InvalidRequest, match="Unknown connection kind 'ftp'"):
        connections.create_connection(admin_actor, name="x", kind="ftp", config={})
    with pytest.raises(InvalidRequest, match="needs a name"):
        connections.create_connection(
            admin_actor, name=" ", kind="localfs", config={"root_path": "/d"}
        )
    with pytest.raises(InvalidRequest, match="config must be an object"):
        connections.create_connection(admin_actor, name="x", kind="localfs", config=[])
    with pytest.raises(InvalidRequest, match="allowed_roles must be a list"):
        connections.create_connection(
            admin_actor,
            name="x",
            kind="localfs",
            config={"root_path": "/d"},
            allowed_roles=["root"],
        )
    first = connections.create_connection(
        admin_actor,
        name="d",
        kind="localfs",
        config={"root_path": "/d"},
        allowed_roles=["admin", "viewer", "viewer"],
    )
    assert first.allowed_roles == ["viewer", "admin"]
    with pytest.raises(Conflict, match="already exists"):
        connections.create_connection(
            admin_actor, name="d", kind="localfs", config={"root_path": "/e"}
        )


def test_update_keeps_unnamed_secrets_and_audits_names_only(admin_actor):
    connection = connections.create_connection(
        admin_actor, name="db", kind="sql", config=SQL_CONFIG, secrets={"password": "old-pw"}
    )
    updated = connections.update_connection(
        admin_actor,
        connection.pk,
        name="sales db",
        description="Sales replica",
        config={**SQL_CONFIG, "port": 6432},
        allowed_roles=["operator", "author"],
        secrets={"password": "new-pw"},
    )
    assert updated.name == "sales db" and updated.config["port"] == 6432
    assert connections.secrets_of(updated) == {"password": "new-pw"}
    entry = AuditLog.objects.get(action="connection.update")
    assert entry.details["secrets_changed"] == ["password"]
    assert "new-pw" not in str(entry.details) and "old-pw" not in str(entry.details)
    unchanged = connections.update_connection(
        admin_actor, connection.pk, name="sales db", config={**SQL_CONFIG, "port": 6432}
    )
    assert AuditLog.objects.filter(action="connection.update").count() == 1
    assert unchanged.description == "Sales replica"
    with pytest.raises(InvalidRequest, match="need the secrets: password"):
        connections.update_connection(admin_actor, connection.pk, secrets={"password": None})
    with pytest.raises(InvalidRequest, match="needs a name"):
        connections.update_connection(admin_actor, connection.pk, name=" ")
    connections.create_connection(
        admin_actor, name="taken", kind="localfs", config={"root_path": "/t"}
    )
    with pytest.raises(Conflict, match="'taken' already exists"):
        connections.update_connection(admin_actor, connection.pk, name="taken")
    with pytest.raises(NotFound):
        connections.update_connection(admin_actor, "00000000-0000-0000-0000-000000000000")


def test_removing_an_optional_secret(admin_actor):
    connection = connections.create_connection(
        admin_actor,
        name="s",
        kind="s3",
        config=s3_config(),
        secrets={**S3_SECRETS, "session_token": "t"},
    )
    updated = connections.update_connection(
        admin_actor, connection.pk, secrets={"session_token": None}
    )
    assert updated.secret_fields == ["access_key_id", "secret_access_key"]


def test_connections_used_by_datasets_cannot_be_deleted(admin_actor):
    world = World.build()
    Dataset.objects.filter(pk=world.dataset.pk).update(destination_connection=world.connection)
    with pytest.raises(Conflict, match=f"used by the datasets {world.dataset.name}"):
        connections.delete_connection(admin_actor, world.connection.pk)
    assert not AuditLog.objects.filter(action="connection.delete").exists()
    connections.delete_connection(admin_actor, world.spare_connection.pk)
    assert AuditLog.objects.filter(action="connection.delete").count() == 1
    with pytest.raises(NotFound):
        connections.delete_connection(admin_actor, world.spare_connection.pk)


def test_roles_see_and_use_only_allowed_connections():
    world = World.build()
    operator = Actor.for_user(world.operator)
    author = Actor.for_user(world.author)
    assert list(connections.list_connections(operator)) == []
    assert set(connections.list_connections(author)) == {world.connection, world.spare_connection}
    assert list(connections.list_connections(author, kind="sql")) == []
    with pytest.raises(PermissionDenied):
        connections.get_connection(operator, world.connection.pk)
    with pytest.raises(NotFound):
        connections.get_connection(author, "00000000-0000-0000-0000-000000000000")
    with pytest.raises(InvalidRequest, match="does not exist"):
        connections.usable_connection(
            author, "00000000-0000-0000-0000-000000000000", kinds={"s3"}, purpose="source"
        )
    with pytest.raises(InvalidRequest, match="cannot be a dataset source"):
        connections.usable_connection(author, world.connection.pk, kinds={"sql"}, purpose="source")


# --------------------------------------------------------------------------- tests


def test_s3_connection_test_reaches_the_bucket(admin_actor):
    good = s3_connection()
    result = connections.check_connection(admin_actor, good.pk)
    assert result.ok is True and good.config["bucket"] in result.message
    stored = Connection.objects.get(pk=good.pk)
    assert stored.last_test_ok is True and stored.last_tested_at is not None
    wrong = s3_connection(bucket="forklift-web-no-such-bucket")
    failed = connections.check_connection(admin_actor, wrong.pk)
    assert failed.ok is False and "forklift-web-no-such-bucket" in failed.message
    bad_key = connections.create_connection(
        admin_actor,
        name="badkey",
        kind="s3",
        config={"bucket": good.config["bucket"], "endpoint_url": STORE["endpoint_url"]},
        secrets={"access_key_id": "nobody", "secret_access_key": "wrong-secret-xyz"},
    )
    refused = connections.check_connection(admin_actor, bad_key.pk)
    assert refused.ok is False and "wrong-secret-xyz" not in refused.message
    assert AuditLog.objects.filter(action="connection.test").count() == 3


def test_localfs_connection_test(admin_actor, tmp_path):
    seen = connections.create_connection(
        admin_actor, name="seen", kind="localfs", config={"root_path": str(tmp_path)}
    )
    assert connections.check_connection(admin_actor, seen.pk).ok is True
    unseen = connections.create_connection(
        admin_actor, name="unseen", kind="localfs", config={"root_path": str(tmp_path / "missing")}
    )
    result = connections.check_connection(admin_actor, unseen.pk)
    assert result.ok is None and "not visible to the gateway" in result.message


def test_sql_connection_test_checks_reachability(admin_actor):
    db = settings.DATABASES["default"]
    reachable = connections.create_connection(
        admin_actor,
        name="pg",
        kind="sql",
        config={**SQL_CONFIG, "host": db["HOST"], "port": int(db["PORT"])},
        secrets={"password": "p"},
    )
    result = connections.check_connection(admin_actor, reachable.pk)
    assert result.ok is True and "accepts connections" in result.message
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        closed_port = sock.getsockname()[1]
    closed = connections.create_connection(
        admin_actor,
        name="closed",
        kind="sql",
        config={**SQL_CONFIG, "host": "127.0.0.1", "port": closed_port},
        secrets={"password": "p"},
    )
    failed = connections.check_connection(admin_actor, closed.pk)
    assert failed.ok is False and "does not accept connections" in failed.message
    with pytest.raises(NotFound):
        connections.check_connection(admin_actor, "00000000-0000-0000-0000-000000000000")


def test_connection_test_through_the_api(as_user, admin):
    connection = s3_connection()
    response = as_user(admin).post(f"/api/v1/connections/{connection.pk}/test")
    assert response.status_code == 200 and response.json()["ok"] is True


# --------------------------------------------------------------------------- connection strings


@pytest.mark.parametrize(
    "dialect,expected",
    [
        (
            "postgresql",
            "Driver={PostgreSQL Unicode};Server=db;Port=5432;Database=sales;"
            "Uid=reader;Pwd={p;w=d}",
        ),
        (
            "mysql",
            "Driver={MariaDB Unicode};Server=db;Port=3306;Database=sales;Uid=reader;"
            "Pwd={p;w=d}",
        ),
        (
            "sqlserver",
            "Driver={ODBC Driver 18 for SQL Server};Server=db,1433;Database=sales;"
            "Uid=reader;Pwd={p;w=d};Encrypt=yes",
        ),
        ("oracle", "Driver={Oracle 23 ODBC driver};DBQ=db:1521/sales;Uid=reader;Pwd={p;w=d}"),
    ],
)
def test_sql_connection_strings(admin_actor, dialect, expected):
    options = {"Encrypt": "yes"} if dialect == "sqlserver" else {}
    connection = connections.create_connection(
        admin_actor,
        name=dialect,
        kind="sql",
        config={
            "dialect": dialect,
            "host": "db",
            "database": "sales",
            "username": "reader",
            "options": options,
        },
        secrets={"password": "p;w=d"},
    )
    assert connections.sql_connection_string(connection) == expected


def test_odbc_values_cannot_add_attributes(admin_actor):
    connection = connections.create_connection(
        admin_actor,
        name="tricky",
        kind="sql",
        config={
            **SQL_CONFIG,
            "username": " spaced ",
            "driver": "My {Driver}",
            "options": {"ApplicationName": "a}b"},
        },
        secrets={"password": "x}y;Server=evil"},
    )
    text = connections.sql_connection_string(connection)
    assert "Driver={My {Driver}}}" in text
    assert "Pwd={x}}y;Server=evil}" in text and "ApplicationName={a}}b}" in text
    assert "Uid=spaced" in text  # surrounding whitespace is stripped at validation


def test_secret_rotation_and_damaged_ciphertext(admin_actor, monkeypatch):
    from cryptography.fernet import Fernet

    connection = connections.create_connection(
        admin_actor, name="rot", kind="sql", config=SQL_CONFIG, secrets={"password": "pw"}
    )
    new_key = Fernet.generate_key().decode()
    backend = secret_backend.EnvSecretBackend([new_key, *settings.FORKLIFT_SECRETS_KEYS])
    rotated = backend.rotate(connection.secret_ciphertext)
    assert secret_backend.EnvSecretBackend([new_key]).decrypt(rotated) == {"password": "pw"}
    assert backend.rotate("") == "" and backend.encrypt({}) == "" and backend.decrypt("") == {}
    other = secret_backend.EnvSecretBackend([Fernet.generate_key().decode()])
    with pytest.raises(secret_backend.SecretError, match="could not be decrypted"):
        other.decrypt(connection.secret_ciphertext)
    with pytest.raises(secret_backend.SecretError, match="cannot be rotated"):
        other.rotate(connection.secret_ciphertext)


def test_secret_backend_configuration(settings):
    from django.core.exceptions import ImproperlyConfigured

    with pytest.raises(ImproperlyConfigured, match="at least one Fernet key"):
        secret_backend.EnvSecretBackend([])
    with pytest.raises(ImproperlyConfigured, match="not a Fernet key"):
        secret_backend.EnvSecretBackend(["not-a-key"])
    secret_backend.backend.cache_clear()
    settings.FORKLIFT_SECRET_BACKEND = "vault"
    try:
        with pytest.raises(ImproperlyConfigured, match="'vault' is not supported"):
            secret_backend.backend()
    finally:
        secret_backend.backend.cache_clear()


def test_operator_cannot_manage_connections():
    operator = Actor.for_user(make_user(Role.OPERATOR))
    with pytest.raises(PermissionDenied):
        connections.create_connection(
            operator, name="x", kind="localfs", config={"root_path": "/x"}
        )


def test_aws_s3_needs_no_endpoint_and_unchanged_roles_are_not_recorded(admin_actor):
    aws = connections.create_connection(
        admin_actor, name="aws", kind="s3", config={"bucket": "my-exports"}, secrets=S3_SECRETS
    )
    assert aws.config["endpoint_url"] == ""
    assert connections.bucket_of(aws).host(storage.Audience.WORKER) is None
    connections.update_connection(admin_actor, aws.pk, allowed_roles=["author", "admin"])
    assert not AuditLog.objects.filter(action="connection.update").exists()
