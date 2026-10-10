"""Clients and helpers for the service tests (the fixtures in conftest.py build on these).

Connection settings default to the values in ``compose.yaml`` and can be overridden with
``FORKLIFT_TEST_S3_ENDPOINT``, ``FORKLIFT_TEST_S3_ACCESS_KEY``, ``FORKLIFT_TEST_S3_SECRET_KEY``,
``FORKLIFT_TEST_PG_{HOST,PORT,USER,PASSWORD,DATABASE,DRIVER}`` and
``FORKLIFT_TEST_MYSQL_{HOST,PORT,USER,PASSWORD,DATABASE,DRIVER}``.
"""

from __future__ import annotations

import json
import os
import secrets
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

ENABLED = os.environ.get("FORKLIFT_TEST_SERVICES") == "1"
SCHEMA_ID = "https://github.com/cornyhorse/forklift/schema-standards/integration-test.json"


def setting(name: str, default: str) -> str:
    return os.environ.get(f"FORKLIFT_TEST_{name}", default)


# --------------------------------------------------------------------------- object store


@dataclass
class ObjectStore:
    """An S3-compatible store (RustFS) with its root credentials."""

    endpoint: str
    access_key: str
    secret_key: str
    region: str = "us-east-1"

    @classmethod
    def from_environment(cls) -> "ObjectStore":
        return cls(
            endpoint=setting("S3_ENDPOINT", "http://127.0.0.1:19000"),
            access_key=setting("S3_ACCESS_KEY", "forklift-test"),
            secret_key=setting("S3_SECRET_KEY", "forklift-test-secret"),
        )

    @property
    def root_credentials(self) -> Dict[str, str]:
        return {"aws_access_key_id": self.access_key, "aws_secret_access_key": self.secret_key}

    def client(self, credentials: Optional[Dict[str, str]] = None):
        """A boto3 S3 client; root credentials unless others are given."""
        import boto3
        from botocore.config import Config

        return boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            region_name=self.region,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
            **(credentials or self.root_credentials),
        )

    def streaming_client(self, credentials: Optional[Dict[str, str]] = None):
        """Forklift's own S3 client, as a caller passes it with ``s3_client=``."""
        from forklift.io import S3StreamingClient

        return S3StreamingClient(
            endpoint_url=self.endpoint,
            region_name=self.region,
            **(credentials or self.root_credentials),
        )

    def scoped_credentials(self, *statements: Dict[str, Any]) -> Dict[str, str]:
        """Temporary credentials limited to ``statements`` (an STS session policy).

        The session may do only what both the root user and the policy allow, which is how the
        tests create readers and writers with exactly the permissions they need.
        """
        import boto3

        sts = boto3.client(
            "sts", endpoint_url=self.endpoint, region_name=self.region, **self.root_credentials
        )
        response = sts.assume_role(
            RoleArn="arn:aws:iam::000000000000:role/forklift-integration-test",
            RoleSessionName=f"forklift-it-{uuid.uuid4().hex[:8]}",
            Policy=json.dumps({"Version": "2012-10-17", "Statement": list(statements)}),
            DurationSeconds=900,
        )
        credentials = response["Credentials"]
        return {
            "aws_access_key_id": credentials["AccessKeyId"],
            "aws_secret_access_key": credentials["SecretAccessKey"],
            "aws_session_token": credentials["SessionToken"],
        }

    def keys(self, bucket: str, prefix: str = "") -> List[str]:
        """Every object key under ``prefix`` (read with root credentials)."""
        paginator = self.client().get_paginator("list_objects_v2")
        return sorted(
            item["Key"]
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
            for item in page.get("Contents", [])
        )

    def read(self, bucket: str, key: str) -> bytes:
        return self.client().get_object(Bucket=bucket, Key=key)["Body"].read()

    def put(self, bucket: str, key: str, body) -> None:
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.client().put_object(Bucket=bucket, Key=key, Body=body)

    def unfinished_uploads(self, bucket: str) -> List[str]:
        uploads = self.client().list_multipart_uploads(Bucket=bucket).get("Uploads", [])
        return sorted(upload["Key"] for upload in uploads)

    def empty_and_delete(self, bucket: str) -> None:
        client = self.client()
        for upload in client.list_multipart_uploads(Bucket=bucket).get("Uploads", []):
            client.abort_multipart_upload(
                Bucket=bucket, Key=upload["Key"], UploadId=upload["UploadId"]
            )
        for key in self.keys(bucket):
            client.delete_object(Bucket=bucket, Key=key)
        client.delete_bucket(Bucket=bucket)


def allow(actions: Sequence[str], *resources: str) -> Dict[str, Any]:
    """One ``Allow`` statement for a session policy (``resources`` as ``bucket/key-pattern``)."""
    return {
        "Effect": "Allow",
        "Action": list(actions),
        "Resource": [f"arn:aws:s3:::{resource}" for resource in resources],
    }


# --------------------------------------------------------------------------- databases


@dataclass
class Login:
    user: str
    password: str


def _new_login() -> Login:
    return Login(f"fl_{uuid.uuid4().hex[:10]}", secrets.token_urlsafe(16))


def find_driver(setting_name: str, preferences: Iterable[str]) -> Optional[str]:
    """The ODBC driver to use: ``FORKLIFT_TEST_<setting>_DRIVER`` or the first installed match."""
    configured = os.environ.get(f"FORKLIFT_TEST_{setting_name}_DRIVER")
    if configured:
        return configured
    import pyodbc

    installed = pyodbc.drivers()
    for preference in preferences:
        for name in installed:
            if preference.lower() in name.lower():
                return name
    return None


class Database:
    """A database server reached over ODBC, with an admin login that creates what tests need.

    ``namespace`` is where a test's tables live: a schema on PostgreSQL, a database on MySQL
    (MySQL calls databases schemas, and forklift's ``x-sql`` ``select.schema`` names one).
    Logins and the namespace are created per test and dropped afterwards.
    """

    kind = ""

    def __init__(self, host: str, port: str, user: str, password: str, database: str, driver):
        self.host, self.port, self.database = host, port, database
        self.admin_user, self.admin_password = user, password
        self.driver = driver
        self.namespace = f"forklift_it_{uuid.uuid4().hex[:10]}"
        self.logins: List[Login] = []

    def connection_string(self, user: str, password: str, database: Optional[str] = None) -> str:
        return (
            f"Driver={{{self.driver}}};Server={self.host};Port={self.port};"
            f"Database={database or self.database};Uid={user};Pwd={password}"
        )

    def login_connection_string(self, login: Login) -> str:
        return self.connection_string(login.user, login.password)

    def admin(self, *statements: str) -> List[Any]:
        """Run statements as the admin (autocommit); returns the last statement's rows."""
        import pyodbc

        connection = pyodbc.connect(
            self.connection_string(self.admin_user, self.admin_password), autocommit=True
        )
        try:
            cursor = connection.cursor()
            rows: List[Any] = []
            for statement in statements:
                cursor.execute(statement)
                rows = [tuple(row) for row in cursor.fetchall()] if cursor.description else []
            return rows
        finally:
            connection.close()

    def table(self, name: str) -> str:
        return f"{self.namespace}.{name}"


class Postgres(Database):
    kind = "postgres"

    @classmethod
    def from_environment(cls) -> "Postgres":
        return cls(
            setting("PG_HOST", "127.0.0.1"),
            setting("PG_PORT", "15432"),
            setting("PG_USER", "forklift_admin"),
            setting("PG_PASSWORD", "forklift-admin-secret"),
            setting("PG_DATABASE", "forklift_test"),
            find_driver("PG", ["PostgreSQL Unicode", "PostgreSQL"]),
        )

    def create_namespace(self) -> None:
        self.admin(f"CREATE SCHEMA {self.namespace}")

    def drop_everything(self) -> None:
        statements = [f"DROP SCHEMA IF EXISTS {self.namespace} CASCADE"]
        for login in self.logins:
            statements += [f"DROP OWNED BY {login.user}", f"DROP ROLE IF EXISTS {login.user}"]
        self.admin(*statements)

    def create_user(self, schema_usage: bool = True) -> Login:
        login = _new_login()
        self.logins.append(login)
        statements = [f"CREATE ROLE {login.user} LOGIN PASSWORD '{login.password}'"]
        if schema_usage:
            statements.append(f"GRANT USAGE ON SCHEMA {self.namespace} TO {login.user}")
        self.admin(*statements)
        return login

    def grant_select(self, login: Login, table: str, columns: Sequence[str] = ()) -> None:
        target = f"({', '.join(columns)})" if columns else ""
        self.admin(f"GRANT SELECT {target} ON {self.table(table)} TO {login.user}")

    def grant_write(self, login: Login, table: str) -> None:
        self.admin(f"GRANT SELECT, INSERT, UPDATE, DELETE ON {self.table(table)} TO {login.user}")


class MySql(Database):
    kind = "mysql"

    @classmethod
    def from_environment(cls) -> "MySql":
        return cls(
            setting("MYSQL_HOST", "127.0.0.1"),
            setting("MYSQL_PORT", "13306"),
            setting("MYSQL_USER", "root"),
            setting("MYSQL_PASSWORD", "forklift-admin-secret"),
            setting("MYSQL_DATABASE", "forklift_test"),
            find_driver("MYSQL", ["MySQL ODBC 9", "MySQL ODBC 8", "MariaDB Unicode", "MariaDB"]),
        )

    def login_connection_string(self, login: Login) -> str:
        # A MySQL user may connect only to a database it holds privileges in.
        return self.connection_string(login.user, login.password, database=self.namespace)

    def create_namespace(self) -> None:
        self.admin(f"CREATE DATABASE {self.namespace}")

    def drop_everything(self) -> None:
        statements = [f"DROP DATABASE IF EXISTS {self.namespace}"]
        statements += [f"DROP USER IF EXISTS '{login.user}'@'%'" for login in self.logins]
        self.admin(*statements)

    def create_user(self) -> Login:
        login = _new_login()
        self.logins.append(login)
        self.admin(f"CREATE USER '{login.user}'@'%' IDENTIFIED BY '{login.password}'")
        return login

    def grant_select(self, login: Login, table: str, columns: Sequence[str] = ()) -> None:
        target = f"({', '.join(columns)})" if columns else ""
        self.admin(f"GRANT SELECT {target} ON {self.table(table)} TO '{login.user}'@'%'")

    def grant_write(self, login: Login, table: str) -> None:
        self.admin(
            f"GRANT SELECT, INSERT, UPDATE, DELETE ON {self.table(table)} TO '{login.user}'@'%'"
        )


def sql_schema_file(directory: Path, namespace: str, tables: Sequence[str]) -> Path:
    """A forklift SQL schema file that imports ``tables`` from ``namespace``."""
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": SCHEMA_ID,
        "title": "Integration test tables",
        "type": "object",
        "x-sql": {
            "tables": [
                {"select": {"schema": namespace, "name": table}, "outputName": table}
                for table in tables
            ]
        },
    }
    path = directory / "sql-schema.json"
    path.write_text(json.dumps(schema), encoding="utf-8")
    return path
