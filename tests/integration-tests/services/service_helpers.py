"""Clients and helpers for the service tests (the fixtures in conftest.py build on these).

Connection settings default to the values in ``compose.yaml`` and can be overridden with
``FORKLIFT_TEST_S3_ENDPOINT``, ``FORKLIFT_TEST_S3_ACCESS_KEY``, ``FORKLIFT_TEST_S3_SECRET_KEY``
and ``FORKLIFT_TEST_<DB>_{HOST,PORT,USER,PASSWORD,DATABASE,DRIVER}`` for ``<DB>`` = ``PG``,
``MYSQL``, ``MSSQL`` and ``ORACLE`` (Oracle's ``DATABASE`` is the service name, ``FREEPDB1``).
"""

from __future__ import annotations

import json
import os
import secrets
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

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
    # The suffix satisfies password-complexity rules (upper and lower case, digit, symbol)
    return Login(f"fl_{uuid.uuid4().hex[:10]}", secrets.token_urlsafe(16) + "-Aa1")


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


def _disable_odbc_pooling() -> None:
    # A pooled connection keeps its session open after close(), and SQL Server and Oracle refuse
    # to drop a login that is still connected. The setting only counts before the first connect.
    try:
        import pyodbc
    except ImportError:  # the S3 tests run without pyodbc
        return
    pyodbc.pooling = False


_disable_odbc_pooling()

# The privileges ``Database.grant`` knows; each database grants them on a table
TABLE_PRIVILEGES = ("SELECT", "INSERT", "UPDATE", "DELETE")


class Database:
    """A database server reached over ODBC, with an admin login that creates what tests need.

    ``namespace`` is where a test's tables live: a schema on PostgreSQL and SQL Server, a
    database on MySQL (MySQL calls databases schemas, and forklift's ``x-sql`` ``select.schema``
    names one), a user on Oracle (each Oracle user is a schema). Logins and the namespace are
    created per test and dropped afterwards, with everything in them.

    The same calls work on every database:

    * ``create_user()`` makes a login that may connect and nothing else (on PostgreSQL it also
      gets ``USAGE`` on the namespace unless ``schema_usage=False``);
    * ``grant(login, table, "SELECT", "INSERT", ..., columns=())`` grants table privileges
      (``grant_select`` and ``grant_write`` are shorthands);
    * ``owner_login()`` makes a login that may create tables in the namespace, and write to and
      drop the tables it creates (see its docstring for what else it may do per database);
    * ``table_exists``, ``table_names``, ``column_names``, ``row_count`` and ``rows`` look at
      the namespace as the admin, matching table names the way the database does.
    """

    kind = ""
    #: The ``FORKLIFT_TEST_<prefix>_*`` settings of this database
    setting_prefix = ""
    #: Whether a login can be granted SELECT on some columns of a table only
    supports_column_grants = True
    #: How the database refuses a write in a read-only session (SQLSTATE, driver code), or
    #: None when forklift cannot make its sessions read-only
    read_only_refusal: Optional[Tuple[Optional[str], Optional[int]]] = ("25006", None)
    quote_char = '"'

    def __init__(self, host: str, port: str, user: str, password: str, database: str, driver):
        self.host, self.port, self.database = host, port, database
        self.admin_user, self.admin_password = user, password
        self.driver = driver
        self.namespace = f"forklift_it_{uuid.uuid4().hex[:10]}"
        self.logins: List[Login] = []

    # ------------------------------------------------------------------ connections

    def connection_string(self, user: str, password: str, database: Optional[str] = None) -> str:
        return (
            f"Driver={{{self.driver}}};Server={self.host};Port={self.port};"
            f"Database={database or self.database};Uid={user};Pwd={password}"
        )

    def login_connection_string(self, login: Login) -> str:
        return self.connection_string(login.user, login.password)

    def admin(self, *statements: str) -> List[Any]:
        """Run statements as the admin (autocommit); returns the last statement's rows."""
        return self._run(self.connection_string(self.admin_user, self.admin_password), statements)

    def query(self, statement: str, *parameters: Any) -> List[Tuple]:
        """Run one parameterised query as the admin and return its rows."""
        import pyodbc

        connection = pyodbc.connect(
            self.connection_string(self.admin_user, self.admin_password), autocommit=True
        )
        try:
            cursor = connection.cursor()
            cursor.execute(statement, *parameters)
            return [tuple(row) for row in cursor.fetchall()] if cursor.description else []
        finally:
            connection.close()

    @staticmethod
    def _run(connection_string: str, statements: Sequence[str]) -> List[Any]:
        import pyodbc

        connection = pyodbc.connect(connection_string, autocommit=True)
        try:
            cursor = connection.cursor()
            rows: List[Any] = []
            for statement in statements:
                cursor.execute(statement)
                rows = [tuple(row) for row in cursor.fetchall()] if cursor.description else []
            return rows
        finally:
            connection.close()

    def ping(self) -> None:
        """Fail unless the server answers (and has the test database)."""
        self.admin("SELECT 1")

    # ------------------------------------------------------------------ names

    def table(self, name: str) -> str:
        """``namespace.name`` as written in the tests' own SQL (unquoted)."""
        return f"{self.namespace}.{name}"

    def quote(self, identifier: str) -> str:
        quote = self.quote_char
        return f"{quote}{identifier.replace(quote, quote * 2)}{quote}"

    @property
    def catalog_namespace(self) -> str:
        """The namespace as the catalog spells it."""
        return self.namespace

    # ------------------------------------------------------------------ logins and grants

    def create_user(self) -> Login:
        raise NotImplementedError

    def owner_login(self) -> Login:
        raise NotImplementedError

    def grantee(self, login: Login) -> str:
        return login.user

    def grant(self, login: Login, table: str, *privileges: str, columns: Sequence[str] = ()):
        """Grant ``privileges`` (``SELECT``, ``INSERT``, ``UPDATE``, ``DELETE``) on ``table``.

        With ``columns`` each privilege covers only those columns (not on Oracle, which grants
        SELECT on whole tables only: see ``supports_column_grants``).
        """
        unknown = [p for p in privileges if p not in TABLE_PRIVILEGES]
        if unknown or not privileges:
            raise ValueError(f"grant() takes some of {TABLE_PRIVILEGES}, not {privileges}")
        target = f" ({', '.join(columns)})" if columns else ""
        granted = ", ".join(f"{privilege}{target}" for privilege in privileges)
        self.admin(f"GRANT {granted} ON {self.table(table)} TO {self.grantee(login)}")

    def grant_select(self, login: Login, table: str, columns: Sequence[str] = ()) -> None:
        self.grant(login, table, "SELECT", columns=columns)

    def grant_write(self, login: Login, table: str) -> None:
        self.grant(login, table, *TABLE_PRIVILEGES)

    # ------------------------------------------------------------------ inspection

    def table_names(self) -> List[str]:
        """The tables and views in the namespace, spelled as the catalog spells them."""
        rows = self.query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
            self.catalog_namespace,
        )
        return sorted(row[0] for row in rows)

    def catalog_name(self, name: str) -> str:
        """The catalog's spelling of table ``name`` (exact, else the one that differs in case).

        Raises:
            LookupError: If no table of the namespace has that name
        """
        names = self.table_names()
        if name in names:
            return name
        matches = [n for n in names if n.casefold() == name.casefold()]
        if len(matches) != 1:
            raise LookupError(f"{name!r} is not one table of {self.namespace}: {names}")
        return matches[0]

    def table_exists(self, name: str) -> bool:
        try:
            self.catalog_name(name)
        except LookupError:
            return False
        return True

    def qualified(self, name: str) -> str:
        """The table's quoted ``namespace.table``, as the catalog spells both."""
        return f"{self.quote(self.catalog_namespace)}.{self.quote(self.catalog_name(name))}"

    def column_names(self, name: str) -> List[str]:
        """The table's columns in order, spelled as the catalog spells them."""
        rows = self.query(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            self.catalog_namespace,
            self.catalog_name(name),
        )
        return [row[0] for row in rows]

    def row_count(self, name: str) -> int:
        return int(self.admin(f"SELECT COUNT(*) FROM {self.qualified(name)}")[0][0])

    def rows(self, name: str, order_by: Optional[str] = None) -> List[Tuple]:
        """Every row of the table, ordered by ``order_by`` (default: its first column)."""
        order = self.quote(order_by) if order_by else "1"
        return self.admin(f"SELECT * FROM {self.qualified(name)} ORDER BY {order}")

    # ------------------------------------------------------------------ test objects

    def create_slow_view(self, name: str, seconds: int = 5) -> None:
        """A view whose query runs for about ``seconds`` seconds (for timeout tests)."""
        raise NotImplementedError

    def is_read_only_refusal(self, error: BaseException) -> bool:
        """True when ``error`` is the database refusing a write in a read-only session."""
        from forklift.inputs.sql.errors import database_error_codes

        if self.read_only_refusal is None:
            return False
        state, native = database_error_codes(error)
        expected_state, expected_native = self.read_only_refusal
        return (expected_state is None or state == expected_state) and (
            expected_native is None or native == expected_native
        )


class Postgres(Database):
    kind = "postgres"
    setting_prefix = "PG"

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

    def owner_login(self) -> Login:
        """A login that owns the namespace (schema): it may create tables in it and drop any.

        It owns the tables it creates; on tables the admin created it needs grants to read or
        write them, like any other login.
        """
        login = self.create_user()
        self.admin(f"ALTER SCHEMA {self.namespace} OWNER TO {login.user}")
        return login

    def create_slow_view(self, name: str, seconds: int = 5) -> None:
        self.admin(f"CREATE VIEW {self.table(name)} AS SELECT 1 AS x FROM pg_sleep({seconds})")


class MySql(Database):
    kind = "mysql"
    setting_prefix = "MYSQL"
    quote_char = "`"

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

    def grantee(self, login: Login) -> str:
        return f"'{login.user}'@'%'"

    def create_user(self) -> Login:
        login = _new_login()
        self.logins.append(login)
        self.admin(f"CREATE USER '{login.user}'@'%' IDENTIFIED BY '{login.password}'")
        return login

    def owner_login(self) -> Login:
        """A login with every privilege on the namespace (database): it may create tables in it.

        MySQL has no table owners, so it may also read, write and drop every table in it.
        """
        login = self.create_user()
        self.admin(f"GRANT ALL PRIVILEGES ON {self.namespace}.* TO {self.grantee(login)}")
        return login

    def create_slow_view(self, name: str, seconds: int = 5) -> None:
        self.admin(f"CREATE VIEW {self.table(name)} AS SELECT SLEEP({seconds}) AS x")


class MsSql(Database):
    """SQL Server: the namespace is a schema in the database ``forklift_test`` (created on use).

    Logins are server logins with a user of the same name in that database. SQL Server has no
    read-only session a login could set, so forklift warns instead (``read_only_refusal`` is
    None).
    """

    kind = "mssql"
    setting_prefix = "MSSQL"
    read_only_refusal = None

    @classmethod
    def from_environment(cls) -> "MsSql":
        return cls(
            setting("MSSQL_HOST", "127.0.0.1"),
            setting("MSSQL_PORT", "11433"),
            setting("MSSQL_USER", "sa"),
            setting("MSSQL_PASSWORD", "Forklift-Admin-Secret-1"),
            setting("MSSQL_DATABASE", "forklift_test"),
            find_driver(
                "MSSQL", ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]
            ),
        )

    def connection_string(self, user: str, password: str, database: Optional[str] = None) -> str:
        # The test server's certificate is self-signed
        return (
            f"Driver={{{self.driver}}};Server={self.host},{self.port};"
            f"Database={database or self.database};Uid={user};Pwd={password};"
            "Encrypt=yes;TrustServerCertificate=yes"
        )

    def master(self, *statements: str) -> List[Any]:
        """Run statements in the server's ``master`` database (logins, the test database)."""
        return self._run(
            self.connection_string(self.admin_user, self.admin_password, database="master"),
            statements,
        )

    def ping(self) -> None:
        self.master("SELECT 1")
        try:
            self.master(f"IF DB_ID(N'{self.database}') IS NULL CREATE DATABASE [{self.database}]")
        except Exception:
            # Another test process created it at the same moment
            if not self.master(f"SELECT DB_ID(N'{self.database}')")[0][0]:
                raise

    def create_namespace(self) -> None:
        self.admin(f"CREATE SCHEMA {self.namespace}")

    def _kill_sessions(self, login: Login) -> None:
        for (session,) in self.master(
            f"SELECT session_id FROM sys.dm_exec_sessions WHERE login_name = N'{login.user}'"
        ):
            self.master(f"KILL {int(session)}")

    def _schema_objects(self) -> List[Tuple[str, str]]:
        return self.query(
            "SELECT o.name, o.type FROM sys.objects o WHERE o.schema_id = SCHEMA_ID(?) "
            "AND o.parent_object_id = 0",
            self.namespace,
        )

    def _drop_schema_objects(self) -> None:
        """Drop everything in the namespace; SQL Server has no DROP SCHEMA ... CASCADE."""
        kinds = {
            "U": "TABLE",
            "V": "VIEW",
            "P": "PROCEDURE",
            "FN": "FUNCTION",
            "IF": "FUNCTION",
            "TF": "FUNCTION",
            "SO": "SEQUENCE",
            "SN": "SYNONYM",
        }
        for name, table in self.query(
            "SELECT fk.name, OBJECT_NAME(fk.parent_object_id) FROM sys.foreign_keys fk "
            "WHERE fk.schema_id = SCHEMA_ID(?)",
            self.namespace,
        ):
            self.admin(
                f"ALTER TABLE {self.table(self.quote(table))} DROP CONSTRAINT {self.quote(name)}"
            )
        # Views can depend on views and tables: drop what can be dropped until nothing is left
        for _ in range(10):
            remaining = [
                (n, kinds[t.strip()]) for n, t in self._schema_objects() if t.strip() in kinds
            ]
            if not remaining:
                return
            for name, kind in sorted(remaining, key=lambda item: item[1] == "TABLE"):
                try:
                    self.admin(f"DROP {kind} {self.namespace}.{self.quote(name)}")
                except Exception:
                    pass  # another object still depends on it; the next round drops it

    def drop_everything(self) -> None:
        for login in self.logins:
            self._kill_sessions(login)
        if self.admin(f"SELECT SCHEMA_ID(N'{self.namespace}')")[0][0] is not None:
            self._drop_schema_objects()
            self.admin(f"DROP SCHEMA {self.namespace}")
        for login in self.logins:
            self.admin(f"IF USER_ID(N'{login.user}') IS NOT NULL DROP USER [{login.user}]")
            self.master(f"IF SUSER_ID(N'{login.user}') IS NOT NULL DROP LOGIN [{login.user}]")

    def grantee(self, login: Login) -> str:
        return f"[{login.user}]"

    def create_user(self) -> Login:
        login = _new_login()
        self.logins.append(login)
        self.master(
            f"CREATE LOGIN [{login.user}] WITH PASSWORD = N'{login.password}', "
            f"CHECK_POLICY = OFF, DEFAULT_DATABASE = [{self.database}]"
        )
        self.admin(f"CREATE USER [{login.user}] FOR LOGIN [{login.user}]")
        return login

    def owner_login(self) -> Login:
        """A login that owns the namespace (schema) and may create tables and views.

        A schema's owner owns the objects in it, so it may also read, write and drop the tables
        the admin creates there.
        """
        login = self.create_user()
        self.admin(
            f"ALTER AUTHORIZATION ON SCHEMA::{self.namespace} TO [{login.user}]",
            f"GRANT CREATE TABLE, CREATE VIEW TO [{login.user}]",
        )
        return login

    def create_slow_view(self, name: str, seconds: int = 5) -> None:
        # A view cannot WAITFOR; summing a three-way cross join of the catalog takes minutes
        # (SQL Server would compute a plain COUNT(*) from the row counts).
        self.admin(
            f"CREATE VIEW {self.table(name)} AS SELECT SUM(CAST(a.object_id AS BIGINT) % 7 "
            "+ b.object_id % 5 + c.object_id % 3) AS x FROM sys.all_objects a "
            "CROSS JOIN sys.all_objects b CROSS JOIN sys.all_objects c"
        )


class Oracle(Database):
    """Oracle: the namespace is a schema-only user (no password) in the pluggable database.

    Unquoted names are stored in upper case (``orders`` becomes ``ORDERS``); forklift finds
    them case-insensitively and reports the catalog's spelling. Oracle grants SELECT on whole
    tables only, and makes only transactions read-only (ORA-01456 refuses a write in one).
    """

    kind = "oracle"
    setting_prefix = "ORACLE"
    supports_column_grants = False
    read_only_refusal = (None, 1456)  # ORA-01456: may not perform ... inside a READ ONLY txn

    @classmethod
    def from_environment(cls) -> "Oracle":
        return cls(
            setting("ORACLE_HOST", "127.0.0.1"),
            setting("ORACLE_PORT", "11521"),
            setting("ORACLE_USER", "system"),
            setting("ORACLE_PASSWORD", "forklift-admin-secret"),
            setting("ORACLE_DATABASE", "FREEPDB1"),
            find_driver("ORACLE", ["Oracle 23", "Oracle 21", "Oracle 19", "Oracle"]),
        )

    def connection_string(self, user: str, password: str, database: Optional[str] = None) -> str:
        return (
            f"Driver={{{self.driver}}};DBQ={self.host}:{self.port}/{database or self.database};"
            f"Uid={user};Pwd={password}"
        )

    def ping(self) -> None:
        self.admin("SELECT 1 FROM dual")

    @property
    def catalog_namespace(self) -> str:
        return self.namespace.upper()

    def create_namespace(self) -> None:
        self.admin(
            f"CREATE USER {self.namespace} NO AUTHENTICATION "
            "DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS"
        )

    def _drop_user(self, user: str) -> None:
        """Drop a user and its objects, ending its sessions first (Oracle refuses otherwise)."""
        for _ in range(20):
            for sid, serial in self.query(
                "SELECT sid, serial# FROM v$session WHERE username = ?", user.upper()
            ):
                try:
                    self.admin(f"ALTER SYSTEM KILL SESSION '{int(sid)},{int(serial)}' IMMEDIATE")
                except Exception:
                    pass  # the session ended by itself in the meantime
            if not self.query("SELECT 1 FROM all_users WHERE username = ?", user.upper()):
                return
            try:
                self.admin(f"DROP USER {user} CASCADE")
                return
            except Exception:  # ORA-01940: still connected while the killed session ends
                time.sleep(0.5)
        self.admin(f"DROP USER {user} CASCADE")

    def drop_everything(self) -> None:
        for login in self.logins:
            if login.user != self.namespace:
                self._drop_user(login.user)
        self._drop_user(self.namespace)

    def create_user(self) -> Login:
        login = _new_login()
        self.logins.append(login)
        self.admin(
            f'CREATE USER {login.user} IDENTIFIED BY "{login.password}"',
            f"GRANT CREATE SESSION TO {login.user}",
        )
        return login

    def owner_login(self) -> Login:
        """The namespace's own user, given a password: only it may create tables in its schema.

        It owns every table in the namespace (the admin's too), so it may read, write and drop
        them all. (Another login could only do this with system-wide ``ANY`` privileges.)
        """
        login = Login(self.namespace, _new_login().password)
        self.logins.append(login)
        self.admin(
            f'ALTER USER {self.namespace} IDENTIFIED BY "{login.password}"',
            f"GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE TO "
            f"{self.namespace}",
        )
        return login

    def table_names(self) -> List[str]:
        rows = self.query(
            "SELECT table_name FROM all_tables WHERE owner = ? "
            "UNION ALL SELECT view_name FROM all_views WHERE owner = ?",
            self.catalog_namespace,
            self.catalog_namespace,
        )
        return sorted(row[0] for row in rows)

    def column_names(self, name: str) -> List[str]:
        rows = self.query(
            "SELECT column_name FROM all_tab_columns WHERE owner = ? AND table_name = ? "
            "ORDER BY column_id",
            self.catalog_namespace,
            self.catalog_name(name),
        )
        return [row[0] for row in rows]

    def create_slow_view(self, name: str, seconds: int = 5) -> None:
        function = f"{self.namespace}.{name}_sleep"
        self.admin(
            f"CREATE FUNCTION {function} RETURN NUMBER AS BEGIN "
            f"DBMS_SESSION.SLEEP({seconds}); RETURN 1; END;",
            f"CREATE VIEW {self.table(name)} AS SELECT {function}() AS x FROM dual",
        )


DATABASES = {"postgres": Postgres, "mysql": MySql, "mssql": MsSql, "oracle": Oracle}


def sql_schema_file(
    directory: Path,
    namespace: str,
    tables: Sequence[str],
    columns: Optional[Mapping[str, Sequence[str]]] = None,
) -> Path:
    """A forklift SQL schema file that imports ``tables`` from ``namespace``.

    ``columns`` maps a table to the columns its entry declares in ``select.columns``.
    """
    entries = []
    for table in tables:
        select: Dict[str, Any] = {"schema": namespace, "name": table}
        if columns and table in columns:
            select["columns"] = list(columns[table])
        entries.append({"select": select, "outputName": table})
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": SCHEMA_ID,
        "title": "Integration test tables",
        "type": "object",
        "x-sql": {"tables": entries},
    }
    path = directory / "sql-schema.json"
    path.write_text(json.dumps(schema), encoding="utf-8")
    return path
