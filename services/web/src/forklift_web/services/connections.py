"""Connections: S3-compatible buckets, mounted directories and SQL databases.

Admins create, change, test and delete connections; the roles a connection allows can see it
and use it in datasets. Secrets are write-only: they are encrypted by the secret backend on the
way in and are only ever decrypted to sign URLs, to test the connection or to give one SQL job
its connection string at lease time.
"""

from __future__ import annotations

import os
import posixpath
import re
import socket
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlsplit

from django.db import IntegrityError, transaction
from django.db.models import ProtectedError, Q
from django.utils import timezone

from forklift_web import secret_backend, storage
from forklift_web.core.choices import ConnectionKind, Role, SqlDialect
from forklift_web.core.models import Connection, Dataset
from forklift_web.errors import Conflict, InvalidRequest, NotFound, StoreUnavailable
from forklift_web.policy import Action, Actor, check, visible_connections
from forklift_web.services import audit

_BUCKET = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
_ODBC_KEY = re.compile(r"^[A-Za-z][A-Za-z0-9_ ]{0,63}$")
# Attributes the connection string builder sets itself; options may not override them.
_RESERVED_ODBC_KEYS = {
    "driver",
    "server",
    "port",
    "database",
    "dbq",
    "uid",
    "pwd",
    "user",
    "password",
    "dsn",
    "filedsn",
    "savefile",
}

SECRET_FIELDS = {
    ConnectionKind.S3: {
        "required": {"access_key_id", "secret_access_key"},
        "optional": {"session_token"},
    },
    ConnectionKind.LOCALFS: {"required": set(), "optional": set()},
    ConnectionKind.SQL: {"required": {"password"}, "optional": set()},
}


@dataclass(frozen=True)
class Dialect:
    driver: str
    port: int


DIALECTS = {
    SqlDialect.POSTGRESQL: Dialect("PostgreSQL Unicode", 5432),
    SqlDialect.MYSQL: Dialect("MariaDB Unicode", 3306),
    SqlDialect.SQLSERVER: Dialect("ODBC Driver 18 for SQL Server", 1433),
    SqlDialect.ORACLE: Dialect("Oracle 23 ODBC driver", 1521),
}


@dataclass(frozen=True)
class ConnectionCheck:
    """The outcome of a connection test: ok is None when the gateway cannot tell."""

    ok: Optional[bool]
    message: str


# --------------------------------------------------------------------------- validation


def _string(config: dict, key: str, *, required: bool, kind: str) -> str:
    value = config.get(key, "")
    if not isinstance(value, str):
        raise InvalidRequest(f"{kind} connections: config.{key} must be a string.")
    if required and not value.strip():
        raise InvalidRequest(f"{kind} connections need config.{key}.")
    return value.strip()


def relative_prefix(value: str, what: str) -> str:
    """A relative object-key prefix without '..' or empty segments ('' for none)."""
    if not value:
        return ""
    stripped = value.strip("/")
    parts = stripped.split("/")
    if value.startswith("/") or any(part in {"", ".", ".."} for part in parts):
        raise InvalidRequest(
            f"{what} must be a relative key prefix such as 'team/exports' (no leading '/', "
            "no empty, '.' or '..' segments)."
        )
    return stripped


def _validate_s3(config: dict) -> dict:
    bucket = _string(config, "bucket", required=True, kind="s3")
    if not _BUCKET.match(bucket):
        raise InvalidRequest(
            f"config.bucket {bucket!r} is not a valid bucket name (3 to 63 lower-case letters, "
            "digits, dots and hyphens)."
        )
    endpoint = _string(config, "endpoint_url", required=False, kind="s3")
    if endpoint:
        parts = urlsplit(endpoint)
        has_path = parts.path not in {"", "/"}
        if parts.scheme not in {"http", "https"} or not parts.hostname or has_path:
            raise InvalidRequest(
                "config.endpoint_url must be an http(s) URL of the store without a path, for "
                "example https://s3.example.org (leave it out for AWS S3)."
            )
    style = _string(config, "addressing_style", required=False, kind="s3") or "path"
    if style not in {"path", "virtual", "auto"}:
        raise InvalidRequest("config.addressing_style must be 'path', 'virtual' or 'auto'.")
    return {
        "bucket": bucket,
        "endpoint_url": endpoint,
        "prefix": relative_prefix(
            _string(config, "prefix", required=False, kind="s3"), "config.prefix"
        ),
        "region": _string(config, "region", required=False, kind="s3") or "us-east-1",
        "addressing_style": style,
    }


def _validate_localfs(config: dict) -> dict:
    root = _string(config, "root_path", required=True, kind="localfs")
    if not root.startswith("/") or posixpath.normpath(root) != root or root.startswith("//"):
        raise InvalidRequest(
            f"config.root_path {root!r} must be an absolute, normalised path such as "
            "/data/exports (no trailing '/', '.' or '..')."
        )
    return {"root_path": root}


def _validate_sql(config: dict) -> dict:
    dialect = _string(config, "dialect", required=True, kind="sql")
    if dialect not in SqlDialect.values:
        raise InvalidRequest(
            f"config.dialect {dialect!r} is not supported; dialects: "
            f"{', '.join(SqlDialect.values)}."
        )
    port = config.get("port", DIALECTS[dialect].port)
    if isinstance(port, bool) or not isinstance(port, int) or not 0 < port < 65536:
        raise InvalidRequest("config.port must be a TCP port number (1 to 65535).")
    options = config.get("options", {})
    if not isinstance(options, dict):
        raise InvalidRequest("config.options must be an object of ODBC attributes.")
    for key, value in options.items():
        if not _ODBC_KEY.match(key) or key.lower() in _RESERVED_ODBC_KEYS:
            raise InvalidRequest(
                f"config.options may not set {key!r}: option names are letters, digits, "
                "spaces and underscores, and driver, server, port, database, user and "
                "password are set from the other fields."
            )
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            raise InvalidRequest(f"config.options.{key} must be a string or an integer.")
    return {
        "dialect": dialect,
        "host": _string(config, "host", required=True, kind="sql"),
        "port": port,
        "database": _string(config, "database", required=True, kind="sql"),
        "username": _string(config, "username", required=True, kind="sql"),
        "driver": _string(config, "driver", required=False, kind="sql")
        or DIALECTS[dialect].driver,
        "options": options,
    }


_VALIDATORS = {
    ConnectionKind.S3: _validate_s3,
    ConnectionKind.LOCALFS: _validate_localfs,
    ConnectionKind.SQL: _validate_sql,
}


def _validate_kind(kind: str) -> str:
    if kind not in ConnectionKind.values:
        raise InvalidRequest(
            f"Unknown connection kind {kind!r}; kinds: {', '.join(ConnectionKind.values)}."
        )
    return kind


def _validate_config(kind: str, config) -> dict:
    if not isinstance(config, dict):
        raise InvalidRequest("config must be an object.")
    return _VALIDATORS[kind](config)


def _validate_secrets(kind: str, secrets: dict) -> dict:
    fields = SECRET_FIELDS[kind]
    unknown = sorted(set(secrets) - fields["required"] - fields["optional"])
    if unknown:
        known = sorted(fields["required"] | fields["optional"])
        raise InvalidRequest(
            f"{kind} connections have no secret named {', '.join(unknown)} (their secrets: "
            f"{', '.join(known) or 'none'})."
        )
    for name, value in secrets.items():
        if not isinstance(value, str) or not value:
            raise InvalidRequest(f"The secret {name} must be a non-empty string.")
    missing = sorted(fields["required"] - set(secrets))
    if missing:
        raise InvalidRequest(f"{kind} connections need the secrets: {', '.join(missing)}.")
    return secrets


def _validate_roles(roles) -> list:
    if not isinstance(roles, list) or any(role not in Role.values for role in roles):
        raise InvalidRequest(f"allowed_roles must be a list of roles ({', '.join(Role.values)}).")
    return sorted(set(roles), key=Role.values.index)


# --------------------------------------------------------------------------- reading


def list_connections(actor: Actor, *, kind: Optional[str] = None):
    found = visible_connections(actor, Connection.objects.all())
    return found.filter(kind=kind) if kind else found


def get_connection(actor: Actor, connection_id) -> Connection:
    check(actor, Action.CONNECTION_VIEW)
    connection = Connection.objects.filter(pk=connection_id).first()
    if connection is None:
        raise NotFound(f"There is no connection with id {connection_id}.")
    check(actor, Action.CONNECTION_VIEW, connection)
    return connection


def usable_connection(actor: Actor, connection_id, *, kinds, purpose: str) -> Connection:
    """A connection ``actor`` may use in a dataset as ``purpose`` (source or destination)."""
    check(actor, Action.CONNECTION_USE)
    connection = Connection.objects.filter(pk=connection_id).first()
    if connection is None:
        raise InvalidRequest(f"The {purpose} connection {connection_id} does not exist.")
    check(actor, Action.CONNECTION_USE, connection)
    if connection.kind not in kinds:
        raise InvalidRequest(
            f"Connection {connection.name!r} is a {connection.kind} connection, which cannot "
            f"be a dataset {purpose} (possible: {', '.join(sorted(kinds))})."
        )
    return connection


def secrets_of(connection: Connection) -> dict:
    return secret_backend.backend().decrypt(connection.secret_ciphertext)


def bucket_of(connection: Connection) -> storage.Bucket:
    return storage.connection_bucket(connection.config, secrets_of(connection))


def _odbc_value(value) -> str:
    """An ODBC attribute value, braced (with '}' doubled) whenever it could end the value."""
    text = str(value)
    if text == "" or text != text.strip() or any(ch in text for ch in ";{}=+"):
        return "{" + text.replace("}", "}}") + "}"
    return text


def sql_connection_string(connection: Connection) -> str:
    """The ODBC connection string of a ``sql`` connection, with its decrypted password.

    Built only at lease time for the one job that needs it; never stored or logged.
    """
    config = connection.config
    secrets = secrets_of(connection)
    host, port, database = config["host"], config["port"], config["database"]
    if config["dialect"] == SqlDialect.SQLSERVER:
        location = [("Server", f"{host},{port}"), ("Database", database)]
    elif config["dialect"] == SqlDialect.ORACLE:
        location = [("DBQ", f"{host}:{port}/{database}")]
    else:
        location = [("Server", host), ("Port", port), ("Database", database)]
    pairs = [
        ("Driver", "{" + config["driver"].replace("}", "}}") + "}"),
        *[(key, _odbc_value(value)) for key, value in location],
        ("Uid", _odbc_value(config["username"])),
        ("Pwd", _odbc_value(secrets["password"])),
        *[(key, _odbc_value(value)) for key, value in config.get("options", {}).items()],
    ]
    return ";".join(f"{key}={value}" for key, value in pairs)


# --------------------------------------------------------------------------- changing


def create_connection(
    actor: Actor,
    *,
    name: str,
    kind: str,
    config: dict,
    secrets: Optional[dict] = None,
    description: str = "",
    allowed_roles: Optional[list] = None,
) -> Connection:
    check(actor, Action.CONNECTION_MANAGE)
    kind = _validate_kind(kind)
    if not name.strip():
        raise InvalidRequest("A connection needs a name.")
    config = _validate_config(kind, config)
    secrets = _validate_secrets(kind, dict(secrets or {}))
    roles = _validate_roles(
        allowed_roles if allowed_roles is not None else [Role.AUTHOR.value, Role.ADMIN.value]
    )
    connection = Connection(
        name=name,
        kind=kind,
        description=description,
        config=config,
        secret_ciphertext=secret_backend.backend().encrypt(secrets),
        secret_fields=sorted(secrets),
        allowed_roles=roles,
        created_by=actor.user,
    )
    try:
        with transaction.atomic():
            connection.save()
            audit.record(
                actor,
                "connection.create",
                connection,
                {
                    "kind": kind,
                    "config": config,
                    "secret_fields": sorted(secrets),
                    "allowed_roles": roles,
                },
            )
    except IntegrityError:
        raise Conflict(f"A connection named {name!r} already exists.") from None
    return connection


def update_connection(
    actor: Actor,
    connection_id,
    *,
    name: Optional[str] = None,
    description: Optional[str] = None,
    config: Optional[dict] = None,
    secrets: Optional[dict] = None,
    allowed_roles: Optional[list] = None,
) -> Connection:
    """Change a connection. ``secrets`` sets the named secrets (a null value removes one);
    secrets not named keep their values. The kind cannot change."""
    check(actor, Action.CONNECTION_MANAGE)
    with transaction.atomic():
        connection = Connection.objects.select_for_update().filter(pk=connection_id).first()
        if connection is None:
            raise NotFound(f"There is no connection with id {connection_id}.")
        changed: dict = {}
        if name is not None and name != connection.name:
            if not name.strip():
                raise InvalidRequest("A connection needs a name.")
            changed["name"] = {"from": connection.name, "to": name}
            connection.name = name
        if description is not None and description != connection.description:
            changed["description"] = True
            connection.description = description
        if config is not None:
            config = _validate_config(connection.kind, config)
            if config != connection.config:
                changed["config"] = {"from": connection.config, "to": config}
                connection.config = config
        if allowed_roles is not None:
            roles = _validate_roles(allowed_roles)
            if roles != connection.allowed_roles:
                changed["allowed_roles"] = {"from": connection.allowed_roles, "to": roles}
                connection.allowed_roles = roles
        if secrets:
            current = secrets_of(connection)
            for key, value in secrets.items():
                if value is None:
                    current.pop(key, None)
                else:
                    current[key] = value
            current = _validate_secrets(connection.kind, current)
            connection.secret_ciphertext = secret_backend.backend().encrypt(current)
            connection.secret_fields = sorted(current)
            changed["secrets_changed"] = sorted(secrets)
        if changed:
            connection.updated_at = timezone.now()
            try:
                with transaction.atomic():
                    connection.save()
            except IntegrityError:
                raise Conflict(f"A connection named {name!r} already exists.") from None
            audit.record(actor, "connection.update", connection, changed)
    return connection


def delete_connection(actor: Actor, connection_id) -> None:
    check(actor, Action.CONNECTION_MANAGE)
    connection = Connection.objects.filter(pk=connection_id).first()
    if connection is None:
        raise NotFound(f"There is no connection with id {connection_id}.")
    try:
        with transaction.atomic():
            audit.record(actor, "connection.delete", connection, {"kind": connection.kind})
            connection.delete()
    except ProtectedError:
        users = Dataset.objects.filter(
            Q(source_connection=connection) | Q(destination_connection=connection)
        )
        raise Conflict(
            f"Connection {connection.name!r} is used by the datasets "
            f"{', '.join(sorted(d.name for d in users))}; change them first."
        ) from None


def _check(connection: Connection) -> ConnectionCheck:
    if connection.kind == ConnectionKind.S3:
        try:
            bucket_of(connection).check_access()
        except StoreUnavailable as error:
            return ConnectionCheck(False, error.message)
        return ConnectionCheck(
            True, f"Bucket {connection.config['bucket']!r} is reachable with these credentials."
        )
    if connection.kind == ConnectionKind.LOCALFS:
        root = connection.config["root_path"]
        if os.path.isdir(root):
            return ConnectionCheck(True, f"{root} is a directory the gateway can see.")
        return ConnectionCheck(
            None,
            f"{root} is not visible to the gateway. Workers use this connection and must mount "
            "the directory at the same path; the gateway can only check paths it can see.",
        )
    host, port = connection.config["host"], connection.config["port"]
    try:
        with socket.create_connection((host, port), timeout=5):
            pass
    except OSError as error:
        return ConnectionCheck(
            False, f"{host}:{port} does not accept connections ({error.strerror or error})."
        )
    return ConnectionCheck(
        True,
        f"{host}:{port} accepts connections. The login itself is checked by the first job: "
        "the gateway holds no database drivers.",
    )


def check_store(actor: Actor) -> ConnectionCheck:
    """Whether the installation's own bucket answers with the gateway's credentials (for the
    admin overview; a read, so not audited)."""
    check(actor, Action.SETTINGS_VIEW)
    bucket = storage.store()
    try:
        bucket.check_access()
    except StoreUnavailable as error:
        return ConnectionCheck(False, error.message)
    return ConnectionCheck(True, f"The bucket {bucket.bucket!r} is reachable.")


def check_connection(actor: Actor, connection_id) -> ConnectionCheck:
    """Check that the connection can be reached (and, for buckets, that its credentials work)."""
    check(actor, Action.CONNECTION_MANAGE)
    connection = Connection.objects.filter(pk=connection_id).first()
    if connection is None:
        raise NotFound(f"There is no connection with id {connection_id}.")
    result = _check(connection)
    Connection.objects.filter(pk=connection.pk).update(
        last_tested_at=timezone.now(), last_test_ok=result.ok, last_test_message=result.message
    )
    audit.record(actor, "connection.test", connection, {"ok": result.ok})
    return result
