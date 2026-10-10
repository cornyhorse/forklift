"""Connections, the schema registry and datasets."""

from __future__ import annotations

import uuid

from django.db import models
from django.utils import timezone

from forklift_web.core.choices import Classification, ConnectionKind, InputFormat, Role
from forklift_web.core.models.accounts import User
from forklift_web.core.models.fields import OrderedJSONField


def _default_allowed_roles() -> list:
    return [Role.AUTHOR.value, Role.ADMIN.value]


class Connection(models.Model):
    """A source or destination: an S3-compatible bucket, a mounted directory or a database.

    ``config`` holds what is not secret (endpoint, bucket, prefix / root path / DSN parts).
    Secrets (keys, passwords) are encrypted into ``secret_ciphertext`` by the secret backend
    and are write-only: the API reports which ones are set (``secret_fields``), never their
    values. ``allowed_roles`` are the roles that may use the connection in datasets (admins
    always may).
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=100, unique=True)
    kind = models.CharField(max_length=16, choices=ConnectionKind.choices)
    description = models.TextField(blank=True, default="")
    config = models.JSONField(default=dict)
    secret_ciphertext = models.TextField(blank=True, default="")
    secret_fields = models.JSONField(default=list)
    allowed_roles = models.JSONField(default=_default_allowed_roles)
    created_by = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True, related_name="+"
    )
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(default=timezone.now)
    last_tested_at = models.DateTimeField(null=True, blank=True)
    last_test_ok = models.BooleanField(null=True, blank=True)
    last_test_message = models.TextField(blank=True, default="")

    class Meta:
        ordering = ["name"]

    def __str__(self) -> str:
        return self.name


class Schema(models.Model):
    """A named schema; its documents are the immutable versions."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=200, unique=True)
    description = models.TextField(blank=True, default="")
    created_by = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True, related_name="+"
    )
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["name"]

    def __str__(self) -> str:
        return self.name


class ImmutableError(Exception):
    """An attempt to change or delete a record that is immutable once written."""


class SchemaVersion(models.Model):
    """One version of a schema document. Versions are immutable: editing creates a new one."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    schema = models.ForeignKey(Schema, on_delete=models.PROTECT, related_name="versions")
    number = models.PositiveIntegerField()
    document = OrderedJSONField()  # key order is meaningful (properties order)
    sha256 = models.CharField(max_length=64)
    author = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True, related_name="+"
    )
    created_at = models.DateTimeField(default=timezone.now)
    notes = models.TextField(blank=True, default="")

    class Meta:
        ordering = ["schema", "-number"]
        constraints = [
            models.UniqueConstraint(fields=["schema", "number"], name="schema_version_number")
        ]

    def save(self, *args, **kwargs):
        if not self._state.adding:
            raise ImmutableError(
                f"Schema version {self.number} of {self.schema.name!r} is immutable; "
                "create a new version instead."
            )
        super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        raise ImmutableError(
            f"Schema version {self.number} of {self.schema.name!r} is immutable and cannot be "
            "deleted."
        )

    def __str__(self) -> str:
        return f"{self.schema.name} v{self.number}"


class Dataset(models.Model):
    """The unit people run and permission: a source, a schema version and a destination.

    Source: ``source_connection`` + ``source_path`` (an object key under an s3 connection's
    prefix; for sql connections the tables come from the schema's ``x-sql``), or no connection,
    in which case every run names an upload. Destination: none (the job's artifacts only), an
    s3 connection + ``destination_prefix`` (published manifest-last when a run succeeds) or a
    sql connection with ``destination_options`` (table, schema_name, mode, key_columns).
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=200, unique=True)
    description = models.TextField(blank=True, default="")
    classification = models.CharField(
        max_length=16, choices=Classification.choices, default=Classification.INTERNAL
    )
    input_format = models.CharField(
        max_length=16, choices=InputFormat.choices, default=InputFormat.CSV
    )
    input_options = models.JSONField(default=dict, blank=True)
    source_connection = models.ForeignKey(
        Connection, on_delete=models.PROTECT, null=True, blank=True, related_name="+"
    )
    source_path = models.CharField(max_length=1024, blank=True, default="")
    schema_version = models.ForeignKey(
        SchemaVersion, on_delete=models.PROTECT, related_name="datasets"
    )
    destination_connection = models.ForeignKey(
        Connection, on_delete=models.PROTECT, null=True, blank=True, related_name="+"
    )
    destination_prefix = models.CharField(max_length=1024, blank=True, default="")
    destination_options = models.JSONField(default=dict, blank=True)
    compression = models.CharField(max_length=16, default="snappy")
    options = models.JSONField(default=dict, blank=True)
    created_by = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True, related_name="+"
    )
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["name"]

    def __str__(self) -> str:
        return self.name
