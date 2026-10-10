"""Fixtures for the forklift-web tests (PostgreSQL and RustFS, see django_settings.py).

The session creates its own bucket in the store and removes it with everything in it at the
end; pytest-django creates and drops the test database. A service that cannot be reached fails
the tests: there is no "skip when the services are missing" mode, so CI cannot pass by testing
nothing.
"""

from __future__ import annotations

import json
import urllib.request
import uuid
from dataclasses import dataclass
from typing import Optional

import boto3
import pytest
from botocore.client import Config
from django.conf import settings
from django.test import Client

from forklift_web import storage
from forklift_web.core.choices import Role
from forklift_web.core.models import User
from forklift_web.middleware import INTERNAL, SURFACE_KEY
from forklift_web.policy import Actor
from forklift_web.services import tokens


def root_client():
    """A client of the test store with its root credentials (for the tests' own checks)."""
    conf = settings.FORKLIFT_STORE
    return boto3.client(
        "s3",
        endpoint_url=conf["endpoint_url"],
        region_name=conf["region"],
        aws_access_key_id=conf["credentials"]["upload"][0],
        aws_secret_access_key=conf["credentials"]["upload"][1],
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def empty_and_delete_bucket(client, bucket: str) -> None:
    for upload in client.list_multipart_uploads(Bucket=bucket).get("Uploads", []):
        client.abort_multipart_upload(
            Bucket=bucket, Key=upload["Key"], UploadId=upload["UploadId"]
        )
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        keys = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if keys:
            client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
    client.delete_bucket(Bucket=bucket)


@pytest.fixture(scope="session", autouse=True)
def test_bucket():
    """The session's own bucket, used as the installation's store."""
    client = root_client()
    name = f"forklift-web-{uuid.uuid4().hex[:12]}"
    try:
        client.create_bucket(Bucket=name)
    except Exception as error:  # any failure means the store is not usable
        pytest.fail(
            f"The object store at {settings.FORKLIFT_STORE['endpoint_url']} is not reachable "
            f"({type(error).__name__}: {error}); start it with "
            "`docker compose -f tests/integration-tests/services/compose.yaml up -d --wait`.",
            pytrace=False,
        )
    previous = settings.FORKLIFT_STORE["bucket"]
    settings.FORKLIFT_STORE["bucket"] = name
    storage.reset_store()
    yield name
    settings.FORKLIFT_STORE["bucket"] = previous
    storage.reset_store()
    empty_and_delete_bucket(client, name)


@pytest.fixture
def s3():
    return root_client()


def put_url(url: str, data: bytes, headers: Optional[dict] = None):
    """PUT ``data`` to a presigned URL, as a browser or a worker would; returns the response
    headers (case-insensitive)."""
    request = urllib.request.Request(url, data=data, method="PUT", headers=headers or {})
    with urllib.request.urlopen(request, timeout=30) as response:
        assert response.status == 200
        return response.headers


def get_url(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=30) as response:
        return response.read()


# --------------------------------------------------------------------------- users


@pytest.fixture
def make_user(db):
    counter = iter(range(1, 10_000))

    def make(role: str = Role.VIEWER, *, raw_rows: bool = False, username: str = "", **fields):
        name = username or f"{role}-{next(counter)}-{uuid.uuid4().hex[:6]}"
        user = User(username=name, role=role, can_view_raw_rows=raw_rows, **fields)
        user.set_password("correct horse battery staple")
        user.save()
        return user

    return make


@pytest.fixture
def admin(make_user):
    return make_user(Role.ADMIN)


@pytest.fixture
def admin_actor(admin):
    return Actor.for_user(admin)


@dataclass
class Caller:
    """A test client acting as one principal, with JSON helpers."""

    client: Client
    user: Optional[User] = None
    headers: Optional[dict] = None

    def call(self, method: str, path: str, body=None, **headers):
        kwargs = {**(self.headers or {}), **headers}
        if body is not None:
            kwargs["data"] = json.dumps(body)
            kwargs["content_type"] = "application/json"
        return getattr(self.client, method.lower())(path, **kwargs)

    def get(self, path, **headers):
        return self.call("GET", path, **headers)

    def post(self, path, body=None, **headers):
        return self.call("POST", path, body if body is not None else {}, **headers)

    def patch(self, path, body, **headers):
        return self.call("PATCH", path, body, **headers)

    def put(self, path, body, **headers):
        return self.call("PUT", path, body, **headers)

    def delete(self, path, **headers):
        return self.call("DELETE", path, **headers)


@pytest.fixture
def as_user():
    """``as_user(user)``: a Caller signed in as ``user`` (session)."""

    def make(user: Optional[User]) -> Caller:
        client = Client()
        if user is not None:
            client.force_login(user)
        return Caller(client=client, user=user)

    return make


@pytest.fixture
def as_token():
    """``as_token(raw)``: a Caller sending ``Authorization: Bearer raw``."""

    def make(raw: str, user: Optional[User] = None) -> Caller:
        return Caller(client=Client(), user=user, headers={"HTTP_AUTHORIZATION": f"Bearer {raw}"})

    return make


@pytest.fixture
def api_token(db):
    """``api_token(user, scopes)``: a stored API token and its raw value."""
    from forklift_web.core.models import ApiToken

    def make(user: User, scopes, **fields):
        token, raw = tokens.create(
            ApiToken,
            tokens.API_TOKEN_PREFIX,
            owner=user,
            name="test",
            scopes=sorted(scopes),
            **fields,
        )
        return token, raw

    return make


# --------------------------------------------------------------------------- workers


@pytest.fixture
def worker_token(db):
    from forklift_web.core.models import WorkerToken

    token, raw = tokens.create(WorkerToken, tokens.WORKER_TOKEN_PREFIX, name="test workers")
    return token, raw


@pytest.fixture
def internal():
    """``internal(raw)``: a Caller on the internal port with a worker token (or none)."""

    def make(raw: Optional[str] = None) -> Caller:
        headers = {"HTTP_AUTHORIZATION": f"Bearer {raw}"} if raw else {}
        return Caller(client=Client(**{SURFACE_KEY: INTERNAL}), headers=headers)

    return make
