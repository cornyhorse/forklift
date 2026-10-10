"""Fixtures for the worker's integration tests: the real engine, RustFS and PostgreSQL.

The RustFS and PostgreSQL tests use the services and helpers of
``tests/integration-tests/services`` (start them with ``scripts/test-services.sh up``) and run only
with ``FORKLIFT_TEST_SERVICES=1``; with it set, a service that cannot be reached fails the tests
instead of skipping them.
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path
from typing import Any, Iterator

import pytest
from fake_gateway import FakeGateway

SERVICES = Path(__file__).resolve().parents[4] / "tests" / "integration-tests" / "services"
sys.path.insert(0, str(SERVICES))

import service_helpers  # noqa: E402
from it_helpers import REAL_ENGINE  # noqa: E402

from forklift_worker.transport import host_of  # noqa: E402


@pytest.fixture
def services_enabled():
    if not service_helpers.ENABLED:
        pytest.skip(
            "these tests need FORKLIFT_TEST_SERVICES=1 and the services in "
            "tests/integration-tests/services/compose.yaml (scripts/test-services.sh up)"
        )


def _unreachable(service: str, error: Exception) -> None:
    pytest.fail(
        f"{service} is not reachable ({type(error).__name__}: {error}). Start the services with "
        "scripts/test-services.sh up, or unset FORKLIFT_TEST_SERVICES to skip these tests.",
        pytrace=False,
    )


@pytest.fixture
def store(services_enabled) -> service_helpers.ObjectStore:
    store = service_helpers.ObjectStore.from_environment()
    try:
        store.client().list_buckets()
    except Exception as error:  # any failure means the store is not usable
        _unreachable(f"The object store at {store.endpoint}", error)
    return store


@pytest.fixture
def bucket(store) -> Iterator[str]:
    name = f"forklift-worker-it-{uuid.uuid4().hex[:12]}"
    store.client().create_bucket(Bucket=name)
    yield name
    store.empty_and_delete(name)


class StoreGateway(FakeGateway):
    """The fake gateway, signing real presigned URLs for RustFS the way the gateway does."""

    def __init__(self, store: service_helpers.ObjectStore, bucket: str):
        super().__init__()
        self.store, self.bucket = store, bucket
        self.s3 = store.client()

    def put_object(self, key: str, data: bytes, etag: str | None = None) -> dict[str, Any]:
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=data)
        head = self.s3.head_object(Bucket=self.bucket, Key=key)
        return self.location(key, head["ContentLength"], head["ETag"])

    def location(self, key: str, size: int, etag: str) -> dict[str, Any]:
        url = self.s3.generate_presigned_url(
            "get_object", Params={"Bucket": self.bucket, "Key": key}, ExpiresIn=900
        )
        return {"type": "presigned_url", "url": url, "size": size, "etag": etag}

    def upload_target(self, key: str) -> tuple[str, dict[str, str]]:
        url = self.s3.generate_presigned_url(
            "put_object",
            Params={"Bucket": self.bucket, "Key": key, "ContentType": "application/octet-stream"},
            ExpiresIn=900,
        )
        return url, {"Content-Type": "application/octet-stream"}

    def read(self, key: str) -> bytes:
        return self.store.read(self.bucket, key)


@pytest.fixture
def store_gateway(store, bucket) -> Iterator[StoreGateway]:
    with StoreGateway(store, bucket) as fake:
        yield fake
    assert fake.contract_errors == [], "the worker reported a JobResult the contract refuses"


@pytest.fixture
def make_store_settings(tmp_path, store_gateway):
    from forklift_worker.settings import Settings

    def build(**overrides: Any) -> Settings:
        token_file = tmp_path / "secrets" / "worker-token"
        token_file.parent.mkdir(exist_ok=True)
        token_file.write_text(store_gateway.token)
        values: dict[str, Any] = {
            "gateway": store_gateway.url + "/internal/v1",
            "token_file": token_file,
            "scratch": tmp_path / "scratch",
            "worker_id": "it-worker",
            "engine_command": REAL_ENGINE,
            "allow_root": True,
            "idle_min_seconds": 0.01,
            "idle_max_seconds": 0.05,
            "heartbeat_seconds": 0.2,
            "http_timeout": 30,
            "max_jobs": 1,
            "store_host": [host_of(store_gateway.store.endpoint)],
        }
        values.update(overrides)
        return Settings(**values)

    return build
