"""Artifacts: listing, presigned download URLs, downloads to a path checked against the
artifact's size and sha256, retries with a fresh URL, and streams."""

from __future__ import annotations

import hashlib

import pytest

from forklift_client import (
    Artifact,
    ArtifactStream,
    Gone,
    RetryPolicy,
    StoreError,
    TransportError,
    VerificationError,
    _downloads,
)

PARQUET = bytes(range(256)) * 9  # 2304 bytes


@pytest.fixture
def job(fake):
    return fake.add_job("succeeded")


@pytest.fixture
def artifact(fake, job):
    return fake.add_artifact(job, "data.parquet", PARQUET)


def leftovers(directory):
    return sorted(path.name for path in directory.iterdir())


class TestArtifactCalls:
    def test_list_get_and_download_url(self, fake, client, job, artifact):
        fake.add_artifact(job, "bad_rows.parquet", b"x", kind="bad_rows", sha256="")
        found = client.list_artifacts(job["id"])
        assert [a.name for a in found] == ["data.parquet", "bad_rows.parquet"]
        assert all(isinstance(a, Artifact) for a in found)
        one = client.get_artifact(artifact["id"])
        assert one.bytes == len(PARQUET) and one.sha256 == hashlib.sha256(PARQUET).hexdigest()
        link = client.get_download_url(one)
        assert link.filename == f"{job['id']}-data.parquet" and "/store/" in link.url

    def test_a_deleted_artifact_is_gone(self, fake, client, artifact, tmp_path):
        artifact["deleted_at"] = "2026-10-01T00:00:00Z"
        with pytest.raises(Gone, match="was deleted"):
            client.download_artifact(artifact["id"], tmp_path)
        assert leftovers(tmp_path) == []


class TestDownload:
    def test_into_a_directory(self, fake, client, artifact, tmp_path):
        path = client.download_artifact(Artifact.from_dict(artifact), tmp_path)
        assert path == tmp_path / "data.parquet" and path.read_bytes() == PARQUET
        assert leftovers(tmp_path) == ["data.parquet"], "no partial file stays"
        (get,) = fake.sent("GET store")
        assert "authorization" not in get["headers"], "the token never goes to the store"
        assert fake.sent("GET /artifacts/{id}") == [], "an Artifact needs no second lookup"

    def test_to_a_file_by_id(self, fake, client, artifact, tmp_path):
        target = tmp_path / "people.parquet"
        assert client.download_artifact(artifact["id"], str(target)) == target
        assert target.read_bytes() == PARQUET
        assert len(fake.sent("GET /artifacts/{id}")) == 1

    def test_an_existing_file_is_replaced(self, client, artifact, tmp_path):
        target = tmp_path / "out.parquet"
        target.write_bytes(b"old")
        client.download_artifact(artifact["id"], target)
        assert target.read_bytes() == PARQUET

    @pytest.mark.parametrize("name, expected", [("data/part-0.parquet", "part-0.parquet")])
    def test_names_with_directories_keep_the_last_part(
        self, fake, client, job, tmp_path, name, expected
    ):
        found = fake.add_artifact(job, name, PARQUET)
        assert client.download_artifact(found["id"], tmp_path).name == expected

    def test_a_name_that_is_no_file_name_becomes_the_id(self, fake, client, job, tmp_path):
        found = fake.add_artifact(job, "..", PARQUET)
        assert client.download_artifact(found["id"], tmp_path).name == found["id"]

    def test_bytes_that_do_not_have_the_sha256(self, fake, client, artifact, tmp_path):
        fake.fail("GET store", corrupt=True)
        with pytest.raises(VerificationError, match="do not have the artifact's sha256"):
            client.download_artifact(artifact["id"], tmp_path)
        assert leftovers(tmp_path) == []
        assert len(fake.sent("GET store")) == 1, "a corrupt download is not retried"

    def test_without_a_sha256_only_the_size_is_checked(self, fake, client, job, tmp_path):
        found = fake.add_artifact(job, "x.bin", PARQUET, sha256="")
        fake.fail("GET store", corrupt=True)
        assert client.download_artifact(found["id"], tmp_path).stat().st_size == len(PARQUET)

    def test_more_bytes_than_the_artifact_has(self, fake, client, artifact, tmp_path):
        artifact["bytes"] = 100
        with pytest.raises(VerificationError, match="more than the artifact's 100 bytes"):
            client.download_artifact(artifact["id"], tmp_path)
        assert leftovers(tmp_path) == []

    def test_fewer_bytes_than_the_artifact_has(self, fake, client, artifact, tmp_path):
        artifact["bytes"] = len(PARQUET) + 1
        with pytest.raises(VerificationError, match="received 2304 bytes, the artifact has 2305"):
            client.download_artifact(artifact["id"], tmp_path)

    def test_a_dropped_connection_starts_again_with_a_fresh_url(
        self, fake, client, artifact, tmp_path
    ):
        fake.fail("GET store", truncate=True)
        fake.fail("GET store", status=503)
        path = client.download_artifact(artifact["id"], tmp_path)
        assert path.read_bytes() == PARQUET
        assert len(fake.sent("GET /artifacts/{id}/download")) == 3
        assert leftovers(tmp_path) == ["data.parquet"]

    def test_an_expired_url_is_asked_for_again(self, fake, client, artifact, tmp_path):
        fake.fail("GET store", status=403, body=b"<Error><Code>AccessDenied</Code></Error>")
        assert client.download_artifact(artifact["id"], tmp_path).read_bytes() == PARQUET

    def test_a_missing_object_is_not_retried(self, fake, client, artifact, tmp_path):
        del fake.objects[artifact["_key"]]
        with pytest.raises(StoreError) as raised:
            client.download_artifact(artifact["id"], tmp_path)
        assert raised.value.status == 404 and raised.value.code == "NoSuchKey"
        assert "X-Amz-Signature" not in str(raised.value)
        assert len(fake.sent("GET store")) == 1

    def test_the_attempts_run_out(self, fake, client, artifact, tmp_path):
        fake.fail("GET store", truncate=True, times=3)
        with pytest.raises(TransportError, match="closed after 1152 of 2304 bytes"):
            client.download_artifact(artifact["id"], tmp_path)
        assert leftovers(tmp_path) == []

    def test_a_stalled_download_times_out(self, fake, artifact, tmp_path):
        from fake_api import TOKEN

        from forklift_client import ForkliftClient

        client = ForkliftClient(fake.url, TOKEN, timeout=0.2, retry=RetryPolicy(attempts=1))
        fake.fail("GET store", stall=1.0)
        with pytest.raises(TransportError, match="timed out"):
            client.download_artifact(artifact["id"], tmp_path)
        assert leftovers(tmp_path) == []

    def test_a_tls_failure_is_not_retried(self, client, artifact, tmp_path, monkeypatch):
        def failing(*args, **kwargs):
            raise TransportError("Downloading", "store", "TLS error", retryable=False)

        monkeypatch.setattr(_downloads, "open_stream", failing)
        with pytest.raises(TransportError, match="TLS error"):
            client.download_artifact(Artifact.from_dict(_public(artifact)), tmp_path)


def _public(artifact):
    return {key: value for key, value in artifact.items() if not key.startswith("_")}


class TestStream:
    def test_read_in_pieces(self, client, artifact):
        with client.open_artifact(artifact["id"]) as stream:
            assert isinstance(stream, ArtifactStream) and stream.readable()
            first = stream.read(1000)
            assert stream.read(0) == b""
            rest = stream.read()
            assert stream.verified and stream.received == len(PARQUET)
            assert stream.read(10) == b"", "after the end"
        assert first + rest == PARQUET

    def test_iterate(self, client, artifact):
        with client.open_artifact(Artifact.from_dict(_public(artifact))) as stream:
            assert b"".join(stream) == PARQUET
        assert stream.verified

    def test_a_bad_stream_raises_at_its_end(self, fake, client, artifact):
        fake.fail("GET store", corrupt=True)
        with client.open_artifact(artifact["id"]) as stream:
            assert len(stream.read(len(PARQUET))) == len(PARQUET)
            with pytest.raises(VerificationError):
                stream.read(1)
            assert not stream.verified

    def test_an_error_answer(self, fake, client, artifact):
        fake.fail("GET store", status=500)
        with pytest.raises(StoreError, match="answered HTTP 500: InternalError"):
            with client.open_artifact(artifact["id"]):
                pass
