"""Streamed inputs: a CSV read from RustFS through a presigned URL (ADR 0006).

``run_job`` reads a ``presigned_url`` input as a forward-only HTTP stream, with small range
requests for header detection, and gives the same output as the same file staged locally. The
URL carries no credentials beyond its signature for one object, the job contacts only the hosts
it is allowed to, and the signature never shows up in results or output files. A URL that
expires is replaced through ``refresh_input_url`` (before a request, or after the store refused
it in the middle of the stream) and the job goes on with identical output. The last test runs
the whole SQL-lane path: a streamed CSV loaded into a PostgreSQL table with a login that owns
only the test's schema.
"""

from __future__ import annotations

import dataclasses
import socket
import threading
import time
import urllib.parse
from typing import List, Optional

import pyarrow.parquet as pq
import pytest

from forklift.jobs import run_job

pytestmark = pytest.mark.services

ROWS = 30_000
PEOPLE = "id,name,age\n" + "".join(f"{i},name{i},{18 + i % 60}\n" for i in range(ROWS))
SCHEMA = {
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": "string"},
        "age": {"type": "integer"},
    },
    "required": ["id"],
}


@pytest.fixture
def presigned(object_store, bucket):
    """Upload ``text`` as ``key`` and return a presigned GET URL for it (and its host)."""

    def make(key: str = "in/people.csv", text: str = PEOPLE, expires: int = 600):
        object_store.put(bucket, key, text)
        url = object_store.client().generate_presigned_url(
            "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=expires
        )
        return url, urllib.parse.urlsplit(url).hostname

    return make


def _spec(url, kind="run", **extra):
    spec = {
        "spec_version": 1,
        "job_id": "streamed-it",
        "kind": kind,
        "input": {
            "format": "csv",
            "location": dict({"type": "presigned_url", "url": url}, **extra.pop("location", {})),
            "options": extra.pop("input_options", {}),
        },
        "schema": SCHEMA,
        "output": {"location": {"type": "file", "path": "out/"}},
    }
    spec.update(extra)
    return spec


class TestPresignedCsv:
    def test_streamed_output_equals_the_staged_output(self, presigned, tmp_path):
        url, host = presigned()
        streamed, staged = tmp_path / "streamed", tmp_path / "staged"
        streamed.mkdir()
        (staged / "in").mkdir(parents=True)
        (staged / "in" / "people.csv").write_text(PEOPLE)
        events = []

        result = run_job(
            _spec(url, options={"batch_size": 5000}),
            base_dir=streamed,
            allowed_url_hosts=[host],
            progress=events.append,
        )
        local = _spec(url, options={"batch_size": 5000})
        local["input"]["location"] = {"type": "file", "path": "in/people.csv"}
        expected = run_job(local, base_dir=staged)

        assert result.status == "succeeded", result.error
        assert result.counts == expected.counts
        assert result.counts["total_rows"] == ROWS
        assert pq.read_table(streamed / "out" / "data.parquet").equals(
            pq.read_table(staged / "out" / "data.parquet")
        )
        assert events[-1]["rows_read"] == ROWS
        assert events[-1]["bytes_read"] == len(PEOPLE.encode())
        signature = urllib.parse.urlsplit(url).query
        for path in (streamed / "out").iterdir():
            if path.suffix == ".json":
                assert signature not in path.read_text()
        assert signature not in str(result.to_dict())

    def test_footer_detection_on_a_streamed_input(self, presigned, tmp_path):
        url, host = presigned("in/footer.csv", "id,name,age\n1,a,30\n2,b,40\nTOTAL,2,\n")
        spec = _spec(
            url, input_options={"footer_detection": {"column_index": 0, "patterns": ["^TOTAL$"]}}
        )
        result = run_job(spec, base_dir=tmp_path, allowed_url_hosts=[host])
        assert result.status == "succeeded", result.error
        assert result.counts["total_rows"] == 2

    @pytest.mark.parametrize(
        "kind, artifact", [("preview", "preview"), ("validate_schema", "report")]
    )
    def test_interactive_kinds(self, presigned, tmp_path, kind, artifact):
        url, host = presigned()
        spec = _spec(url, kind=kind, options={"preview_rows": 25, "sample_rows": 25})
        result = run_job(spec, base_dir=tmp_path, allowed_url_hosts=[host])
        assert result.status == "succeeded", result.error
        assert result.counts["total_rows"] == 25
        assert [a.kind for a in result.artifacts] == [artifact]

    def test_generate_schema(self, presigned, tmp_path):
        url, host = presigned()
        result = run_job(
            _spec(url, kind="generate_schema", options={"sample_rows": 100}),
            base_dir=tmp_path,
            allowed_url_hosts=[host],
        )
        assert result.status == "succeeded", result.error
        assert result.counts["total_rows"] == 100

    def test_a_url_signed_for_another_object_is_refused(self, presigned, tmp_path):
        url, host = presigned()
        moved = url.replace("/in/people.csv?", "/in/other.csv?")
        result = run_job(_spec(moved), base_dir=tmp_path, allowed_url_hosts=[host])
        assert result.error.code == "PERMISSION_DENIED"
        assert "HTTP 403" in result.error.message
        assert urllib.parse.urlsplit(url).query not in result.error.message

    def test_a_changed_object_is_refused(self, presigned, tmp_path):
        url, host = presigned()
        spec = _spec(url, location={"etag": '"0123456789abcdef0123456789abcdef"'})
        result = run_job(spec, base_dir=tmp_path, allowed_url_hosts=[host])
        assert result.error.code == "INPUT_UNREADABLE"
        assert "changed while it was being read" in result.error.message

    def test_hosts_that_are_not_allowed_are_never_contacted(self, presigned, tmp_path):
        url, _ = presigned()
        result = run_job(_spec(url), base_dir=tmp_path, allowed_url_hosts=["store.example"])
        assert result.error.code == "SPEC_INVALID"


def test_streamed_csv_loaded_into_a_postgres_table(presigned, postgres, tmp_path):
    """The SQL lane end to end: presigned CSV -> validated Parquet -> table (write_table)."""
    url, host = presigned("in/load.csv", "id,name,age\n1,Ana,34\n2,Bo,x\n3,Cy,29\n")
    login = postgres.owner_login()
    spec = _spec(url)
    spec["output"] = {
        "location": {
            "type": "sql_table",
            "connection_string": postgres.login_connection_string(login),
            "table": "people",
            "schema_name": postgres.namespace,
            "mode": "create",
        },
        "artifacts": {"type": "file", "path": "parquet/"},
    }

    result = run_job(spec, base_dir=tmp_path, allowed_url_hosts=[host])

    assert result.status == "succeeded", result.error
    assert result.counts["rows_written"] == 2 and result.counts["invalid_rows"] == 1
    assert sorted(postgres.rows("people", order_by="id")) == [(1, "Ana", 34), (3, "Cy", 29)]
    assert [a.path for a in result.artifacts][:2] == [
        "parquet/data.parquet",
        "parquet/bad_rows.parquet",
    ]
    assert login.password not in str(result.to_dict())


# --------------------------------------------------------------------------- fresh URLs

LIFETIME = 8  # seconds a short-lived URL is signed for


class Relay:
    """A TCP relay in front of the store that records the status of every response.

    With ``cut`` set it cuts the input's forward stream (the ``Range: bytes=0-`` request) after
    that many bytes, and holds every later request until ``hold_until``, by when its URL has
    expired. URLs are signed for the relay's address, as SigV4 needs (the store checks the
    signature against the Host header it receives).
    """

    def __init__(self, upstream: str):
        parts = urllib.parse.urlsplit(upstream)
        self.upstream = (parts.hostname, parts.port)
        self.cut: Optional[int] = None
        self.hold_until = 0.0
        self.statuses: List[int] = []
        self._cut_done = False
        self._server = socket.create_server(("127.0.0.1", 0))
        self.endpoint = f"http://127.0.0.1:{self._server.getsockname()[1]}"
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self) -> None:
        while True:
            try:
                client, _ = self._server.accept()
            except OSError:  # closed
                return
            threading.Thread(target=self._relay, args=(client,), daemon=True).start()

    def _relay(self, client: socket.socket) -> None:
        with client, socket.create_connection(self.upstream) as upstream:
            request = b""
            while b"\r\n\r\n" not in request:
                chunk = client.recv(65536)
                if not chunk:
                    return
                request += chunk
            forward = b"Range: bytes=0-\r\n" in request
            if self._cut_done:
                time.sleep(max(0.0, self.hold_until - time.time()))
            upstream.sendall(request)
            sent = 0
            while True:
                data = upstream.recv(65536)
                if not data:
                    return
                if sent == 0:
                    self.statuses.append(int(data.split(b" ", 2)[1]))
                if forward and self.cut is not None and not self._cut_done:
                    if sent + len(data) > self.cut:
                        client.sendall(data[: self.cut - sent])
                        self._cut_done = True
                        return
                client.sendall(data)
                sent += len(data)

    def close(self) -> None:
        self._server.close()


@pytest.fixture
def relay(object_store):
    proxy = Relay(object_store.endpoint)
    yield proxy
    proxy.close()


@pytest.fixture
def signer(object_store, bucket, relay):
    """Upload PEOPLE and sign GET URLs for it through the relay; ``signer.fresh`` holds the URLs
    ``signer.refresh`` (the refresh callback) handed out."""
    object_store.put(bucket, "in/people.csv", PEOPLE)
    client = dataclasses.replace(object_store, endpoint=relay.endpoint).client()

    class Signer:
        def __init__(self):
            self.fresh: List[str] = []

        def sign(self, expires: int) -> str:
            return client.generate_presigned_url(
                "get_object", Params={"Bucket": bucket, "Key": "in/people.csv"}, ExpiresIn=expires
            )

        def refresh(self) -> str:
            self.fresh.append(self.sign(600))
            return self.fresh[-1]

    return Signer()


def _staged_output(tmp_path):
    staged = tmp_path / "staged"
    (staged / "in").mkdir(parents=True)
    (staged / "in" / "people.csv").write_text(PEOPLE)
    local = _spec("https://unused.invalid/")
    local["input"]["location"] = {"type": "file", "path": "in/people.csv"}
    return run_job(local, base_dir=staged), pq.read_table(staged / "out" / "data.parquet")


class TestFreshUrls:
    def test_a_url_that_expires_mid_stream_is_refreshed_and_the_output_is_the_same(
        self, relay, signer, tmp_path
    ):
        relay.cut = 100_000
        url = signer.sign(LIFETIME)
        relay.hold_until = time.time() + LIFETIME + 1  # the resume reaches the store expired
        streamed = tmp_path / "streamed"
        streamed.mkdir()

        result = run_job(
            _spec(url),
            base_dir=streamed,
            allowed_url_hosts=["127.0.0.1"],
            refresh_input_url=signer.refresh,
        )
        expected, table = _staged_output(tmp_path)

        assert result.status == "succeeded", result.error
        assert result.counts == expected.counts and result.counts["total_rows"] == ROWS
        assert pq.read_table(streamed / "out" / "data.parquet").equals(table)
        assert relay.statuses.count(403) == 1, "the store refused the expired URL once"
        assert len(signer.fresh) == 1
        for secret in (urllib.parse.urlsplit(url).query, signer.fresh[0]):
            assert secret not in str(result.to_dict())

    def test_an_expired_url_is_replaced_before_it_is_used(self, relay, signer, tmp_path):
        url = signer.sign(1)
        time.sleep(2)
        result = run_job(
            _spec(url),
            base_dir=tmp_path,
            allowed_url_hosts=["127.0.0.1"],
            refresh_input_url=signer.refresh,
        )
        assert result.status == "succeeded", result.error
        assert result.counts["total_rows"] == ROWS
        assert len(signer.fresh) == 1 and 403 not in relay.statuses

    def test_without_a_refresh_callback_an_expired_url_fails_the_job(
        self, relay, signer, tmp_path
    ):
        url = signer.sign(1)
        time.sleep(2)
        result = run_job(_spec(url), base_dir=tmp_path, allowed_url_hosts=["127.0.0.1"])
        assert result.error.code == "PERMISSION_DENIED"
        assert "refused the presigned URL (HTTP 403)" in result.error.message
        assert relay.statuses == [403]
