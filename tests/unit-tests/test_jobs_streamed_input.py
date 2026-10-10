"""Streamed inputs (``presigned_url``) against a local HTTP server.

The server supports Range requests, ETags and ``If-Match`` like an S3-compatible store, and can
be told to drop connections part way through a response, ignore ranges, redirect or answer with
an error status. The tests check that the stream resumes where it stopped, that header detection
reads the start in small ranges, that only allowed hosts are contacted, and that a streamed CSV
gives the same output as the same file staged locally.
"""

from __future__ import annotations

import socket
import threading
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List

import pyarrow.parquet as pq
import pytest

from forklift.engine.exceptions import INPUT_UNREADABLE, PERMISSION_DENIED, SPEC_INVALID
from forklift.jobs import run_job
from forklift.jobs.http_input import (
    HEAD_CHUNK_BYTES,
    PresignedUrlSource,
    RemoteInputError,
    check_url,
    redact_url,
)

ROWS = 20_000
PEOPLE = ("id,name,age\n" + "".join(f"{i},name{i},{20 + i % 50}\n" for i in range(ROWS))).encode()
SIGNATURE = "X-Amz-Signature=0123456789abcdef"


class Store:
    """What the server serves and how it misbehaves; records every request."""

    def __init__(self):
        self.objects: Dict[str, bytes] = {}
        self.etags: Dict[str, str] = {}
        self.requests: List[Dict[str, str]] = []
        # Bytes into the body after which the next responses drop: for open-ended ranges (the
        # forward stream) and for closed ones (header detection)
        self.drops: List[int] = []
        self.head_drops: List[int] = []
        self.statuses: List[int] = []  # statuses for the next responses
        self.ignore_range = False
        self.redirect_to = None
        self.lock = threading.Lock()

    def put(self, path: str, data: bytes, etag: str = '"v1"') -> None:
        self.objects[path] = data
        self.etags[path] = etag


class _Handler(BaseHTTPRequestHandler):
    store: Store

    def log_message(self, *args):  # keep test output quiet
        pass

    def finish(self):
        try:
            super().finish()
        except OSError:
            pass

    def do_GET(self):
        store = self.store
        path = self.path.split("?")[0]
        with store.lock:
            store.requests.append(
                {
                    "path": path,
                    "query": self.path.partition("?")[2],
                    "range": self.headers.get("Range", ""),
                    "if_match": self.headers.get("If-Match", ""),
                }
            )
            status = store.statuses.pop(0) if store.statuses else None
            drops = (
                store.drops if self.headers.get("Range", "").endswith("-") else store.head_drops
            )
            drop = drops.pop(0) if drops else None
        if store.redirect_to and path == "/redirect":
            self.send_response(302)
            self.send_header("Location", store.redirect_to)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if status is not None:
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if path not in store.objects:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        data, etag = store.objects[path], store.etags[path]
        if self.headers.get("If-Match") and self.headers["If-Match"] != etag:
            self.send_response(412)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        start, end, partial = 0, len(data) - 1, False
        requested = self.headers.get("Range", "")
        if requested.startswith("bytes=") and not store.ignore_range:
            first, _, last = requested[len("bytes=") :].partition("-")
            start = int(first)
            end = min(int(last), len(data) - 1) if last else len(data) - 1
            if start >= len(data):
                self.send_response(416)
                self.send_header("Content-Range", f"bytes */{len(data)}")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            partial = True
        body = data[start : end + 1]
        self.send_response(206 if partial else 200)
        if partial:
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("ETag", etag)
        self.end_headers()
        if drop is None:
            self.wfile.write(body)
            return
        self.wfile.write(body[:drop])
        self.wfile.flush()
        self.connection.shutdown(socket.SHUT_RDWR)
        self.close_connection = True


@pytest.fixture
def store():
    state = Store()
    handler = type("Handler", (_Handler,), {"store": state})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    state.host = "127.0.0.1"
    state.base = f"http://127.0.0.1:{server.server_address[1]}"
    state.put("/bucket/people.csv", PEOPLE)
    yield state
    server.shutdown()
    server.server_close()


def _source(store, path="/bucket/people.csv", **kwargs):
    kwargs.setdefault("sleep", lambda seconds: None)
    kwargs.setdefault("allowed_hosts", [store.host])
    return PresignedUrlSource(f"{store.base}{path}?{SIGNATURE}", **kwargs)


def _read(stream) -> bytes:
    chunks = []
    while True:
        chunk = stream.read(100_000)
        if not chunk:
            stream.close()
            return b"".join(chunks)
        chunks.append(chunk)


class TestUrlChecks:
    def test_allowed_hosts_with_and_without_port(self):
        assert check_url("https://store.example.org/b/k?sig=1", ["Store.Example.org"]).hostname
        assert check_url("http://store:9000/b/k", ["store:9000"])
        assert check_url("https://store/b/k", ["store:443"])

    @pytest.mark.parametrize(
        "url, allowed, fragment",
        [
            ("https://evil.example/b/k?sig=1", ["store"], "host 'evil.example' is not one of"),
            ("https://store:8443/b/k", ["store:9000"], "host 'store' is not one of"),
            ("https://store/b/k", [], "allowed_url_hosts: none"),
            ("https://user:pw@store/b/k", ["store"], "must not contain user information"),
            ("file:///etc/passwd", ["store"], "must be an http:// or https:// URL"),
        ],
    )
    def test_refused_urls_are_spec_errors_without_the_query(self, url, allowed, fragment):
        with pytest.raises(RemoteInputError) as caught:
            check_url(url, allowed)
        assert fragment in str(caught.value)
        assert caught.value.error_code == SPEC_INVALID
        assert "sig=1" not in str(caught.value)

    def test_redact_url_drops_secrets(self):
        assert redact_url("https://u:p@store:9000/b/k?X-Amz-Signature=s#f") == (
            "https://store:9000/b/k"
        )
        assert redact_url("http://[::1]:9000/b/k?s=1") == "http://[::1]:9000/b/k"

    def test_repr_and_name_hide_the_signature(self, store):
        source = _source(store)
        assert SIGNATURE not in repr(source) and SIGNATURE not in source.name
        assert source.name == f"{store.base}/bucket/people.csv"


class TestForwardStream:
    def test_whole_object_in_one_request(self, store):
        source = _source(store)

        assert _read(source.open()) == PEOPLE
        assert [r["range"] for r in store.requests] == ["bytes=0-"]
        assert store.requests[0]["query"] == SIGNATURE
        assert source.size == len(PEOPLE) and source.etag == '"v1"'

    def test_dropped_connection_resumes_with_a_range_and_if_match(self, store):
        store.drops = [100_000, 50_000]
        waits = []
        source = _source(store, sleep=waits.append, retry_delay=0.25)

        assert _read(source.open()) == PEOPLE
        ranges = [r["range"] for r in store.requests]
        assert ranges == ["bytes=0-", "bytes=100000-", "bytes=150000-"]
        assert [r["if_match"] for r in store.requests[1:]] == ['"v1"', '"v1"']
        assert waits == [0.25, 0.25]  # progress in between resets the back-off

    def test_known_etag_is_sent_from_the_first_request(self, store):
        source = _source(store, etag="v1", size=len(PEOPLE))
        assert _read(source.open()) == PEOPLE
        assert store.requests[0]["if_match"] == '"v1"'
        assert _source(store, etag='W/"weak"').etag == 'W/"weak"'

    def test_gives_up_after_repeated_drops_without_progress(self, store):
        store.drops = [1000] + [0] * 10
        waits = []
        source = _source(store, sleep=waits.append, max_retries=3, retry_delay=1)

        with pytest.raises(RemoteInputError) as caught:
            _read(source.open())
        assert caught.value.retryable and caught.value.error_code == INPUT_UNREADABLE
        assert "failed 4 times in a row at byte 1000" in str(caught.value)
        assert waits == [1, 2, 4]

    def test_server_errors_are_retried(self, store):
        store.statuses = [503, 429]
        assert _read(_source(store).open()) == PEOPLE
        assert len(store.requests) == 3

    def test_unreachable_server_gives_up_with_a_retryable_error(self, store):
        source = _source(store, max_retries=1)
        source._url = "http://127.0.0.1:1/bucket/people.csv"  # nothing listens there
        with pytest.raises(RemoteInputError) as caught:
            _read(source.open())
        assert caught.value.retryable

    def test_changed_object_is_refused_on_resume(self, store):
        store.drops = [10_000]
        source = _source(store)
        stream = source.open()
        stream.read(5000)
        store.put("/bucket/people.csv", PEOPLE, etag='"v2"')
        with pytest.raises(RemoteInputError, match="changed while it was being read"):
            _read(stream)

    def test_server_ignoring_ranges_cannot_resume(self, store):
        store.drops = [10_000]
        store.ignore_range = True
        with pytest.raises(RemoteInputError, match="ignored the Range request"):
            _read(_source(store).open())

    def test_size_mismatch_is_refused(self, store):
        with pytest.raises(RemoteInputError, match=f"is {len(PEOPLE)} bytes, not the 5 bytes"):
            _read(_source(store, size=5).open())

    def test_full_response_to_a_ranged_request_is_accepted_at_the_start(self, store):
        store.ignore_range = True
        assert _read(_source(store).open()) == PEOPLE

    @pytest.mark.parametrize(
        "status, code, fragment",
        [
            (403, PERMISSION_DENIED, "refused the presigned URL (HTTP 403)"),
            (401, PERMISSION_DENIED, "refused the presigned URL (HTTP 401)"),
            (404, INPUT_UNREADABLE, "does not exist (HTTP 404"),
            (418, INPUT_UNREADABLE, "answered HTTP 418"),
        ],
    )
    def test_error_statuses(self, store, status, code, fragment):
        store.statuses = [status]
        with pytest.raises(RemoteInputError) as caught:
            _read(_source(store).open())
        assert caught.value.error_code == code and fragment in str(caught.value)
        assert SIGNATURE not in str(caught.value)

    def test_empty_object(self, store):
        store.put("/bucket/empty.csv", b"")
        source = _source(store, path="/bucket/empty.csv")
        assert _read(source.open()) == b""
        assert source.size == 0
        assert _read(_source(store, path="/bucket/empty.csv").open_head()) == b""

    def test_same_host_redirect_is_followed(self, store):
        store.redirect_to = f"{store.base}/bucket/people.csv?{SIGNATURE}"
        assert _read(_source(store, path="/redirect").open()) == PEOPLE

    @pytest.mark.parametrize("target", ["http://other.example/b/k", "ftp://127.0.0.1/x"])
    def test_redirect_to_another_host_is_refused(self, store, target):
        store.redirect_to = target
        with pytest.raises(RemoteInputError, match="redirected the input to another host"):
            _read(_source(store, path="/redirect").open())

    def test_unexpected_content_range_is_refused(self, store):
        class WrongRange:
            status = 206
            headers = {"Content-Range": "bytes 5-9/10"}

            def close(self):
                pass

        source = _source(store)
        with pytest.raises(RemoteInputError, match="with a different range"):
            source._learn(WrongRange(), 0)

    def test_unknown_total_in_content_range(self, store):
        class Unknown:
            status = 206
            headers = {"Content-Range": "bytes 0-9/*"}

        source = _source(store)
        source._learn(Unknown(), 0)
        assert source.size is None


class TestHead:
    def test_head_is_read_in_small_ranges(self, store):
        source = _source(store)
        head = source.open_head()
        assert len(head.read(HEAD_CHUNK_BYTES)) == HEAD_CHUNK_BYTES
        assert len(head.read(10)) == 10
        head.close()

        assert [r["range"] for r in store.requests] == [
            f"bytes=0-{HEAD_CHUNK_BYTES - 1}",
            f"bytes={HEAD_CHUNK_BYTES}-{2 * HEAD_CHUNK_BYTES - 1}",
        ]

    def test_head_of_a_small_object_ends(self, store):
        store.put("/bucket/small.csv", b"a,b\n1,2\n")
        assert _read(_source(store, path="/bucket/small.csv").open_head()) == b"a,b\n1,2\n"
        assert len(store.requests) == 1

    def test_dropped_range_is_fetched_again(self, store):
        store.head_drops = [100]
        assert len(_source(store).open_head().read(HEAD_CHUNK_BYTES)) == HEAD_CHUNK_BYTES
        assert [r["range"] for r in store.requests] == [f"bytes=0-{HEAD_CHUNK_BYTES - 1}"] * 2

    def test_gives_up_after_repeated_drops(self, store):
        store.head_drops = [100] * 5
        with pytest.raises(RemoteInputError, match="start of the input .* failed 3 times"):
            _source(store, max_retries=2).open_head().read(10)


class _FakeResponse:
    """A response whose body comes in the given pieces; an exception in them is raised."""

    def __init__(self, *pieces, close_error=False):
        self.pieces = list(pieces)
        self.close_error = close_error

    def readinto(self, buffer):
        piece = self.pieces.pop(0) if self.pieces else b""
        if isinstance(piece, BaseException):
            raise piece
        buffer[: len(piece)] = piece
        return len(piece)

    def read(self, size):
        return self.pieces.pop(0) if self.pieces else b""

    def close(self):
        if self.close_error:
            raise OSError("already gone")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class TestStreamEdges:
    def _source(self, store, responses, size=None):
        source = _source(store, size=size)
        source.get = lambda start, end=None: responses.pop(0)
        return source

    def test_a_read_that_raises_resumes(self, store):
        responses = [
            _FakeResponse(b"abc", ConnectionResetError("reset"), close_error=True),
            _FakeResponse(b"def"),
        ]
        stream = self._source(store, responses, size=6).open()
        assert stream.readable() and _read(stream) == b"abcdef"

    def test_short_head_of_unknown_size_ends(self, store):
        head = self._source(store, [_FakeResponse(b"short")]).open_head()
        assert head.readable()
        assert _read(head) == b"short"

    def test_range_past_the_end_is_the_end(self, store):
        source = _source(store, size=len(PEOPLE))
        assert source.get(len(PEOPLE)) is None
        assert source.size == len(PEOPLE)


def _spec(store, kind="run", path="/bucket/people.csv", **extra):
    location = {"type": "presigned_url", "url": f"{store.base}{path}?{SIGNATURE}"}
    location.update(extra.pop("location", {}))
    spec = {
        "spec_version": 1,
        "job_id": "streamed",
        "kind": kind,
        "input": {"format": "csv", "location": location, "options": extra.pop("options", {})},
        "schema": {
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "age": {"type": "integer"},
            }
        },
        "output": {"location": {"type": "file", "path": "out/"}},
    }
    spec.update(extra)
    return spec


class TestRunJob:
    def test_streamed_csv_gives_the_same_output_as_the_staged_file(self, store, tmp_path):
        streamed = tmp_path / "streamed"
        streamed.mkdir()
        store.drops = [300_000]  # one dropped connection on the way
        result = run_job(_spec(store), base_dir=streamed, allowed_url_hosts=[store.host])

        staged = tmp_path / "staged"
        (staged / "in").mkdir(parents=True)
        (staged / "in" / "people.csv").write_bytes(PEOPLE)
        local = _spec(store)
        local["input"]["location"] = {"type": "file", "path": "in/people.csv"}
        expected = run_job(local, base_dir=staged)

        assert result.status == "succeeded", result.error
        assert (
            result.counts
            == expected.counts
            == {
                "total_rows": ROWS,
                "valid_rows": ROWS,
                "invalid_rows": 0,
                "truncated_rows": 0,
            }
        )
        data = pq.read_table(streamed / "out" / "data.parquet")
        assert data.equals(pq.read_table(staged / "out" / "data.parquet"))
        resumed = [r for r in store.requests if r["range"] not in ("bytes=0-",)]
        assert any(r["range"] == "bytes=300000-" for r in resumed)
        metadata = (streamed / "out" / "metadata.json").read_text()
        assert SIGNATURE not in metadata and "/bucket/people.csv" in metadata

    def test_streamed_footer_detection_needs_no_copy(self, store, tmp_path):
        store.put("/bucket/footer.csv", b"id,name,age\n1,a,2\n3,b,4\nTOTAL,2,\n")
        spec = _spec(
            store,
            path="/bucket/footer.csv",
            options={"footer_detection": {"column_index": 0, "patterns": ["^TOTAL$"]}},
        )
        result = run_job(spec, base_dir=tmp_path, allowed_url_hosts=[store.host])
        assert result.status == "succeeded", result.error
        assert result.counts["total_rows"] == 2

    def test_host_not_allowed_is_spec_invalid(self, store, tmp_path):
        result = run_job(_spec(store), base_dir=tmp_path, allowed_url_hosts=["store.example"])
        assert result.error.code == SPEC_INVALID
        assert "host '127.0.0.1' is not one of" in result.error.message
        assert SIGNATURE not in result.error.message
        assert store.requests == []

    def test_known_size_over_the_limit_is_refused_before_reading(self, store, tmp_path):
        spec = _spec(store, location={"size": len(PEOPLE)}, limits={"max_input_bytes": 1000})
        result = run_job(spec, base_dir=tmp_path, allowed_url_hosts=[store.host])
        assert result.error.code == "LIMIT_EXCEEDED" and store.requests == []

    def test_bytes_read_over_the_limit_stop_the_job(self, store, tmp_path):
        spec = _spec(store, limits={"max_input_bytes": 1000})
        result = run_job(spec, base_dir=tmp_path, allowed_url_hosts=[store.host])
        assert result.error.code == "LIMIT_EXCEEDED"
        assert "more than 1000 bytes" in result.error.message

    def test_refused_url_is_permission_denied(self, store, tmp_path):
        store.statuses = [403]
        result = run_job(_spec(store), base_dir=tmp_path, allowed_url_hosts=[store.host])
        assert result.error.code == PERMISSION_DENIED and not result.error.retryable
        assert SIGNATURE not in result.error.message

    @pytest.mark.parametrize("kind", ["preview", "validate_schema", "generate_schema"])
    def test_interactive_kinds_read_only_the_start(self, store, tmp_path, kind):
        spec = _spec(store, kind=kind)
        spec["options"] = {"sample_rows": 10} if kind != "preview" else {"preview_rows": 10}
        result = run_job(spec, base_dir=tmp_path, allowed_url_hosts=[store.host])
        assert result.status == "succeeded", result.error
        assert result.counts["total_rows"] == 10
        if kind == "preview":
            assert all(r["range"].endswith(str(HEAD_CHUNK_BYTES - 1)) for r in store.requests)


def test_proxy_settings_of_the_environment_are_used(monkeypatch, store):
    """The default opener honours HTTPS_PROXY (an egress proxy can allow-list the store)."""
    for name in ("http_proxy", "https_proxy", "all_proxy", "no_proxy"):
        monkeypatch.delenv(name, raising=False)
        monkeypatch.delenv(name.upper(), raising=False)
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example:3128")
    handlers = _source(store)._opener.handlers
    proxies = [h.proxies for h in handlers if isinstance(h, urllib.request.ProxyHandler)]
    assert [p.get("https") for p in proxies] == ["http://proxy.example:3128"]
