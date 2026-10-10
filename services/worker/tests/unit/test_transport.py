"""HTTP on the standard library: no redirects, http(s) only, errors that name the host only."""

from __future__ import annotations

import socket
import threading

import pytest

from forklift_worker.transport import (
    HttpClient,
    HttpStatusError,
    TransportError,
    error_detail,
    host_of,
    is_transient,
    port_of,
)


def client(timeout: float = 5) -> HttpClient:
    return HttpClient(timeout=timeout, user_agent="test")


class RawServer:
    """A one-connection-at-a-time TCP server that answers each request with ``reply`` bytes."""

    def __init__(self, reply: bytes | None, close_after: bool = True):
        self.reply = reply
        self.close_after = close_after
        self.socket = socket.create_server(("127.0.0.1", 0))
        self.port = self.socket.getsockname()[1]
        self.received: list[bytes] = []
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _serve(self) -> None:
        while True:
            try:
                connection, _ = self.socket.accept()
            except OSError:
                return
            with connection:
                self.received.append(connection.recv(65536))
                if self.reply is None:
                    connection.recv(1)  # never answer; wait for the client to give up
                    continue
                connection.sendall(self.reply)

    def close(self) -> None:
        self.socket.close()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/object?X-Amz-Signature=secret-signature"


@pytest.fixture
def raw():
    servers = []

    def start(reply: bytes | None) -> RawServer:
        server = RawServer(reply)
        servers.append(server)
        return server

    yield start
    for server in servers:
        server.close()


def test_a_request_and_its_response(raw):
    server = raw(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nX-Thing: yes\r\n\r\n{}")
    response = client().request("PUT", server.url, headers={"A": "b"}, body=b"data")
    assert response.status == 200
    assert response.headers["x-thing"] == "yes"
    assert response.json() == {}
    assert b"User-Agent: test" in server.received[0]


def test_redirects_are_not_followed(raw):
    server = raw(b"HTTP/1.1 302 Found\r\nLocation: http://elsewhere/\r\nContent-Length: 0\r\n\r\n")
    with pytest.raises(HttpStatusError) as caught:
        client().request("GET", server.url)
    assert caught.value.status == 302
    assert not caught.value.transient


def test_errors_name_the_host_but_never_the_url(raw):
    server = raw(
        b"HTTP/1.1 503 Busy\r\nContent-Length: 39\r\n\r\n<Error><Code>SlowDown</Code></Error>   "
    )
    with pytest.raises(HttpStatusError) as caught:
        client().request("GET", server.url)
    assert str(caught.value) == f"127.0.0.1:{server.port} answered HTTP 503: SlowDown"
    assert caught.value.transient and is_transient(caught.value)
    assert "secret-signature" not in str(caught.value)


def test_a_refused_connection_is_a_transport_error():
    unused = socket.create_server(("127.0.0.1", 0))
    port = unused.getsockname()[1]
    unused.close()
    with pytest.raises(TransportError) as caught:
        client().request("GET", f"http://127.0.0.1:{port}/")
    assert "ConnectionRefusedError" in caught.value.reason
    assert is_transient(caught.value)


def test_a_server_that_never_answers_times_out(raw):
    server = raw(None)
    with pytest.raises(TransportError, match="timed out"):
        client(timeout=0.2).request("GET", server.url)


def test_tls_errors_are_reported_as_such(raw):
    server = raw(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
    with pytest.raises(TransportError, match="TLS error"):
        client().request("GET", f"https://127.0.0.1:{server.port}/")


def test_an_oversized_or_truncated_body_is_a_transport_error(raw):
    big = raw(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n0123456789")
    with pytest.raises(TransportError, match="larger than 4 bytes"):
        client().request("GET", big.url, max_response_bytes=4)
    short = raw(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n01234")
    with pytest.raises(TransportError, match="closed after 5 of 10 bytes"):
        client().request("GET", short.url)


def test_a_download_reads_in_chunks_and_reports_a_cut(raw):
    whole = raw(b'HTTP/1.1 200 OK\r\nContent-Length: 6\r\nETag: "abc"\r\n\r\nabcdef')
    with client().download(whole.url) as response:
        assert response.headers["etag"] == '"abc"'
        assert list(response.chunks(4)) == [b"abcd", b"ef"]
    cut = raw(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nabc")
    with client().download(cut.url) as response:
        with pytest.raises(TransportError, match="closed after 3 of 6 bytes"):
            list(response.chunks(4))


def test_only_http_urls_are_opened():
    with pytest.raises(ValueError):
        client().request("GET", "file:///etc/passwd")
    with pytest.raises(ValueError):
        host_of("http:///no-host")


def test_hosts_and_ports():
    assert host_of("https://Store.Example.org/key") == "store.example.org"
    assert host_of("http://store:9000/key?x=1") == "store:9000"
    assert host_of("http://[::1]:9000/") == "[::1]:9000"
    assert port_of("https://store/") == 443
    assert port_of("http://store/") == 80
    assert port_of("http://store:9000/") == 9000


def test_error_details():
    assert error_detail(b'{"detail": "no such job"}') == "no such job"
    assert error_detail(b'{"other": 1}') == ""
    assert error_detail(b"[1]") == ""
    assert error_detail(b"<Error><Code>AccessDenied</Code></Error>") == "AccessDenied"
    assert error_detail(b"plain text") == ""
    assert not is_transient(ValueError("x"))


class FailingResponse:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def getheaders(self):
        return [("Content-Length", "5")]

    def read(self, *args):
        raise ConnectionResetError(104, "Connection reset by peer")


def test_read_errors_become_transport_errors(monkeypatch):
    http = client()
    monkeypatch.setattr(http, "_open", lambda host, request: FailingResponse())
    with pytest.raises(TransportError, match="ConnectionResetError: Connection reset by peer"):
        http.request("GET", "http://store/key")
    with http.download("http://store/key") as response:
        with pytest.raises(TransportError, match="Connection reset"):
            list(response.chunks(10))
