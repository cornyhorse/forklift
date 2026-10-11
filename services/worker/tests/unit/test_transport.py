"""HTTP on the standard library: no redirects, http(s) only, errors that name the host only,
no proxy from the environment and, when asked, every request through one proxy."""

from __future__ import annotations

import socket
import threading

import pytest
from forward_proxy import ForwardProxy

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


# --------------------------------------------------------------------------- proxies


@pytest.fixture
def forward_proxy():
    proxies = []

    def start(allow=None) -> ForwardProxy:
        proxy = ForwardProxy(allow).__enter__()
        proxies.append(proxy)
        return proxy

    yield start
    for proxy in proxies:
        proxy.__exit__(None, None, None)


def test_proxies_in_the_environment_are_ignored(raw, forward_proxy, monkeypatch):
    # urllib's default would send this request, the worker token with it, to that proxy.
    ambient = forward_proxy()
    for name in ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy", "ALL_PROXY"):
        monkeypatch.setenv(name, ambient.url)
    monkeypatch.delenv("NO_PROXY", raising=False)
    monkeypatch.delenv("no_proxy", raising=False)
    server = raw(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}")

    assert client().request("GET", server.url).status == 200
    assert server.received and ambient.requests == []


def test_a_store_proxy_carries_every_request_whatever_no_proxy_says(
    raw, forward_proxy, monkeypatch
):
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost,*")
    monkeypatch.setenv("no_proxy", "*")
    proxy = forward_proxy()
    server = raw(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
    http = HttpClient(timeout=5, user_agent="test", proxy=proxy.url)

    response = http.request("PUT", server.url, headers={"Content-MD5": "x"}, body=b"data")

    assert response.status == 200 and response.body == b"ok"
    assert http.proxy == f"127.0.0.1:{proxy.port}"
    (sent,) = proxy.requests
    assert sent["method"] == "PUT"
    assert sent["target"] == server.url, "an absolute URL, its signature for the store only"
    assert sent["headers"]["host"] == f"127.0.0.1:{server.port}", "the Host that was signed"
    assert sent["headers"]["content-md5"] == "x"
    assert server.received[0].startswith(b"PUT /object?X-Amz-Signature=secret-signature HTTP/1.1")


def test_https_goes_through_a_tunnel_and_errors_name_the_proxy(raw, forward_proxy):
    proxy = forward_proxy()
    server = raw(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")  # not TLS
    http = HttpClient(timeout=5, user_agent="test", proxy=proxy.url)

    with pytest.raises(TransportError) as caught:
        http.request("GET", f"https://127.0.0.1:{server.port}/key?X-Amz-Signature=secret")

    assert proxy.targets("CONNECT") == [f"127.0.0.1:{server.port}"]
    assert "TLS error" in caught.value.reason
    assert caught.value.reason.endswith(f", through the store proxy 127.0.0.1:{proxy.port}")
    assert caught.value.host == f"127.0.0.1:{server.port}"


def test_a_refusal_of_the_proxy_is_told_from_one_of_the_store(raw, forward_proxy):
    refusing = forward_proxy(allow=[])
    http = HttpClient(timeout=5, user_agent="test", proxy=refusing.url)
    with pytest.raises(HttpStatusError) as caught:
        http.request("GET", "http://store.example.org:9000/key?X-Amz-Signature=secret")
    assert caught.value.status == 403
    assert caught.value.detail == (
        f"refused by the store proxy 127.0.0.1:{refusing.port} (ERR_ACCESS_DENIED)"
    )
    assert "secret" not in str(caught.value)

    proxy = forward_proxy()
    store = raw(b"HTTP/1.1 503 Busy\r\nContent-Length: 30\r\n\r\n<Error><Code>SlowDown</Code>  ")
    http = HttpClient(timeout=5, user_agent="test", proxy=proxy.url)
    with pytest.raises(HttpStatusError) as caught:
        http.request("GET", store.url)
    assert caught.value.status == 503 and caught.value.detail == "SlowDown"

    squid_header_from_a_store = raw(
        b"HTTP/1.1 404 Not Found\r\nX-Squid-Error: ERR_ACCESS_DENIED 0\r\n"
        b"Content-Length: 0\r\n\r\n"
    )
    with pytest.raises(HttpStatusError) as caught:
        client().request("GET", squid_header_from_a_store.url)
    assert caught.value.detail == "", "without a proxy, no proxy refused anything"


def test_an_unreachable_proxy_is_named():
    unused = socket.create_server(("127.0.0.1", 0))
    port = unused.getsockname()[1]
    unused.close()
    http = HttpClient(timeout=5, user_agent="test", proxy=f"http://127.0.0.1:{port}")
    with pytest.raises(TransportError) as caught:
        http.request("GET", "http://store.example.org/key")
    assert caught.value.reason == (
        f"ConnectionRefusedError: Connection refused, through the store proxy 127.0.0.1:{port}"
    )
    assert caught.value.host == "store.example.org"
