"""The webhook delivery client: what the SSRF guard refuses (with a stub resolver, so nothing is
looked up or contacted), and real deliveries to receivers on local ports over HTTP and HTTPS
(a CA of the tests' own), with redirects, timeouts and broken answers."""

from __future__ import annotations

import socket
import ssl
import threading
import time
import types

import pytest
from webhook_support import Receiver, StubResolver, certificates, server_context

from forklift_web import webhook_client
from forklift_web.webhook_client import Client, Outcome, UrlRefused, check_url, is_public

BODY = b'{"event":"webhook.test"}'
HEADERS = {"Content-Type": "application/json", "Forklift-Event": "webhook.test"}


@pytest.mark.parametrize(
    "address",
    [
        "8.8.8.8",
        "1.1.1.1",
        "2606:4700:4700::1111",
        "::ffff:8.8.8.8",  # IPv4-mapped: counts as 8.8.8.8
        "64:ff9b::808:808",  # NAT64 of 8.8.8.8
    ],
)
def test_public_addresses(address):
    assert is_public(address)


@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",  # loopback
        "127.255.255.254",
        "10.1.2.3",  # private
        "172.16.0.1",
        "192.168.1.1",
        "169.254.169.254",  # link-local: the cloud metadata address
        "169.254.1.1",
        "100.64.0.1",  # shared address space (CGNAT)
        "100.100.100.200",
        "0.0.0.0",  # unspecified
        "0.1.2.3",  # "this network"
        "224.0.0.1",  # multicast
        "239.255.255.250",
        "240.0.0.1",  # reserved
        "255.255.255.255",  # broadcast
        "192.0.2.10",  # documentation
        "198.18.0.1",  # benchmarking
        "::1",  # IPv6 loopback
        "::",  # IPv6 unspecified
        "fe80::1",  # link-local
        "fe80::1%1",
        "fc00::1",  # unique local
        "fd00:ec2::254",  # the AWS metadata address over IPv6
        "ff02::1",  # multicast
        "2001:db8::1",  # documentation
        "100::1",  # discard
        "::ffff:127.0.0.1",  # IPv4-mapped loopback
        "::ffff:169.254.169.254",  # IPv4-mapped metadata
        "::ffff:10.0.0.1",
        "::127.0.0.1",  # IPv4-compatible (deprecated) loopback
        "::10.0.0.1",
        "64:ff9b::7f00:1",  # NAT64 of loopback
        "64:ff9b::a9fe:a9fe",  # NAT64 of the metadata address
        "2002:7f00:1::1",  # 6to4
        "2001::1",  # Teredo
    ],
)
def test_addresses_that_are_not_public(address):
    assert not is_public(address)


# --------------------------------------------------------------------------- the URL's form


@pytest.mark.parametrize(
    "url,message",
    [
        ("http://example.org/hook", "must start with https://"),
        ("ftp://example.org/hook", "must start with https://"),
        ("example.org/hook", "must start with https://"),
        ("https://user:password@example.org/hook", "must not contain a user name or password"),
        ("https://user@example.org/hook", "must not contain a user name or password"),
        ("https://:x@example.org/", "must not contain a user name or password"),
        ("https:///hook", "needs a host name or an IP address"),
        ("https://exa mple.org/", "printable ASCII characters without spaces"),
        ("https://example.org/a\nb", "printable ASCII characters without spaces"),
        ("https://exämple.org/", "xn-- form"),
        ("https://example.org/" + "a" * 2048, "at most 2048"),
        ("https://example.org:99999/", "port of a webhook URL"),
        ("https://example.org:x/", "port of a webhook URL"),
        ("https://example.org:0/", "port of a webhook URL"),
        ("https://example.org/hook#part", "must not have a fragment"),
        ("https://exa<mple.org/", "needs a host name or an IP address"),
        ("https://127.0.0.1/hook", "127.0.0.1 is not a publicly routable address"),
        ("https://[::1]/hook", "::1 is not a publicly routable address"),
        ("https://[::ffff:169.254.169.254]/", "is not a publicly routable address"),
        ("https://169.254.169.254/latest/meta-data/", "is not a publicly routable address"),
        (None, "printable ASCII"),
    ],
)
def test_urls_refused_by_their_form(url, message, settings):
    settings.FORKLIFT_WEBHOOK_ALLOW_HTTP = False
    settings.FORKLIFT_WEBHOOK_ALLOWED_HOSTS = []
    with pytest.raises(UrlRefused, match=message):
        check_url(url)


def test_urls_webhooks_may_use(settings):
    settings.FORKLIFT_WEBHOOK_ALLOW_HTTP = False
    settings.FORKLIFT_WEBHOOK_ALLOWED_HOSTS = ["10.0.0.5"]
    target = check_url("https://Hooks.Example.ORG./in/forklift?key=abc&x=1")
    assert target == webhook_client.Target(
        "https", "hooks.example.org", 443, "/in/forklift?key=abc&x=1"
    )
    assert check_url("https://example.org").path == "/"
    assert check_url("https://example.org:8443/x").port == 8443
    assert check_url("https://8.8.8.8/x").host == "8.8.8.8"
    assert check_url("https://[2606:4700::1111]:444/").host == "2606:4700::1111"
    assert check_url("https://10.0.0.5/x").host == "10.0.0.5"  # an allowed host
    assert check_url("https://internal_host/").host == "internal_host"
    with pytest.raises(UrlRefused, match=r"this installation does not send webhooks over http"):
        check_url("http://example.org/")
    settings.FORKLIFT_WEBHOOK_ALLOW_HTTP = True
    assert check_url("http://example.org/").port == 80
    with pytest.raises(UrlRefused, match=r"must start with https:// or http://\.$"):
        check_url("ftp://example.org/")


# --------------------------------------------------------------------------- refusals when sending


@pytest.fixture
def no_connections(monkeypatch):
    """Fails the test if the client tries to connect anywhere."""

    def refuse(*args, **kwargs):  # pragma: no cover - reached only when the guard fails
        raise AssertionError(f"the client connected to {args}")

    monkeypatch.setattr(webhook_client.socket, "create_connection", refuse)


@pytest.mark.parametrize(
    "addresses",
    [
        ["127.0.0.1"],
        ["::1"],
        ["10.0.0.7"],
        ["192.168.0.10"],
        ["169.254.169.254"],
        ["100.64.12.1"],
        ["0.0.0.0"],
        ["224.0.0.251"],
        ["fd00::1"],
        ["fe80::1%2"],
        ["::ffff:127.0.0.1"],
        ["::ffff:169.254.169.254"],
        ["64:ff9b::7f00:1"],
        ["8.8.8.8", "127.0.0.1"],  # one address that is not public is enough to refuse
        ["2606:4700::1111", "fd00::1"],
    ],
)
def test_hosts_that_resolve_to_addresses_that_are_not_public_are_refused(
    addresses, no_connections
):
    resolver = StubResolver({"hooks.example.org": addresses})
    client = Client(allowed_hosts=[], allow_http=False, resolver=resolver)
    outcome = client.post("https://hooks.example.org/in", BODY, HEADERS)
    assert outcome == Outcome(
        False,
        None,
        "hooks.example.org resolves to an address that is not publicly routable; webhooks are "
        "sent only to public addresses unless the deployment lists the host in "
        "FORKLIFT_WEBHOOK_ALLOWED_HOSTS.",
    )
    assert resolver.calls == [("hooks.example.org", 443)]
    assert not any(address in outcome.error for address in addresses)  # no DNS answers leak


@pytest.mark.parametrize(
    "host", ["localhost", "2130706433", "0x7f.1", "127.1", "0177.0.0.1", "017700000001"]
)
def test_other_spellings_of_loopback_are_refused_by_the_system_resolver(host, no_connections):
    # Not IP addresses to ipaddress, so they pass the URL's form; the resolver reads them as
    # 127.0.0.1, and the check after the lookup refuses them.
    outcome = Client(allowed_hosts=[], allow_http=False).post(f"https://{host}/", BODY, HEADERS)
    assert outcome.error == (
        f"{host} resolves to an address that is not publicly routable; webhooks are sent only "
        "to public addresses unless the deployment lists the host in "
        "FORKLIFT_WEBHOOK_ALLOWED_HOSTS."
    )


@pytest.mark.parametrize("answer", [socket.gaierror(-2, "Name or service not known"), []])
def test_hosts_that_do_not_resolve(answer, no_connections):
    client = Client(resolver=StubResolver({"nowhere.example": answer}))
    outcome = client.post("https://nowhere.example/", BODY, HEADERS)
    assert outcome == Outcome(False, None, "nowhere.example could not be resolved.")


def test_urls_refused_by_their_form_are_never_looked_up(no_connections):
    resolver = StubResolver({})
    for url in ("http://example.org/", "https://u:p@example.org/", "https://127.0.0.1/"):
        outcome = Client(allow_http=False, allowed_hosts=[], resolver=resolver).post(
            url, BODY, HEADERS
        )
        assert not outcome.delivered and outcome.status_code is None and outcome.error
    assert resolver.calls == []


def test_the_client_follows_the_deployment_settings(settings, no_connections):
    settings.FORKLIFT_WEBHOOK_ALLOW_HTTP = True
    settings.FORKLIFT_WEBHOOK_ALLOWED_HOSTS = ["receiver.internal"]
    client = Client(resolver=StubResolver({}))
    assert client.allow_http and client.allowed_hosts == {"receiver.internal"}
    assert client.tls_context.verify_mode == ssl.CERT_REQUIRED
    assert client.tls_context.check_hostname


def test_the_system_resolver():
    assert "127.0.0.1" in webhook_client.resolve("localhost", 80) or "::1" in (
        webhook_client.resolve("localhost", 80)
    )


# --------------------------------------------------------------------------- real deliveries


@pytest.fixture
def receiver():
    servers = []

    def start(status=200, tls=None) -> Receiver:
        servers.append(Receiver(status, tls))
        return servers[-1]

    yield start
    for server in servers:
        server.stop()


def _local(**fields) -> Client:
    """A client that may reach receiver.test (and only it) on 127.0.0.1 over http."""
    fields.setdefault("resolver", StubResolver({"receiver.test": ["127.0.0.1"]}))
    return Client(allowed_hosts=["receiver.test"], allow_http=True, **fields)


def test_a_delivery_to_an_allowed_host(receiver):
    server = receiver(204)
    resolver = StubResolver({"receiver.test": ["127.0.0.1"]})
    url = f"http://receiver.test:{server.port}/in/forklift?key=abc"
    outcome = _local(resolver=resolver).post(url, BODY, {**HEADERS, "Forklift-Delivery": "d-1"})
    assert outcome == Outcome(True, 204, "")
    [request] = server.requests
    assert (request.method, request.path, request.body) == ("POST", "/in/forklift?key=abc", BODY)
    assert request.headers["host"] == f"receiver.test:{server.port}"  # the name, not the address
    assert request.headers["forklift-delivery"] == "d-1"
    assert request.headers["content-length"] == str(len(BODY))
    # Looked up once; the connection went to the address that was checked (receiver.test is not
    # a real name, so a second lookup by anyone else would have failed).
    assert resolver.calls == [("receiver.test", server.port)]


def test_the_first_checked_address_that_answers_is_used(receiver):
    server = receiver()
    resolver = StubResolver({"receiver.test": ["127.0.0.2", "127.0.0.1"]})  # .2 refuses
    outcome = _local(resolver=resolver).post(f"http://receiver.test:{server.port}/", BODY, {})
    assert outcome.delivered and len(server.requests) == 1


def test_a_receiver_that_refuses_the_connection():
    with socket.socket() as probe:  # a port nobody listens on
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    outcome = _local().post(f"http://receiver.test:{port}/", BODY, {})
    assert outcome == Outcome(False, None, f"receiver.test:{port} refused the connection.")


@pytest.mark.parametrize("status", [301, 302, 307, 308])
def test_redirects_are_not_followed(receiver, status):
    server = receiver(status)
    outcome = _local().post(f"http://receiver.test:{server.port}/", BODY, {})
    assert outcome == Outcome(
        False,
        status,
        f"The receiver answered {status}; redirects are not followed, so give the webhook the "
        "URL it redirects to.",
    )
    assert len(server.requests) == 1


@pytest.mark.parametrize("status", [400, 404, 410, 500, 503])
def test_answers_other_than_2xx_are_failures(receiver, status):
    server = receiver(status)
    outcome = _local().post(f"http://receiver.test:{server.port}/", BODY, {})
    assert outcome == Outcome(False, status, f"The receiver answered {status}.")
    assert "the receiver's answer" not in outcome.error  # its body is never read or kept


def _serve_once(behaviour):
    """A raw TCP server on 127.0.0.1 that runs ``behaviour(connection)`` for one connection."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)

    def run():
        connection, _ = listener.accept()
        with connection:
            behaviour(connection)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return listener, thread


def _drip(connection):
    connection.recv(65536)
    for byte in b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n":
        time.sleep(0.1)
        try:
            connection.sendall(bytes([byte]))
        except OSError:
            return


def test_a_receiver_that_answers_a_byte_at_a_time_runs_out_of_time():
    listener, thread = _serve_once(_drip)
    port = listener.getsockname()[1]
    started = time.monotonic()
    outcome = _local(total_seconds=0.8).post(f"http://receiver.test:{port}/", BODY, {})
    elapsed = time.monotonic() - started
    assert outcome == Outcome(
        False,
        None,
        f"receiver.test:{port} did not answer in time (a delivery may take 0.8 seconds in all).",
    )
    assert elapsed < 2.5  # the whole exchange is bounded, not each read
    listener.close()
    thread.join(timeout=10)


def test_a_receiver_that_never_answers():
    listener = socket.socket()  # it listens, so connecting works, but nobody ever accepts
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]
    with listener:
        outcome = _local(total_seconds=0.3).post(f"http://receiver.test:{port}/", BODY, {})
    assert not outcome.delivered and "did not answer in time" in outcome.error


def test_no_time_left_after_the_lookup(monkeypatch):
    clock = {"offset": 0.0}
    monkeypatch.setattr(
        webhook_client,
        "time",
        types.SimpleNamespace(monotonic=lambda: time.monotonic() + clock["offset"]),
    )

    def slow(host, port):  # answers just as the delivery's time runs out
        clock["offset"] += 5
        return ["127.0.0.1"]

    outcome = _local(resolver=slow, total_seconds=1).post("http://receiver.test:9/", BODY, {})
    assert outcome.error == (
        "receiver.test:9 did not answer in time (a delivery may take 1 seconds in all)."
    )


def test_a_lookup_that_takes_too_long_is_given_up(no_connections):
    release = threading.Event()

    def stalled(host, port):
        release.wait(timeout=30)
        return ["8.8.8.8"]

    client = Client(resolver=stalled, total_seconds=0.3)
    started = time.monotonic()
    outcome = client.post("https://hooks.example.org/", BODY, HEADERS)
    elapsed = time.monotonic() - started
    release.set()
    assert outcome == Outcome(
        False,
        None,
        "hooks.example.org could not be resolved in time (a delivery may take 0.3 seconds in "
        "all).",
    )
    assert elapsed < 1.5  # the lookup goes on in its own thread; the delivery does not wait


def test_a_resolver_that_fails_in_another_way(no_connections):
    def broken(host, port):
        raise RuntimeError("the resolver is broken")

    outcome = Client(resolver=broken).post("https://hooks.example.org/", BODY, HEADERS)
    assert outcome.error == "hooks.example.org could not be resolved."


@pytest.mark.parametrize(
    "answer,message",
    [
        (b"hello there\r\n\r\n", "receiver.test did not answer with valid HTTP."),
        (b"", "The connection to receiver.test:{port} failed."),  # closed without an answer
    ],
)
def test_answers_that_are_not_http(answer, message):
    def reply(connection):
        connection.recv(65536)
        connection.sendall(answer)

    listener, thread = _serve_once(reply)
    port = listener.getsockname()[1]
    outcome = _local().post(f"http://receiver.test:{port}/", BODY, {})
    assert outcome == Outcome(False, None, message.format(port=port))
    listener.close()
    thread.join(timeout=10)


# --------------------------------------------------------------------------- TLS


@pytest.fixture
def tls(tmp_path):
    ca, cert, key = certificates(tmp_path, "receiver.test")
    return ca, server_context(cert, key)


def test_a_delivery_over_tls_is_verified_against_the_host_name(receiver, tls):
    ca, server_tls = tls
    server = receiver(200, server_tls)
    trusted = ssl.create_default_context(cadata=ca)
    client = Client(
        allowed_hosts=["receiver.test"],
        allow_http=False,
        resolver=StubResolver({"receiver.test": ["127.0.0.1"]}),
        tls_context=trusted,
    )
    outcome = client.post(f"https://receiver.test:{server.port}/hook", BODY, HEADERS)
    assert outcome == Outcome(True, 200, "")
    assert server.sni == ["receiver.test"]  # the host name went out as SNI
    assert server.requests[0].headers["host"] == f"receiver.test:{server.port}"


def test_certificates_that_cannot_be_verified(receiver, tls, tmp_path):
    ca, server_tls = tls
    server = receiver(200, server_tls)
    resolver = StubResolver({"receiver.test": ["127.0.0.1"], "other.test": ["127.0.0.1"]})
    allowed = ["receiver.test", "other.test"]
    untrusted = Client(allowed_hosts=allowed, resolver=resolver)  # the system's CAs only
    outcome = untrusted.post(f"https://receiver.test:{server.port}/", BODY, {})
    assert outcome == Outcome(
        False,
        None,
        "The TLS certificate of receiver.test could not be verified (expired, self-signed or "
        "for another host).",
    )
    wrong_host = Client(
        allowed_hosts=allowed, resolver=resolver, tls_context=ssl.create_default_context(cadata=ca)
    )
    outcome = wrong_host.post(f"https://other.test:{server.port}/", BODY, {})
    assert "TLS certificate of other.test could not be verified" in outcome.error
    assert server.requests == []


def _drip_tls(connection):
    connection.recv(65536)  # the client hello
    # A handshake record header announcing 64 bytes, then the bytes one by one
    for byte in b"\x16\x03\x03\x00\x40" + bytes(64):
        time.sleep(0.1)
        try:
            connection.sendall(bytes([byte]))
        except OSError:
            return


def test_a_receiver_that_sends_its_handshake_a_byte_at_a_time_runs_out_of_time():
    listener, thread = _serve_once(_drip_tls)
    port = listener.getsockname()[1]
    client = Client(
        allowed_hosts=["receiver.test"],
        resolver=StubResolver({"receiver.test": ["127.0.0.1"]}),
        total_seconds=0.8,
    )
    started = time.monotonic()
    outcome = client.post(f"https://receiver.test:{port}/", BODY, {})
    elapsed = time.monotonic() - started
    assert outcome == Outcome(
        False,
        None,
        f"receiver.test:{port} did not answer in time (a delivery may take 0.8 seconds in all).",
    )
    assert elapsed < 2.5  # the handshake is bounded as a whole, not each read in it
    listener.close()
    thread.join(timeout=10)


def test_a_receiver_that_does_not_speak_tls(receiver):
    server = receiver(200)  # plain HTTP on the port
    client = Client(
        allowed_hosts=["receiver.test"], resolver=StubResolver({"receiver.test": ["127.0.0.1"]})
    )
    outcome = client.post(f"https://receiver.test:{server.port}/", BODY, {})
    assert outcome == Outcome(
        False, None, f"The TLS handshake with receiver.test:{server.port} failed."
    )
