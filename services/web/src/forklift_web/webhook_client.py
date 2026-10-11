"""Sending webhook deliveries: an HTTP client that talks only to public addresses.

The gateway POSTs to URLs its users chose, from inside the installation's network, so every
request is guarded against server-side request forgery:

- only ``https://`` URLs (``http://`` too where FORKLIFT_WEBHOOK_ALLOW_HTTP is set), with a host
  and without user information;
- the host is resolved once, and every address it resolves to must be globally routable: not
  loopback, private, link-local (which includes the cloud metadata address 169.254.169.254),
  shared (CGNAT, 100.64.0.0/10), multicast, reserved or unspecified, counting IPv6 addresses
  that embed an IPv4 address (mapped, compatible, NAT64) as that address. Hosts listed in
  FORKLIFT_WEBHOOK_ALLOWED_HOSTS may resolve to any address (receivers on an internal network);
- the connection goes to exactly an address that was checked (there is no second lookup, so DNS
  rebinding cannot redirect it); TLS is verified against the host name, which is also the SNI
  and the Host header;
- redirects are not followed, the whole exchange (looking the host up, connecting, TLS, the
  request, the status line and headers of the answer) must finish within 10 seconds, and the
  answer's body is never read.

The client does not use proxies: it connects to receivers directly. Certificates are verified
with the system's CA certificates (``SSL_CERT_FILE`` / ``SSL_CERT_DIR`` point OpenSSL at others).
"""

from __future__ import annotations

import http.client
import io
import ipaddress
import re
import socket
import ssl
import threading
import time
from dataclasses import dataclass
from typing import Callable, Iterable, Optional
from urllib.parse import urlsplit

from django.conf import settings

CONNECT_SECONDS = 5.0
TOTAL_SECONDS = 10.0
MAX_URL_LENGTH = 2048
DEFAULT_PORTS = {"https": 443, "http": 80}
_HOST_NAME = re.compile(r"^[a-z0-9_-]+(\.[a-z0-9_-]+)*$")
_IPV4_COMPATIBLE = ipaddress.ip_network("::/96")
_NAT64 = ipaddress.ip_network("64:ff9b::/96")


class UrlRefused(Exception):
    """A webhook URL the gateway will not send to: its form, or the addresses its host resolves
    to (or does not resolve to in time)."""


@dataclass(frozen=True)
class Target:
    """Where a URL points: ``host`` is lower-case without a trailing dot, ``path`` includes the
    query string."""

    scheme: str
    host: str
    port: int
    path: str


@dataclass(frozen=True)
class Outcome:
    """How one POST went. ``status_code`` is None when no HTTP answer came; ``error`` is the
    gateway's own description of a failure, never text from the receiver."""

    delivered: bool
    status_code: Optional[int] = None
    error: str = ""


def is_public(address) -> bool:
    """Whether ``address`` is globally routable (an IPv6 form that embeds an IPv4 address counts
    as that IPv4 address)."""
    ip = ipaddress.ip_address(address)
    if ip.version == 6:
        embedded = ip.ipv4_mapped
        if embedded is None and (ip in _IPV4_COMPATIBLE or ip in _NAT64):
            embedded = ipaddress.IPv4Address(int(ip) & 0xFFFFFFFF)
        if embedded is not None:
            return is_public(embedded)
    # is_global already excludes private, loopback, link-local, shared (CGNAT) and unspecified
    # ranges; it counts multicast and some reserved IPv6 ranges as global.
    return ip.is_global and not ip.is_multicast and not ip.is_reserved


def _literal(host: str):
    try:
        return ipaddress.ip_address(host)
    except ValueError:
        return None


def check_url(
    url, *, allow_http: Optional[bool] = None, allowed_hosts: Optional[Iterable[str]] = None
) -> Target:
    """``url`` as a :class:`Target` if webhooks may use it, else UrlRefused. Only its form is
    checked here (and an IP address in it); the addresses a host name resolves to are checked
    each time a delivery is sent."""
    allow_http = settings.FORKLIFT_WEBHOOK_ALLOW_HTTP if allow_http is None else allow_http
    allowed = settings.FORKLIFT_WEBHOOK_ALLOWED_HOSTS if allowed_hosts is None else allowed_hosts
    if (
        not isinstance(url, str)
        or len(url) > MAX_URL_LENGTH
        or not url.isascii()
        or any(char.isspace() or not char.isprintable() for char in url)
    ):
        raise UrlRefused(
            f"A webhook URL is at most {MAX_URL_LENGTH} printable ASCII characters without "
            "spaces (write international domain names in their xn-- form)."
        )
    parts = urlsplit(url)
    schemes = ("https", "http") if allow_http else ("https",)
    if parts.scheme not in schemes:
        raise UrlRefused(
            "A webhook URL must start with "
            + (" or ".join(f"{scheme}://" for scheme in schemes))
            + ("." if allow_http else " (this installation does not send webhooks over http).")
        )
    if "@" in parts.netloc:
        raise UrlRefused(
            "A webhook URL must not contain a user name or password; deliveries are "
            "authenticated by their signature."
        )
    if parts.fragment:
        raise UrlRefused("A webhook URL must not have a fragment (#...).")
    host = (parts.hostname or "").rstrip(".")
    literal = _literal(host)
    if literal is None and not _HOST_NAME.match(host):
        raise UrlRefused("A webhook URL needs a host name or an IP address.")
    try:
        port = DEFAULT_PORTS[parts.scheme] if parts.port is None else parts.port
    except ValueError:
        port = 0
    if not port:
        raise UrlRefused("The port of a webhook URL must be a number from 1 to 65535.")
    if literal is not None and host not in allowed and not is_public(literal):
        raise UrlRefused(
            f"{host} is not a publicly routable address; webhooks are sent only to public "
            "addresses unless the deployment lists the host in FORKLIFT_WEBHOOK_ALLOWED_HOSTS."
        )
    path = (parts.path or "/") + (f"?{parts.query}" if parts.query else "")
    return Target(scheme=parts.scheme, host=host, port=port, path=path)


def resolve(host: str, port: int) -> list:
    """The addresses ``host`` resolves to for TCP, in the resolver's order."""
    infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    return list(dict.fromkeys(info[4][0] for info in infos))


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("The delivery's time is up.")
    return remaining


class _DeadlineSocket:
    """A connected socket whose every send and receive must finish before ``deadline``, so a
    receiver that answers a byte at a time cannot hold a delivery longer than that."""

    def __init__(self, sock, deadline: float):
        self._sock = sock
        self._deadline = deadline

    def sendall(self, data) -> None:
        self._sock.settimeout(_remaining(self._deadline))
        self._sock.sendall(data)

    def recv_into(self, buffer) -> int:
        self._sock.settimeout(_remaining(self._deadline))
        return self._sock.recv_into(buffer)

    def makefile(self, mode):
        return io.BufferedReader(_Reader(self))

    def close(self) -> None:
        self._sock.close()


class _Reader(io.RawIOBase):
    def __init__(self, sock: _DeadlineSocket):
        self._sock = sock

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:
        return self._sock.recv_into(buffer)


class Client:
    """POSTs webhook deliveries through the guard above. ``resolver`` and ``tls_context`` are
    there for tests; the other arguments default to the deployment settings."""

    def __init__(
        self,
        *,
        allowed_hosts: Optional[Iterable[str]] = None,
        allow_http: Optional[bool] = None,
        resolver: Callable[[str, int], list] = resolve,
        tls_context: Optional[ssl.SSLContext] = None,
        connect_seconds: float = CONNECT_SECONDS,
        total_seconds: float = TOTAL_SECONDS,
    ):
        hosts = settings.FORKLIFT_WEBHOOK_ALLOWED_HOSTS if allowed_hosts is None else allowed_hosts
        self.allowed_hosts = frozenset(hosts)
        self.allow_http = (
            settings.FORKLIFT_WEBHOOK_ALLOW_HTTP if allow_http is None else allow_http
        )
        self.resolver = resolver
        self.tls_context = tls_context or ssl.create_default_context()
        self.connect_seconds = connect_seconds
        self.total_seconds = total_seconds

    def post(self, url: str, body: bytes, headers: dict) -> Outcome:
        deadline = time.monotonic() + self.total_seconds
        try:
            target = check_url(url, allow_http=self.allow_http, allowed_hosts=self.allowed_hosts)
            addresses = self._addresses(target, deadline)
            status = self._exchange(target, addresses, body, headers, deadline)
        except UrlRefused as refused:
            return Outcome(False, error=str(refused))
        except OSError as error:
            return Outcome(False, error=self._describe(error, target))
        except http.client.HTTPException:
            return Outcome(False, error=f"{target.host} did not answer with valid HTTP.")
        if 200 <= status < 300:
            return Outcome(True, status)
        if 300 <= status < 400:
            return Outcome(
                False,
                status,
                f"The receiver answered {status}; redirects are not followed, so give the "
                "webhook the URL it redirects to.",
            )
        return Outcome(False, status, f"The receiver answered {status}.")

    def _lookup(self, target: Target, deadline: float) -> list:
        """What the resolver answers ([] when it fails), waited for until ``deadline`` only: the
        system resolver has no timeout of its own to set, so it runs in a thread of its own,
        which a slow lookup outlives without holding the caller."""
        answer = []

        def lookup():
            try:
                answer.extend(self.resolver(target.host, target.port))
            except Exception:  # any failure means the host could not be resolved
                pass

        thread = threading.Thread(target=lookup, name="webhook-lookup", daemon=True)
        thread.start()
        thread.join(max(deadline - time.monotonic(), 0))
        if thread.is_alive():
            raise UrlRefused(
                f"{target.host} could not be resolved in time (a delivery may take "
                f"{self.total_seconds:g} seconds in all)."
            )
        return answer

    def _addresses(self, target: Target, deadline: float) -> list:
        addresses = self._lookup(target, deadline)
        if not addresses:
            raise UrlRefused(f"{target.host} could not be resolved.")
        if target.host not in self.allowed_hosts and not all(map(is_public, addresses)):
            raise UrlRefused(
                f"{target.host} resolves to an address that is not publicly routable; webhooks "
                "are sent only to public addresses unless the deployment lists the host in "
                "FORKLIFT_WEBHOOK_ALLOWED_HOSTS."
            )
        return addresses

    def _connect(self, target: Target, addresses: list, deadline: float):
        """A socket connected to the first checked address that answers (TLS for https)."""
        failure: OSError = ConnectionError()
        for address in addresses:
            timeout = min(self.connect_seconds, _remaining(deadline))
            try:
                sock = socket.create_connection((address, target.port), timeout)
            except OSError as error:
                failure = error
                continue
            if target.scheme == "http":
                return sock
            sock.settimeout(_remaining(deadline))
            try:
                return self.tls_context.wrap_socket(sock, server_hostname=target.host)
            except BaseException:
                sock.close()
                raise
        raise failure

    def _exchange(self, target: Target, addresses: list, body: bytes, headers, deadline) -> int:
        """POST ``body`` and return the status of the answer (its body is not read)."""
        sock = self._connect(target, addresses, deadline)
        if target.scheme == "https":
            connection = http.client.HTTPSConnection(
                target.host, target.port, context=self.tls_context
            )
        else:
            connection = http.client.HTTPConnection(target.host, target.port)
        connection.sock = _DeadlineSocket(sock, deadline)  # used as is: no second connect
        try:
            connection.request("POST", target.path, body=body, headers=headers)
            return connection.getresponse().status
        finally:
            connection.close()

    def _describe(self, error: OSError, target: Target) -> str:
        where = f"{target.host}:{target.port}"
        if isinstance(error, TimeoutError):
            return (
                f"{where} did not answer in time (a delivery may take {self.total_seconds:g} "
                "seconds in all)."
            )
        if isinstance(error, ssl.SSLCertVerificationError):
            return (
                f"The TLS certificate of {target.host} could not be verified (expired, "
                "self-signed or for another host)."
            )
        if isinstance(error, ssl.SSLError):
            return f"The TLS handshake with {where} failed."
        if isinstance(error, ConnectionRefusedError):
            return f"{where} refused the connection."
        return f"The connection to {where} failed."
