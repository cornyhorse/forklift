"""Helpers for the webhook tests: webhooks built through the ORM, a receiver on a local port
(HTTP or HTTPS, with certificates of the tests' own), a resolver that answers from a table, and
a stand-in for the delivery client that answers from a list."""

from __future__ import annotations

import datetime
import http.server
import ssl
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from forklift_web import secret_backend
from forklift_web.core.choices import JOB_EVENTS, WebhookScope
from forklift_web.core.models import Webhook
from forklift_web.webhook_client import Outcome

SECRET = "fkwh_" + "s3cr3t" * 7 + "x"
ALL_EVENTS = [event.value for event in JOB_EVENTS]


def make_webhook(
    owner,
    *,
    url: str = "https://192.0.2.10/hook",
    events=ALL_EVENTS,
    scope: str = WebhookScope.OWN_JOBS,
    dataset=None,
    kinds=("run",),
    secret: str = SECRET,
    name: str = "hook",
    **fields,
) -> Webhook:
    """A webhook through the ORM. Its default URL (a documentation address) is refused by the
    guard before anything is sent, so tests that do not mean to deliver never reach a network."""
    return Webhook.objects.create(
        owner=owner,
        name=name,
        url=url,
        events=sorted(events),
        kinds=list(kinds),
        scope=scope,
        dataset=dataset,
        secret_ciphertext=secret_backend.backend().encrypt({"secret": secret}),
        secret_prefix=secret[:12],
        **fields,
    )


class StubResolver:
    """Answers lookups from ``table`` (host -> addresses, or an exception to raise)."""

    def __init__(self, table: dict):
        self.table = table
        self.calls = []

    def __call__(self, host: str, port: int) -> list:
        self.calls.append((host, port))
        answer = self.table[host]
        if isinstance(answer, BaseException):
            raise answer
        return list(answer)


class FakeClient:
    """Answers each POST with the next outcome (the last one again once they run out)."""

    def __init__(self, *outcomes: Outcome):
        self.outcomes = list(outcomes) or [Outcome(True, 200)]
        self.requests = []

    def post(self, url: str, body: bytes, headers: dict) -> Outcome:
        self.requests.append((url, body, headers))
        return self.outcomes.pop(0) if len(self.outcomes) > 1 else self.outcomes[0]


def failing(status: int = 500) -> Outcome:
    return Outcome(False, status, f"The receiver answered {status}.")


# --------------------------------------------------------------------------- a local receiver


@dataclass
class Received:
    method: str
    path: str
    headers: dict  # lower-case names
    body: bytes


class _Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        headers = {name.lower(): value for name, value in self.headers.items()}
        self.server.requests.append(Received(self.command, self.path, headers, body))
        self.send_response(self.server.status)
        if 300 <= self.server.status < 400:
            self.send_header("Location", "http://127.0.0.1:9/elsewhere")
        self.send_header("Content-Length", "21")
        self.end_headers()
        self.wfile.write(b"the receiver's answer")

    def log_message(self, format, *args):
        pass


class Receiver:
    """An HTTP server on 127.0.0.1 (HTTPS with ``tls``, an ssl server context) in a thread; it
    records every POST and answers ``status``. Server names asked for with SNI are in ``sni``."""

    def __init__(self, status: int = 200, tls: Optional[ssl.SSLContext] = None):
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.server.status = status
        self.server.requests = []
        self.sni = []
        if tls is not None:
            tls.sni_callback = lambda sock, name, context: self.sni.append(name)
            self.server.socket = tls.wrap_socket(self.server.socket, server_side=True)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def port(self) -> int:
        return self.server.server_address[1]

    @property
    def requests(self) -> list:
        return self.server.requests

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()


# --------------------------------------------------------------------------- certificates


def _name(common_name: str) -> x509.Name:
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])


def certificates(directory: Path, host: str = "receiver.test") -> tuple:
    """A CA of the tests' own and a server certificate for ``host`` it signed; returns (CA
    certificate as PEM text, server certificate file, server key file)."""
    now = datetime.datetime.now(datetime.timezone.utc)
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca = (
        x509.CertificateBuilder()
        .subject_name(_name("forklift tests CA"))
        .issuer_name(_name("forklift tests CA"))
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), False)
        .sign(ca_key, hashes.SHA256())
    )
    key = ec.generate_private_key(ec.SECP256R1())
    server = (
        x509.CertificateBuilder()
        .subject_name(_name(host))
        .issuer_name(ca.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(host)]), critical=False)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), False)
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), False
        )
        .sign(ca_key, hashes.SHA256())
    )
    cert_file, key_file = directory / f"{host}.pem", directory / f"{host}.key"
    cert_file.write_bytes(server.public_bytes(serialization.Encoding.PEM))
    key_file.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return ca.public_bytes(serialization.Encoding.PEM).decode(), cert_file, key_file


def server_context(cert_file: Path, key_file: Path) -> ssl.SSLContext:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert_file, key_file)
    return context
