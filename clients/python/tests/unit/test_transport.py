"""The HTTP layer: when requests are sent again, Retry-After, bounded answers, redirects that
are not followed, and TLS with a private CA (a real https server)."""

from __future__ import annotations

import email.utils
import random
import shutil
import ssl
import subprocess
import time
import urllib.error

import pytest
from fake_api import TOKEN, FakeForklift

from forklift_client import ApiError, ForkliftClient, RetryPolicy, TransportError
from forklift_client._transport import (
    Response,
    Transport,
    _reason,
    _transport_error,
    host_of,
    retry_after,
    send_with_retries,
    store_error_code,
)


class TestRetryPolicy:
    def test_validation(self):
        with pytest.raises(ValueError, match="attempts"):
            RetryPolicy(attempts=0)
        with pytest.raises(ValueError, match="negative"):
            RetryPolicy(initial_delay=-1)

    def test_delays_grow_with_jitter_up_to_the_maximum(self):
        policy = RetryPolicy(initial_delay=1, max_delay=8)
        rng = random.Random(7)
        for retry, step in [(1, 1), (2, 2), (3, 4), (4, 8), (9, 8)]:
            delay = policy.delay(retry, rng)
            assert step / 2 <= delay <= step


class TestRetryAfter:
    def test_seconds_and_dates(self):
        assert retry_after({"retry-after": "120"}) == 120.0
        assert retry_after({}) is None and retry_after({"retry-after": " "}) is None
        now = time.time()
        later = email.utils.formatdate(now + 30, usegmt=True)
        assert 28 <= retry_after({"retry-after": later}, now=now) <= 30
        earlier = email.utils.formatdate(now - 30, usegmt=True)
        assert retry_after({"retry-after": earlier}, now=now) == 0.0

    @pytest.mark.parametrize("value", ["soon", "-5", "1.5", "Mon, 32 Foo 2026 25:61:00 -0000"])
    def test_values_that_are_neither(self, value):
        assert retry_after({"retry-after": value}) is None

    def test_a_date_without_a_zone_is_ignored(self):
        assert retry_after({"retry-after": "Sun, 11 Oct 2026 10:00:00 -0000"}) is None


def _sender(*outcomes):
    queue = list(outcomes)
    calls = []

    def send():
        calls.append(1)
        outcome = queue.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    return send, calls


def _run(send, replay_safe, attempts=3):
    waits = []
    response = send_with_retries(
        send,
        policy=RetryPolicy(attempts=attempts, initial_delay=0.5),
        replay_safe=replay_safe,
        what="POST /api/v1/x",
        sleep=waits.append,
        rng=random.Random(1),
    )
    return response, waits


OK = Response(200, {}, b"{}")


class TestSendWithRetries:
    def test_a_request_that_never_reached_the_server_is_sent_again_even_if_unsafe(self):
        refused = TransportError("POST /x", "h", "ConnectionRefusedError", sent=False)
        send, calls = _sender(refused, OK)
        response, waits = _run(send, replay_safe=False)
        assert response is OK and len(calls) == 2 and len(waits) == 1

    def test_a_request_that_may_have_reached_it_is_not(self):
        reset = TransportError("POST /x", "h", "ConnectionResetError")
        send, calls = _sender(reset)
        with pytest.raises(TransportError):
            _run(send, replay_safe=False)
        assert len(calls) == 1

    def test_tls_failures_are_never_retried(self):
        tls = TransportError("GET /x", "h", "TLS error", sent=False, retryable=False)
        send, calls = _sender(tls)
        with pytest.raises(TransportError):
            _run(send, replay_safe=True)
        assert len(calls) == 1

    def test_the_attempts_run_out(self):
        reset = TransportError("GET /x", "h", "reset")
        send, calls = _sender(reset, reset, reset)
        with pytest.raises(TransportError):
            _run(send, replay_safe=True)
        assert len(calls) == 3

    @pytest.mark.parametrize("status", [502, 504])
    def test_gateway_errors_only_for_safe_requests(self, status):
        bad = Response(status, {}, b"")
        send, _ = _sender(bad, OK)
        assert _run(send, replay_safe=True)[0] is OK
        send, _ = _sender(bad, OK)
        assert _run(send, replay_safe=False)[0] is bad

    def test_answers_that_are_not_worth_retrying(self):
        for status in (400, 404, 409, 500):
            answer = Response(status, {}, b"")
            send, calls = _sender(answer)
            assert _run(send, replay_safe=True)[0] is answer and len(calls) == 1


class TestHelpers:
    def test_host_of(self):
        assert host_of("https://Example.org/a?b") == "example.org"
        assert host_of("http://127.0.0.1:9000/x") == "127.0.0.1:9000"
        assert host_of("http://[::1]:8080/") == "[::1]:8080"
        for url in ("ftp://example.org", "https:///nohost", "file:///etc/passwd"):
            with pytest.raises(ValueError):
                host_of(url)

    def test_store_error_code(self):
        assert store_error_code(b"<Error><Code>NoSuchKey</Code></Error>") == "NoSuchKey"
        assert store_error_code(b"not xml") == ""

    def test_reasons(self):
        assert _reason(TimeoutError()) == "timed out"
        assert _reason(ssl.SSLError(1, "bad")) == "TLS error: SSLError"
        assert _reason(OSError(5, "Input/output error")) == "OSError: Input/output error"
        assert _reason(ConnectionResetError()) == "ConnectionResetError"

    def test_url_errors(self):
        text = _transport_error("GET /x", "h", urllib.error.URLError("unknown url type: x"))
        assert text.reason == "unknown url type: x" and text.sent
        tls = _transport_error("GET /x", "h", urllib.error.URLError(ssl.SSLError(1, "x")))
        assert not tls.retryable and not tls.sent

    def test_answers_larger_than_allowed(self, fake):
        transport = Transport(timeout=5, user_agent="t")
        job = fake.add_job()
        url = f"{fake.url}/api/v1/jobs/{job['id']}"
        headers = {"Authorization": f"Bearer {TOKEN}"}
        with pytest.raises(TransportError, match="larger than 10 bytes"):
            transport.request("GET", url, headers=headers, what="GET job", max_body=10)

    def test_a_short_answer(self, fake):
        transport = Transport(timeout=5, user_agent="t")
        artifact = fake.add_artifact(fake.add_job(), "a.bin", b"0123456789")
        fake.fail("GET store", truncate=True)
        with pytest.raises(TransportError, match="closed after 5 of 10 bytes"):
            transport.request("GET", fake.presign(artifact["_key"]), headers={}, what="GET")

    def test_reading_an_answer_that_stalls(self, fake):
        transport = Transport(timeout=0.2, user_agent="t")
        artifact = fake.add_artifact(fake.add_job(), "a.bin", b"0123456789")
        fake.fail("GET store", stall=1.0)
        with pytest.raises(TransportError, match="timed out"):
            transport.request("GET", fake.presign(artifact["_key"]), headers={}, what="GET")


class TestRedirects:
    def test_a_redirect_is_not_followed(self, fake, client):
        fake.fail("GET /me", status=302, headers={"Location": "https://elsewhere.example/"})
        with pytest.raises(ApiError) as raised:
            client.me()
        assert raised.value.status == 302
        assert len(fake.requests) == 1


@pytest.fixture(scope="module")
def certificate(tmp_path_factory):
    """A self-signed certificate for 127.0.0.1 (the CA bundle is the certificate itself)."""
    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("openssl is needed to make a test certificate")
    directory = tmp_path_factory.mktemp("tls")
    cert, key = directory / "cert.pem", directory / "key.pem"
    subprocess.run(
        [
            openssl,
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=127.0.0.1",
            "-addext",
            "subjectAltName=IP:127.0.0.1",
            "-keyout",
            str(key),
            "-out",
            str(cert),
        ],
        check=True,
        capture_output=True,
    )
    return str(cert), str(key)


class TestTls:
    def test_a_private_ca(self, certificate):
        with FakeForklift(tls=certificate) as fake:
            trusted = ForkliftClient(fake.url, TOKEN, ca_file=certificate[0])
            assert trusted.me().user.username == "ana"
            assert fake.url.startswith("https://")

    def test_a_certificate_that_does_not_verify_is_not_retried(self, certificate):
        with FakeForklift(tls=certificate) as fake:
            client = ForkliftClient(fake.url, TOKEN)
            with pytest.raises(TransportError) as raised:
                client.me()
            error = raised.value
            assert not error.retryable and not error.sent
            assert error.reason.startswith("TLS error: CERTIFICATE_VERIFY_FAILED")
            assert len(fake.requests) == 0
