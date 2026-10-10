"""Fresh presigned URLs for a streamed input, asked for by the engine (ADR 0006).

The engine of a streamed input runs with ``--input-url-requests``: when its URL is about to
expire, or the store refused it, it writes ``{"type": "input_url"}`` on stdout and waits for one
line on stdin. :class:`InputUrls` answers it from ``POST /jobs/{id}/input-url`` for the job's
lease: ``{"url": ...}`` when the gateway gives a presigned URL on the host the engine may reach,
``{"error": ...}`` otherwise. Every request gets exactly one answer, sooner than the engine stops
waiting (:attr:`InputUrls.timeout`, passed as ``--input-url-timeout``). A lost lease is answered
with an error and stops the job as a lost heartbeat does; a refused worker token also stops the
worker. The engine is not trusted to ask sparingly: after :data:`MAX_REQUESTS` requests the
gateway is no longer asked.
"""

from __future__ import annotations

from typing import Any, Callable, Sequence

from . import logs
from .gateway import (
    GatewayAuthError,
    GatewayClient,
    GatewayError,
    GatewayUnavailable,
    Lease,
    LeaseLost,
)
from .redact import Redactor, secrets_of
from .retry import Backoff, Interrupted, retry
from .transport import host_of

log = logs.logger("input_url")

ATTEMPTS = 3
MAX_REQUESTS = 100  # per job, as many as the engine asks for at most
BACKOFF = (0.5, 4.0)  # first and longest wait between attempts, in seconds
MAX_URL_BYTES = 16 * 1024  # the engine reads answers of up to 64 KiB
MAX_ERROR_CHARS = 1000


class InputUrls:
    """Answers the engine's requests for a fresh URL for the job's streamed input.

    Args:
        gateway: The internal API client
        lease: The job's lease (job id and attempt)
        control: The job's ``JobControl`` (stopping it ends a wait; a lost lease is marked there)
        hosts: Hosts (``host`` or ``host:port``) the engine may reach (``--allow-url-host``)
        redactor: The job's redactor; it learns every fresh URL
        http_timeout: The gateway client's timeout, which bounds every attempt
        on_fatal: Called with a refused-token error (the worker stops)
        context: Log fields of the job
    """

    def __init__(
        self,
        gateway: GatewayClient,
        lease: Lease,
        control: Any,
        hosts: Sequence[str],
        redactor: Redactor,
        *,
        http_timeout: float,
        on_fatal: Callable[[Exception], None],
        context: dict[str, Any],
    ):
        self.gateway, self.lease, self.control = gateway, lease, control
        self.hosts = list(hosts)
        self.redactor = redactor
        self.http_timeout = http_timeout
        self.on_fatal = on_fatal
        self.context = context
        self.requests = 0

    @property
    def timeout(self) -> float:
        """Seconds the engine waits for an answer: room for every attempt and the waits."""
        return ATTEMPTS * (self.http_timeout + BACKOFF[1]) + 30

    def answer(self) -> dict[str, str]:
        """The answer to one request: ``{"url": ...}`` or ``{"error": ...}``."""
        self.requests += 1
        if self.requests > MAX_REQUESTS:
            return self._error(f"the engine already asked for {MAX_REQUESTS} fresh input URLs")
        url, problem = self._ask_gateway()
        problem = problem or self._unusable(url)
        if problem:
            return self._error(problem)
        self.redactor.add(
            secrets_of({"input": {"location": {"type": "presigned_url", "url": url}}})
        )
        log.info(
            "fresh input URL passed to the engine", extra={**self.context, "host": host_of(url)}
        )
        return {"url": url}

    def _ask_gateway(self) -> tuple[str, str | None]:
        """The gateway's fresh URL, or "" and what went wrong."""
        lease = self.lease
        try:
            url = retry(
                lambda: self.gateway.input_url(lease.job_id, lease.attempt),
                attempts=ATTEMPTS,
                retryable=lambda error: isinstance(error, GatewayUnavailable),
                backoff=Backoff(*BACKOFF),
                wait=self.control.stopped.wait,
            )
        except Interrupted:
            return "", "the job is being stopped"
        except LeaseLost as error:
            self.control.mark_lost(str(error))
            return "", str(error)
        except GatewayAuthError as error:
            self.control.mark_lost(str(error))
            self.on_fatal(error)
            return "", str(error)
        except GatewayError as error:
            return "", f"the gateway gave no fresh input URL: {error}"
        return url, None

    def _unusable(self, url: str) -> str | None:
        """Why ``url`` must not reach the engine, if it must not."""
        try:
            host = host_of(url)
        except ValueError:
            return "the gateway's fresh input URL is not an http(s) URL"
        if host not in self.hosts:
            return (
                f"the gateway's fresh input URL points at {host}, not at the store this job "
                f"reads from ({', '.join(self.hosts)})"
            )
        if len(url.encode("utf-8")) > MAX_URL_BYTES:
            return f"the gateway's fresh input URL is longer than {MAX_URL_BYTES} bytes"
        return None

    def _error(self, message: str) -> dict[str, str]:
        text = self.redactor(message)[:MAX_ERROR_CHARS]
        log.warning("no fresh input URL for the engine: %s", text, extra=self.context)
        return {"error": text}
