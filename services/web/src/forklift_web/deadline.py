"""Outbound HTTP exchanges with a deadline for the whole exchange, not only for each read.

A socket timeout bounds one read; a peer that answers a byte at a time could still hold a
request for as long as it likes. :class:`DeadlineSocket` gives every send and receive only what
is left of one deadline, so the webhook client (forklift_web.webhook_client) and the OpenID
Provider client (forklift_web.oidc.provider) cannot be held longer than their limit.
"""

from __future__ import annotations

import io
import time


def remaining(deadline: float) -> float:
    """Seconds left until ``deadline`` (a ``time.monotonic()`` value); TimeoutError when none
    are."""
    left = deadline - time.monotonic()
    if left <= 0:
        raise TimeoutError("The exchange's time is up.")
    return left


class DeadlineSocket:
    """A connected socket whose every send and receive must finish before ``deadline``. It
    stands in for ``http.client``'s socket after the connection is made.

    Like a socket, it stays open while a file made from it is: ``http.client`` closes the
    connection as soon as an answer that ends with the connection has its headers, and reads
    the body through the file afterwards."""

    def __init__(self, sock, deadline: float):
        self._sock = sock
        self._deadline = deadline
        self._files = 0
        self._closing = False

    def sendall(self, data) -> None:
        self._sock.settimeout(remaining(self._deadline))
        self._sock.sendall(data)

    def recv_into(self, buffer) -> int:
        self._sock.settimeout(remaining(self._deadline))
        return self._sock.recv_into(buffer)

    def makefile(self, mode):
        self._files += 1
        return io.BufferedReader(_Reader(self))

    def file_closed(self) -> None:
        self._files -= 1
        if self._closing and not self._files:
            self._sock.close()

    def close(self) -> None:
        self._closing = True
        if not self._files:
            self._sock.close()


class _Reader(io.RawIOBase):
    def __init__(self, sock: DeadlineSocket):
        self._sock = sock

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:
        return self._sock.recv_into(buffer)

    def close(self) -> None:
        if not self.closed:
            super().close()
            self._sock.file_closed()
