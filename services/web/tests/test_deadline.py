"""The deadline socket shared by the outbound HTTP clients (webhooks, the OpenID Provider)."""

from __future__ import annotations

import socket
import time

import pytest

from forklift_web.deadline import DeadlineSocket, remaining


def test_remaining_time():
    assert 0 < remaining(time.monotonic() + 5) <= 5
    with pytest.raises(TimeoutError, match="The exchange's time is up."):
        remaining(time.monotonic() - 1)


def test_it_stays_open_while_a_file_made_from_it_is():
    near, far = socket.socketpair()
    with far:
        wrapped = DeadlineSocket(near, time.monotonic() + 5)
        reader = wrapped.makefile("rb")
        far.sendall(b"answer")
        wrapped.close()  # what http.client does once the headers of a closing answer are in
        assert near.fileno() != -1 and reader.read(6) == b"answer"
        reader.close()
        assert near.fileno() == -1
        reader.raw.close()  # closing it again changes nothing
        assert wrapped._files == 0


def test_a_file_closed_first():
    near, far = socket.socketpair()
    with far:
        wrapped = DeadlineSocket(near, time.monotonic() + 5)
        wrapped.makefile("rb").close()
        assert near.fileno() != -1  # the connection may be reused
        wrapped.close()
        assert near.fileno() == -1
