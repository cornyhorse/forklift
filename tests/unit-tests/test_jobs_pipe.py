"""``forklift.jobs.pipe``: run-job's JSON lines on stdout and the answers it reads from stdin.

The tests play the supervisor over real OS pipes: they read the engine's request lines and write
answers (or none, or malformed ones) and check that a request never waits longer than its timeout
and that an unusable answer ends the conversation.
"""

from __future__ import annotations

import io
import json
import os
import threading

import pytest

from forklift.jobs.pipe import MAX_ANSWER_BYTES, InputUrlUnavailable, JobPipe

URL = "https://store.example/bucket/big.csv?X-Amz-Signature=fresh"


class Supervisor:
    """The other end: what the engine wrote, and a stdin to answer on."""

    def __init__(self):
        self.out = io.StringIO()
        read_end, self._write_end = os.pipe()
        self.stdin = os.fdopen(read_end, "rb", buffering=0)

    def answer(self, data: bytes) -> None:
        os.write(self._write_end, data)

    def close(self) -> None:
        if self._write_end is not None:
            os.close(self._write_end)
            self._write_end = None

    def lines(self):
        return [json.loads(line) for line in self.out.getvalue().splitlines()]


@pytest.fixture
def supervisor():
    other_end = Supervisor()
    yield other_end
    other_end.close()
    other_end.stdin.close()


def _pipe(supervisor, timeout=5.0):
    return JobPipe(supervisor.out, supervisor.stdin, timeout=timeout)


def test_lines_are_whole_even_from_several_threads(supervisor):
    pipe = _pipe(supervisor)
    events = [{"rows_read": n, "padding": "x" * 1000} for n in range(200)]
    threads = [threading.Thread(target=pipe.send, args=(event,)) for event in events]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(line["rows_read"] for line in supervisor.lines()) == list(range(200))


def test_a_request_and_its_answer(supervisor):
    pipe = _pipe(supervisor)
    supervisor.answer(json.dumps({"url": URL}).encode() + b"\n")
    assert pipe.input_url() == URL
    supervisor.answer(b'{"url": "https://store.example/second"}\r\n')
    assert pipe.input_url() == "https://store.example/second"
    assert supervisor.lines() == [{"type": "input_url"}, {"type": "input_url"}]


def test_an_answer_may_arrive_in_pieces(supervisor):
    pipe = _pipe(supervisor)
    line = json.dumps({"url": URL}).encode() + b"\n"
    threading.Timer(0.05, supervisor.answer, args=(line[:10],)).start()
    threading.Timer(0.1, supervisor.answer, args=(line[10:],)).start()
    assert pipe.input_url() == URL


def test_an_error_answer_fails_this_request_only(supervisor):
    pipe = _pipe(supervisor)
    supervisor.answer(b'{"error": "the gateway could not be reached"}\n')
    with pytest.raises(InputUrlUnavailable, match="^the gateway could not be reached$"):
        pipe.input_url()
    supervisor.answer(json.dumps({"url": URL}).encode() + b"\n")
    assert pipe.input_url() == URL


def test_no_answer_in_time_ends_the_conversation(supervisor):
    pipe = _pipe(supervisor, timeout=0.2)
    with pytest.raises(InputUrlUnavailable) as caught:
        pipe.input_url()
    assert str(caught.value) == (
        "no answer to the input_url request came on stdin within 0.2 seconds"
    )
    supervisor.answer(json.dumps({"url": URL}).encode() + b"\n")  # too late: never read
    with pytest.raises(InputUrlUnavailable) as again:
        pipe.input_url()
    assert str(again.value) == (
        "an earlier request got no usable answer (no answer to the input_url request came on "
        "stdin within 0.2 seconds), so no more are sent"
    )
    assert len(supervisor.lines()) == 1, "the second request was not sent"


@pytest.mark.parametrize(
    "answer",
    [
        b"not json\n",
        b"[1]\n",
        b'{"url": 5}\n',
        b'{"location": "https://store.example/x"}\n',
        b'{"url": "https://store.example/x", "error": "both"}\n',
    ],
)
def test_a_malformed_answer_ends_the_conversation(supervisor, answer):
    pipe = _pipe(supervisor)
    supervisor.answer(answer)
    with pytest.raises(InputUrlUnavailable, match=r'is not \{"url": "\.\.\."\} or \{"error"'):
        pipe.input_url()
    supervisor.answer(json.dumps({"url": URL}).encode() + b"\n")
    with pytest.raises(InputUrlUnavailable, match="so no more are sent"):
        pipe.input_url()


@pytest.mark.parametrize("newline", [b"\n", b""])
def test_an_over_long_answer_is_refused(supervisor, newline):
    pipe = _pipe(supervisor)
    long_url = "https://store.example/" + "x" * MAX_ANSWER_BYTES
    threading.Thread(
        target=supervisor.answer, args=(json.dumps({"url": long_url}).encode() + newline,)
    ).start()
    with pytest.raises(InputUrlUnavailable, match=f"longer than {MAX_ANSWER_BYTES} bytes"):
        pipe.input_url()


def test_a_closed_stdin_fails_the_request_at_once(supervisor):
    pipe = _pipe(supervisor, timeout=30)
    supervisor.close()
    with pytest.raises(InputUrlUnavailable, match="^stdin was closed$"):
        pipe.input_url()


def test_a_stdin_without_a_file_descriptor_is_like_a_closed_one():
    pipe = JobPipe(io.StringIO(), io.BytesIO(), timeout=30)
    with pytest.raises(InputUrlUnavailable, match="^stdin was closed$"):
        pipe.input_url()


def test_the_default_timeout():
    assert JobPipe(io.StringIO(), io.BytesIO()).timeout == 120.0
