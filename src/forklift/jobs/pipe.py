"""The line protocol ``forklift run-job`` speaks with the process that started it (a worker).

stdout carries one JSON object per line and nothing else:

* with ``--progress-jsonl``, every progress event (``{"rows_read": ..., "rows_rejected": ...,
  "bytes_read": ...}``);
* with ``--input-url-requests``, a request for a fresh URL for the job's ``presigned_url`` input,
  ``{"type": "input_url"}``, when its URL is about to expire or the store refused it. The engine
  then reads exactly one line from stdin, ``{"url": "https://..."}`` or ``{"error": "why there
  is none"}``, waiting at most ``--input-url-timeout`` seconds. The URL must name the same
  object as the one it replaces (see :mod:`forklift.jobs.http_input`).

Requests are sent one at a time. An answer that does not come in time, or is not one of those two
objects, ends the conversation: the engine could not tell which request a late line answers, so
every later request fails at once (and the job fails when the store refuses the URL it has).
stdin is read only after the first request.
"""

from __future__ import annotations

import json
import os
import queue
import threading
from typing import IO, Any, Dict, Optional

#: Seconds to wait for the answer to a request
INPUT_URL_TIMEOUT = 120.0
#: Longest answer line in bytes (a presigned URL with a session token is a few KiB)
MAX_ANSWER_BYTES = 64 * 1024


class InputUrlUnavailable(Exception):
    """No fresh input URL: the answer was an error, or no usable answer came."""


class JobPipe:
    """stdout (JSON lines out) and stdin (answers in) of one ``run-job``.

    Args:
        out: Text stream the lines are written to (stdout)
        answers: Stream whose file descriptor the answers are read from (stdin); it is read
            directly, from a thread, so that a wait can time out
        timeout: Seconds to wait for an answer (default :data:`INPUT_URL_TIMEOUT`)
    """

    def __init__(self, out: IO[str], answers: IO[Any], timeout: Optional[float] = None):
        self._out = out
        self._answers = answers
        self.timeout = INPUT_URL_TIMEOUT if timeout is None else timeout
        self._send_lock = threading.Lock()
        self._request_lock = threading.Lock()
        self._lines: "queue.Queue[Optional[bytes]]" = queue.Queue()
        self._reader: Optional[threading.Thread] = None
        self._broken: Optional[str] = None

    def send(self, document: Dict[str, Any]) -> None:
        """Write ``document`` as one line, whole even when several threads write."""
        line = json.dumps(document, separators=(",", ":")) + "\n"
        with self._send_lock:
            self._out.write(line)
            self._out.flush()

    def input_url(self) -> str:
        """Ask for a fresh input URL and return the answer's URL.

        Raises:
            InputUrlUnavailable: The answer was an error, or no usable answer came
        """
        with self._request_lock:
            if self._broken is not None:
                raise InputUrlUnavailable(
                    f"an earlier request got no usable answer ({self._broken}), so no more "
                    "are sent"
                )
            if self._reader is None:
                self._reader = threading.Thread(
                    target=self._read, name="input-url-answers", daemon=True
                )
                self._reader.start()
            self.send({"type": "input_url"})
            answer = self._answer()
        if "error" in answer:
            raise InputUrlUnavailable(answer["error"])
        return answer["url"]

    def _answer(self) -> Dict[str, str]:
        """The next answer line, checked; a missing or malformed one breaks the pipe."""
        try:
            line = self._lines.get(timeout=self.timeout)
        except queue.Empty:
            raise self._break(
                f"no answer to the input_url request came on stdin within {self.timeout:g} "
                "seconds"
            ) from None
        if line is None:
            raise self._break("stdin was closed")
        if len(line) > MAX_ANSWER_BYTES:
            raise self._break(f"the answer on stdin is longer than {MAX_ANSWER_BYTES} bytes")
        try:
            answer = json.loads(line)
        except ValueError:
            answer = None
        if not _well_formed(answer):
            raise self._break('the answer on stdin is not {"url": "..."} or {"error": "..."}')
        return answer

    def _break(self, problem: str) -> InputUrlUnavailable:
        self._broken = problem
        return InputUrlUnavailable(problem)

    def _read(self) -> None:
        """Split stdin into lines for :meth:`_answer` (from a thread: reads block).

        It reads a duplicate of stdin's descriptor: should stdin be closed under it, the number
        could be given to another file, whose bytes it would then take for answers.
        """
        descriptor = None
        try:
            descriptor = os.dup(self._answers.fileno())
            self._split(descriptor)
        except (OSError, ValueError):  # no usable stdin: like a closed one
            pass
        finally:
            if descriptor is not None:
                os.close(descriptor)
        self._lines.put(None)

    def _split(self, descriptor: int) -> None:
        pending = b""
        while True:
            chunk = os.read(descriptor, MAX_ANSWER_BYTES)
            if not chunk:
                return
            *lines, pending = (pending + chunk).split(b"\n")
            for line in lines:
                self._lines.put(line)
            if len(pending) > MAX_ANSWER_BYTES:
                self._lines.put(pending)  # too long: refused, and nothing after it is read
                return


def _well_formed(answer: Any) -> bool:
    return (
        isinstance(answer, dict)
        and len(answer) == 1
        and next(iter(answer)) in ("url", "error")
        and isinstance(next(iter(answer.values())), str)
    )
