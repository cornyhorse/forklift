"""Inputs that are read as streams instead of opened by path, and byte counting.

``import_csv`` normally opens its input by path (a local file or an ``s3://`` URI). An
:class:`InputSource` lets a caller hand it something else that can be read from the start as a
forward-only byte stream, such as a presigned URL (``forklift.jobs`` streams those, see
``forklift.jobs.http_input``). The engine opens a new stream for every pass it makes over the
input: header detection (:meth:`InputSource.open_head`, which only needs the first rows), the main
read, and the row reader that takes over when rows have too many or too few fields. Footer
detection reads such an input with the row reader, so no copy of it is ever written.

Sources are passed explicitly (``ForkliftCore(config, input_source=...)``); ``import_csv`` never
turns a URL string into one, so user-supplied URLs cannot make the engine fetch anything.
"""

from __future__ import annotations

import io
from abc import ABC, abstractmethod
from typing import BinaryIO, Optional


class InputSource(ABC):
    """A CSV input read as byte streams from its first byte.

    Attributes:
        name: What the input is called in metadata files and messages. It must not contain
            secrets (a presigned URL's signature, for example).
        size: Size in bytes if known, otherwise None. A size of 0 is an empty input.
    """

    name: str = "input"
    size: Optional[int] = None

    @abstractmethod
    def open(self) -> BinaryIO:
        """A new forward-only binary stream over the whole input, from the first byte.

        Any object with ``read(n)`` (or ``readinto``) and ``close()`` will do; the engine adds
        its own buffering.
        """

    def open_head(self) -> BinaryIO:
        """A stream for reading only the first rows (header detection).

        Defaults to :meth:`open`; a remote source can read the start in small pieces instead.
        """
        return self.open()


class CountingReader(io.RawIOBase):
    """A raw stream that counts the bytes read through it.

    Wrap the result in ``io.BufferedReader`` (for Arrow) or ``io.TextIOWrapper`` (for ``csv``).
    Closing it closes the wrapped stream.

    Attributes:
        count: Bytes read so far
    """

    def __init__(self, stream: BinaryIO):
        super().__init__()
        self._stream = stream
        self.count = 0

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:
        readinto = getattr(self._stream, "readinto", None)
        if readinto is not None:
            size = readinto(buffer) or 0
        else:
            data = self._stream.read(len(buffer))
            size = len(data)
            buffer[:size] = data
        self.count += size
        return size

    def close(self) -> None:
        if not self.closed:
            try:
                self._stream.close()
            finally:
                super().close()
