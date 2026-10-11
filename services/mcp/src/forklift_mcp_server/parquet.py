"""Rejected rows read from a ``bad_rows`` Parquet artifact with HTTP range requests.

The gateway hands out a short-lived presigned GET for an artifact
(``GET /artifacts/{id}/download``, permission-checked and audited). The proxy never downloads
the file whole: it reads the footer and then only the column chunks of the row groups it shows,
through a seekable file object whose reads are ``Range`` requests, and stops at
:data:`MAX_READ_BYTES` per call. Only that URL is contacted, without the API token, and
redirects are not followed. Messages name the store's host, never the URL (its query string is
a signature).
"""

from __future__ import annotations

import http.client
import io
import logging
import re
import socket
import ssl
import time
import urllib.error
import urllib.request
from collections import Counter
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlsplit

from .values import RowCollector, split_finding

log = logging.getLogger("forklift_mcp_server")

#: The column of bad_rows.parquet that says why each row was rejected
REASON_COLUMN = "_rejection_reason"
#: Bytes one tool call may read from the store
MAX_READ_BYTES = 16 * 1024 * 1024
#: The smallest range requested (the footer is read in one request of this size)
MIN_FETCH_BYTES = 64 * 1024
#: Rows whose rejection reasons are counted at most
MAX_REASON_SCAN_ROWS = 5_000_000
_CONTENT_RANGE = re.compile(r"^bytes (\d+)-(\d+)/(\d+)$")
_ATTEMPTS = 3


class StoreReadError(Exception):
    """The store did not serve a range of the artifact; the message says why (host only)."""


class TooLarge(StoreReadError):
    """Showing the rows asked for would read more than the call may read."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None  # urllib then raises HTTPError with the 3xx status


class RangeReader:
    """Byte ranges of one object behind a presigned URL.

    ``url_source`` returns a presigned URL from the gateway; it is called again (once per range)
    when the store answers 403, since the URL may have expired. ``size`` is the artifact's size
    as the gateway recorded it: the store's ``Content-Range`` must say the same.
    """

    def __init__(
        self,
        url_source: Callable[[], str],
        size: int,
        *,
        what: str,
        timeout: float,
        ca_file: Optional[str] = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self._url_source = url_source
        self._url = _checked_url(url_source())
        self.host = urlsplit(self._url).netloc
        self.size = size
        self.what = what
        self.timeout = timeout
        self._sleep = sleep
        context = ssl.create_default_context(cafile=ca_file)
        self._opener = urllib.request.build_opener(
            _NoRedirect(), urllib.request.HTTPSHandler(context=context)
        )
        self.requests = 0

    def fetch(self, start: int, end: int) -> bytes:
        """Bytes ``start`` to ``end`` (inclusive)."""
        refreshed = False
        attempt = 0
        while True:
            attempt += 1
            try:
                return self._get(start, end)
            except urllib.error.HTTPError as error:
                status = error.code
                error.close()
                if status == 403 and not refreshed:
                    refreshed = True  # an expired URL: ask the gateway for a fresh one
                    self._url = _checked_url(self._url_source())
                    continue
                if (status in (429, 500, 502, 503, 504)) and attempt < _ATTEMPTS:
                    self._sleep(0.25 * 2**attempt)
                    continue
                raise StoreReadError(
                    f"The object store at {self.host} answered HTTP {status} to a range request "
                    f"of {self.what}"
                    + (
                        " (the download URL was refused even after a fresh one)"
                        if status == 403
                        else ""
                    )
                ) from None
            except (OSError, http.client.HTTPException) as error:
                if attempt < _ATTEMPTS and not isinstance(error, ssl.SSLError):
                    self._sleep(0.25 * 2**attempt)
                    continue
                raise StoreReadError(
                    f"No answer from the object store at {self.host} for {self.what} "
                    f"({_reason(error)}); the MCP server reads rejected rows from the store's "
                    "public address, which must be reachable from it"
                ) from None

    def _get(self, start: int, end: int) -> bytes:
        request = urllib.request.Request(
            self._url, headers={"Range": f"bytes={start}-{end}"}, method="GET"
        )
        self.requests += 1
        with self._opener.open(request, timeout=self.timeout) as response:
            if response.status != 206:
                raise StoreReadError(
                    f"The object store at {self.host} answered HTTP {response.status} instead of "
                    f"206 to a range request of {self.what}: without range requests the file "
                    "would have to be downloaded whole, which this server does not do"
                )
            match = _CONTENT_RANGE.match(response.headers.get("Content-Range", "").strip())
            if (
                match is None
                or (int(match.group(1)), int(match.group(2))) != (start, end)
                or int(match.group(3)) != self.size
            ):
                raise StoreReadError(
                    f"The object store at {self.host} sent another range of {self.what} than the "
                    f"one asked for (bytes {start}-{end} of {self.size}); the file may have "
                    "changed"
                )
            wanted = end - start + 1
            data = response.read(wanted + 1)
        if len(data) != wanted:
            raise StoreReadError(
                f"The object store at {self.host} sent {len(data)} bytes of {self.what} for a "
                f"range of {wanted}"
            )
        return data


def _checked_url(url: str) -> str:
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise StoreReadError("The gateway's download URL is not an http(s) URL; nothing was read")
    return url


def _reason(error: BaseException) -> str:
    if isinstance(error, urllib.error.URLError) and not isinstance(error.reason, str):
        error = error.reason
    if isinstance(error, (socket.timeout, TimeoutError)):
        return "timed out"
    if isinstance(error, ssl.SSLError):
        return f"TLS error: {getattr(error, 'reason', None) or type(error).__name__}"
    text = getattr(error, "strerror", None) or str(error)
    name = type(error).__name__
    return f"{name}: {text}" if text and text != name else name


class RangeFile(io.RawIOBase):
    """A read-only, seekable file over a :class:`RangeReader`, for pyarrow.

    Reads are served from the ranges fetched so far; a read outside them fetches at least
    :data:`MIN_FETCH_BYTES`. At most ``budget`` bytes are fetched (:class:`TooLarge` after).
    """

    def __init__(self, reader: RangeReader, budget: int = MAX_READ_BYTES):
        super().__init__()
        self.reader = reader
        self.budget = budget
        self.fetched = 0
        self._position = 0
        self._blocks: List[Tuple[int, bytes]] = []

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._position

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        base = {io.SEEK_SET: 0, io.SEEK_CUR: self._position, io.SEEK_END: self.reader.size}
        self._position = max(0, base[whence] + offset)
        return self._position

    def remaining(self) -> int:
        return self.budget - self.fetched

    def read(self, size: int = -1) -> bytes:
        start = self._position
        if size is None or size < 0:
            size = self.reader.size - start
        end = min(start + size, self.reader.size)
        if end <= start:
            return b""
        data = self._cached(start, end)
        if data is None:
            fetch_end = min(max(end, start + MIN_FETCH_BYTES), self.reader.size)
            if self.fetched + (fetch_end - start) > self.budget:
                raise TooLarge(
                    f"Reading {self.reader.what} needs more than the {self.budget} bytes one "
                    "call may read from the store"
                )
            block = self.reader.fetch(start, fetch_end - 1)
            self.fetched += len(block)
            self._blocks.append((start, block))
            data = block[: end - start]
        self._position = end
        return data

    def _cached(self, start: int, end: int) -> Optional[bytes]:
        for offset, block in self._blocks:
            if offset <= start and end <= offset + len(block):
                return block[start - offset : end - offset]
        return None


class BadRows:
    """A bad_rows Parquet file read through a :class:`RangeFile`; ``download`` is the API path
    that gives the whole file (named when a call cannot show what was asked)."""

    def __init__(self, file: RangeFile, download: str):
        import pyarrow as pa
        import pyarrow.parquet as pq

        self.file = file
        #: Where the whole file can be had instead (the API path of its download)
        self.download = download
        try:
            self._parquet = pq.ParquetFile(pa.PythonFile(file, mode="r"), pre_buffer=False)
        except pa.ArrowException as error:
            raise StoreReadError(
                f"{file.reader.what} cannot be read as Parquet: {_arrow_message(error)}"
            ) from None
        self.metadata = self._parquet.metadata
        self.columns: List[str] = list(self._parquet.schema_arrow.names)
        self.total_rows: int = self.metadata.num_rows

    def _chunk_bytes(self, group: int, names: Sequence[str]) -> int:
        row_group = self.metadata.row_group(group)
        return sum(
            row_group.column(self.columns.index(name)).total_compressed_size for name in names
        )

    def _read(self, group: int, names: Sequence[str]):
        import pyarrow as pa

        try:
            return self._parquet.read_row_group(group, columns=list(names), use_threads=False)
        except pa.ArrowException as error:
            raise StoreReadError(
                f"{self.file.reader.what} cannot be read as Parquet: {_arrow_message(error)}"
            ) from None

    def rows(
        self, names: Sequence[str], offset: int, limit: int, max_bytes: int
    ) -> Dict[str, Any]:
        """Rows ``offset`` to ``offset + limit`` (within ``max_bytes``) of the ``names`` columns.

        Whole row groups before ``offset`` are skipped without being read. When the next row
        group would take the call past its read budget, the rows so far are returned with
        ``limit: "read_bytes"``; if there are none yet, :class:`TooLarge` is raised.
        """
        collector = RowCollector(limit, max_bytes)
        group, skip = 0, offset
        while group < self.metadata.num_row_groups:
            rows = self.metadata.row_group(group).num_rows
            if skip < rows:
                break
            skip -= rows
            group += 1
        while group < self.metadata.num_row_groups and collector.limit is None:
            needed = self._chunk_bytes(group, names)
            if needed > self.file.remaining():
                if not collector.rows:
                    raise TooLarge(
                        f"Row group {group} of {self.file.reader.what} holds {needed} bytes of "
                        f"the columns asked for, more than the {self.file.budget} bytes one call "
                        "may read from the store; ask for fewer columns, or download the file "
                        f"(GET {self.download})"
                    )
                collector.limit = "read_bytes"
                break
            table = self._read(group, names)
            for record in table.slice(skip).to_pylist():
                if not collector.add([record[name] for name in names]):
                    break
            skip = 0
            group += 1
        view = collector.view()
        shown_end = offset + view["row_count"]
        view["next_offset"] = shown_end if shown_end < self.total_rows else None
        return view

    def reasons(self, top: int) -> Dict[str, Any]:
        """How often each reason appears in ``_rejection_reason``, over the row groups the read
        budget reaches (``scanned_rows`` of ``total_rows``); a row with several reasons
        ("A; B") counts for each."""
        import pyarrow.compute as pc

        if REASON_COLUMN not in self.columns:
            return {"column": None, "reasons": [], "scanned_rows": 0, "distinct": 0}
        counts: Counter = Counter()
        scanned = 0
        for group in range(self.metadata.num_row_groups):
            if scanned >= MAX_REASON_SCAN_ROWS:
                break
            # The first row group's chunk is usually cached already (rows were read from it)
            needed = self._chunk_bytes(group, [REASON_COLUMN])
            if needed > self.file.remaining() and group > 0:
                break
            try:
                column = self._read(group, [REASON_COLUMN]).column(0)
            except TooLarge:
                break
            column = column.slice(0, MAX_REASON_SCAN_ROWS - scanned)
            scanned += len(column)
            for entry in pc.value_counts(column).to_pylist():
                for reason in (entry["values"] or "REJECTED").split("; "):
                    counts[reason] += entry["counts"]
        ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:top]
        return {
            "column": REASON_COLUMN,
            "reasons": [dict(split_finding(r), reason=r, rows=n) for r, n in ranked],
            "scanned_rows": scanned,
            "distinct": len(counts),
        }


def _arrow_message(error: BaseException) -> str:
    """Arrow's message without what it may quote from the file."""
    text = str(error).splitlines()[0] if str(error) else type(error).__name__
    return text[:300]
