"""Private copies of an input for the interactive tools (``preview``, ``validate_schema``,
``generate_schema``).

The engine's job writes its artifact and its scratch into its base directory, so these tools run
it on a copy in a private temporary directory instead of inside a root: a CSV file's first
records (as many as the job can need), or a whole workbook. The copy of a CSV file is read and
written in chunks, so neither a huge file nor a huge first line costs more memory than a chunk.
"""

from __future__ import annotations

import codecs
import contextlib
import csv
import re
import shutil
import tempfile
from pathlib import Path
from typing import IO, Any, BinaryIO, Dict, Iterator, List, Optional, Tuple

from ..engine.processors.text_utils import read_encoding
from ..jobs import JobResult
from ..jobs.spec import InputOptions
from .workspace import ToolInputError

#: The beginning of a CSV input copied for the interactive tools (characters)
MAX_SAMPLE_COPY_CHARS = 256 * 1024 * 1024
#: Largest Excel workbook the interactive tools copy (bytes)
MAX_EXCEL_COPY_BYTES = 256 * 1024 * 1024
#: Characters (or bytes) read and written at a time
COPY_CHUNK = 1024 * 1024
#: The engine's private directory inside a job's base directory, named in some messages
_JOB_SCRATCH = re.compile(r"\.forklift-job-[A-Za-z0-9_]+/")


@contextlib.contextmanager
def scratch_directory() -> Iterator[Path]:
    """A private directory (mode 0700) in the system's temporary directory, removed afterwards."""
    try:
        directory = Path(tempfile.mkdtemp(prefix="forklift-mcp-"))
    except OSError as error:
        raise ToolInputError(
            f"cannot create a private temporary directory in {tempfile.gettempdir()} "
            f"({error.strerror}); set TMPDIR to a writable directory"
        ) from None
    try:
        yield directory
    finally:
        shutil.rmtree(directory, ignore_errors=True)


class Staged:
    """An input copied into a job's scratch directory: where it is and how results name it."""

    def __init__(self, path: str, fmt: str, view: Dict[str, Any], records: Optional[int]):
        self.path = path
        self.fmt = fmt
        self.view = view
        #: Non-empty CSV records in the copy (None for a workbook)
        self.records = records
        #: Paths of other files in the copy's directory -> what messages say instead
        self.replacements: Dict[str, str] = {}

    def name_artifacts(self, result: JobResult, **descriptions: str) -> None:
        """Messages name the job's files in the copy, which the caller cannot open: say what
        they are instead (``descriptions`` by artifact kind, a generic text for the others)."""
        for artifact in result.artifacts:
            self.replacements[artifact.path] = descriptions.get(
                artifact.kind, f"the {artifact.kind} file of the temporary copy (not kept)"
            )

    def clean(self, text: str) -> str:
        """Messages name the copy; results name the input the caller gave."""
        text = text.replace(self.path, self.view["path"])
        for path in sorted(self.replacements, key=len, reverse=True):
            text = text.replace(path, self.replacements[path])
        return _JOB_SCRATCH.sub("<temporary directory>/", text)

    def clean_list(self, texts: List[str]) -> List[str]:
        return [self.clean(text) for text in texts]

    def error(self, result: JobResult) -> Optional[Dict[str, Any]]:
        if result.error is None:
            return None
        return dict(result.error.to_dict(), message=self.clean(result.error.message))


def check_dialect(options: InputOptions) -> None:
    """The CSV options must make a dialect Python's csv module accepts (the engine's preview and
    checks parse with it)."""
    try:
        csv.reader(
            [],
            delimiter=options.delimiter or ",",
            quotechar=options.quote_char or '"',
            escapechar=options.escape_char,
        )
    except (TypeError, ValueError) as error:
        raise ToolInputError(
            f"input_options delimiter, quote_char and escape_char do not make a CSV dialect: "
            f"{error}"
        ) from None


def stage(
    source: Path, shown: str, fmt: str, scratch: Path, options: InputOptions, records: int
) -> Staged:
    """Copy the input (or its first ``records`` non-empty CSV records) into ``scratch``.

    Raises:
        ToolInputError: The input cannot be read, the copy cannot be written, a workbook is
            too large to copy, or a CSV file's first record is longer than the copy may be
    """
    name = source.name
    if "\\" in name:
        name = "input" + source.suffix.lower()
    target = scratch / "in" / name
    try:
        size = source.stat().st_size
    except OSError as error:
        raise _read_error(shown, error) from None
    try:
        target.parent.mkdir()
    except OSError as error:
        raise _write_error(error) from None
    found: Optional[int] = None
    if fmt == "excel":
        if size > MAX_EXCEL_COPY_BYTES:
            raise ToolInputError(
                f"input_path is a {size}-byte workbook; preview, validate_schema and "
                f"generate_schema read workbooks up to {MAX_EXCEL_COPY_BYTES} bytes (they "
                "work on a private copy); run_job reads it in place"
            )
        _copy_bytes(source, shown, target, size)
        whole = True
    else:
        check_dialect(options)
        whole, found = _copy_csv_head(source, shown, target, records, options)
        if found == 0 and size and not whole:
            raise ToolInputError(
                f"the first record of input_path {shown!r} is longer than the "
                f"{MAX_SAMPLE_COPY_CHARS} characters preview, validate_schema and "
                "generate_schema read; run_job reads the whole file"
            )
    view = {"path": shown, "format": fmt, "bytes": size, "sampled": not whole}
    return Staged(f"in/{name}", fmt, view, found)


class RecordCounter:
    """Counts CSV records in text fed in chunks: a line break ends a record unless it is inside
    quotes; a doubled quote stays inside; the escape character protects the next character.
    Blank records are not counted. Only quotes, escapes and line breaks are looked at."""

    def __init__(self, quote: str, escape: Optional[str]):
        specials = [re.escape(quote), "\n"] + ([re.escape(escape)] if escape else [])
        self._specials = re.compile("|".join(specials))
        self._text = re.compile(r"[^\r\n]")
        self.quote = quote
        self.escape = escape
        self.records = 0
        self._quoted = False
        self._escaped = False
        self._content = False

    def feed(self, chunk: str, wanted: int) -> Optional[int]:
        """Count the records that end in ``chunk``; the index just after the line break that
        ends record number ``wanted``, or None when that record does not end in it."""
        position = 0
        if self._escaped and chunk:
            self._escaped, self._content, position = False, True, 1
        for match in self._specials.finditer(chunk, position):
            start = match.start()
            if start < position:
                continue  # escaped by the character before it
            if self._text.search(chunk, position, start):
                self._content = True
            position = start + 1
            char = match.group()
            if char == self.escape:
                self._content = True
                if position == len(chunk):
                    self._escaped = True
                position += 1
            elif char == self.quote:
                self._quoted = not self._quoted
                self._content = True
            elif not self._quoted:  # a line break that ends a record
                if self._content:
                    self.records += 1
                    self._content = False
                    if self.records >= wanted:
                        return position
        if self._text.search(chunk, position):
            self._content = True
        return None

    def finish(self) -> None:
        """The end of the input ends its last record."""
        if self._content:
            self.records += 1
            self._content = False


def _copy_csv_head(
    source: Path, shown: str, target: Path, records: int, options: InputOptions
) -> Tuple[bool, Optional[int]]:
    """Copy the first ``records`` non-empty records; ``(whole_file, records_copied)``.

    The text is copied as the file has it (same lines, same line breaks), so the engine sees
    exactly the beginning of the input. When it cannot be decoded here, the bytes read so far
    (and one chunk more) are copied as they are and the engine reports the problem itself;
    ``records_copied`` is None then.
    """
    encoding = options.encoding or "utf-8"
    counter = RecordCounter(options.quote_char or '"', options.escape_char)
    with _open_source(source, shown) as raw:
        try:
            return _copy_text(raw, shown, target, encoding, counter, records), counter.records
        except (UnicodeError, LookupError):
            consumed = raw.tell()
    limit = min(consumed + COPY_CHUNK, MAX_SAMPLE_COPY_CHARS)
    _copy_bytes(source, shown, target, limit)
    return source.stat().st_size <= limit, None


def _copy_text(
    raw: BinaryIO, shown: str, target: Path, encoding: str, counter: RecordCounter, records: int
) -> bool:
    """Decode ``raw`` chunk by chunk and write it until ``records`` records; True at the end."""
    decoder = codecs.getincrementaldecoder(read_encoding(encoding))()
    try:
        sink = open(target, "w", encoding=encoding, newline="")
    except OSError as error:
        raise _write_error(error) from None
    with sink:
        copied = 0
        while True:
            data = _read(raw, shown)
            chunk = decoder.decode(data, final=not data)
            room = MAX_SAMPLE_COPY_CHARS - copied
            capped = len(chunk) > room
            chunk = chunk[:room]
            end = counter.feed(chunk, records)
            if end is not None:
                _write(sink, chunk[:end])
                return False
            _write(sink, chunk)
            copied += len(chunk)
            if capped:
                return False
            if not data:
                counter.finish()
                return True


def _copy_bytes(source: Path, shown: str, target: Path, limit: int) -> None:
    """Copy the first ``limit`` bytes of ``source``, a chunk at a time."""
    with _open_source(source, shown) as raw:
        try:
            sink = open(target, "wb")
        except OSError as error:
            raise _write_error(error) from None
        with sink:
            left = limit
            while left > 0:
                data = _read(raw, shown, min(COPY_CHUNK, left))
                if not data:
                    break
                _write(sink, data)
                left -= len(data)


def _open_source(source: Path, shown: str) -> BinaryIO:
    try:
        return open(source, "rb")
    except OSError as error:
        raise _read_error(shown, error) from None


def _read(raw: BinaryIO, shown: str, size: Optional[int] = None) -> bytes:
    try:
        return raw.read(COPY_CHUNK if size is None else size)
    except OSError as error:
        raise _read_error(shown, error) from None


def _write(sink: IO[Any], data: Any) -> None:
    try:
        sink.write(data)
    except OSError as error:
        raise _write_error(error) from None


def _read_error(shown: str, error: OSError) -> ToolInputError:
    return ToolInputError(f"input_path {shown!r} cannot be read: {error.strerror or error}")


def _write_error(error: OSError) -> ToolInputError:
    return ToolInputError(
        f"the private copy of the input cannot be written in {tempfile.gettempdir()} "
        f"({error.strerror or error}); set TMPDIR to a directory with free space"
    )
