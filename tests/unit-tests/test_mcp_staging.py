"""forklift-mcp: the private copies the interactive tools run on (``forklift.mcp.staging``).

Counting CSV records across chunks, copying exactly the first records in bounded memory, the
fallback for undecodable files, and a specific message for every way reading or writing fails.
"""

from __future__ import annotations

import errno
import io

import pytest

from forklift.jobs import JobResult
from forklift.jobs.spec import InputOptions
from forklift.mcp import staging
from forklift.mcp.staging import RecordCounter, Staged, check_dialect, scratch_directory, stage
from forklift.mcp.workspace import ToolInputError

CSV = 'id,text\n1,"two\nlines"\n\n2,"a ""quote"""\r\n3,plain\n4,last'


def _count(text, chunk, wanted=10**9, quote='"', escape=None):
    counter = RecordCounter(quote, escape)
    ends = []
    for start in range(0, len(text), chunk):
        end = counter.feed(text[start : start + chunk], wanted)
        if end is not None:
            ends.append(start + end)
            break
    else:
        counter.finish()
    return counter.records, ends


class TestRecordCounter:
    @pytest.mark.parametrize("chunk", [1, 2, 3, 5, 7, 1000])
    def test_records_are_the_same_whatever_the_chunks(self, chunk):
        # header, a quoted line break, a blank line (not a record), doubled quotes, CRLF, and a
        # last record without a line break
        assert _count(CSV, chunk) == (5, [])

    @pytest.mark.parametrize("chunk", [1, 4, 1000])
    def test_where_the_wanted_record_ends(self, chunk):
        records, ends = _count(CSV, chunk, wanted=2)
        assert records == 2 and CSV[: ends[0]] == 'id,text\n1,"two\nlines"\n'

    @pytest.mark.parametrize("chunk", [1, 2, 3, 1000])
    def test_escapes(self, chunk):
        text = 'a\\\nb\nc\\"d\n"q\\"x"\nlast\\'
        # \<newline> and \" are escaped; the escape at the very end protects nothing
        assert _count(text, chunk, escape="\\")[0] == 4

    def test_blank_and_carriage_return_only_lines_are_not_records(self):
        assert _count("\n\r\n\n", 2) == (0, [])
        assert _count("x\n\r\n", 1000) == (1, [])

    def test_a_quote_or_escape_is_content(self):
        assert _count('""\n', 1000)[0] == 1
        assert _count("\\\n\n", 1000, escape="\\")[0] == 1


def _options(**values):
    return InputOptions.from_dict(values)


@pytest.fixture
def scratch(tmp_path):
    directory = tmp_path / "scratch"
    directory.mkdir()
    return directory


class TestStage:
    def test_the_first_records_are_copied_as_they_are(self, tmp_path, scratch):
        source = tmp_path / "people.csv"
        source.write_bytes(CSV.encode())
        staged = stage(source, "people.csv", "csv", scratch, _options(), 3)
        assert staged.path == "in/people.csv" and staged.records == 3
        copied = (scratch / "in" / "people.csv").read_bytes()
        assert copied == b'id,text\n1,"two\nlines"\n\n2,"a ""quote"""\r\n'
        assert staged.view == {
            "path": "people.csv",
            "format": "csv",
            "bytes": len(CSV),
            "sampled": True,
        }

    def test_the_whole_file_when_it_has_fewer_records(self, tmp_path, scratch, monkeypatch):
        monkeypatch.setattr(staging, "COPY_CHUNK", 4)
        source = tmp_path / "people.csv"
        source.write_bytes(("﻿" + CSV).encode())
        staged = stage(source, "people.csv", "csv", scratch, _options(), 100)
        assert staged.records == 5 and staged.view["sampled"] is False
        assert (scratch / "in" / "people.csv").read_bytes() == CSV.encode()  # without the BOM

    def test_an_empty_file(self, tmp_path, scratch):
        source = tmp_path / "empty.csv"
        source.write_text("")
        staged = stage(source, "empty.csv", "csv", scratch, _options(), 5)
        assert staged.records == 0 and staged.view["sampled"] is False

    def test_a_first_record_longer_than_the_copy_may_be(self, tmp_path, scratch, monkeypatch):
        # Review finding 7: the copy was empty and validate_schema then said valid
        monkeypatch.setattr(staging, "MAX_SAMPLE_COPY_CHARS", 50)
        monkeypatch.setattr(staging, "COPY_CHUNK", 16)
        source = tmp_path / "wide.csv"
        source.write_text("a" * 200 + "\n1\n")
        with pytest.raises(ToolInputError, match="first record of input_path 'wide.csv' is long"):
            stage(source, "wide.csv", "csv", scratch, _options(), 5)

    def test_the_copy_stops_at_its_size_limit_with_records(self, tmp_path, scratch, monkeypatch):
        monkeypatch.setattr(staging, "MAX_SAMPLE_COPY_CHARS", 10)
        source = tmp_path / "x.csv"
        source.write_text("id\n1\n2\n3\n4\n5\n6\n7\n")
        staged = stage(source, "x.csv", "csv", scratch, _options(), 100)
        assert staged.view["sampled"] is True and staged.records == 4
        assert (scratch / "in" / "x.csv").read_text() == "id\n1\n2\n3\n4"

    def test_an_undecodable_file_is_copied_as_bytes_up_to_where_it_was_read(
        self, tmp_path, scratch, monkeypatch
    ):
        monkeypatch.setattr(staging, "COPY_CHUNK", 8)
        source = tmp_path / "latin.csv"
        source.write_bytes(b"id,name\n1,Ren\xe9\n" + b"2,x\n" * 100)
        staged = stage(source, "latin.csv", "csv", scratch, _options(), 1000)
        copied = (scratch / "in" / "latin.csv").read_bytes()
        assert staged.records is None and staged.view["sampled"] is True
        assert copied.startswith(b"id,name\n1,Ren\xe9\n") and len(copied) <= 32

    def test_an_unknown_encoding_is_left_to_the_engine(self, tmp_path, scratch):
        source = tmp_path / "x.csv"
        source.write_text("id\n1\n")
        staged = stage(source, "x.csv", "csv", scratch, _options(encoding="nope"), 5)
        assert staged.records is None and staged.view["sampled"] is False
        assert (scratch / "in" / "x.csv").read_text() == "id\n1\n"

    def test_a_name_the_contract_cannot_hold(self, tmp_path, scratch):
        source = tmp_path / "a\\b.CSV"
        source.write_text("id\n")
        assert stage(source, "a\\b.CSV", "csv", scratch, _options(), 5).path == "in/input.csv"

    def test_a_workbook_is_copied_whole(self, tmp_path, scratch, monkeypatch):
        monkeypatch.setattr(staging, "COPY_CHUNK", 3)
        source = tmp_path / "book.xlsx"
        source.write_bytes(b"0123456789")
        staged = stage(source, "book.xlsx", "excel", scratch, _options(), 5)
        assert staged.records is None and staged.view["sampled"] is False
        assert (scratch / "in" / "book.xlsx").read_bytes() == b"0123456789"
        monkeypatch.setattr(staging, "MAX_EXCEL_COPY_BYTES", 5)
        with pytest.raises(ToolInputError, match="a 10-byte workbook; .* up to 5 bytes"):
            stage(source, "book.xlsx", "excel", scratch / "in", _options(), 5)

    @pytest.mark.parametrize(
        "options",
        [
            {"delimiter": '"'},
            {"quote_char": ","},
            {"delimiter": ";", "quote_char": ";"},
        ],
    )
    def test_options_that_make_no_dialect(self, tmp_path, scratch, options):
        # Review finding 8: these used to end as "Error executing tool preview"
        source = tmp_path / "x.csv"
        source.write_text("id\n")
        with pytest.raises(ToolInputError, match="do not make a CSV dialect: bad delimiter"):
            stage(source, "x.csv", "csv", scratch, _options(**options), 5)
        check_dialect(_options(delimiter=";", escape_char="\\"))


class TestStageFailures:
    """Review finding 8: every way reading or writing fails says so (never a bare crash)."""

    @pytest.fixture
    def source(self, tmp_path):
        path = tmp_path / "x.csv"
        path.write_text("id\n1\n2\n")
        return path

    def test_the_input_vanished(self, tmp_path, scratch):
        with pytest.raises(ToolInputError, match="input_path 'x.csv' cannot be read: No such"):
            stage(tmp_path / "gone.csv", "x.csv", "csv", scratch, _options(), 5)

    def test_the_scratch_directory_cannot_be_used(self, source, tmp_path):
        blocker = tmp_path / "file"
        blocker.write_text("")
        with pytest.raises(ToolInputError, match="the private copy of the input cannot be writ"):
            stage(source, "x.csv", "csv", blocker, _options(), 5)

    @pytest.mark.parametrize("fmt", ["csv", "excel"])
    def test_the_input_cannot_be_opened(self, source, scratch, monkeypatch, fmt):
        real_open = open

        def refuse(path, *args, **kwargs):
            if str(path) == str(source):
                raise PermissionError(errno.EACCES, "Permission denied", str(path))
            return real_open(path, *args, **kwargs)

        monkeypatch.setattr(staging, "open", refuse, raising=False)
        with pytest.raises(ToolInputError, match="'x.csv' cannot be read: Permission denied"):
            stage(source, "x.csv", fmt, scratch, _options(), 5)

    @pytest.mark.parametrize(
        "fmt, options", [("csv", {}), ("csv", {"encoding": "nope"}), ("excel", {})]
    )
    def test_the_copy_cannot_be_written(self, source, scratch, monkeypatch, fmt, options):
        real_open = open

        def full(path, mode="r", *args, **kwargs):
            if "w" in mode:
                raise OSError(errno.ENOSPC, "No space left on device", str(path))
            return real_open(path, mode, *args, **kwargs)

        monkeypatch.setattr(staging, "open", full, raising=False)
        with pytest.raises(ToolInputError, match=r"cannot be written in .* \(No space left"):
            stage(source, "x.csv", fmt, scratch, _options(**options), 5)

    @pytest.mark.parametrize("fmt", ["csv", "excel"])
    def test_reading_fails_part_way(self, source, scratch, monkeypatch, fmt):
        class Broken(io.BytesIO):
            def read(self, size=-1):
                raise OSError(errno.EIO, "Input/output error")

        monkeypatch.setattr(staging, "_open_source", lambda path, shown: Broken())
        with pytest.raises(ToolInputError, match="cannot be read: Input/output error"):
            stage(source, "x.csv", fmt, scratch, _options(), 5)

    @pytest.mark.parametrize("fmt", ["csv", "excel"])
    def test_writing_fails_part_way(self, source, scratch, monkeypatch, fmt):
        real_open = open

        class Full:
            def __init__(self, handle):
                self.handle = handle

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                self.handle.close()

            def write(self, data):
                raise OSError(errno.EFBIG, "File too large")

        def opener(path, mode="r", *args, **kwargs):
            handle = real_open(path, mode, *args, **kwargs)
            return Full(handle) if "w" in mode else handle

        monkeypatch.setattr(staging, "open", opener, raising=False)
        with pytest.raises(ToolInputError, match=r"cannot be written .* \(File too large\)"):
            stage(source, "x.csv", fmt, scratch, _options(), 5)

    def test_no_private_directory(self, monkeypatch):
        def refuse(**kwargs):
            raise OSError(errno.EROFS, "Read-only file system")

        monkeypatch.setattr(staging.tempfile, "mkdtemp", refuse)
        with pytest.raises(ToolInputError, match=r"private temporary directory .*Read-only"):
            with scratch_directory():
                pass

    def test_the_private_directory_is_removed(self):
        with scratch_directory() as directory:
            (directory / "x").write_text("x")
        assert not directory.exists()


class TestStaged:
    def test_messages_name_what_the_caller_can_open(self):
        staged = Staged("in/x.csv", "csv", {"path": "data/x.csv"}, 3)
        result = JobResult.from_dict(
            {
                "spec_version": 1,
                "job_id": "j",
                "status": "succeeded",
                "counts": {},
                "schema_extensions": [],
                "validation_summary": {},
                "warnings": [],
                "artifacts": [
                    {
                        "kind": "bad_rows",
                        "path": "out/bad_rows.parquet",
                        "rows": 1,
                        "bytes": 1,
                        "sha256": None,
                    },
                    {
                        "kind": "data",
                        "path": "out/data.parquet",
                        "rows": 1,
                        "bytes": 1,
                        "sha256": None,
                    },
                ],
                "error": None,
            }
        )
        staged.name_artifacts(result, bad_rows="BAD")
        text = "in/x.csv: out/bad_rows.parquet, out/data.parquet, .forklift-job-a1/s.json"
        assert staged.clean(text) == (
            "data/x.csv: BAD, the data file of the temporary copy (not kept), "
            "<temporary directory>/s.json"
        )
