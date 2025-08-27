from __future__ import annotations
from pathlib import Path
import io
import os

import pytest

from forklift.inputs.csv_input import CSVInput, _dedupe_column_names, _looks_like_header, _skip_prologue_lines, get_csv_reader


def write(p: Path, text: str, encoding: str = "utf-8") -> None:
    p.write_text(text, encoding=encoding, newline="\n")


def rows_from(path: Path, **opts):
    inp = CSVInput(str(path), **opts)
    return list(inp.iter_rows())


def test_prologue_header_detection_footer_and_normalization(tmp_path: Path):
    f = tmp_path / "customers.csv"
    write(
        f,
        "# vendor: acme\n# exported: 2024-01-01\n"
        "ID, Name Weird, Amount USD\n"
        "1, Amy, 10.00\n"
        "2, Ben, 20.50\n"
        "TOTAL, 2, 30.50\n"  # should be skipped
    )
    rs = rows_from(f, delimiter=",", encoding_priority=["utf-8"])
    assert len(rs) == 2
    # keys are normalized to PG-safe + deduped
    assert list(rs[0].keys()) == ["id", "name_weird", "amount_usd"]
    assert rs[0]["id"] == "1"
    assert rs[1]["name_weird"] == "Ben"


def test_header_override_headerless_file(tmp_path: Path):
    f = tmp_path / "headerless.csv"
    write(
        f,
        "1\tAmy\n"
        "2\tBen\n"
        "2\tBen\n"  # consecutive duplicate → skipped
    )
    rs = rows_from(
        f,
        delimiter="\t",
        encoding_priority=["utf-8"],
        header_override=["id", "name"],
        has_header=False
    )
    # third row is a consecutive duplicate of the second, so we only keep 2
    assert len(rs) == 2
    assert list(rs[0].keys()) == ["id", "name"]
    assert rs[0]["name"] == "Amy"
    assert rs[1]["id"] == "2"


def test_utf8_sig_bom_and_empty_row_skip(tmp_path: Path):
    f = tmp_path / "bom.csv"
    write(
        f,
        "A,B\n"
        "x,y\n"
        "\n"  # empty → skipped
        "p,q\n",
        encoding="utf-8-sig",
    )
    rs = rows_from(f, delimiter=",", encoding_priority=["utf-8-sig", "utf-8"])
    assert len(rs) == 2
    assert rs[0]["a"] == "x" and rs[0]["b"] == "y"
    assert rs[1]["a"] == "p" and rs[1]["b"] == "q"


def test_summary_footer_is_skipped(tmp_path: Path):
    f = tmp_path / "with_summary.csv"
    write(
        f,
        "col1,col2\n"
        "a,1\n"
        "b,2\n"
        "SUMMARY,3\n"
    )
    rs = rows_from(f, delimiter=",", encoding_priority=["utf-8"])
    assert len(rs) == 2
    assert rs[1]["col1"] == "b"


def test_tab_delimiter_with_normalization_and_dedupe_headers(tmp_path: Path):
    f = tmp_path / "tab.tsv"
    # Two identical header names to trigger _dedupe → col and col_1
    write(
        f,
        "Col\tCol\tOther Val\n"
        "a\tb\tc\n"
    )
    rs = rows_from(f, delimiter="\t", encoding_priority=["utf-8"])
    assert len(rs) == 1
    assert list(rs[0].keys()) == ["col", "col_1", "other_val"]
    assert rs[0]["col_1"] == "b"


def test_empty_file_yields_no_rows(tmp_path: Path):
    f = tmp_path / "empty.csv"
    write(f, "")
    rs = rows_from(f, delimiter=",", encoding_priority=["utf-8"])
    assert rs == []


def test_dedupe_column_names_unique():
    assert _dedupe_column_names(["a", "b", "c"]) == ["a", "b", "c"]


def test_dedupe_column_names_duplicates():
    assert _dedupe_column_names(["x", "x", "x"]) == ["x", "x_1", "x_2"]


def test_dedupe_column_names_mixed():
    assert _dedupe_column_names(["id", "name", "name", "amount", "name"]) == ["id", "name", "name_1", "amount", "name_2"]


def test_dedupe_column_names_empty():
    assert _dedupe_column_names([]) == []


def test_looks_like_header_all_non_digit():
    assert _looks_like_header(["id", "name", "amount"]) is True


def test_looks_like_header_some_digits():
    assert _looks_like_header(["id", "name", "2024"]) is False


def test_looks_like_header_empty():
    assert _looks_like_header([]) is True


def test_skip_prologue_lines_skips_comments_and_blanks():
    data = io.StringIO("# comment\n\nID,Name\n1,Alice\n")
    _skip_prologue_lines(data)
    assert data.readline().strip() == "ID,Name"


def test_skip_prologue_lines_header_row_detection():
    data = io.StringIO("# comment\n\nID,Name\n1,Alice\n")
    _skip_prologue_lines(data, ["ID", "Name"])
    assert data.readline().strip() == "ID,Name"


def test_skip_prologue_lines_scan_limit():
    data = io.StringIO("# comment\n\nID,Name\n1,Alice\n")
    with pytest.raises(ValueError):
        _skip_prologue_lines(data, ["Not", "Found"], max_scan_rows=2)


def test_get_csv_reader_delimiter_and_skipinitialspace():
    data = io.StringIO("id, name ,age\n1, Alice , 30\n2,Bob,25")
    reader = get_csv_reader(data, ",")
    rows = list(reader)
    assert rows[0] == ["id", "name ", "age"]
    assert rows[1] == ["1", "Alice ", "30"]
    assert rows[2] == ["2", "Bob", "25"]


def test_csvinput_get_raw_header_with_header():
    data = io.StringIO("id,name\n1,Alice\n")
    reader = get_csv_reader(data, ",")
    inp = CSVInput("dummy.csv")
    header = inp._get_raw_header(reader, True, None)
    assert header == ["id", "name"]


def test_csvinput_get_raw_header_with_override():
    data = io.StringIO("id,name\n1,Alice\n")
    reader = get_csv_reader(data, ",")
    inp = CSVInput("dummy.csv")
    override = ["foo", "bar"]
    header = inp._get_raw_header(reader, True, override)
    assert header == override


def test_csvinput_get_raw_header_headerless_with_override():
    data = io.StringIO("1,Alice\n")
    reader = get_csv_reader(data, ",")
    inp = CSVInput("dummy.csv")
    override = ["foo", "bar"]
    header = inp._get_raw_header(reader, False, override)
    assert header == override


def test_csvinput_get_raw_header_headerless_no_override():
    data = io.StringIO("1,Alice\n")
    reader = get_csv_reader(data, ",")
    inp = CSVInput("dummy.csv")
    with pytest.raises(ValueError):
        inp._get_raw_header(reader, False, None)
