from __future__ import annotations
from pathlib import Path
import io
import os

import pytest

from forklift.inputs.csv_input import CSVInput, _dedupe_column_names, _skip_prologue_lines, \
    get_csv_reader


def write(p: Path, text: str, encoding: str = "utf-8") -> None:
    """
    Write the given text to the specified file path using the provided encoding.
    Ensures newlines are normalized to '\n'.

    Args:
        p (Path): The file path to write to.
        text (str): The text content to write.
        encoding (str): The encoding to use (default: 'utf-8').
    """
    p.write_text(text, encoding=encoding, newline="\n")


def rows_from(path: Path, **opts):
    """
    Helper to read all rows from a CSVInput instance for the given file path and options.

    Args:
        path (Path): The file path to read from.
        **opts: Additional options to pass to CSVInput.

    Returns:
        list: List of row dictionaries from the CSVInput.
    """
    inp = CSVInput(str(path), **opts)
    return list(inp.iter_rows())


def test_prologue_header_detection_footer_and_normalization(tmp_path: Path):
    """
    Test reading a CSV file with prologue comments, a header row, data rows, and a footer row.
    - Ensures prologue lines (comments) are skipped
    - Ensures footer row ("TOTAL") is skipped
    - Ensures header normalization and deduplication to PG-safe names
    - Ensures data rows are parsed correctly and mapped to normalized headers
    """
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
    """
    Tests reading a headerless file with a header_override provided.
    Verifies that:
    - The override is used for column names
    - Consecutive duplicate rows are skipped
    - Data rows are parsed correctly
    """
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
    """
    Tests reading a CSV file with a UTF-8 BOM and empty rows.
    Verifies that:
    - BOM is handled correctly
    - Empty rows are skipped
    - Data rows are parsed correctly
    """
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
    """
    Tests that a summary/footer row ("SUMMARY") is skipped when reading a CSV file.
    Verifies that only data rows are returned.
    """
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
    """
    Tests reading a tab-delimited file with duplicate header names.
    Verifies that:
    - Tab delimiter is handled
    - Duplicate headers are deduped
    - Header normalization to PG-safe names
    """
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
    """
    Tests that reading an empty file yields no rows.
    """
    f = tmp_path / "empty.csv"
    write(f, "")
    rs = rows_from(f, delimiter=",", encoding_priority=["utf-8"])
    assert rs == []


def test_dedupe_column_names_unique():
    """
    Tests that _dedupe_column_names returns the same list when all names are unique.
    """
    assert _dedupe_column_names(["a", "b", "c"]) == ["a", "b", "c"]


def test_dedupe_column_names_duplicates():
    """
    Tests that _dedupe_column_names correctly dedupes duplicate column names by appending numeric suffixes.
    """
    assert _dedupe_column_names(["x", "x", "x"]) == ["x", "x_1", "x_2"]


def test_dedupe_column_names_mixed():
    """
    Tests _dedupe_column_names with a mix of unique and duplicate column names, verifying correct suffixing and order.
    """
    assert _dedupe_column_names(["id", "name", "name", "amount", "name"]) == ["id", "name", "name_1", "amount",
                                                                              "name_2"]


def test_dedupe_column_names_empty():
    """
    Tests that _dedupe_column_names returns an empty list when given an empty input list.
    """
    assert _dedupe_column_names([]) == []


def test_dedupe_column_names_suffix_collision():
    """
    Tests _dedupe_column_names for suffix collision cases, e.g., when a name with a suffix already exists.
    Verifies that further duplicates are correctly suffixed to avoid collision.
    """
    # This triggers the while new_name in seen_counts loop
    # e.g., x, x, x_1, x should produce x, x_1, x_1_1, x_2
    result = _dedupe_column_names(["x", "x", "x_1", "x"])
    assert result == ["x", "x_1", "x_1_1", "x_2"]


def test_skip_prologue_lines_skips_comments_and_blanks():
    """
    Tests that _skip_prologue_lines skips prologue comments and blank lines, positioning the file handle at the header row.
    """
    data = io.StringIO("# comment\n\nID,Name\n1,Alice\n")
    _skip_prologue_lines(data)
    assert data.readline().strip() == "ID,Name"


def test_skip_prologue_lines_header_row_detection():
    """
    Tests that _skip_prologue_lines can detect and position the file handle at a specified header row.
    """
    data = io.StringIO("# comment\n\nID,Name\n1,Alice\n")
    _skip_prologue_lines(data, ["ID", "Name"])
    assert data.readline().strip() == "ID,Name"


def test_skip_prologue_lines_scan_limit():
    """
    Tests that _skip_prologue_lines raises ValueError if the header row is not found within the scan limit.
    """
    data = io.StringIO("# comment\n\nID,Name\n1,Alice\n")
    with pytest.raises(ValueError):
        _skip_prologue_lines(data, ["Not", "Found"], max_scan_rows=2)


def test_skip_prologue_lines_scan_limit_error():
    """
    Tests that _skip_prologue_lines raises ValueError with a specific message when the scan limit is reached and the header is not found.
    """
    # Should raise ValueError after scan limit reached and header not found
    data = io.StringIO("# comment\nfoo,bar\n1,2\n")
    with pytest.raises(ValueError, match="Provided header_row not found in first 2 rows."):
        _skip_prologue_lines(data, ["not", "found"], max_scan_rows=2)


def test_skip_prologue_lines_eof_error():
    """
    Tests that _skip_prologue_lines raises ValueError if the header row is not found before EOF.
    """
    # Should raise ValueError if header row not found before EOF
    data = io.StringIO("# comment\nfoo,bar\n1,2\n")
    with pytest.raises(ValueError, match="Provided header_row not found in file."):
        _skip_prologue_lines(data, ["not", "found"], max_scan_rows=10)


def test_get_csv_reader_delimiter_and_skipinitialspace():
    """
    Tests get_csv_reader for correct delimiter handling and skipinitialspace behavior.
    Verifies that spaces after delimiters are handled as expected.
    """
    data = io.StringIO("id, name ,age\n1, Alice , 30\n2,Bob,25")
    reader = get_csv_reader(data, ",")
    rows = list(reader)
    assert rows[0] == ["id", "name ", "age"]
    assert rows[1] == ["1", "Alice ", "30"]
    assert rows[2] == ["2", "Bob", "25"]


def test_csvinput_get_raw_header_with_header():
    """
    Tests CSVInput._get_raw_header with has_header=True and no override, expecting the file header row to be returned.
    """
    data = io.StringIO("id,name\n1,Alice\n")
    reader = get_csv_reader(data, ",")
    inp = CSVInput("dummy.csv")
    header = inp._get_raw_header(reader, True, None)
    assert header == ["id", "name"]


def test_csvinput_get_raw_header_with_override():
    """
    Tests CSVInput._get_raw_header with has_header=True and a header_override, expecting the override to be returned.
    """
    data = io.StringIO("id,name\n1,Alice\n")
    reader = get_csv_reader(data, ",")
    inp = CSVInput("dummy.csv")
    override = ["foo", "bar"]
    header = inp._get_raw_header(reader, True, override)
    assert header == override


def test_csvinput_get_raw_header_headerless_with_override():
    """
    Tests CSVInput._get_raw_header with has_header=False and a header_override, expecting the override to be returned.
    """
    data = io.StringIO("1,Alice\n")
    reader = get_csv_reader(data, ",")
    inp = CSVInput("dummy.csv")
    override = ["foo", "bar"]
    header = inp._get_raw_header(reader, False, override)
    assert header == override


def test_csvinput_get_raw_header_headerless_no_override():
    """
    Tests CSVInput._get_raw_header with has_header=False and no override, expecting a ValueError to be raised.
    """
    data = io.StringIO("1,Alice\n")
    reader = get_csv_reader(data, ",")
    inp = CSVInput("dummy.csv")
    with pytest.raises(ValueError):
        inp._get_raw_header(reader, False, None)
