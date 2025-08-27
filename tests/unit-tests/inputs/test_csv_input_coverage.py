from forklift.inputs.csv_input import _dedupe_column_names, _skip_prologue_lines, get_csv_reader
import io
import pytest

def test_dedupe_column_names_multiple_duplicates():
    names = ["a", "a", "a", "b", "b", "a"]
    result = _dedupe_column_names(names)
    assert result == ["a", "a_1", "a_2", "b", "b_1", "a_3"]

def test_dedupe_column_names_fallback_branch():
    # This triggers the fallback branch where regex does not match
    # e.g., a name with no digits at the end
    names = ["foo", "foo", "foo_abc", "foo_abc"]
    result = _dedupe_column_names(names)
    # The second "foo_abc" should trigger the fallback and become "foo_abc_1"
    assert result == ["foo", "foo_1", "foo_abc", "foo_abc_1"]

def test_dedupe_column_names_fallback_deep():
    # This triggers the fallback branch multiple times
    names = ["foo_abc", "foo_abc", "foo_abc", "foo_abc"]
    result = _dedupe_column_names(names)
    # Actual behavior: foo_abc, foo_abc_1, foo_abc_2, foo_abc_3
    assert result == ["foo_abc", "foo_abc_1", "foo_abc_2", "foo_abc_3"]

def test_dedupe_column_names_fallback_multiple():
    # This triggers the fallback branch repeatedly
    names = ["foo_abc", "foo_abc", "foo_abc_1", "foo_abc_1"]
    result = _dedupe_column_names(names)
    # Actual behavior: foo_abc, foo_abc_1, foo_abc_1_1, foo_abc_1_2
    assert result == ["foo_abc", "foo_abc_1", "foo_abc_1_1", "foo_abc_1_2"]

def test_dedupe_column_names_fallback_deepest():
    # This triggers the fallback branch in the while loop multiple times
    names = ["foo", "foo", "foo_1", "foo_1", "foo_1_1", "foo_1_1"]
    result = _dedupe_column_names(names)
    # Should produce foo, foo_1, foo_1_1, foo_1_2, foo_1_1_1, foo_1_1_2
    assert result == ["foo", "foo_1", "foo_1_1", "foo_1_2", "foo_1_1_1", "foo_1_1_2"]


def test_dedupe_column_names_fallback_non_numeric_suffix():
    # This test guarantees the fallback branch in the while loop is hit for non-numeric suffixes
    names = ["foo_abc", "foo_abc", "foo_abc_abc", "foo_abc_abc", "foo_abc_abc_abc", "foo_abc_abc_abc"]
    result = _dedupe_column_names(names)
    # The output should keep appending _1 for non-numeric suffixes
    assert result == [
        "foo_abc", "foo_abc_1", "foo_abc_abc", "foo_abc_abc_1", "foo_abc_abc_abc", "foo_abc_abc_abc_1"
    ]


def test_skip_prologue_lines_header_not_found_scan_limit():
    fh = io.StringIO("# comment\n# another\nnot_header\n")
    with pytest.raises(ValueError, match="header_row not found in first 2 rows"):
        _skip_prologue_lines(fh, header_row=["header"], max_scan_rows=2)

def test_skip_prologue_lines_header_not_found_eof():
    fh = io.StringIO("# comment\n# another\nnot_header\n")
    with pytest.raises(ValueError, match="header_row not found in file"):
        _skip_prologue_lines(fh, header_row=["header"], max_scan_rows=None)

def test_skip_prologue_lines_scan_limit_no_header():
    fh = io.StringIO("# comment\n# another\nnot_header\n")
    # Should hit scan limit and return, not raise
    _skip_prologue_lines(fh, header_row=None, max_scan_rows=2)
    # No assertion needed; just ensure no exception is raised

def test_get_csv_reader_skipinitialspace():
    fh = io.StringIO("a, b\nc, d\n")
    reader = list(get_csv_reader(fh, ","))
    assert reader == [["a", "b"], ["c", "d"]]
