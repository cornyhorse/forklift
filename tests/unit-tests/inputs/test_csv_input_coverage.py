from forklift.utils.dedupe import dedupe_column_names
from forklift.inputs.csv_input import _skip_prologue_lines, get_csv_reader
import io
import pytest

def test_dedupe_column_names_multiple_duplicates():
    """
    Test dedupe_column_names with multiple duplicate column names.
    Verifies that each duplicate is suffixed with an incrementing number and the output order is correct.
    """
    names = ["a", "a", "a", "b", "b", "a"]
    result = dedupe_column_names(names)
    assert result == ["a", "a_1", "a_2", "b", "b_1", "a_3"]

def test_dedupe_column_names_fallback_branch():
    """
    Test dedupe_column_names fallback branch when regex does not match numeric suffixes.
    Verifies that non-numeric suffixes are handled and suffixed correctly.
    """
    names = ["foo", "foo", "foo_abc", "foo_abc"]
    result = dedupe_column_names(names)
    assert result == ["foo", "foo_1", "foo_abc", "foo_abc_1"]

def test_dedupe_column_names_fallback_deep():
    """
    Test dedupe_column_names fallback branch with repeated non-numeric suffixes.
    Verifies that each duplicate is suffixed with an incrementing number.
    """
    names = ["foo_abc", "foo_abc", "foo_abc", "foo_abc"]
    result = dedupe_column_names(names)
    assert result == ["foo_abc", "foo_abc_1", "foo_abc_2", "foo_abc_3"]

def test_dedupe_column_names_fallback_multiple():
    """
    Test dedupe_column_names fallback branch with repeated numeric and non-numeric suffixes.
    Verifies that suffixes are incremented correctly for each duplicate.
    """
    names = ["foo_abc", "foo_abc", "foo_abc_1", "foo_abc_1"]
    result = dedupe_column_names(names)
    assert result == ["foo_abc", "foo_abc_1", "foo_abc_1_1", "foo_abc_1_2"]

def test_dedupe_column_names_fallback_deepest():
    """
    Test dedupe_column_names fallback branch with deeply nested numeric suffixes.
    Verifies that suffixes are incremented correctly for each duplicate.
    """
    names = ["foo", "foo", "foo_1", "foo_1", "foo_1_1", "foo_1_1"]
    result = dedupe_column_names(names)
    assert result == ["foo", "foo_1", "foo_1_1", "foo_1_2", "foo_1_1_1", "foo_1_1_2"]

def test_dedupe_column_names_fallback_non_numeric_suffix():
    """
    Test dedupe_column_names fallback branch for non-numeric suffixes.
    Verifies that non-numeric suffixes are handled and suffixed correctly for each duplicate.
    """
    names = ["foo_abc", "foo_abc", "foo_abc_abc", "foo_abc_abc", "foo_abc_abc_abc", "foo_abc_abc_abc"]
    result = dedupe_column_names(names)
    assert result == [
        "foo_abc", "foo_abc_1", "foo_abc_abc", "foo_abc_abc_1", "foo_abc_abc_abc", "foo_abc_abc_abc_1"
    ]

def test_skip_prologue_lines_header_not_found_scan_limit():
    """
    Test _skip_prologue_lines for scan limit exceeded when header row is not found.
    Verifies that ValueError is raised with the correct message.
    """
    fh = io.StringIO("# comment\n# another\nnot_header\n")
    with pytest.raises(ValueError, match="header_row not found in first 2 rows"):
        _skip_prologue_lines(fh, header_row=["header"], max_scan_rows=2)

def test_skip_prologue_lines_header_not_found_eof():
    """
    Test _skip_prologue_lines for EOF reached when header row is not found.
    Verifies that ValueError is raised with the correct message.
    """
    fh = io.StringIO("# comment\n# another\nnot_header\n")
    with pytest.raises(ValueError, match="header_row not found in file"):
        _skip_prologue_lines(fh, header_row=["header"], max_scan_rows=None)

def test_skip_prologue_lines_scan_limit_no_header():
    """
    Test _skip_prologue_lines for scan limit exceeded when no header row is provided.
    Verifies that no exception is raised and function returns normally.
    """
    fh = io.StringIO("# comment\n# another\nnot_header\n")
    _skip_prologue_lines(fh, header_row=None, max_scan_rows=2)
    # No assertion needed; just ensure no exception is raised

def test_get_csv_reader_skipinitialspace():
    fh = io.StringIO("a, b\nc, d\n")
    reader = list(get_csv_reader(fh, ","))
    assert reader == [["a", "b"], ["c", "d"]]
