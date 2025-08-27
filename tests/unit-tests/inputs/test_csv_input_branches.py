from pathlib import Path
from forklift.inputs.csv_input import CSVInput


def _write(p: Path, s: str):
    p.write_text(s, encoding="utf-8")


def test_header_override(tmp_path: Path):
    f = tmp_path / "h.csv"
    _write(f, "A,B\n1,2\n")
    inp = CSVInput(source=str(f), delimiter=",", encoding_priority=["utf-8"], header_override=["X", "Y"])
    rows = list(inp.iter_rows())
    assert rows == [{"x": "1", "y": "2"}]


def test_duplicate_headers_suffix(tmp_path: Path):
    f = tmp_path / "d.csv"
    _write(f, "name,name,amt\nAmy,A.,10\n")
    inp = CSVInput(source=str(f), delimiter=",", encoding_priority=["utf-8"])
    rows = list(inp.iter_rows())
    assert set(rows[0].keys()) == {"name", "name_1", "amt"}


def test_footer_regex_skip(tmp_path: Path):
    f = tmp_path / "t.csv"
    _write(f, "id\tval\n1\t10\n2\t20\nTOTAL\t2\n")
    inp = CSVInput(source=str(f), delimiter="\t", encoding_priority=["utf-8"])
    rows = list(inp.iter_rows())
    # 2 data rows; footer dropped
    assert len(rows) == 2


def test_header_mode_present(tmp_path: Path):
    f = tmp_path / "present.csv"
    f.write_text("col1,col2\n1,2\n3,4\n", encoding="utf-8")
    inp = CSVInput(source=str(f), delimiter=",", encoding_priority=["utf-8"], header_mode="present")
    rows = list(inp.iter_rows())
    assert rows == [{"col1": "1", "col2": "2"}, {"col1": "3", "col2": "4"}]


def test_header_mode_absent_with_override(tmp_path: Path):
    f = tmp_path / "absent.csv"
    f.write_text("1,2\n3,4\n", encoding="utf-8")
    inp = CSVInput(source=str(f), delimiter=",", encoding_priority=["utf-8"], header_mode="absent", header_override=["foo", "bar"])
    rows = list(inp.iter_rows())
    assert rows == [{"foo": "1", "bar": "2"}, {"foo": "3", "bar": "4"}]


def test_header_mode_absent_mismatched_columns(tmp_path: Path):
    f = tmp_path / "absent_mismatch.csv"
    f.write_text("1,2,3\n4,5,6\n", encoding="utf-8")
    inp = CSVInput(source=str(f), delimiter=",", encoding_priority=["utf-8"], header_mode="absent", header_override=["a", "b"])
    rows = list(inp.iter_rows())
    # Extra columns are mapped to None as per implementation
    assert rows == [
        {"a": "1", "b": "2", None: ["3"]},
        {"a": "4", "b": "5", None: ["6"]}
    ]


def test_skip_empty_rows(tmp_path: Path):
    f = tmp_path / "empty_rows.csv"
    # Header, data row, empty row, whitespace row, delimiter-only row, another data row
    f.write_text("a,b\n1,2\n\n   \n,,\n3,4\n", encoding="utf-8")
    inp = CSVInput(source=str(f), delimiter=",", encoding_priority=["utf-8"], header_mode="present")
    rows = list(inp.iter_rows())
    # Only the two data rows should be present
    assert rows == [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]
