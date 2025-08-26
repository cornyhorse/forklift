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
