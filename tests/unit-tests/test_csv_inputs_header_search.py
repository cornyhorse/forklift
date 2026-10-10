"""CsvInputHandler.find_header_row: encodings, quoting and the ``auto`` header heuristic."""

from __future__ import annotations

from forklift.inputs.config import CsvInputConfig
from forklift.inputs.csv import CsvInputHandler


class TestHeaderSearchEncodingAndQuoting:
    def test_non_utf8_encoding_is_used_as_configured(self, tmp_path):
        path = tmp_path / "latin.csv"
        path.write_bytes("caf\xe9,na\xefve\n1,2\n".encode("latin-1"))
        handler = CsvInputHandler(CsvInputConfig(encoding="latin-1"))
        assert handler.find_header_row(path) == (0, ["café", "naïve"])

    def test_without_a_quote_char_quotes_are_ordinary_characters(self, tmp_path):
        path = tmp_path / "noquote.csv"
        path.write_text('"id","a,b"\n1,2\n', encoding="utf-8")
        handler = CsvInputHandler(CsvInputConfig(quote_char=""))
        assert handler.find_header_row(path) == (0, ['"id"', '"a', 'b"'])


class TestAutoHeaderMode:
    def test_all_numeric_rows_fall_back_to_the_first_candidate(self, tmp_path):
        path = tmp_path / "numbers.csv"
        path.write_text("\n1,2,3\n4,5,6\n", encoding="utf-8")
        handler = CsvInputHandler(CsvInputConfig(header_mode="auto"))
        assert handler.find_header_row(path) == (1, ["1", "2", "3"])

    def test_empty_cells_do_not_count_as_text_or_numbers(self, tmp_path):
        # Row 0 has one number and two empty cells: were empty cells text, it would win.
        path = tmp_path / "gaps.csv"
        path.write_text("1,,\nid,,amount\n7,,8\n", encoding="utf-8")
        handler = CsvInputHandler(CsvInputConfig(header_mode="auto"))
        assert handler.find_header_row(path) == (1, ["id", "", "amount"])
