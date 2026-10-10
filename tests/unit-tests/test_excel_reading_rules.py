"""Behaviour of ExcelInputHandler: cell typing, column mappings, header settings, archive
checks and .xls cell conversion.

Workbooks are written with openpyxl into ``tmp_path``. Where openpyxl cannot produce a cell
(it serialises every number through a float), the sheet XML is patched afterwards. ``.xls``
books are fakes built from real ``xlrd`` cells, returned by a patched ``xlrd.open_workbook``.
"""

from __future__ import annotations

import datetime as dt
import decimal
import os
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import openpyxl
import pyarrow as pa
import pytest
import xlrd
from openpyxl.chart import BarChart, Reference

from forklift.inputs.config import ExcelInputConfig, ExcelSheetConfig
from forklift.inputs.excel import ExcelInputHandler


def _write_xlsx(path: Path, rows, title: str = "data", iso_dates: bool = False) -> Path:
    wb = openpyxl.Workbook()
    wb.iso_dates = iso_dates
    ws = wb.active
    ws.title = title
    for row in rows:
        ws.append(row)
    wb.save(path)
    return path


def _replace_in_sheet_xml(path: Path, replacements) -> Path:
    """Rewrite raw ``<v>`` texts of the first worksheet (e.g. to store an exact big integer)."""
    with zipfile.ZipFile(path) as source:
        entries = [(info, source.read(info.filename)) for info in source.infolist()]
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as target:
        for info, data in entries:
            if info.filename == "xl/worksheets/sheet1.xml":
                text = data.decode()
                for old, new in replacements.items():
                    assert text.count(old) == 1
                    text = text.replace(old, new)
                data = text.encode()
            target.writestr(info, data)
    return path


def _add_parts(path: Path, parts) -> Path:
    """Append extra archive members ``{name: (data, compress_type)}`` to an .xlsx file."""
    with zipfile.ZipFile(path, "a") as archive:
        for name, (data, compress_type) in parts.items():
            archive.writestr(name, data, compress_type=compress_type)
    return path


def _read_one(path: Path, config: ExcelInputConfig = None, **sheet_kwargs) -> pa.Table:
    if config is None:
        config = ExcelInputConfig(sheets=[ExcelSheetConfig(select={"index": 0}, **sheet_kwargs)])
    ((_, table),) = list(ExcelInputHandler(config).process_sheets(path))
    return table


class TestCellTypes:
    """Each uniform column keeps its natural Arrow type; awkward mixes fall back to text."""

    def test_date_only_cells_become_a_date32_column(self, tmp_path):
        path = _write_xlsx(
            tmp_path / "d.xlsx",
            [["d"], [dt.date(2024, 1, 2)], [dt.date(2024, 2, 3)]],
            iso_dates=True,
        )
        table = _read_one(path)
        assert table.schema.field("d").type == pa.date32()
        assert table["d"].to_pylist() == [dt.date(2024, 1, 2), dt.date(2024, 2, 3)]

    def test_time_cells_become_a_time64_column(self, tmp_path):
        path = _write_xlsx(tmp_path / "t.xlsx", [["t"], [dt.time(8, 30)], [dt.time(17, 5, 9)]])
        table = _read_one(path)
        assert table.schema.field("t").type == pa.time64("us")
        assert table["t"].to_pylist() == [dt.time(8, 30), dt.time(17, 5, 9)]

    def test_duration_cells_become_a_duration_column(self, tmp_path):
        path = _write_xlsx(
            tmp_path / "td.xlsx", [["td"], [dt.timedelta(hours=5)], [dt.timedelta(minutes=7)]]
        )
        table = _read_one(path)
        assert table.schema.field("td").type == pa.duration("us")
        assert table["td"].to_pylist() == [dt.timedelta(hours=5), dt.timedelta(minutes=7)]

    def test_dates_mixed_with_datetimes_become_timestamps_at_midnight(self, tmp_path):
        rows = [["when"], [dt.date(2024, 1, 2)], [dt.datetime(2024, 1, 3, 4, 5)], [None, "x"]]
        table = _read_one(_write_xlsx(tmp_path / "m.xlsx", rows, iso_dates=True))
        assert table.schema.field("when").type == pa.timestamp("us")
        assert table["when"].to_pylist() == [
            dt.datetime(2024, 1, 2, 0, 0),
            dt.datetime(2024, 1, 3, 4, 5),
            None,
        ]

    def test_integer_beyond_int64_is_kept_as_exact_text(self, tmp_path):
        path = _write_xlsx(tmp_path / "big.xlsx", [["n"], [111], [5]])
        _replace_in_sheet_xml(path, {"<v>111</v>": "<v>123456789012345678901234567890</v>"})
        table = _read_one(path)
        assert table.schema.field("n").type == pa.string()
        assert table["n"].to_pylist() == ["123456789012345678901234567890", "5"]

    def test_integral_float_in_a_text_column_is_written_without_decimal_point(self, tmp_path):
        path = _write_xlsx(tmp_path / "code.xlsx", [["code"], ["A7"], [777], [2.5]])
        _replace_in_sheet_xml(path, {"<v>777</v>": "<v>7.0</v>"})
        table = _read_one(path)
        assert table["code"].to_pylist() == ["A7", "7", "2.5"]

    def test_dates_in_a_text_column_are_rendered_in_iso_format(self, tmp_path):
        rows = [["code"], ["A7"], [dt.datetime(2024, 1, 2, 3, 4)], [dt.time(5, 6)]]
        table = _read_one(_write_xlsx(tmp_path / "iso.xlsx", rows))
        assert table["code"].to_pylist() == ["A7", "2024-01-02T03:04:00", "05:06:00"]


class TestColumnMappings:
    """``columns`` selects, positions and types the output columns."""

    @pytest.fixture
    def path(self, tmp_path):
        return _write_xlsx(tmp_path / "cols.xlsx", [["id", "amount"], [1, 2.5], [2, 3.25]])

    def test_decimal_parquet_type_casts_to_decimal128(self, path):
        table = _read_one(path, columns=[{"name": "amount", "parquetType": "decimal(10, 2)"}])
        assert table.schema.field("amount").type == pa.decimal128(10, 2)
        assert table["amount"].to_pylist() == [decimal.Decimal("2.50"), decimal.Decimal("3.25")]

    def test_parquet_type_matching_the_inferred_type_keeps_the_values(self, path):
        table = _read_one(path, columns=[{"name": "id", "parquetType": "int64"}])
        assert table.schema.field("id").type == pa.int64()
        assert table["id"].to_pylist() == [1, 2]

    def test_boolean_position_is_rejected(self, path):
        with pytest.raises(ValueError, match="^Invalid column position for 'id'$"):
            _read_one(path, columns=[{"name": "id", "position": True}])

    def test_position_that_is_neither_letters_nor_a_number_is_rejected(self, path):
        with pytest.raises(ValueError, match=r"^Invalid column position 'A1' for 'id'$"):
            _read_one(path, columns=[{"name": "id", "position": "A1"}])

    def test_position_zero_is_rejected_because_positions_are_one_based(self, path):
        with pytest.raises(ValueError, match="^Column position for 'id' must be >= 1$"):
            _read_one(path, columns=[{"name": "id", "position": 0}])

    def test_mapping_without_a_name_is_rejected(self, path):
        with pytest.raises(ValueError, match="^Each column mapping needs a 'name'$"):
            _read_one(path, columns=[{"position": "A"}])

    def test_two_mappings_with_the_same_output_name_are_rejected(self, path):
        columns = [{"name": "x", "position": "A"}, {"name": "x", "position": "B"}]
        with pytest.raises(ValueError, match="^Duplicate output column names"):
            _read_one(path, columns=columns)


class TestHeaderSettings:
    """``header`` may be a row offset or a mapping; other shapes are configuration errors."""

    def test_integer_header_is_the_zero_based_header_row(self, tmp_path):
        rows = [["Quarterly report"], ["a", "b"], [1, 2]]
        table = _read_one(_write_xlsx(tmp_path / "h.xlsx", rows), header=1)
        assert table.to_pydict() == {"a": [1], "b": [2]}

    def test_header_that_is_not_a_mapping_is_rejected(self, tmp_path):
        path = _write_xlsx(tmp_path / "h.xlsx", [["a"], [1]])
        with pytest.raises(ValueError, match="^header must be a mapping"):
            _read_one(path, header="auto")

    def test_override_that_is_not_a_list_is_rejected(self, tmp_path):
        path = _write_xlsx(tmp_path / "h.xlsx", [["a"], [1]])
        with pytest.raises(ValueError, match="^header.override must be a list of column names$"):
            _read_one(path, header={"override": "x,y"})


class TestNullTokens:
    def test_without_any_null_tokens_whitespace_text_is_kept(self, tmp_path):
        path = _write_xlsx(tmp_path / "n.xlsx", [["a", "b"], ["  ", 1], ["x", 2]])
        sheet = ExcelSheetConfig(select={"index": 0})
        kept = _read_one(path, ExcelInputConfig(sheets=[sheet], keep_default_na=False))
        assert kept["a"].to_pylist() == ["  ", "x"]
        # The default empty-string token turns the same cell into a null
        assert _read_one(path)["a"].to_pylist() == [None, "x"]


class TestSheetKinds:
    def test_chart_sheet_is_refused_with_a_clear_error(self, tmp_path):
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "data"
        for row in (["n"], [1], [2]):
            ws.append(row)
        chart = BarChart()
        chart.add_data(Reference(ws, min_col=1, min_row=1, max_row=3), titles_from_data=True)
        wb.create_chartsheet("chart").add_chart(chart)
        path = tmp_path / "chart.xlsx"
        wb.save(path)

        config = ExcelInputConfig(sheets=[ExcelSheetConfig(select={"name": "chart"})])
        with pytest.raises(ValueError, match=r"^Sheet 'chart' is not a worksheet"):
            list(ExcelInputHandler(config).process_sheets(path))

    def test_empty_sheet_reads_as_a_table_without_columns(self, tmp_path):
        wb = openpyxl.Workbook()
        wb.active.title = "empty"
        path = tmp_path / "empty.xlsx"
        wb.save(path)

        ((name, table),) = list(ExcelInputHandler(ExcelInputConfig()).process_sheets(path))
        assert name == "empty"
        assert (table.num_columns, table.num_rows) == (0, 0)

    def test_context_manager_closes_the_workbook_on_exit(self, tmp_path):
        path = _write_xlsx(tmp_path / "cm.xlsx", [["a"], [1]])
        with ExcelInputHandler(ExcelInputConfig()) as handler:
            handler.open_workbook(path)
            assert handler.get_sheet_names() == ["data"]
        with pytest.raises(RuntimeError, match="Workbook not opened"):
            handler.get_sheet_names()


class TestSheetSelection:
    def test_empty_sheet_list_selects_nothing_and_raises(self, tmp_path):
        path = _write_xlsx(tmp_path / "s.xlsx", [["a"], [1]])
        with pytest.raises(
            ValueError, match="^No sheets selected based on configuration criteria$"
        ):
            list(ExcelInputHandler(ExcelInputConfig(sheets=[])).process_sheets(path))

    def test_regex_changed_after_construction_is_still_validated(self, tmp_path):
        path = _write_xlsx(tmp_path / "s.xlsx", [["a"], [1]])
        sheet = ExcelSheetConfig(select={"regex": "^d"})
        sheet.select = {"regex": "("}  # assignment skips the config-time check
        with pytest.raises(ValueError, match="^Invalid sheet selection regex"):
            list(ExcelInputHandler(ExcelInputConfig(sheets=[sheet])).process_sheets(path))


class TestArchiveChecks:
    """.xlsx archives are inspected before openpyxl is allowed to open them."""

    def test_archive_with_too_many_entries_is_refused(self, tmp_path):
        path = tmp_path / "many.xlsx"
        with zipfile.ZipFile(path, "w") as archive:
            for index in range(10_001):
                archive.writestr(f"x/{index}", b"")
        with pytest.raises(ValueError, match=r"^Excel archive has 10001 entries \(limit 10000\)"):
            ExcelInputHandler(ExcelInputConfig()).get_sheet_info(path)

    def test_single_highly_compressed_part_is_refused(self, tmp_path):
        path = _write_xlsx(tmp_path / "part.xlsx", [["a"], [1]])
        _add_parts(
            path,
            {
                "xl/media/zeros.bin": (b"\0" * (1024 * 1024), zipfile.ZIP_DEFLATED),
                "xl/media/noise.bin": (os.urandom(2 * 1024 * 1024), zipfile.ZIP_STORED),
            },
        )
        with pytest.raises(
            ValueError,
            match=r"^Excel archive part 'xl/media/zeros.bin' compression ratio exceeds "
            r"max_compression_ratio=200.0",
        ):
            ExcelInputHandler(ExcelInputConfig()).get_sheet_info(path)

    def test_ratio_checks_are_skipped_when_max_compression_ratio_is_none(self, tmp_path):
        path = _write_xlsx(tmp_path / "ratio.xlsx", [["a"], [1]])
        _add_parts(path, {"xl/media/zeros.bin": (b"\0" * (4 * 1024 * 1024), zipfile.ZIP_DEFLATED)})

        with pytest.raises(ValueError, match="compression ratio exceeds"):
            ExcelInputHandler(ExcelInputConfig()).get_sheet_info(path)
        unlimited = ExcelInputConfig(max_compression_ratio=None)
        assert _read_one(path, unlimited).to_pydict() == {"a": [1]}


def _xls_book(rows, datemode: int = 0):
    """A stand-in for an xlrd Book holding one sheet named ``S`` with real xlrd cells."""
    sheet = SimpleNamespace(nrows=len(rows), ncols=1, row=lambda index: rows[index])
    return SimpleNamespace(
        datemode=datemode,
        sheet_names=lambda: ["S"],
        sheet_by_name=lambda name: sheet,
        release_resources=lambda: None,
    )


class TestXlsCells:
    """.xls cells are converted from xlrd's (ctype, value) pairs."""

    def _read_xls(self, tmp_path, cells):
        rows = [[xlrd.sheet.Cell(xlrd.XL_CELL_TEXT, "v")]] + [[cell] for cell in cells]
        with patch("xlrd.open_workbook", return_value=_xls_book(rows)) as open_workbook:
            table = _read_one(tmp_path / "book.xls")
        open_workbook.assert_called_once_with(str(tmp_path / "book.xls"))
        return table["v"].to_pylist()

    def test_error_cells_become_their_excel_error_text(self, tmp_path):
        cells = [
            xlrd.sheet.Cell(xlrd.XL_CELL_ERROR, 0x07),
            xlrd.sheet.Cell(xlrd.XL_CELL_ERROR, 0x7F),  # not a known error code
        ]
        assert self._read_xls(tmp_path, cells) == ["#DIV/0!", "#ERROR"]

    def test_date_cell_without_a_date_part_becomes_a_time(self, tmp_path):
        cells = [xlrd.sheet.Cell(xlrd.XL_CELL_DATE, 0.5)]
        assert self._read_xls(tmp_path, cells) == [dt.time(12, 0, 0)]

    def test_date_cell_xlrd_cannot_convert_keeps_its_serial_number(self, tmp_path):
        cells = [xlrd.sheet.Cell(xlrd.XL_CELL_DATE, -1.5)]
        assert self._read_xls(tmp_path, cells) == [-1.5]
