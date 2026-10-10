"""Tests for sheet selection and partial-output cleanup in ExcelImporter."""

import openpyxl
import pyarrow.parquet as pq
import pytest

from forklift.engine.importers import excel_importer
from forklift.engine.importers.excel_importer import ExcelImporter


@pytest.fixture
def workbook(tmp_path):
    """A workbook with two sheets, ``North`` and ``South``."""
    path = tmp_path / "sales.xlsx"
    book = openpyxl.Workbook()
    north = book.active
    north.title = "North"
    north.append(["region", "amount"])
    north.append(["n", 1])
    south = book.create_sheet("South")
    south.append(["region", "amount"])
    south.append(["s", 2])
    south.append(["s", 3])
    book.save(path)
    return path


class TestSheetSelection:
    @pytest.mark.parametrize("kwargs", [{}, {"sheet": None}])
    def test_no_sheet_imports_every_sheet(self, workbook, tmp_path, kwargs):
        results = ExcelImporter.import_excel(workbook, tmp_path / "out", **kwargs)

        assert [p.rsplit("/", 1)[-1] for p in results.output_files] == [
            "sales_North.parquet",
            "sales_South.parquet",
        ]
        assert results.total_rows == 3

    @pytest.mark.parametrize("sheet", [1.5, True])
    def test_sheet_that_is_neither_name_nor_index_is_rejected(self, workbook, tmp_path, sheet):
        with pytest.raises(ValueError, match="Sheet must be a sheet name or a 0-based index"):
            ExcelImporter.import_excel(workbook, tmp_path / "out", sheet=sheet)

        assert list((tmp_path / "out").iterdir()) == []


class TestPartialOutputCleanup:
    def test_failed_local_write_leaves_no_partial_file(self, workbook, tmp_path, monkeypatch):
        def write_partial_then_fail(table, target):
            with open(target, "wb") as handle:
                handle.write(b"PAR1 truncated")
            raise OSError("disk full")

        monkeypatch.setattr(excel_importer.pq, "write_table", write_partial_then_fail)

        with pytest.raises(OSError, match="disk full"):
            ExcelImporter.import_excel(workbook, tmp_path / "out", sheet="North")

        assert list((tmp_path / "out").iterdir()) == []

    def test_failed_s3_write_aborts_the_upload(self, workbook, monkeypatch):
        writers = []

        class FailingS3Writer:
            def __init__(self, target, schema, s3_client=None):
                self.target, self.s3_client = target, s3_client
                self.aborted = self.closed = False
                writers.append(self)

            def write_table(self, table):
                raise OSError("connection reset")

            def close(self):
                self.closed = True

            def abort(self):
                self.aborted = True

        monkeypatch.setattr(excel_importer, "create_parquet_writer", FailingS3Writer)
        client = object()

        with pytest.raises(OSError, match="connection reset"):
            ExcelImporter.import_excel(
                workbook, "s3://bucket/exports", sheet="South", s3_client=client
            )

        assert len(writers) == 1
        assert writers[0].target == "s3://bucket/exports/sales_South.parquet"
        assert writers[0].s3_client is client
        assert writers[0].aborted and not writers[0].closed

    def test_successful_local_write_is_readable(self, workbook, tmp_path):
        results = ExcelImporter.import_excel(workbook, tmp_path / "out", sheet="South")

        table = pq.read_table(results.output_files[0])
        assert table.column("amount").to_pylist() == [2, 3]
