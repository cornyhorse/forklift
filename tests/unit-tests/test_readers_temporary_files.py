"""Tests for the temporary directories of the forklift.readers functions."""

import openpyxl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from forklift import readers
from forklift.engine.config import ProcessingResults
from forklift.engine.exceptions import ProcessingError


@pytest.fixture
def temp_dir(tmp_path, monkeypatch):
    """The directory the next reader call uses for its parquet files."""
    target = tmp_path / "reader_tmp"

    def mkdtemp(prefix=""):
        target.mkdir()
        return str(target)

    monkeypatch.setattr(readers.tempfile, "mkdtemp", mkdtemp)
    return target


def _workbook(tmp_path):
    path = tmp_path / "book.xlsx"
    book = openpyxl.Workbook()
    book.active.append(["a", "b"])
    book.active.append([1, 2])
    book.save(path)
    return path


class TestReadExcel:
    def test_without_a_sheet_every_sheet_is_read(self, tmp_path, temp_dir):
        with readers.read_excel(_workbook(tmp_path)) as reader:
            table = reader.as_pyarrow()

        assert table.to_pydict() == {"a": [1], "b": [2]}
        assert not temp_dir.exists()

    def test_failure_removes_the_temporary_directory(self, tmp_path, temp_dir):
        with pytest.raises(FileNotFoundError, match="Input file not found"):
            readers.read_excel(tmp_path / "missing.xlsx")

        assert not temp_dir.exists()
        assert str(temp_dir) not in readers._temp_dirs


class TestReadFwf:
    def test_unimplemented_importer_error_removes_the_temporary_directory(
        self, tmp_path, temp_dir
    ):
        with pytest.raises(NotImplementedError, match="FWF import not yet implemented"):
            readers.read_fwf(tmp_path / "in.txt", tmp_path / "schema.json")

        assert not temp_dir.exists()

    def test_reader_holds_the_data_files_of_the_import(self, tmp_path, temp_dir, monkeypatch):
        calls = []

        def import_fwf(input_path, output_path, schema_file, **kwargs):
            calls.append((input_path, output_path, schema_file, kwargs))
            data = f"{output_path}/data.parquet"
            bad = f"{output_path}/bad_rows.parquet"
            pq.write_table(pa.table({"n": [1, 2]}), data)
            pq.write_table(pa.table({"n": ["x"]}), bad)
            results = ProcessingResults()
            results.output_files = [data, bad]
            results.bad_rows_file = bad
            return results

        monkeypatch.setattr(readers, "import_fwf", import_fwf)

        reader = readers.read_fwf("in.txt", "schema.json", encoding="latin-1")

        assert calls == [("in.txt", str(temp_dir), "schema.json", {"encoding": "latin-1"})]
        assert reader.parquet_files == [f"{temp_dir}/data.parquet"]
        assert reader.as_pyarrow().to_pydict() == {"n": [1, 2]}
        reader.close()
        assert not temp_dir.exists()


class TestReadSql:
    def test_failure_removes_the_temporary_directory(self, temp_dir):
        with pytest.raises(ProcessingError, match="Schema file is required"):
            readers.read_sql("Driver=x;Server=db")

        assert not temp_dir.exists()
