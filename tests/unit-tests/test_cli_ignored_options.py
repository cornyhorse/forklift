"""Tests for the CLI warnings about options that do not apply to the chosen input kind."""

import openpyxl
import pyarrow.parquet as pq

from forklift.cli import main


def _csv(tmp_path):
    path = tmp_path / "in.csv"
    path.write_text("a,b\n1,2\n", encoding="utf-8")
    return str(path)


def _workbook(tmp_path):
    path = tmp_path / "book.xlsx"
    book = openpyxl.Workbook()
    book.active.append(["a", "b"])
    book.active.append([1, 2])
    book.save(path)
    return str(path)


class TestOptionsIgnoredForCsv:
    def test_fwf_spec_and_sheet_are_reported_as_ignored_and_the_import_runs(
        self, tmp_path, capsys
    ):
        out = tmp_path / "out"

        main(
            [
                "ingest",
                _csv(tmp_path),
                "--dest",
                str(out),
                "--input-kind",
                "csv",
                "--fwf-spec",
                "spec.json",
                "--sheet",
                "Sheet1",
            ]
        )

        captured = capsys.readouterr()
        assert "Warning: --fwf-spec only applies to --input-kind fwf and is ignored" in (
            captured.err
        )
        assert "Warning: --sheet only applies to --input-kind excel and is ignored" in (
            captured.err
        )
        assert "Processing complete. Processed 1 rows." in captured.out
        assert pq.read_table(out / "data.parquet").num_rows == 1


class TestOptionsIgnoredForExcel:
    def test_csv_only_options_are_reported_as_ignored_and_the_import_runs(self, tmp_path, capsys):
        out = tmp_path / "out"

        main(
            [
                "ingest",
                _workbook(tmp_path),
                "--dest",
                str(out),
                "--input-kind",
                "excel",
                "--include-value-stats",
                "--no-schema-extensions",
            ]
        )

        captured = capsys.readouterr()
        assert (
            "Warning: --include-value-stats only affects the CSV output metadata and is ignored"
            in captured.err
        )
        assert "Warning: --no-schema-extensions only affects CSV imports and is ignored" in (
            captured.err
        )
        assert "Processing complete. Processed 1 rows." in captured.out
        assert pq.read_table(out / "book_Sheet.parquet").num_rows == 1
