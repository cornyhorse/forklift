"""forklift-mcp: cells as JSON values, bounded rows, Parquet reads and rejection reasons."""

from __future__ import annotations

import datetime
import decimal
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from forklift.mcp import values
from forklift.mcp.values import (
    MAX_CELL_CHARS,
    MAX_CELL_ITEMS,
    RowCollector,
    cell,
    compact_size,
    iter_parquet_rows,
    parquet_columns,
    parquet_row_count,
    rejection_reasons,
    split_finding,
)


class TestCell:
    @pytest.mark.parametrize("value", [None, True, 3, 2.5, "text"])
    def test_json_values_stay(self, value):
        assert cell(value) == (value, False)

    @pytest.mark.parametrize("value, text", [(float("nan"), "nan"), (float("inf"), "inf")])
    def test_non_finite_floats_become_text(self, value, text):
        assert cell(value) == (text, False)

    def test_long_text_is_cut(self):
        converted, cut = cell("x" * (MAX_CELL_CHARS + 5))
        assert cut and len(converted) == MAX_CELL_CHARS

    def test_bytes(self):
        assert cell(b"\x00\xff") == ("base64:AP8=", False)
        assert cell(bytearray(b"a")) == ("base64:YQ==", False)

    def test_decimals_dates_and_times(self):
        assert cell(decimal.Decimal("1.10")) == ("1.10", False)
        assert cell(datetime.date(2026, 1, 2)) == ("2026-01-02", False)
        assert cell(datetime.datetime(2026, 1, 2, 3, 4)) == ("2026-01-02T03:04:00", False)
        assert cell(datetime.time(3, 4)) == ("03:04:00", False)
        assert cell(datetime.timedelta(seconds=90)) == ("0:01:30", False)

    def test_nested_values(self):
        assert cell({"a": [1, b"x"], 2: None}) == ({"a": [1, "base64:eA=="], "2": None}, False)
        converted, cut = cell(list(range(MAX_CELL_ITEMS + 1)))
        assert cut and len(converted) == MAX_CELL_ITEMS
        converted, cut = cell({str(i): i for i in range(MAX_CELL_ITEMS + 1)})
        assert cut and len(converted) == MAX_CELL_ITEMS
        assert cell(("x" * (MAX_CELL_CHARS + 1),))[1] is True
        assert cell({"k": "x" * (MAX_CELL_CHARS + 1)})[1] is True

    def test_compact_size_counts_utf8_bytes(self):
        assert compact_size(["é"]) == len(json.dumps(["é"], ensure_ascii=False).encode())


class TestRowCollector:
    def test_rows_limit(self):
        collector = RowCollector(max_rows=2, max_bytes=10_000)
        assert collector.add([1, "a"]) and collector.add([2, "b"])
        assert not collector.add([3, "c"])
        assert collector.view() == {
            "rows": [[1, "a"], [2, "b"]],
            "row_count": 2,
            "truncated": True,
            "limit": "rows",
            "truncated_cells": 0,
        }

    def test_bytes_limit_and_cut_cells(self):
        collector = RowCollector(max_rows=10, max_bytes=MAX_CELL_CHARS + 20)
        assert collector.add(["y" * (MAX_CELL_CHARS + 50)])
        assert not collector.add(["z" * 30])
        view = collector.view()
        assert view["limit"] == "bytes" and view["row_count"] == 1
        assert view["truncated_cells"] == 1

    def test_nothing_reached(self):
        collector = RowCollector(max_rows=10, max_bytes=1000)
        collector.add([1])
        assert collector.view()["truncated"] is False and collector.view()["limit"] is None


def _write(path, columns, row_group_size):
    pq.write_table(pa.table(columns), str(path), row_group_size=row_group_size)
    return path


class TestParquet:
    def test_columns_and_count(self, tmp_path):
        path = _write(tmp_path / "t.parquet", {"a": [1, 2], "b": ["x", "y"]}, 10)
        assert parquet_columns(path) == [
            {"name": "a", "type": "int64"},
            {"name": "b", "type": "string"},
        ]
        assert parquet_row_count(path) == 2

    @pytest.mark.parametrize("offset", [0, 1, 5, 7, 9, 10, 2500])
    def test_rows_from_an_offset_across_row_groups(self, tmp_path, offset, monkeypatch):
        monkeypatch.setattr(values, "_BATCH_ROWS", 2)
        path = _write(tmp_path / "t.parquet", {"a": list(range(10)), "b": list("abcdefghij")}, 3)
        rows = list(iter_parquet_rows(path, ["b", "a"], offset))
        assert rows == [[chr(97 + i), i] for i in range(offset, 10)]

    def test_rows_of_an_empty_file(self, tmp_path):
        path = _write(tmp_path / "t.parquet", {"a": pa.array([], pa.int64())}, 3)
        assert list(iter_parquet_rows(path, ["a"])) == []


class TestRejectionReasons:
    def test_reasons_are_split_and_counted(self, tmp_path):
        reasons = [
            "VALIDATION_ERROR:age",
            "VALIDATION_ERROR:age; UNIQUE_VIOLATION:id",
            "type_conversion_failed",
            None,
            "VALIDATION_ERROR:age",
        ]
        path = _write(
            tmp_path / "bad.parquet",
            {"id": list("12345"), "_rejection_reason": reasons},
            2,
        )
        found = rejection_reasons(path, max_rows=100, top=3)
        assert found["column"] == "_rejection_reason"
        assert found["scanned_rows"] == 5 and found["distinct"] == 4
        assert found["reasons"] == [
            {
                "rule": "VALIDATION_ERROR",
                "column": "age",
                "reason": "VALIDATION_ERROR:age",
                "rows": 3,
            },
            {"rule": "REJECTED", "column": None, "reason": "REJECTED", "rows": 1},
            {
                "rule": "UNIQUE_VIOLATION",
                "column": "id",
                "reason": "UNIQUE_VIOLATION:id",
                "rows": 1,
            },
        ]

    def test_scanning_stops_at_max_rows(self, tmp_path):
        path = _write(tmp_path / "bad.parquet", {"_rejection_reason": ["A"] * 5 + ["B"] * 5}, 4)
        found = rejection_reasons(path, max_rows=6, top=10)
        assert found["scanned_rows"] == 6
        assert [(r["reason"], r["rows"]) for r in found["reasons"]] == [("A", 5), ("B", 1)]

    def test_a_file_without_the_reason_column(self, tmp_path):
        path = _write(tmp_path / "bad.parquet", {"id": ["1"]}, 4)
        assert rejection_reasons(path, 10, 10) == {
            "column": None,
            "reasons": [],
            "scanned_rows": 0,
            "distinct": 0,
        }

    def test_split_finding(self):
        assert split_finding("RANGE_VIOLATION:a:b") == {"rule": "RANGE_VIOLATION", "column": "a:b"}
        assert split_finding("type_conversion_failed") == {
            "rule": "type_conversion_failed",
            "column": None,
        }
