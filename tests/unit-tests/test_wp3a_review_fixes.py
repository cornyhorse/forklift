"""Regression tests for the WP3a review fixes (CSV engine, config, header detection, readers).

Each test reproduces a problem found in review: schema types ignored on CSV output,
``required`` checked by position, rejected rows leaking into ``read_csv(...)`` frames,
unsafe writer lifecycle, duplicate rows after the Arrow column-mismatch fallback, and so on.
"""

import gc
import io
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from forklift.engine.config import ExcessColumnMode, HeaderMode, ImportConfig, ProcessingResults
from forklift.engine.forklift_core import import_csv
from forklift.engine.processors import csv_processor as csv_processor_module
from forklift.engine.processors.batch_processor import BatchProcessor
from forklift.engine.processors.csv_processor import CSVProcessor
from forklift.engine.processors.header_detector import HeaderDetector
from forklift.engine.processors.schema_processor import SchemaProcessor
from forklift.engine.processors.text_utils import sanitize_arrow_error
from forklift.engine.processors.type_conversion import (
    ColumnConverter,
    NullPolicy,
    parse_arrow_type,
)
from forklift.io import S3ParquetWriter, UnifiedIOHandler
from forklift.readers import DataFrameReader, read_csv, read_sql

# --------------------------------------------------------------------------- helpers


def _write(path, text, encoding="utf-8"):
    Path(path).write_bytes(text.encode(encoding))
    return str(path)


def _schema(tmp_path, properties, required=None, csv_ext=None):
    schema = {"type": "object", "properties": properties, "required": required or []}
    if csv_ext is not None:
        schema["x-csv"] = csv_ext
    path = Path(tmp_path) / "schema.json"
    path.write_text(json.dumps(schema), encoding="utf-8")
    return str(path)


def _run(tmp_path, csv_text, **kwargs):
    """Import ``csv_text`` into ``tmp_path/out``; returns (results, out_dir)."""
    csv_path = _write(Path(tmp_path) / "in.csv", csv_text)
    out_dir = str(Path(tmp_path) / "out")
    return import_csv(csv_path, out_dir, **kwargs), out_dir


def _table(out_dir, name="data.parquet"):
    return pq.read_table(os.path.join(out_dir, name))


@dataclass
class _CapturedResults(ProcessingResults):
    """ProcessingResults that remembers its instances (a failed run raises, losing them)."""

    instances = []

    def __post_init__(self):
        _CapturedResults.instances.append(self)


@pytest.fixture
def captured_results(monkeypatch):
    _CapturedResults.instances = []
    monkeypatch.setattr(csv_processor_module, "ProcessingResults", _CapturedResults)
    return _CapturedResults.instances


# ------------------------------------------------------------------ 1. schema types


class TestSchemaTypesAreApplied:
    def test_string_column_keeps_leading_zeros(self, tmp_path):
        schema = _schema(tmp_path, {"id": {"type": "integer"}, "zip": {"type": "string"}})
        results, out = _run(tmp_path, "id,zip\n1,00123\n2,02134\n", schema_file=schema)

        table = _table(out)
        assert table.schema.field("zip").type == pa.string()
        assert table.column("zip").to_pylist() == ["00123", "02134"]
        assert table.schema.field("id").type == pa.int64()
        assert results.invalid_rows == 0

    def test_json_types_and_parquet_type_mapping(self, tmp_path):
        props = {
            "id": {"type": "integer"},
            "age": {"type": "integer"},
            "amount": {"type": "number"},
            "active": {"type": "boolean"},
            "born": {"type": "string", "format": "date"},
        }
        schema = _schema(tmp_path, props, csv_ext={"parquetTypeMapping": {"age": "int32"}})
        csv_text = "id,age,amount,active,born\n1,30,1.5,true,2020-01-31\n"
        _, out = _run(tmp_path, csv_text, schema_file=schema)

        types = {f.name: f.type for f in _table(out).schema}
        assert types == {
            "id": pa.int64(),
            "age": pa.int32(),
            "amount": pa.float64(),
            "active": pa.bool_(),
            "born": pa.date32(),
        }

    def test_unconvertible_value_goes_to_bad_rows_without_aborting(self, tmp_path):
        schema = _schema(tmp_path, {"id": {"type": "integer"}, "amount": {"type": "number"}})
        csv_text = "id,amount\n1,1.5\n2,not-a-number\n3,3.5\n"
        results, out = _run(tmp_path, csv_text, schema_file=schema)

        assert results.errors == []
        assert (results.total_rows, results.valid_rows, results.invalid_rows) == (3, 2, 1)
        assert _table(out).column("id").to_pylist() == [1, 3]
        bad = _table(out, "bad_rows.parquet")
        assert bad.to_pydict() == {"id": ["2"], "amount": ["not-a-number"]}
        assert results.bad_rows_file == os.path.join(out, "bad_rows.parquet")

    def test_columns_outside_the_schema_keep_inference(self, tmp_path):
        schema = _schema(tmp_path, {"zip": {"type": "string"}})
        _, out = _run(tmp_path, "zip,count\n00123,5\n", schema_file=schema)

        table = _table(out)
        assert table.schema.field("count").type == pa.int64()
        assert table.schema.field("zip").type == pa.string()

    def test_same_schema_on_arrow_fallback_path(self, tmp_path):
        """A ragged row sends the file through the row reader; the schema must not change."""
        props = {
            "id": {"type": "integer"},
            "zip": {"type": "string"},
            "amount": {"type": "number"},
        }
        schema = _schema(tmp_path, props)
        clean_dir, ragged_dir = Path(tmp_path) / "clean", Path(tmp_path) / "ragged"
        clean_dir.mkdir()
        ragged_dir.mkdir()
        _, clean_out = _run(
            clean_dir, "id,zip,amount\n1,00123,1.5\n2,02134,2.5\n", schema_file=schema
        )
        results, ragged_out = _run(
            ragged_dir, "id,zip,amount\n1,00123,1.5\n2,02134,2.5,EXTRA\n", schema_file=schema
        )

        assert _table(ragged_out).schema.equals(_table(clean_out).schema)
        assert _table(ragged_out).column("zip").to_pylist() == ["00123", "02134"]
        assert results.truncated_rows == 1

    def test_same_schema_on_s3_row_reader(self, tmp_path):
        """The S3 reader sees plain strings; the converter gives the same typed batches."""
        props = {
            "id": {"type": "integer"},
            "zip": {"type": "string"},
            "amount": {"type": "number"},
        }
        schema_path = _schema(tmp_path, props)
        config = ImportConfig(
            input_path="s3://b/in.csv", output_path=str(tmp_path / "o"), schema_file=schema_path
        )
        processor = SchemaProcessor(config, UnifiedIOHandler())
        processor.load_schema()

        io_handler = MagicMock()
        io_handler.csv_reader.return_value = iter(
            [["id", "zip", "amount"], ["1", "00123", "1.5"], ["2", "02134", "oops"]]
        )
        rejected = []
        batch_processor = BatchProcessor(
            config,
            io_handler,
            converter=processor.build_converter(),
            reject_handler=rejected.append,
        )
        batches = list(
            batch_processor._create_s3_csv_batches(
                "s3://b/in.csv", ["id", "zip", "amount"], 0, None
            )
        )

        assert len(batches) == 1
        assert batches[0].schema.types == [pa.int64(), pa.string(), pa.float64()]
        assert batches[0].to_pydict() == {"id": [1], "zip": ["00123"], "amount": [1.5]}
        assert rejected[0].to_pydict() == {"id": ["2"], "zip": ["02134"], "amount": ["oops"]}

    def test_s3_end_to_end_matches_local_schema(self, tmp_path, monkeypatch):
        moto = pytest.importorskip("moto")
        boto3 = pytest.importorskip("boto3")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
        props = {"id": {"type": "integer"}, "zip": {"type": "string"}}
        schema = {"type": "object", "properties": props, "required": ["id"]}

        with moto.mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="bkt")
            client.put_object(Bucket="bkt", Key="in.csv", Body=b"id,zip\n1,00123\n\n2,02134\n")
            client.put_object(Bucket="bkt", Key="schema.json", Body=json.dumps(schema).encode())
            client.put_object(Bucket="bkt", Key="out/bad_rows.parquet", Body=b"stale")

            results = import_csv(
                "s3://bkt/in.csv", "s3://bkt/out/", schema_file="s3://bkt/schema.json"
            )

            body = client.get_object(Bucket="bkt", Key="out/data.parquet")["Body"].read()
            table = pq.read_table(io.BytesIO(body))
            keys = {o["Key"] for o in client.list_objects_v2(Bucket="bkt")["Contents"]}

        assert table.schema.types == [pa.int64(), pa.string()]
        assert table.to_pydict() == {"id": [1, 2], "zip": ["00123", "02134"]}
        assert results.total_rows == 2  # the blank line is not a row
        # a clean run removed the stale bad rows file and wrote the metadata beside the data
        assert "out/bad_rows.parquet" not in keys
        assert {"out/manifest.json", "out/metadata.json"} <= keys


class TestColumnConverter:
    def test_parse_arrow_type(self):
        assert parse_arrow_type("int32") == pa.int32()
        assert parse_arrow_type("double") == pa.float64()
        assert parse_arrow_type("timestamp[us]") == pa.timestamp("us")
        assert parse_arrow_type("timestamp[ms, tz=UTC]") == pa.timestamp("ms", tz="UTC")
        assert parse_arrow_type("decimal128(10,2)") == pa.decimal128(10, 2)
        assert parse_arrow_type("dictionary<values=string, indices=int32>") == pa.dictionary(
            pa.int32(), pa.string()
        )
        assert parse_arrow_type("list<string>") is None
        assert parse_arrow_type("struct") is None

    def test_nested_and_unsupported_types_stay_strings(self, tmp_path):
        props = {
            "tags": {"type": "array", "items": {"type": "string"}},
            "span": {"type": "integer"},
            "meta": {"type": "object"},
        }
        schema = _schema(
            tmp_path,
            props,
            csv_ext={"parquetTypeMapping": {"tags": "list<string>", "span": "duration[s]"}},
        )
        config = ImportConfig(input_path="x.csv", output_path=str(tmp_path), schema_file=schema)
        processor = SchemaProcessor(config, UnifiedIOHandler())
        processor.load_schema()
        assert processor.get_column_types() == {
            "tags": pa.string(),
            "span": pa.string(),
            "meta": pa.string(),
        }

    def test_timestamp_accepts_zone_suffixes(self):
        converter = ColumnConverter({"ts": pa.timestamp("us")})
        batch = pa.RecordBatch.from_pydict(
            {"ts": ["2024-01-15T10:30:00Z", "2024-01-15 10:30:00", "2024-01-15T12:30:00+02:00"]}
        )
        converted, rejected = converter.convert(batch)
        assert rejected is None
        values = converted.column("ts").to_pylist()
        assert len({v for v in values}) == 1  # all three denote the same UTC wall time

    def test_failures_are_isolated_per_row(self):
        converter = ColumnConverter({"n": pa.int64(), "d": pa.date32()})
        batch = pa.RecordBatch.from_pydict(
            {
                "n": ["1", "x", "3", "4", "y"],
                "d": ["2024-01-01", "2024-01-02", "bad", "2024-01-04", "2024-01-05"],
            }
        )
        converted, rejected = converter.convert(batch)
        assert converted.column("n").to_pylist() == [1, 4]
        assert rejected.column("n").to_pylist() == ["x", "3", "y"]
        assert rejected.schema.types == [pa.string(), pa.string()]

    def test_schema_null_markers_apply_per_column(self):
        policy = NullPolicy(global_values=["", "NA"], per_column={"b": ["", "0.00"]})
        converter = ColumnConverter({"a": pa.int64(), "b": pa.string(), "c": pa.string()}, policy)
        batch = pa.RecordBatch.from_pydict(
            {"a": ["NA", "2"], "b": ["0.00", "NA"], "c": ["NA", ""]}
        )
        converted, rejected = converter.convert(batch)
        assert rejected is None
        assert converted.to_pydict() == {"a": [None, 2], "b": [None, "NA"], "c": [None, None]}


# ------------------------------------------------------------------ 2. required columns


class TestRequiredColumns:
    def test_required_is_checked_by_name_not_position(self, tmp_path):
        # schema order: id, name ; file order: name, id  (name is required, id is not)
        schema = _schema(
            tmp_path,
            {"id": {"type": "integer"}, "name": {"type": "string"}},
            required=["name"],
        )
        results, out = _run(tmp_path, "name,id\nAnn,1\n,2\nCy,3\n", schema_file=schema)

        assert (results.valid_rows, results.invalid_rows) == (2, 1)
        assert _table(out).column("name").to_pylist() == ["Ann", "Cy"]
        assert _table(out, "bad_rows.parquet").column("id").to_pylist() == ["2"]

    def test_required_column_missing_from_file_raises(self, tmp_path):
        schema = _schema(
            tmp_path, {"id": {"type": "integer"}, "name": {"type": "string"}}, required=["name"]
        )
        with pytest.raises(ValueError, match="'name'"):
            _run(tmp_path, "id,other\n1,x\n", schema_file=schema)

    def test_empty_string_is_missing_for_required_text_column(self, tmp_path):
        schema = _schema(tmp_path, {"name": {"type": "string"}}, required=["name"])
        results, out = _run(tmp_path, 'name\nAnn\n""\nBo\n', schema_file=schema)
        assert (results.valid_rows, results.invalid_rows) == (2, 1)

    def test_empty_string_is_missing_on_the_s3_row_reader_too(self, tmp_path):
        schema = _schema(tmp_path, {"name": {"type": "string"}}, required=["name"])
        config = ImportConfig(input_path="x", output_path=str(tmp_path), schema_file=schema)
        processor = CSVProcessor()
        processor.io_handler = UnifiedIOHandler()
        schema_processor = SchemaProcessor(config, processor.io_handler)
        pa_schema = schema_processor.load_schema()
        batch = pa.RecordBatch.from_pydict({"name": ["Ann", "", None]})
        valid, invalid = processor._validate_batch(batch, pa_schema, config, ["name"])
        assert valid.column("name").to_pylist() == ["Ann"]
        assert len(invalid) == 2

    def test_validate_batch_raises_for_missing_required_column(self, tmp_path):
        config = ImportConfig(input_path="x", output_path=str(tmp_path))
        schema = pa.schema([pa.field("name", pa.string(), nullable=False)])
        batch = pa.RecordBatch.from_pydict({"other": ["x"]})
        with pytest.raises(ValueError, match="name"):
            CSVProcessor()._validate_batch(batch, schema, config)


# ------------------------------------------------------------------ 3. readers & bad rows


class TestRejectedRowsStayOutOfDataFrames:
    @pytest.fixture
    def reader(self, tmp_path):
        schema = _schema(
            tmp_path, {"id": {"type": "integer"}, "name": {"type": "string"}}, required=["name"]
        )
        csv_path = _write(Path(tmp_path) / "in.csv", "id,name\n1,Ann\n2,\n3,Cy\n")
        reader = read_csv(csv_path, schema_file=schema)
        yield reader
        reader.close()

    def test_all_three_conversions_only_return_valid_rows(self, reader):
        assert reader.as_pyarrow().column("id").to_pylist() == [1, 3]
        assert reader.as_pandas()["id"].tolist() == [1, 3]
        assert reader.as_polars()["id"].to_list() == [1, 3]

    def test_results_expose_bad_rows_file_and_keep_output_files(self, tmp_path):
        schema = _schema(tmp_path, {"name": {"type": "string"}}, required=["name"])
        results, out = _run(tmp_path, 'name\nAnn\n""\nCy\n', schema_file=schema)
        assert results.bad_rows_file == os.path.join(out, "bad_rows.parquet")
        assert results.bad_rows_file in results.output_files  # backward compatible listing

    def test_no_bad_rows_means_no_bad_rows_file(self, tmp_path):
        results, _ = _run(tmp_path, "a\n1\n")
        assert results.bad_rows_file is None

    def test_reader_close_and_context_manager(self, tmp_path):
        csv_path = _write(Path(tmp_path) / "in.csv", "a\n1\n")
        with read_csv(csv_path) as reader:
            temp_dir = reader._temp_dir
            assert reader.as_pyarrow().num_rows == 1
            assert os.path.isdir(temp_dir)
        assert not os.path.exists(temp_dir)
        with pytest.raises(ValueError, match="closed"):
            reader.as_pyarrow()


# ------------------------------------------------------------------ 4. readers module


class TestReaders:
    def test_read_sql_passes_connection_string(self):
        with patch("forklift.readers.import_sql") as mock_import:
            mock_import.return_value = MagicMock(output_files=["a.parquet"], bad_rows_file=None)
            reader = read_sql("DSN=db", schema_file="s.json", batch_size=5)
        mock_import.assert_called_once()
        kwargs = mock_import.call_args.kwargs
        assert kwargs["connection_string"] == "DSN=db"
        assert kwargs["schema_file"] == "s.json"
        assert kwargs["batch_size"] == 5
        assert "input_path" not in kwargs
        reader.close()

    def test_lazy_polars_frame_survives_reader_garbage_collection(self, tmp_path):
        csv_path = _write(Path(tmp_path) / "in.csv", "a,b\n1,x\n2,y\n")
        lazy = read_csv(csv_path).as_polars(lazy=True)  # the reader is dropped right away
        gc.collect()
        assert lazy.collect()["a"].to_list() == [1, 2]

    def test_header_only_csv_gives_empty_frames_with_columns(self, tmp_path):
        csv_path = _write(Path(tmp_path) / "in.csv", "a,b\n")
        with read_csv(csv_path) as reader:
            assert reader.as_pyarrow().schema.names == ["a", "b"]
            assert reader.as_pyarrow().num_rows == 0
            assert list(reader.as_pandas().columns) == ["a", "b"]
            assert reader.as_polars().columns == ["a", "b"]

    def test_no_files_gives_empty_results_not_a_concat_error(self):
        reader = DataFrameReader([])
        assert reader.as_pyarrow().num_rows == 0
        assert reader.as_pandas().empty
        assert reader.as_polars().is_empty()
        assert reader.as_polars(lazy=True).collect().is_empty()

    def test_read_functions_drop_the_bad_rows_file(self):
        results = MagicMock(output_files=["d.parquet", "bad.parquet"], bad_rows_file="bad.parquet")
        with patch("forklift.readers.import_csv", return_value=results):
            reader = read_csv("x.csv")
        assert reader.parquet_files == ["d.parquet"]
        reader.close()


# ------------------------------------------------------------------ 5. config coercion


class TestImportConfigCoercion:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("present", HeaderMode.PRESENT),
            ("ABSENT", HeaderMode.ABSENT),
            ("Auto", HeaderMode.AUTO),
            (HeaderMode.AUTO, HeaderMode.AUTO),
        ],
    )
    def test_header_mode_strings(self, value, expected):
        assert ImportConfig("i", "o", header_mode=value).header_mode is expected

    @pytest.mark.parametrize(
        "value,expected",
        [
            ("truncate", ExcessColumnMode.TRUNCATE),
            ("reject", ExcessColumnMode.REJECT),
            ("PASSTHROUGH", ExcessColumnMode.PASSTHROUGH),
        ],
    )
    def test_excess_column_mode_strings(self, value, expected):
        assert ImportConfig("i", "o", excess_column_mode=value).excess_column_mode is expected

    def test_unknown_values_list_the_valid_ones(self):
        with pytest.raises(ValueError, match="'present', 'absent', 'auto'"):
            ImportConfig("i", "o", header_mode="sometimes")
        with pytest.raises(ValueError, match="'truncate', 'reject', 'passthrough'"):
            ImportConfig("i", "o", excess_column_mode="drop")
        with pytest.raises(ValueError):
            ImportConfig("i", "o", header_mode=None)

    def test_include_value_statistics_defaults_off(self):
        assert ImportConfig("i", "o").include_value_statistics is False
        assert ImportConfig("i", "o", include_value_statistics=True).include_value_statistics

    def test_string_header_mode_absent_really_means_absent(self, tmp_path):
        results, out = _run(tmp_path, "1,Ann\n2,Bob\n", header_mode="absent")
        assert results.total_rows == 2
        assert _table(out).schema.names == ["col_1", "col_2"]

    def test_string_excess_mode_reject_really_rejects(self, tmp_path):
        results, _ = _run(tmp_path, "a,b\n1,2\n3,4,5\n", excess_column_mode="reject")
        assert (results.valid_rows, results.invalid_rows) == (1, 1)


# ------------------------------------------------------------------ 6. header detection


class TestHeaderDetection:
    def _detector(self, **config):
        return HeaderDetector(ImportConfig("i", "o", **config), UnifiedIOHandler())

    def test_header_starting_with_hash_is_not_a_comment(self, tmp_path):
        path = _write(Path(tmp_path) / "h.csv", "#,name,amount\n1,Ann,5\n")
        assert self._detector().detect_header_row(path) == (0, ["#", "name", "amount"])

        results, out = _run(tmp_path, "#,name,amount\n1,Ann,5\n2,Bob,6\n")
        assert results.total_rows == 2
        assert _table(out).schema.names == ["#", "name", "amount"]

    def test_lone_hash_lines_are_still_comments_by_default(self, tmp_path):
        path = _write(Path(tmp_path) / "h.csv", "# exported by tool\nid,name\n1,Ann\n")
        assert self._detector().detect_header_row(path) == (1, ["id", "name"])

    def test_absent_without_schema_generates_names_and_keeps_first_row(self, tmp_path):
        path = _write(Path(tmp_path) / "h.csv", "1,Ann,x\n2,Bob,y\n")
        detector = self._detector(header_mode=HeaderMode.ABSENT)
        assert detector.detect_header_row(path) == (-1, ["col_1", "col_2", "col_3"])

        results, out = _run(tmp_path, "1,Ann,x\n2,Bob,y\n", header_mode=HeaderMode.ABSENT)
        assert results.total_rows == 2
        assert _table(out).column("col_1").to_pylist() == [1, 2]

    def test_absent_with_schema_uses_schema_names(self, tmp_path):
        path = _write(Path(tmp_path) / "h.csv", "1,Ann\n")
        detector = self._detector(header_mode=HeaderMode.ABSENT)
        assert detector.detect_header_row(path, ["id", "name"]) == (-1, ["id", "name"])

    def test_header_beyond_search_window_raises(self, tmp_path):
        text = "".join(f"# comment {i}\n" for i in range(12)) + "id,name\n1,Ann\n"
        path = _write(Path(tmp_path) / "h.csv", text)
        with pytest.raises(ValueError, match="No header row found within the first 10"):
            self._detector().detect_header_row(path)
        with pytest.raises(ValueError, match="No header row found"):
            self._detector(header_mode=HeaderMode.AUTO).detect_header_row(path)
        with pytest.raises(ValueError, match="No header row found"):
            _run(tmp_path, text)

    def test_wider_window_finds_the_header(self, tmp_path):
        text = "".join(f"# comment {i}\n" for i in range(12)) + "id,name\n1,Ann\n"
        path = _write(Path(tmp_path) / "h.csv", text)
        assert self._detector(header_search_rows=20).detect_header_row(path) == (
            12,
            ["id", "name"],
        )

    def test_empty_and_blank_only_files_still_report_no_header(self, tmp_path):
        empty = _write(Path(tmp_path) / "e.csv", "")
        blank = _write(Path(tmp_path) / "b.csv", "\n\n")
        for path in (empty, blank):
            assert self._detector().detect_header_row(path) == (-1, [])

    def test_utf8_bom_is_not_part_of_the_first_column(self, tmp_path):
        data = b"\xef\xbb\xbfid,name\n1,Ann\n"
        path = Path(tmp_path) / "bom.csv"
        path.write_bytes(data)
        assert self._detector().detect_header_row(str(path)) == (0, ["id", "name"])

        results = import_csv(str(path), str(Path(tmp_path) / "out"))
        assert _table(str(Path(tmp_path) / "out")).schema.names == ["id", "name"]
        assert results.total_rows == 1

    def test_utf8_bom_without_header_stays_out_of_the_first_value(self, tmp_path):
        path = Path(tmp_path) / "bom.csv"
        path.write_bytes(b"\xef\xbb\xbf1,Ann\n2,Bob\n")
        out = str(Path(tmp_path) / "out")
        import_csv(str(path), out, header_mode=HeaderMode.ABSENT)
        assert _table(out).column("col_1").to_pylist() == [1, 2]


# ------------------------------------------------------------------ 7. writer lifecycle


class _AbortableWriter:
    instances = []

    def __init__(self, path, schema, **kwargs):
        self.path = path
        self.schema = schema
        self.rows = 0
        self.closed = False
        self.aborted = False
        _AbortableWriter.instances.append(self)

    def write_table(self, table):
        self.rows += table.num_rows

    def close(self):
        self.closed = True

    def abort(self):
        self.aborted = True


def _failing_reader(good_batches=1):
    """A replacement for the batch reader that yields a batch and then fails."""

    def reader(self, *args, **kwargs):
        for i in range(good_batches):
            yield pa.RecordBatch.from_pydict({"a": [str(i)]})
        raise RuntimeError("disk on fire")

    return reader


class TestWriterLifecycle:
    def test_failure_removes_partial_local_output_and_records_the_error(
        self, tmp_path, captured_results
    ):
        csv_path = _write(Path(tmp_path) / "in.csv", "a\n1\n")
        out = str(Path(tmp_path) / "out")
        with patch.object(BatchProcessor, "create_s3_batch_reader", _failing_reader()):
            with pytest.raises(RuntimeError, match="disk on fire"):
                import_csv(csv_path, out)

        assert not os.path.exists(os.path.join(out, "data.parquet"))
        assert captured_results[0].errors == ["disk on fire"]

    def test_failure_aborts_writers_that_support_it(self, tmp_path):
        _AbortableWriter.instances = []
        csv_path = _write(Path(tmp_path) / "in.csv", "a\n1\n")
        with patch.object(
            BatchProcessor, "create_s3_batch_reader", _failing_reader()
        ), patch.object(csv_processor_module, "create_parquet_writer", _AbortableWriter):
            with pytest.raises(RuntimeError):
                import_csv(csv_path, str(Path(tmp_path) / "out"))

        (writer,) = _AbortableWriter.instances
        assert writer.aborted and not writer.closed

    def test_failed_s3_writer_is_never_uploaded(self, tmp_path):
        s3_client = MagicMock()
        s3_client._s3_client = MagicMock()
        writers = []

        def make_writer(path, schema, **kwargs):
            writer = S3ParquetWriter("s3://bkt/data.parquet", schema, s3_client=s3_client)
            writers.append(writer)
            return writer

        csv_path = _write(Path(tmp_path) / "in.csv", "a\n1\n")
        with patch.object(
            BatchProcessor, "create_s3_batch_reader", _failing_reader()
        ), patch.object(csv_processor_module, "create_parquet_writer", make_writer), patch.object(
            CSVProcessor, "_remove_stale_outputs"
        ):
            with pytest.raises(RuntimeError):
                import_csv(csv_path, "s3://bkt/out/")

        s3_client._s3_client.upload_fileobj.assert_not_called()
        assert not writers[0]._temp_path.exists()  # temp file did not leak

    def test_stale_outputs_of_a_previous_run_are_removed(self, tmp_path):
        schema = _schema(tmp_path, {"n": {"type": "integer"}})
        out = str(Path(tmp_path) / "out")
        csv_path = _write(Path(tmp_path) / "in.csv", "n\n1\nx\n")
        first = import_csv(csv_path, out, schema_file=schema)
        assert first.bad_rows_file and os.path.exists(first.bad_rows_file)

        _write(csv_path, "n\n1\n2\n")
        second = import_csv(csv_path, out, schema_file=schema)
        assert second.bad_rows_file is None
        assert not os.path.exists(os.path.join(out, "bad_rows.parquet"))
        assert _table(out).column("n").to_pylist() == [1, 2]

    def test_empty_rerun_does_not_expose_old_data(self, tmp_path):
        out = str(Path(tmp_path) / "out")
        csv_path = _write(Path(tmp_path) / "in.csv", "n\n1\n")
        import_csv(csv_path, out)
        _write(csv_path, "")
        results = import_csv(csv_path, out)
        assert results.output_files == []
        assert not os.path.exists(os.path.join(out, "data.parquet"))

    def test_unrelated_files_in_the_destination_are_left_alone(self, tmp_path):
        out = Path(tmp_path) / "out"
        out.mkdir()
        (out / "notes.txt").write_text("keep me")
        csv_path = _write(Path(tmp_path) / "in.csv", "n\n1\n")
        import_csv(csv_path, str(out))
        assert (out / "notes.txt").read_text() == "keep me"


# ------------------------------------------------------------------ 8. fallback duplicates


class TestColumnMismatchFallback:
    def _big_csv(self, rows, tail=""):
        # > 1 MiB so that Arrow yields several blocks before it hits the ragged row
        body = "".join(f"{i},name_{i:07d},{i % 7}\n" for i in range(rows))
        return "id,name,grp\n" + body + tail

    def test_rows_before_the_ragged_row_are_not_emitted_twice(self, tmp_path):
        rows = 120_000
        text = self._big_csv(rows, tail=f"{rows},name_last,1,EXTRA\n")
        results, out = _run(tmp_path, text)

        table = _table(out)
        ids = table.column("id").to_pylist()
        assert ids == list(range(rows + 1))
        assert results.total_rows == rows + 1
        assert results.truncated_rows == 1

    def test_fallback_batches_match_the_established_schema(self, tmp_path):
        rows = 120_000
        text = self._big_csv(rows, tail=f"{rows},name_last\n")  # short row at the end
        results, out = _run(tmp_path, text)

        table = _table(out)
        assert table.schema.field("id").type == pa.int64()
        assert table.schema.field("grp").type == pa.int64()
        assert table.num_rows == rows + 1
        assert table.column("grp")[-1].as_py() is None  # padded value became null, not ""

    def test_late_value_that_does_not_fit_the_inferred_type_is_rejected_not_fatal(self, tmp_path):
        rows = 120_000
        text = self._big_csv(rows, tail="oops,name_last,1\n")
        results, out = _run(tmp_path, text)
        assert results.errors == []
        assert results.invalid_rows == 1
        assert _table(out).num_rows == rows
        assert _table(out).schema.field("id").type == pa.int64()
        assert _table(out, "bad_rows.parquet").column("id").to_pylist() == ["oops"]

    def test_value_that_does_not_fit_the_inferred_type_is_rejected_not_fatal(self, tmp_path):
        rows = 120_000
        text = self._big_csv(rows, tail="oops,name_last,1,EXTRA\n")
        results, out = _run(tmp_path, text)
        assert results.invalid_rows == 1
        assert _table(out, "bad_rows.parquet").column("id").to_pylist() == ["oops"]
        assert _table(out).num_rows == rows


# ------------------------------------------------------------------ 9. batch processor


class TestBatchProcessorRows:
    def _processor(self, **config):
        cfg = ImportConfig("i", "o", **config)
        io_handler = MagicMock()
        rejected = []
        return (
            BatchProcessor(cfg, io_handler, reject_handler=rejected.append),
            io_handler,
            rejected,
        )

    def test_reject_mode_counts_and_writes_rejected_rows(self, tmp_path):
        results, out = _run(
            tmp_path, "a,b\n1,2\n3,4,5\n6,7\n", excess_column_mode=ExcessColumnMode.REJECT
        )
        assert (results.total_rows, results.valid_rows, results.invalid_rows) == (3, 2, 1)
        assert _table(out, "bad_rows.parquet").to_pydict() == {"a": ["3"], "b": ["4"]}
        assert results.bad_rows_file

    def test_reject_mode_on_the_s3_reader(self):
        processor, io_handler, rejected = self._processor(
            excess_column_mode=ExcessColumnMode.REJECT
        )
        io_handler.csv_reader.return_value = iter([["a", "b"], ["1", "2"], ["3", "4", "5"]])
        batches = list(processor._create_s3_csv_batches("s3://b/k.csv", ["a", "b"], 0, None))
        assert [b.to_pydict() for b in batches] == [{"a": ["1"], "b": ["2"]}]
        assert [b.to_pydict() for b in rejected] == [{"a": ["3"], "b": ["4"]}]
        assert processor.rejected_rows == 1

    def test_truncated_rows_are_counted(self, tmp_path):
        results, out = _run(tmp_path, "a,b\n1,2\n3,4,5\n6,7,8\n9,9\n")
        assert results.truncated_rows == 2
        assert results.valid_rows == 4
        assert [str(v) for v in _table(out).column("b").to_pylist()] == ["2", "4", "7", "9"]

    def test_passthrough_widening_after_output_started_is_a_clear_error(self):
        processor, io_handler, _ = self._processor(
            excess_column_mode=ExcessColumnMode.PASSTHROUGH, batch_size=2
        )
        io_handler.csv_reader.return_value = iter(
            [["a", "b"], ["1", "2"], ["3", "4"], ["5", "SECRET-VALUE", "6"]]
        )
        generator = processor._create_s3_csv_batches("s3://b/k.csv", ["a", "b"], 0, None)
        next(generator)  # first batch (2 rows) is out
        with pytest.raises(ValueError, match=r"Data row 3 has 3 fields") as error:
            next(generator)
        assert "SECRET-VALUE" not in str(error.value)

    def test_passthrough_widening_before_the_first_batch_still_works(self, tmp_path):
        results, out = _run(
            tmp_path, "a,b\n1,2\n3,4,5\n", excess_column_mode=ExcessColumnMode.PASSTHROUGH
        )
        assert _table(out).schema.names == ["a", "b", "col_3"]

    def test_blank_lines_are_skipped_on_every_path(self, tmp_path):
        # S3 / row reader
        processor, io_handler, _ = self._processor()
        io_handler.csv_reader.return_value = iter([["a", "b"], ["1", "2"], [], ["3", "4"], []])
        (batch,) = processor._create_s3_csv_batches("s3://b/k.csv", ["a", "b"], 0, None)
        assert batch.to_pydict() == {"a": ["1", "3"], "b": ["2", "4"]}

        # Arrow fallback (a ragged row forces it) agrees with the plain local path
        plain, plain_out = _run(Path(tmp_path), "a,b\n1,2\n\n3,4\n")
        ragged_dir = Path(tmp_path) / "r"
        ragged_dir.mkdir()
        ragged, ragged_out = _run(ragged_dir, "a,b\n1,2\n\n3,4,5\n")
        assert plain.total_rows == 2
        assert ragged.total_rows == 2
        assert _table(ragged_out).to_pydict() == {"a": ["1", "3"], "b": ["2", "4"]}

    def test_stop_on_blank_footer_still_sees_blank_lines(self, tmp_path):
        results, out = _run(
            tmp_path,
            "a,b\n1,2\n3,4\n\nTOTAL,2\n",
            footer_detection={"stop_on_blank": True},
        )
        assert results.total_rows == 2

    def test_filtered_file_is_removed_when_copying_fails(self, tmp_path):
        path = Path(tmp_path) / "in.csv"
        path.write_bytes(b"a,b\n1,\xff\xfe\n")  # not valid UTF-8: copying raises
        processor, _, _ = self._processor(footer_detection={"stop_on_blank": True})

        before = set(os.listdir(tempfile.gettempdir()))
        with pytest.raises(UnicodeDecodeError):
            processor._create_filtered_file(path, 0, lambda row: False)
        assert set(os.listdir(tempfile.gettempdir())) == before

    def test_filtered_file_is_removed_when_option_building_fails(self, tmp_path):
        path = Path(tmp_path) / "in.csv"
        path.write_text("a,b\n1,2\n")
        processor, _, _ = self._processor(footer_detection={"stop_on_blank": True})
        before = set(os.listdir(tempfile.gettempdir()))
        with patch(
            "forklift.engine.processors.batch_processor.pv_csv.ConvertOptions",
            side_effect=RuntimeError("bad options"),
        ):
            with pytest.raises(RuntimeError):
                list(processor.create_batch_reader(path, ["a", "b"], 0, lambda row: False))
        assert set(os.listdir(tempfile.gettempdir())) == before

    def test_filtered_file_honours_the_quote_character(self, tmp_path):
        # with the default quote char the second row would split into 3 fields and its
        # second field "9'" would look like the footer
        text = "a,b\n'p,q',1\n'x,9',2\n'sum',9\n"
        results, out = _run(
            tmp_path,
            text,
            quote_char="'",
            footer_detection={"column_index": 1, "patterns": ["^9"]},
        )
        assert results.total_rows == 2
        assert _table(out).column("a").to_pylist() == ["p,q", "x,9"]

    @staticmethod
    def _invalid_utf8_file(path, valid_rows):
        # long enough that the header scan (first 8 KiB) never decodes the bad bytes
        body = "".join(f"{i},city_{i:06d}\n" for i in range(valid_rows))
        path.write_bytes(("id,city\n" + body).encode() + b"7,SECRETCITY\xff\xfe\n")

    @pytest.mark.parametrize("with_schema", [False, True])
    def test_invalid_utf8_deep_in_the_file_is_a_clear_error(
        self, tmp_path, captured_results, with_schema
    ):
        csv_path = Path(tmp_path) / "in.csv"
        self._invalid_utf8_file(csv_path, 2000)
        kwargs = {}
        if with_schema:
            kwargs["schema_file"] = _schema(
                tmp_path, {"id": {"type": "integer"}, "city": {"type": "string"}}
            )
        out = Path(tmp_path) / "out"
        with pytest.raises(ValueError, match="not valid for encoding 'utf-8'") as error:
            import_csv(str(csv_path), str(out), **kwargs)
        assert "SECRETCITY" not in str(error.value)
        assert "SECRETCITY" not in " ".join(captured_results[0].errors)
        assert not (out / "data.parquet").exists()

    def test_invalid_utf8_in_the_header_scan_is_a_clear_error(self, tmp_path, captured_results):
        csv_path = Path(tmp_path) / "in.csv"
        csv_path.write_bytes(b"name,city\nAnn,SECRETCITY\xff\xfe\n")
        with pytest.raises(ValueError, match="not valid for encoding 'utf-8'") as error:
            import_csv(str(csv_path), str(Path(tmp_path) / "out"))
        assert "SECRETCITY" not in str(error.value)
        assert "SECRETCITY" not in " ".join(captured_results[0].errors)

    def test_arrow_errors_do_not_echo_row_content(self, tmp_path, captured_results):
        csv_path = Path(tmp_path) / "in.csv"
        csv_path.write_bytes(b"a,b\n1,2\nSECRET\x00PAYLOAD,x,y,z\n")
        with pytest.raises(pa.ArrowInvalid) as error:
            import_csv(str(csv_path), str(Path(tmp_path) / "out"))
        assert "SECRET" not in str(error.value)
        assert "SECRET" not in " ".join(captured_results[0].errors)
        assert captured_results[0].errors  # but the failure itself is recorded

    def test_sanitize_arrow_error(self):
        assert "alice" not in sanitize_arrow_error(
            "CSV parse error: Expected 3 columns, got 4: alice,bob,carol,dave"
        )
        assert "alice" not in sanitize_arrow_error(
            "In CSV column #2: CSV conversion error to int64: invalid value 'alice'"
        )
        assert "In CSV column #2" in sanitize_arrow_error(
            "In CSV column #2: CSV conversion error to int64: invalid value 'alice'"
        )


# ------------------------------------------------------------------ 10. config knobs


class TestConfigKnobs:
    def test_escape_char_reaches_the_row_reader(self, tmp_path):
        path = Path(tmp_path) / "in.csv"
        path.write_text("a,b\nx\\,y,1\n")
        cfg = ImportConfig(str(path), "o", escape_char="\\")
        processor = BatchProcessor(cfg, UnifiedIOHandler())
        (batch,) = processor._create_s3_csv_batches(str(path), ["a", "b"], 0, None)
        assert batch.to_pydict() == {"a": ["x,y"], "b": ["1"]}

    def test_escape_char_reaches_the_fallback_reader(self, tmp_path):
        path = Path(tmp_path) / "in.csv"
        path.write_text("x\\,y,1,EXTRA\n")
        cfg = ImportConfig(str(path), "o", escape_char="\\")
        processor = BatchProcessor(cfg, UnifiedIOHandler())
        (batch,) = processor._handle_column_mismatch_reader(path, 0, ["a", "b"])
        assert batch.to_pydict() == {"a": ["x,y"], "b": ["1"]}

    def test_batch_size_caps_local_batches(self, tmp_path):
        path = Path(tmp_path) / "in.csv"
        path.write_text("a\n" + "".join(f"{i}\n" for i in range(1000)))
        cfg = ImportConfig(str(path), "o", batch_size=100)
        processor = BatchProcessor(cfg, UnifiedIOHandler())
        sizes = [len(b) for b in processor.create_batch_reader(path, ["a"], 0, lambda r: False)]
        assert sizes and max(sizes) <= 100 and sum(sizes) == 1000

    def test_documented_knobs_match_behaviour(self):
        doc = ImportConfig.__doc__
        assert "Reserved" in doc and "max_validation_errors" in doc
        assert "Only used while locating the header row" in doc


# ------------------------------------------------------------------ SHOULD: manifest / metadata


class TestManifestAndMetadata:
    def _processor(self):
        processor = CSVProcessor()
        processor.io_handler = MagicMock()
        processor.io_handler.exists.return_value = True
        processor.io_handler.get_size.return_value = 10
        written = {}

        def open_for_write(path, encoding="utf-8"):
            buffer = io.StringIO()
            buffer.close = lambda: written.__setitem__(path, buffer.getvalue())
            return buffer

        processor.io_handler.open_for_write.side_effect = open_for_write
        return processor, written

    def test_s3_destination_uses_s3_uris_not_a_local_path(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        processor, written = self._processor()
        path = processor._create_s3_manifest("s3://bkt/out/", ["s3://bkt/out/data.parquet"])

        assert path == "s3://bkt/out/manifest.json"
        assert json.loads(written[path])["files"][0]["file_path"] == "data.parquet"
        assert not any(p.name.startswith("s3") for p in tmp_path.iterdir())

    def test_nan_values_do_not_break_json_output(self, tmp_path):
        processor, written = self._processor()
        results = ProcessingResults(execution_time=float("nan"))
        processor.schema_processor = MagicMock()
        processor.schema_processor.config = ImportConfig("in.csv", str(tmp_path))
        path = processor._create_s3_metadata(str(tmp_path), results)
        payload = json.loads(written[path])
        assert payload["processing_summary"]["execution_time_seconds"] is None
        assert payload["input_config"]["header_mode"] == "present"

    def test_local_outputs_are_written_through_the_same_helpers(self, tmp_path):
        results, out = _run(tmp_path, "a\n1\n")
        manifest = json.loads(Path(results.manifest_file).read_text())
        assert [f["file_path"] for f in manifest["files"]] == ["data.parquet"]
        metadata = json.loads(Path(results.metadata_file).read_text())
        assert metadata["processing_summary"]["total_rows"] == 1
        assert metadata["bad_rows_file"] is None
