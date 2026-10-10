"""Edge cases of ``DataTypeInferrer``: CSV read failures, header probing for old pyarrow,
non-string input columns and the JSON-schema view of a sampled table."""

import io

import pyarrow as pa
import pytest

from forklift.schema.generator import inference as inference_module
from forklift.schema.generator.inference import DataTypeInferrer, _PrefixedStream

# Python's csv module refuses fields longer than this by default (csv.field_size_limit())
_OVER_CSV_FIELD_LIMIT = 140_000


def _write(tmp_path, data: bytes, name="sample.csv"):
    path = tmp_path / name
    path.write_bytes(data)
    return path


class TestPrefixedStream:
    def test_prefix_bytes_are_returned_before_the_rest_of_the_source(self):
        stream = _PrefixedStream(b"head,", io.BytesIO(b"tail\n"))

        assert stream.readable() is True
        assert io.BufferedReader(stream, buffer_size=2).read() == b"head,tail\n"

    def test_reads_are_split_at_the_end_of_the_prefix(self):
        stream = _PrefixedStream(b"abc", io.BytesIO(b"de"))
        buffer = bytearray(2)

        chunks = []
        while True:
            count = stream.readinto(buffer)
            if count == 0:
                break
            chunks.append(bytes(buffer[:count]))

        assert chunks == [b"ab", b"c", b"de"]


class TestHeaderProbeWithoutDefaultColumnType:
    """pyarrow < 19 needs the column names up front; they are read from the stream itself."""

    @pytest.fixture(autouse=True)
    def _old_pyarrow(self, monkeypatch):
        monkeypatch.setattr(inference_module, "_SUPPORTS_DEFAULT_COLUMN_TYPE", False)

    def test_header_longer_than_the_probe_is_rejected(self, tmp_path, monkeypatch):
        monkeypatch.setattr(inference_module, "_HEADER_PROBE_BYTES", 8)
        path = _write(tmp_path, b"customer_id,amount\n1,2\n")

        with pytest.raises(ValueError, match="larger than the maximum supported size"):
            DataTypeInferrer().read_csv_sample(path, 5)

    def test_header_the_csv_module_cannot_parse_is_rejected_without_its_text(self, tmp_path):
        secret = "x" * _OVER_CSV_FIELD_LIMIT
        path = _write(tmp_path, f'"{secret}",b\n1,2\n'.encode())

        with pytest.raises(ValueError, match="invalid CSV header") as excinfo:
            DataTypeInferrer().read_csv_sample(path, 5)
        assert "xxx" not in str(excinfo.value)

    def test_header_with_invalid_bytes_is_reported_as_an_encoding_error(self, tmp_path):
        path = _write(tmp_path, b"na\xffme\n1\n")

        with pytest.raises(ValueError, match="invalid text for the configured encoding"):
            DataTypeInferrer().read_csv_sample(path, 5)


class TestCsvReadFailures:
    def test_invalid_text_for_a_non_utf8_encoding_does_not_echo_the_data(
        self, tmp_path, monkeypatch
    ):
        # pyarrow < 19 probes the header first: keep the invalid byte outside that probe
        monkeypatch.setattr(inference_module, "_HEADER_PROBE_BYTES", len(b"name\n"))
        path = _write(tmp_path, "name\nsecrét\n".encode("latin-1"))

        with pytest.raises(ValueError, match="data is not valid ascii text") as excinfo:
            DataTypeInferrer().read_csv_sample(path, 5, encoding="ascii")
        assert "secr" not in str(excinfo.value)

    def test_record_larger_than_every_block_size_is_rejected(self, tmp_path, monkeypatch):
        monkeypatch.setattr(inference_module, "_BLOCK_SIZES", (16, 32))
        path = _write(tmp_path, b"a\n" + b"1" * 100 + b"\n")

        with pytest.raises(ValueError, match="larger than the maximum supported size"):
            DataTypeInferrer().read_csv_sample(path, 5)

    def test_record_that_fits_a_larger_block_is_read_on_retry(self, tmp_path, monkeypatch):
        monkeypatch.setattr(inference_module, "_BLOCK_SIZES", (16, 1 << 16))
        path = _write(tmp_path, b"a\n" + b"1" * 100 + b"\n")

        table = DataTypeInferrer().read_csv_sample(path, 5)

        assert table.column_names == ["a"]
        assert table.column("a").to_pylist() == ["1" * 100]

    @pytest.mark.parametrize("terminator", [b"", b"\n1\n"], ids=["header-only", "with-rows"])
    def test_header_larger_than_every_block_size_is_rejected(
        self, tmp_path, monkeypatch, terminator
    ):
        monkeypatch.setattr(inference_module, "_BLOCK_SIZES", (16, 32))
        path = _write(tmp_path, b"h" * 100 + terminator)

        for nrows in (5, None):
            with pytest.raises(ValueError, match="larger than the maximum supported size"):
                DataTypeInferrer().read_csv_sample(path, nrows)

    def test_unrecognised_arrow_error_is_reported_without_its_text(self, tmp_path, monkeypatch):
        def failing_open_csv(*args, **kwargs):
            raise pa.ArrowInvalid("unexpected problem near 'secret-value'")

        monkeypatch.setattr(inference_module.pv_csv, "open_csv", failing_open_csv)
        path = _write(tmp_path, b"a\n1\n")

        with pytest.raises(ValueError) as excinfo:
            DataTypeInferrer().read_csv_sample(path, 5)
        assert str(excinfo.value) == "Failed to read CSV sample: invalid CSV data"


class TestHeaderOnlyFiles:
    """A header without a row terminator makes Arrow fail; the names are read directly."""

    def test_blank_line_only_counts_as_an_empty_file(self, tmp_path):
        path = _write(tmp_path, b"\n")

        with pytest.raises(ValueError, match="file is empty"):
            DataTypeInferrer().read_csv_sample(path, 5)

    def test_byte_order_mark_is_not_part_of_the_first_name(self, tmp_path):
        path = _write(tmp_path, b"\xef\xbb\xbfid,name")

        table = DataTypeInferrer().read_csv_sample(path, 5)

        assert table.column_names == ["id", "name"]
        assert table.num_rows == 0

    def test_invalid_bytes_are_reported_as_an_encoding_error(self, tmp_path):
        path = _write(tmp_path, b"na\xffme")

        with pytest.raises(ValueError, match="invalid text for the configured encoding"):
            DataTypeInferrer().read_csv_sample(path, 5)

    def test_header_the_csv_module_cannot_parse_is_rejected(self, tmp_path):
        path = _write(tmp_path, ('"' + "x" * _OVER_CSV_FIELD_LIMIT + '",b').encode())

        with pytest.raises(ValueError, match="invalid CSV header") as excinfo:
            DataTypeInferrer().read_csv_sample(path, 5)
        assert "xxx" not in str(excinfo.value)


class TestInferTypesFromStrings:
    def test_non_string_columns_are_kept_unchanged(self):
        table = pa.table(
            {
                "count": pa.array([1, 2], pa.int32()),
                "large": pa.array(["7", "8"], pa.large_string()),
                "flag": pa.array(["true", "FALSE"]),
            }
        )

        result = DataTypeInferrer().infer_types_from_strings(table)

        assert result.column("count").type == pa.int32()
        assert result.column("count").to_pylist() == [1, 2]
        assert result.column("large").to_pylist() == [7, 8]
        assert result.column("flag").to_pylist() == [True, False]


class TestInferSchemaFromData:
    def test_properties_follow_the_column_types(self):
        table = pa.table(
            {
                "id": pa.array([1], pa.int64()),
                "name": pa.array(["a"]),
                "active": pa.array([True]),
            }
        )

        schema = DataTypeInferrer().infer_schema_from_data(table)

        assert schema["type"] == "object"
        assert schema["additionalProperties"] is False
        assert list(schema["properties"]) == ["id", "name", "active"]
        assert schema["properties"]["id"]["type"] == "integer"
        assert schema["properties"]["name"]["type"] == "string"
        assert schema["properties"]["active"]["type"] == "boolean"

    def test_empty_table_has_no_properties(self):
        schema = DataTypeInferrer().infer_schema_from_data(pa.table({}))

        assert schema == {"type": "object", "properties": {}, "additionalProperties": False}
