"""Tests for the engine-facing row hash API.

The engine runs ``RowHashProcessor`` as the last step of a per-batch pipeline in which rows were
dropped and columns renamed/added since the raw input. These tests cover:

    1. ``compute_input_hash`` (equal to what ``process_batch`` stores for the same batch)
    2. ``process_batch(..., input_hash=...)`` with dropped rows
    3. ``process_batch(..., source_row_numbers=...)`` (source position vs. processing sequence)
    4. stable column types/metadata for an empty batch, ``get_output_schema``
    5. chunked / sliced batches
    6. ``row_hash_output_columns``
    7. fail-closed behaviour (missing input hash, missing source context, nothing to hash)
"""

import hashlib
import inspect
from datetime import date, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from forklift.processors.pipeline import ProcessorPipeline
from forklift.processors.row_hash import (
    HASH_ALGORITHM_METADATA_KEY,
    HASH_VERSION_METADATA_KEY,
    RowHashConfig,
    RowHashProcessor,
)
from forklift.processors.row_hash_factory import (
    create_row_hash_processor_from_schema,
    row_hash_output_columns,
)

SECRET = "SECRET-CELL-VALUE-123"

ALGORITHMS = ["sha256", "sha512"]
ENCODINGS = [False, True]  # legacy_encoding


def _config(**kwargs) -> RowHashConfig:
    kwargs.setdefault("enabled", True)
    return RowHashConfig(**kwargs)


def _raw_batch() -> pa.RecordBatch:
    """A raw (all-string) batch as the reader produces it, with separators, NULLs and unicode."""
    return pa.RecordBatch.from_pydict(
        {
            "Id": ["1", "x", "3", "4", "5", "6"],
            "Name": ["Ann", "Bob||X", None, "", "NULL", "Zoë"],
            "Amount": ["1.50", "oops", "3", None, "5.25", "0"],
        }
    )


def _typed_batch() -> pa.RecordBatch:
    """One column of each kind the hash encoding distinguishes, with NULLs."""
    return pa.RecordBatch.from_pydict(
        {
            "s": pa.array(["a", "b||c", None, "", "NULL", "é"], pa.string()),
            "i": pa.array([1, None, 3, -4, 5, 6], pa.int64()),
            "f": pa.array([1.5, float("nan"), None, 0.0, -0.0, 2.25], pa.float64()),
            "b": pa.array([True, False, None, True, False, True]),
            "d": pa.array(
                [Decimal("1.50"), None, Decimal("-2.00"), Decimal("0.00"), Decimal("9.99"), None],
                pa.decimal128(10, 2),
            ),
            "dt": pa.array(
                [
                    date(2024, 1, 1),
                    None,
                    date(1999, 12, 31),
                    date(2000, 2, 29),
                    None,
                    date(1970, 1, 1),
                ],
                pa.date32(),
            ),
            "ts": pa.array(
                [datetime(2024, 1, 1, 1, 2, 3), None]
                + [datetime(2000, 1, 1, 0, 0, i) for i in range(4)],
                pa.timestamp("us"),
            ),
            "bin": pa.array([b"a", b"", None, b"\x00", b"z", b"q"], pa.binary()),
            "dict": pa.array(["x", "y", "x", None, "y", "x"]).dictionary_encode(),
            "lst": pa.array([[1], [2, 3], None, [], [4], [5]]),
        }
    )


def _empty_like(batch: pa.RecordBatch) -> pa.RecordBatch:
    """An empty batch with the schema of ``batch`` (not a slice, built from empty arrays)."""
    return pa.RecordBatch.from_arrays(
        [pa.array([], type=field.type) for field in batch.schema], schema=batch.schema
    )


def _everything_config(**kwargs) -> RowHashConfig:
    return RowHashConfig(
        enabled=True,
        input_hash_enabled=True,
        source_uri_enabled=True,
        ingested_at_enabled=True,
        row_number_enabled=True,
        **kwargs,
    )


def _started(config: RowHashConfig, offset: int = 0) -> RowHashProcessor:
    processor = RowHashProcessor(config)
    processor.set_source_context("file:///data/in.csv", offset)
    return processor


# ---------------------------------------------------------------------------------------------
# 1. compute_input_hash
# ---------------------------------------------------------------------------------------------


class TestComputeInputHash:
    @pytest.mark.parametrize("legacy", ENCODINGS)
    @pytest.mark.parametrize("algorithm", ALGORITHMS)
    def test_equals_what_process_batch_stores(self, algorithm, legacy):
        config = _config(input_hash_enabled=True, algorithm=algorithm, legacy_encoding=legacy)
        processor = RowHashProcessor(config)
        batch = _raw_batch()

        stored, _ = processor.process_batch(batch, input_batch=batch)
        computed = processor.compute_input_hash(batch)

        assert isinstance(computed, pa.Array)
        assert computed.type == pa.string()
        assert computed.null_count == 0
        assert computed.to_pylist() == stored.column("_input_hash").to_pylist()

    @pytest.mark.parametrize("legacy", ENCODINGS)
    @pytest.mark.parametrize("algorithm", ALGORITHMS)
    def test_equals_for_every_column_type(self, algorithm, legacy):
        config = _config(input_hash_enabled=True, algorithm=algorithm, legacy_encoding=legacy)
        processor = RowHashProcessor(config)
        batch = _typed_batch()

        stored, _ = processor.process_batch(batch, input_batch=batch)

        assert (
            processor.compute_input_hash(batch).to_pylist()
            == stored.column("_input_hash").to_pylist()
        )

    def test_uses_all_columns_of_the_input_batch_not_the_row_hash_selection(self):
        config = _config(input_hash_enabled=True, exclude_columns=["Amount"])
        processor = RowHashProcessor(config)
        batch = _raw_batch()
        out, _ = processor.process_batch(batch, input_batch=batch)

        # the output hash skips "Amount", the input hash does not
        assert out.column("row_hash").to_pylist() != out.column("_input_hash").to_pylist()
        full = RowHashProcessor(_config())
        assert (
            processor.compute_input_hash(batch).to_pylist()
            == full.process_batch(batch)[0].column("row_hash").to_pylist()
        )

    def test_legacy_value_matches_an_independent_computation(self):
        processor = RowHashProcessor(_config(legacy_encoding=True))
        batch = pa.RecordBatch.from_pydict({"a": ["x", None, "y||z"], "b": [1, 2, None]})

        expected = [
            hashlib.sha256(text.encode("utf-8")).hexdigest()
            for text in ("x||1", "NULL||2", "y||z||NULL")
        ]

        assert processor.compute_input_hash(batch).to_pylist() == expected

    def test_encodings_and_algorithms_give_different_hashes(self):
        batch = _raw_batch()
        values = {
            (algorithm, legacy): tuple(
                RowHashProcessor(_config(algorithm=algorithm, legacy_encoding=legacy))
                .compute_input_hash(batch)
                .to_pylist()
            )
            for algorithm in ALGORITHMS
            for legacy in ENCODINGS
        }
        assert len(set(values.values())) == 4

    def test_distinguishes_nulls_empty_strings_and_separators(self):
        processor = RowHashProcessor(_config())
        batch = pa.RecordBatch.from_pydict(
            {"a": ["a||b", "a", None, "", "NULL"], "b": ["c", "b||c", None, "", None]}
        )
        hashes = processor.compute_input_hash(batch).to_pylist()
        assert len(set(hashes)) == 5

    def test_depends_on_column_names(self):
        processor = RowHashProcessor(_config())
        one = pa.RecordBatch.from_pydict({"a": ["1"]})
        two = pa.RecordBatch.from_pydict({"b": ["1"]})
        assert processor.compute_input_hash(one).to_pylist() != (
            processor.compute_input_hash(two).to_pylist()
        )

    def test_does_not_need_the_input_hash_option_and_keeps_no_state(self):
        processor = RowHashProcessor(_config(row_number_enabled=True))
        processor.set_source_context("f.csv", 5)
        processor.compute_input_hash(_raw_batch())
        out, _ = processor.process_batch(pa.RecordBatch.from_pydict({"x": [1, 2]}))
        assert out.column("_rownum").to_pylist() == [1, 2]
        assert out.column("_rownum_in_source_file").to_pylist() == [6, 7]

    def test_empty_batch_gives_an_empty_string_array(self):
        processor = RowHashProcessor(_config())
        result = processor.compute_input_hash(_empty_like(_typed_batch()))
        assert len(result) == 0
        assert result.type == pa.string()


# ---------------------------------------------------------------------------------------------
# 2. process_batch(input_hash=...) when rows were dropped
# ---------------------------------------------------------------------------------------------


def _drop_and_convert(raw: pa.RecordBatch, keep: list):
    """Engine-like middle of the pipeline: drop rows, convert types, rename, add a column."""
    mask = pa.array([i in keep for i in range(raw.num_rows)])
    kept = raw.filter(mask)
    return pa.RecordBatch.from_arrays(
        [
            pc.cast(kept.column("Id"), pa.int64()),
            kept.column("Name"),
            pc.cast(kept.column("Amount"), pa.float64()),
            pa.array(["batch-1"] * kept.num_rows),
        ],
        names=["customer_id", "customer_name", "amount", "batch_id"],
    )


class TestInputHashWithDroppedRows:
    KEEP = [0, 2, 3, 5]  # rows 1 ("x") and 4 ("oops"/NULL name mix) were dropped

    def _raw_for_keep(self):
        # make the dropped rows non-convertible so the scenario is realistic
        return pa.RecordBatch.from_pydict(
            {
                "Id": ["1", "x", "3", "4", "bad", "6"],
                "Name": ["Ann", "Bob||X", None, "", SECRET, "Zoë"],
                "Amount": ["1.50", "2", "3", None, "5.25", "0"],
            }
        )

    @pytest.mark.parametrize("legacy", ENCODINGS)
    @pytest.mark.parametrize("algorithm", ALGORITHMS)
    def test_input_hash_is_the_hash_of_the_raw_row_of_each_surviving_row(self, algorithm, legacy):
        config = _config(input_hash_enabled=True, algorithm=algorithm, legacy_encoding=legacy)
        processor = _started(config)
        raw = self._raw_for_keep()

        raw_hashes = processor.compute_input_hash(raw)  # before any row is dropped
        keep_mask = pa.array([i in self.KEEP for i in range(raw.num_rows)])
        final = _drop_and_convert(raw, self.KEEP)
        surviving = raw_hashes.filter(keep_mask)

        out, results = processor.process_batch(final, input_hash=surviving)

        assert results == []
        assert out.num_rows == len(self.KEEP)
        for result_idx, raw_idx in enumerate(self.KEEP):
            raw_row = raw.slice(raw_idx, 1)
            expected = processor.compute_input_hash(raw_row)[0].as_py()
            assert out.column("_input_hash")[result_idx].as_py() == expected
            assert expected == raw_hashes[raw_idx].as_py()
        # and it is NOT the hash of the final (converted / renamed) row
        assert out.column("_input_hash").to_pylist() != out.column("row_hash").to_pylist()

    def test_equals_the_old_path_on_the_surviving_raw_rows(self):
        config = _config(input_hash_enabled=True)
        raw = self._raw_for_keep()
        final = _drop_and_convert(raw, self.KEEP)
        surviving_raw = raw.filter(pa.array([i in self.KEEP for i in range(raw.num_rows)]))

        old, _ = RowHashProcessor(config).process_batch(final, input_batch=surviving_raw)
        new, _ = RowHashProcessor(config).process_batch(
            final, input_hash=RowHashProcessor(config).compute_input_hash(surviving_raw)
        )

        assert old.equals(new)

    def test_input_batch_with_a_different_length_fails_with_a_clear_error(self):
        # this is the situation before input_hash existed: the lengths differ after dropping rows
        processor = _started(_config(input_hash_enabled=True))
        raw = self._raw_for_keep()
        final = _drop_and_convert(raw, self.KEEP)

        with pytest.raises(ValueError, match="input_hash") as excinfo:
            processor.process_batch(final, input_batch=raw)

        assert SECRET not in str(excinfo.value)

    def test_input_hash_wins_over_input_batch_which_may_have_any_length(self):
        processor = _started(_config(input_hash_enabled=True))
        raw = self._raw_for_keep()
        final = _drop_and_convert(raw, self.KEEP)
        keep_mask = pa.array([i in self.KEEP for i in range(raw.num_rows)])
        surviving = processor.compute_input_hash(raw).filter(keep_mask)

        with_both, _ = processor.process_batch(final, input_batch=raw, input_hash=surviving)
        only_hash, _ = processor.process_batch(final, input_hash=surviving)
        unrelated, _ = processor.process_batch(
            final,
            input_batch=pa.RecordBatch.from_pydict({"zzz": ["ignored"]}),
            input_hash=surviving,
        )

        assert with_both.column("_input_hash").to_pylist() == surviving.to_pylist()
        assert only_hash.column("_input_hash").to_pylist() == surviving.to_pylist()
        assert unrelated.column("_input_hash").to_pylist() == surviving.to_pylist()

    def test_input_hash_column_has_the_hash_metadata(self):
        processor = _started(_config(input_hash_enabled=True, algorithm="sha512"))
        final = _drop_and_convert(self._raw_for_keep(), self.KEEP)
        hashes = pa.array(["h"] * final.num_rows)

        out, _ = processor.process_batch(final, input_hash=hashes)

        metadata = out.schema.field("_input_hash").metadata
        assert metadata[HASH_VERSION_METADATA_KEY] == b"2"
        assert metadata[HASH_ALGORITHM_METADATA_KEY] == b"sha512"
        assert out.schema.field("_input_hash").type == pa.string()

    @pytest.mark.parametrize("length", [0, 3, 5])
    def test_wrong_length_raises(self, length):
        processor = _started(_config(input_hash_enabled=True))
        final = _drop_and_convert(self._raw_for_keep(), self.KEEP)
        assert final.num_rows == 4

        with pytest.raises(ValueError, match="input_hash has"):
            processor.process_batch(final, input_hash=pa.array(["h"] * length))

    def test_nulls_and_non_string_hashes_raise(self):
        processor = _started(_config(input_hash_enabled=True))
        final = pa.RecordBatch.from_pydict({"x": [1, 2]})

        with pytest.raises(ValueError, match="nulls"):
            processor.process_batch(final, input_hash=pa.array(["a", None]))
        with pytest.raises(ValueError, match="string array"):
            processor.process_batch(final, input_hash=pa.array([1, 2]))
        with pytest.raises(ValueError, match="string array"):
            processor.process_batch(final, input_hash=pa.array([b"a", b"b"]))
        with pytest.raises(TypeError, match="pyarrow Array"):
            processor.process_batch(final, input_hash=["a", "b"])

    def test_chunked_and_large_string_hashes_are_accepted(self):
        processor = _started(_config(input_hash_enabled=True))
        final = pa.RecordBatch.from_pydict({"x": [1, 2, 3]})

        chunked = pa.chunked_array([pa.array(["a"]), pa.array(["b", "c"])])
        out, _ = processor.process_batch(final, input_hash=chunked)
        assert out.column("_input_hash").to_pylist() == ["a", "b", "c"]

        large = pa.array(["a", "b", "c"], pa.large_string())
        out, _ = processor.process_batch(final, input_hash=large)
        assert out.column("_input_hash").to_pylist() == ["a", "b", "c"]
        assert out.schema.field("_input_hash").type == pa.string()

    def test_input_hash_is_ignored_when_the_option_is_off_but_still_validated(self):
        processor = _started(_config())
        final = pa.RecordBatch.from_pydict({"x": [1, 2]})

        out, _ = processor.process_batch(final, input_hash=pa.array(["a", "b"]))
        assert out.schema.names == ["x", "row_hash"]
        with pytest.raises(ValueError, match="input_hash has"):
            processor.process_batch(final, input_hash=pa.array(["a"]))

    def test_enabled_without_input_batch_or_input_hash_raises(self):
        # before: the input hash column was silently left out
        processor = _started(_config(input_hash_enabled=True, row_number_enabled=True))
        final = pa.RecordBatch.from_pydict({"x": [1, 2]})

        with pytest.raises(ValueError, match="neither input_batch nor input_hash"):
            processor.process_batch(final)

        # nothing was consumed by the failed call
        out, _ = processor.process_batch(final, input_hash=pa.array(["a", "b"]))
        assert out.column("_rownum").to_pylist() == [1, 2]

    def test_a_failed_call_does_not_advance_the_counters(self):
        processor = _started(_config(row_number_enabled=True))
        final = pa.RecordBatch.from_pydict({"x": [1, 2]})
        with pytest.raises(ValueError):
            processor.process_batch(final, source_row_numbers=pa.array([1], pa.int64()))
        out, _ = processor.process_batch(final)
        assert out.column("_rownum").to_pylist() == [1, 2]


# ---------------------------------------------------------------------------------------------
# 3. process_batch(source_row_numbers=...)
# ---------------------------------------------------------------------------------------------


class TestSourceRowNumbers:
    def test_internal_counter_counts_the_rows_the_processor_receives(self):
        # documented behaviour that makes source_row_numbers necessary after rows were dropped
        processor = _started(_config(row_number_enabled=True))
        survivors = pa.RecordBatch.from_pydict({"x": ["a", "c"]})  # source rows 1 and 3
        out, _ = processor.process_batch(survivors)
        assert out.column("_rownum_in_source_file").to_pylist() == [1, 2]

    def test_given_positions_are_used_and_the_sequence_keeps_counting(self):
        processor = _started(_config(row_number_enabled=True))

        first, _ = processor.process_batch(
            pa.RecordBatch.from_pydict({"x": ["a", "c", "d"]}),
            source_row_numbers=pa.array([1, 3, 4], pa.int64()),
        )
        second, _ = processor.process_batch(
            pa.RecordBatch.from_pydict({"x": ["g", "i"]}),
            source_row_numbers=pa.array([7, 9], pa.int64()),
        )

        assert first.column("_rownum_in_source_file").to_pylist() == [1, 3, 4]
        assert first.column("_rownum").to_pylist() == [1, 2, 3]
        assert second.column("_rownum_in_source_file").to_pylist() == [7, 9]
        assert second.column("_rownum").to_pylist() == [4, 5]
        assert first.schema.field("_rownum_in_source_file").type == pa.int64()
        assert first.schema.field("_rownum").type == pa.int64()

    def test_source_row_offset_is_not_added_to_given_positions(self):
        processor = _started(_config(row_number_enabled=True), offset=100)
        out, _ = processor.process_batch(
            pa.RecordBatch.from_pydict({"x": ["a", "b"]}),
            source_row_numbers=pa.array([2, 5], pa.int64()),
        )
        assert out.column("_rownum_in_source_file").to_pylist() == [2, 5]

        # ... while the internal counter still honours it
        out, _ = processor.process_batch(pa.RecordBatch.from_pydict({"x": ["c"]}))
        assert out.column("_rownum_in_source_file").to_pylist() == [103]
        assert out.column("_rownum").to_pylist() == [3]

    def test_batches_with_and_without_positions_can_be_mixed(self):
        processor = _started(_config(row_number_enabled=True))
        out1, _ = processor.process_batch(pa.RecordBatch.from_pydict({"x": [1, 2]}))
        out2, _ = processor.process_batch(
            pa.RecordBatch.from_pydict({"x": [3]}), source_row_numbers=pa.array([9], pa.int64())
        )
        out3, _ = processor.process_batch(pa.RecordBatch.from_pydict({"x": [4]}))
        assert out1.column("_rownum_in_source_file").to_pylist() == [1, 2]
        assert out2.column("_rownum_in_source_file").to_pylist() == [9]
        assert out3.column("_rownum_in_source_file").to_pylist() == [4]
        assert out3.column("_rownum").to_pylist() == [4]

    def test_new_source_restarts_the_sequence(self):
        processor = _started(_config(row_number_enabled=True))
        batch = pa.RecordBatch.from_pydict({"x": [1, 2]})
        processor.process_batch(batch, source_row_numbers=pa.array([4, 8], pa.int64()))
        processor.set_source_context("other.csv")
        out, _ = processor.process_batch(batch, source_row_numbers=pa.array([1, 2], pa.int64()))
        assert out.column("_rownum").to_pylist() == [1, 2]

    @pytest.mark.parametrize("dtype", [pa.int8(), pa.int16(), pa.int32(), pa.uint16(), pa.int64()])
    def test_any_integer_type_is_cast_to_int64(self, dtype):
        processor = _started(_config(row_number_enabled=True))
        out, _ = processor.process_batch(
            pa.RecordBatch.from_pydict({"x": [1, 2]}), source_row_numbers=pa.array([3, 5], dtype)
        )
        assert out.schema.field("_rownum_in_source_file").type == pa.int64()
        assert out.column("_rownum_in_source_file").to_pylist() == [3, 5]

    def test_chunked_positions_are_accepted(self):
        processor = _started(_config(row_number_enabled=True))
        positions = pa.chunked_array([pa.array([3], pa.int64()), pa.array([5, 6], pa.int64())])
        out, _ = processor.process_batch(
            pa.RecordBatch.from_pydict({"x": [1, 2, 3]}), source_row_numbers=positions
        )
        assert out.column("_rownum_in_source_file").to_pylist() == [3, 5, 6]

    @pytest.mark.parametrize(
        "positions,match",
        [
            (pa.array([1], pa.int64()), "source_row_numbers has 1 values"),
            (pa.array([1, 2, 3], pa.int64()), "source_row_numbers has 3 values"),
            (pa.array([1, None], pa.int64()), "nulls"),
            (pa.array([1.0, 2.0]), "integer array"),
            (pa.array(["1", "2"]), "integer array"),
            (pa.array([0, 1], pa.int64()), ">= 1"),
            (pa.array([3, -1], pa.int64()), ">= 1"),
        ],
    )
    def test_invalid_positions_raise(self, positions, match):
        processor = _started(_config(row_number_enabled=True))
        with pytest.raises(ValueError, match=match):
            processor.process_batch(
                pa.RecordBatch.from_pydict({"x": [1, 2]}), source_row_numbers=positions
            )

    def test_not_an_arrow_array_raises_type_error(self):
        processor = _started(_config(row_number_enabled=True))
        with pytest.raises(TypeError, match="pyarrow Array"):
            processor.process_batch(
                pa.RecordBatch.from_pydict({"x": [1, 2]}), source_row_numbers=[1, 2]
            )

    def test_ignored_without_row_numbers_but_validated(self):
        processor = _started(_config())
        batch = pa.RecordBatch.from_pydict({"x": [1, 2]})
        out, _ = processor.process_batch(batch, source_row_numbers=pa.array([1, 2], pa.int64()))
        assert out.schema.names == ["x", "row_hash"]
        with pytest.raises(ValueError):
            processor.process_batch(batch, source_row_numbers=pa.array([1], pa.int64()))

    def test_engine_style_flow_with_dropped_rows(self):
        """Raw rows 1..6, rows 2 and 5 dropped; source numbers and input hashes are carried."""
        config = _config(input_hash_enabled=True, row_number_enabled=True)
        processor = _started(config)
        raw = _raw_batch()
        positions = pa.array(range(1, raw.num_rows + 1), type=pa.int64())
        raw_hashes = processor.compute_input_hash(raw)

        keep = pa.array([True, False, True, True, False, True])
        final = pa.RecordBatch.from_pydict({"id": [1, 3, 4, 6], "name": ["Ann", None, "", "Zoë"]})
        out, _ = processor.process_batch(
            final,
            input_hash=raw_hashes.filter(keep),
            source_row_numbers=positions.filter(keep),
        )

        assert out.schema.names == [
            "id",
            "name",
            "row_hash",
            "_input_hash",
            "_rownum_in_source_file",
            "_rownum",
        ]
        assert out.column("_rownum_in_source_file").to_pylist() == [1, 3, 4, 6]
        assert out.column("_rownum").to_pylist() == [1, 2, 3, 4]
        assert out.column("_input_hash").to_pylist() == [
            raw_hashes[i].as_py() for i in (0, 2, 3, 5)
        ]


# ---------------------------------------------------------------------------------------------
# 4. empty batches: stable schema
# ---------------------------------------------------------------------------------------------

_SCHEMAS = {
    "typed": _typed_batch(),
    "raw": _raw_batch(),
    "nulls": pa.RecordBatch.from_pydict({"n": pa.array([None, None], pa.null())}),
    "struct": pa.RecordBatch.from_pydict(
        {
            "st": pa.array(
                [{"a": 1, "b": "x"}, None], pa.struct([("a", pa.int32()), ("b", pa.string())])
            )
        }
    ),
}


class TestEmptyBatch:
    @pytest.mark.parametrize("legacy", ENCODINGS)
    @pytest.mark.parametrize("algorithm", ALGORITHMS)
    @pytest.mark.parametrize("name", sorted(_SCHEMAS))
    def test_empty_batch_has_the_schema_of_a_non_empty_one(self, name, algorithm, legacy):
        batch = _SCHEMAS[name]
        for empty in (_empty_like(batch), batch.slice(0, 0)):
            non_empty_proc = _started(
                _everything_config(algorithm=algorithm, legacy_encoding=legacy)
            )
            empty_proc = _started(_everything_config(algorithm=algorithm, legacy_encoding=legacy))

            full, _ = non_empty_proc.process_batch(
                batch,
                input_hash=non_empty_proc.compute_input_hash(batch),
                source_row_numbers=pa.array(range(1, batch.num_rows + 1), pa.int64()),
            )
            empty_out, _ = empty_proc.process_batch(
                empty,
                input_hash=empty_proc.compute_input_hash(empty),
                source_row_numbers=pa.array([], pa.int64()),
            )

            assert empty_out.num_rows == 0
            assert empty_out.schema.equals(full.schema, check_metadata=True)
            for ours, theirs in zip(empty_out.schema, full.schema):
                assert ours.name == theirs.name
                assert ours.type == theirs.type
                assert ours.metadata == theirs.metadata

    def test_empty_batch_with_input_batch_and_internal_counters(self):
        batch = _raw_batch()
        full_proc = _started(_everything_config())
        empty_proc = _started(_everything_config())
        full, _ = full_proc.process_batch(batch, input_batch=batch)
        empty = _empty_like(batch)
        empty_out, _ = empty_proc.process_batch(empty, input_batch=empty)
        assert empty_out.schema.equals(full.schema, check_metadata=True)
        assert empty_out.num_rows == 0
        # an empty batch consumes no row numbers
        nxt, _ = empty_proc.process_batch(batch, input_batch=batch)
        assert nxt.column("_rownum").to_pylist() == [1, 2, 3, 4, 5, 6]

    def test_hash_columns_of_an_empty_batch_carry_version_and_algorithm(self):
        for legacy, version in ((False, b"2"), (True, b"1")):
            processor = _started(_everything_config(algorithm="sha512", legacy_encoding=legacy))
            empty = _empty_like(_raw_batch())
            out, _ = processor.process_batch(empty, input_batch=empty)
            for name in ("row_hash", "_input_hash"):
                field = out.schema.field(name)
                assert field.type == pa.string()
                assert field.metadata[HASH_VERSION_METADATA_KEY] == version
                assert field.metadata[HASH_ALGORITHM_METADATA_KEY] == b"sha512"
            assert out.schema.field("_rownum").type == pa.int64()
            assert out.schema.field("_rownum_in_source_file").type == pa.int64()
            assert out.schema.field("_source_uri").type == pa.string()
            assert out.schema.field("_ingested_at_utc").type == pa.string()

    def test_null_typed_empty_input_hash_is_accepted_for_an_empty_batch(self):
        processor = _started(_config(input_hash_enabled=True))
        empty = _empty_like(_raw_batch())
        out, _ = processor.process_batch(empty, input_hash=pa.array([]))
        assert out.schema.field("_input_hash").type == pa.string()

    @pytest.mark.parametrize("name", sorted(_SCHEMAS))
    def test_get_output_schema_matches_the_real_output(self, name):
        batch = _SCHEMAS[name]
        processor = _started(_everything_config())
        out, _ = processor.process_batch(batch, input_batch=batch)
        predicted = processor.get_output_schema(batch.schema)
        assert predicted.equals(out.schema, check_metadata=True)

    def test_get_output_schema_without_features_is_the_input_schema(self):
        schema = _raw_batch().schema
        assert (
            RowHashProcessor(RowHashConfig())
            .get_output_schema(schema)
            .equals(schema, check_metadata=True)
        )

    def test_schema_level_metadata_is_preserved(self):
        schema = pa.schema([("x", pa.int64())], metadata={b"owner": b"etl"})
        batch = pa.RecordBatch.from_arrays([pa.array([1])], schema=schema)
        processor = _started(_everything_config())
        out, _ = processor.process_batch(batch, input_batch=batch)
        assert out.schema.metadata[b"owner"] == b"etl"
        assert processor.get_output_schema(schema).metadata[b"owner"] == b"etl"


# ---------------------------------------------------------------------------------------------
# 5. chunked / sliced batches
# ---------------------------------------------------------------------------------------------


class TestChunkedAndSliced:
    @pytest.mark.parametrize("legacy", ENCODINGS)
    @pytest.mark.parametrize("algorithm", ALGORITHMS)
    def test_hash_of_a_slice_equals_the_hash_of_the_same_rows(self, algorithm, legacy):
        processor = RowHashProcessor(_config(algorithm=algorithm, legacy_encoding=legacy))
        batch = _typed_batch()
        whole = processor.compute_input_hash(batch).to_pylist()

        for offset, length in ((0, 6), (1, 3), (2, 4), (5, 1), (3, 0)):
            sliced = batch.slice(offset, length)
            assert (
                processor.compute_input_hash(sliced).to_pylist() == whole[offset : offset + length]
            )

    def test_slices_flow_through_process_batch(self):
        config = _everything_config()
        batch = _typed_batch()
        reference = _started(config)
        whole, _ = reference.process_batch(batch, input_batch=batch)

        processor = _started(config)
        outputs = []
        for offset in range(0, batch.num_rows, 2):
            piece = batch.slice(offset, 2)
            out, _ = processor.process_batch(piece, input_batch=piece)
            outputs.append(out)

        for name in ("row_hash", "_input_hash", "_rownum", "_rownum_in_source_file"):
            combined = [v for out in outputs for v in out.column(name).to_pylist()]
            assert combined == whole.column(name).to_pylist()

    def test_sliced_batch_with_sliced_precomputed_arrays(self):
        processor = _started(_config(input_hash_enabled=True, row_number_enabled=True))
        batch = _typed_batch()
        hashes = processor.compute_input_hash(batch)
        positions = pa.array(range(10, 16), type=pa.int64())

        out, _ = processor.process_batch(
            batch.slice(2, 3),
            input_hash=hashes.slice(2, 3),
            source_row_numbers=positions.slice(2, 3),
        )

        assert out.column("_input_hash").to_pylist() == hashes.to_pylist()[2:5]
        assert out.column("_rownum_in_source_file").to_pylist() == [12, 13, 14]
        assert out.column("_rownum").to_pylist() == [1, 2, 3]

    def test_batches_cut_from_a_chunked_table_hash_like_the_whole(self):
        batch = _typed_batch()
        table = pa.Table.from_batches([batch.slice(0, 1), batch.slice(1, 3), batch.slice(4, 2)])
        processor = RowHashProcessor(_config())
        whole = processor.compute_input_hash(batch).to_pylist()

        pieces = table.to_batches(max_chunksize=2)
        assert len(pieces) > 1
        combined = [v for piece in pieces for v in processor.compute_input_hash(piece).to_pylist()]
        assert combined == whole

        # a batch rebuilt from a multi-chunk table
        rebuilt = table.combine_chunks().to_batches()[0]
        assert processor.compute_input_hash(rebuilt).to_pylist() == whole

    def test_output_hash_of_a_slice_equals_the_input_hash_of_the_same_rows(self):
        processor = _started(_config(input_hash_enabled=True))
        piece = _typed_batch().slice(1, 4)
        out, _ = processor.process_batch(piece, input_batch=piece)
        assert out.column("row_hash").to_pylist() == out.column("_input_hash").to_pylist()

    def test_input_hash_from_a_chunked_array_of_per_batch_results(self):
        processor = _started(_config(input_hash_enabled=True))
        batch = _raw_batch()
        parts = [processor.compute_input_hash(batch.slice(i, 2)) for i in range(0, 6, 2)]
        out, _ = processor.process_batch(batch, input_hash=pa.chunked_array(parts))
        assert out.column("_input_hash").to_pylist() == (
            processor.compute_input_hash(batch).to_pylist()
        )


# ---------------------------------------------------------------------------------------------
# 6. row_hash_output_columns
# ---------------------------------------------------------------------------------------------

_ALL_ON = {
    "enabled": True,
    "inputHashEnabled": True,
    "sourceUriEnabled": True,
    "ingestedAtEnabled": True,
    "rowNumberEnabled": True,
}

_CONFIGS = [
    {"enabled": True},
    {"enabled": True, "columnName": "my_hash"},
    {"inputHashEnabled": True},
    {"sourceUriEnabled": True},
    {"ingestedAtEnabled": True},
    {"rowNumberEnabled": True},
    dict(_ALL_ON),
    {
        **_ALL_ON,
        "columnName": "h",
        "inputHashColumnName": "ih",
        "sourceUriColumnName": "su",
        "ingestedAtColumnName": "ts",
        "sourceRowNumberColumnName": "src_no",
        "processingRowNumberColumnName": "proc_no",
    },
    {"enabled": True, "rowNumberEnabled": True, "algorithm": "sha512", "hashVersion": 1},
    {"enabled": True, "algorithm": "md5", "allowWeakHash": True, "inputHashEnabled": True},
    {"enabled": True, "includeColumns": ["Id"], "excludeColumns": []},
]


class TestRowHashOutputColumns:
    def test_defaults_in_processing_order(self):
        assert row_hash_output_columns(dict(_ALL_ON)) == [
            "row_hash",
            "_input_hash",
            "_source_uri",
            "_ingested_at_utc",
            "_rownum_in_source_file",
            "_rownum",
        ]

    @pytest.mark.parametrize("config", _CONFIGS)
    def test_matches_the_columns_process_batch_adds(self, config):
        processor = create_row_hash_processor_from_schema(config)
        assert processor is not None
        processor.set_source_context("file:///x.csv")
        batch = _raw_batch()

        out, _ = processor.process_batch(batch, input_batch=batch)
        added = out.schema.names[batch.num_columns :]

        assert out.schema.names[: batch.num_columns] == batch.schema.names
        assert row_hash_output_columns(config) == added
        assert processor.output_columns() == added
        assert processor.get_output_schema(batch.schema).names == out.schema.names

    @pytest.mark.parametrize("config", _CONFIGS)
    def test_matches_for_an_empty_batch(self, config):
        processor = create_row_hash_processor_from_schema(config)
        processor.set_source_context("file:///x.csv")
        empty = _empty_like(_raw_batch())
        out, _ = processor.process_batch(empty, input_batch=empty)
        assert row_hash_output_columns(config) == out.schema.names[empty.num_columns :]

    @pytest.mark.parametrize("config", [None, {}, {"enabled": False}, {"columnName": "x"}])
    def test_no_enabled_feature_means_no_columns_and_no_processor(self, config):
        assert row_hash_output_columns(config) == []
        assert create_row_hash_processor_from_schema(config) is None

    def test_invalid_configuration_is_rejected_like_the_factory(self):
        for config in (
            {"enabled": True, "algorithm": "crc32"},
            {"enabled": True, "algorithm": "md5"},
            {"enabled": True, "hashVersion": 3},
            {"enabled": True, "hashVersion": 2, "legacyEncoding": True},
        ):
            with pytest.raises(ValueError):
                create_row_hash_processor_from_schema(config)
            with pytest.raises(ValueError):
                row_hash_output_columns(config)

    def test_lets_the_caller_detect_collisions_up_front(self):
        data_columns = ["id", "row_hash", "_rownum"]
        added = row_hash_output_columns(dict(_ALL_ON))
        assert sorted(set(data_columns) & set(added)) == ["_rownum", "row_hash"]

        # and the processor really does refuse such a batch
        processor = create_row_hash_processor_from_schema(dict(_ALL_ON))
        processor.set_source_context("f.csv")
        batch = pa.RecordBatch.from_pydict({"id": [1], "row_hash": ["x"]})
        with pytest.raises(ValueError, match="already has a column"):
            processor.process_batch(batch, input_hash=pa.array(["h"]))


# ---------------------------------------------------------------------------------------------
# 7. fail-closed behaviour, signature compatibility
# ---------------------------------------------------------------------------------------------


class TestFailClosedAndCompatibility:
    def test_source_uri_without_source_context_raises(self):
        processor = RowHashProcessor(_config(source_uri_enabled=True))
        with pytest.raises(ValueError, match="set_source_context"):
            processor.process_batch(pa.RecordBatch.from_pydict({"x": [1]}))

    def test_ingestion_timestamp_without_source_context_raises(self):
        processor = RowHashProcessor(_config(ingested_at_enabled=True))
        with pytest.raises(ValueError, match="set_source_context"):
            processor.process_batch(pa.RecordBatch.from_pydict({"x": [1]}))

    def test_nothing_left_to_hash_raises_instead_of_dropping_the_column(self):
        batch = pa.RecordBatch.from_pydict({"a": [1], "b": [2]})
        for kwargs in ({"include_columns": ["zzz"]}, {"exclude_columns": ["a", "b"]}):
            processor = RowHashProcessor(_config(**kwargs))
            with pytest.raises(ValueError, match="no column is left to hash"):
                processor.process_batch(batch)

    def test_error_messages_never_contain_cell_values(self):
        batch = pa.RecordBatch.from_pydict({"x": [SECRET, "b"]})
        processor = _started(_config(input_hash_enabled=True, row_number_enabled=True))
        calls = [
            lambda: processor.process_batch(batch),
            lambda: processor.process_batch(batch, input_batch=batch.slice(0, 1)),
            lambda: processor.process_batch(batch, input_hash=pa.array([SECRET])),
            lambda: processor.process_batch(batch, input_hash=pa.array([SECRET, None])),
            lambda: processor.process_batch(
                batch, input_hash=pa.array(["a", "b"]), source_row_numbers=pa.array([0, 1])
            ),
        ]
        for call in calls:
            with pytest.raises(ValueError) as excinfo:
                call()
            assert SECRET not in str(excinfo.value)

    def test_signature_is_backward_compatible(self):
        parameters = list(inspect.signature(RowHashProcessor.process_batch).parameters.values())
        names = [p.name for p in parameters]
        assert names == ["self", "batch", "input_batch", "input_hash", "source_row_numbers"]
        kinds = {p.name: p.kind for p in parameters}
        assert kinds["input_batch"] == inspect.Parameter.POSITIONAL_OR_KEYWORD
        assert kinds["input_hash"] == inspect.Parameter.KEYWORD_ONLY
        assert kinds["source_row_numbers"] == inspect.Parameter.KEYWORD_ONLY
        assert parameters[2].default is None

    def test_existing_calls_still_work(self):
        processor = _started(_everything_config())
        batch = _raw_batch()
        positional, _ = processor.process_batch(batch, batch)
        keyword, _ = _started(_everything_config()).process_batch(batch, input_batch=batch)
        assert positional.column("_input_hash").to_pylist() == (
            keyword.column("_input_hash").to_pylist()
        )
        single, _ = _started(_config()).process_batch(batch)
        assert single.schema.names == batch.schema.names + ["row_hash"]

    def test_pipeline_still_hands_over_the_input_batch(self):
        config = _config(input_hash_enabled=True)
        pipeline = ProcessorPipeline([RowHashProcessor(config)])
        batch = _raw_batch()
        out, _ = pipeline.process_batch(batch)
        reference = RowHashProcessor(config)
        assert out.column("_input_hash").to_pylist() == (
            reference.compute_input_hash(batch).to_pylist()
        )
