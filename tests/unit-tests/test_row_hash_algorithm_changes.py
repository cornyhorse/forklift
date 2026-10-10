"""RowHashProcessor: the algorithm is checked again when hashing, not only in the config."""

from __future__ import annotations

import pyarrow as pa
import pytest

from forklift.processors.row_hash import RowHashConfig, RowHashProcessor


class TestAlgorithmChangedAfterConfiguration:
    def test_an_unsupported_algorithm_is_refused_when_hashing(self):
        processor = RowHashProcessor(RowHashConfig(enabled=True))
        processor.config.algorithm = "blake2b"  # supported by hashlib, not by forklift

        with pytest.raises(ValueError, match="^Unsupported hash algorithm: blake2b$"):
            processor.process_batch(pa.RecordBatch.from_pydict({"a": [1]}))

    def test_a_supported_algorithm_set_later_is_used(self):
        processor = RowHashProcessor(RowHashConfig(enabled=True))
        processor.config.algorithm = "sha512"

        batch, _ = processor.process_batch(pa.RecordBatch.from_pydict({"a": [1]}))

        assert len(batch.column("row_hash")[0].as_py()) == 128
