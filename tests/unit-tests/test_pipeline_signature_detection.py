"""ProcessorPipeline: processors whose process_batch signature cannot be inspected."""

from __future__ import annotations

import pyarrow as pa

from forklift.processors.base import ValidationResult
from forklift.processors.pipeline import ProcessorPipeline


class _OpaqueCallable:
    """A callable whose signature cannot be read (``inspect.signature`` raises TypeError)."""

    __signature__ = 42

    def __init__(self):
        self.calls = []

    def __call__(self, batch, **kwargs):
        self.calls.append(kwargs)
        return batch, [ValidationResult(is_valid=True, error_code="SEEN")]


class OpaqueProcessor:
    """Duck-typed processor whose process_batch is such a callable."""

    def __init__(self):
        self.process_batch = _OpaqueCallable()


class TestUninspectableProcessors:
    def test_they_get_only_the_current_batch_and_the_answer_is_cached(self):
        processor = OpaqueProcessor()
        pipeline = ProcessorPipeline([processor])
        batch = pa.RecordBatch.from_pydict({"a": [1]})

        for _ in range(2):
            out, results = pipeline.process_batch(batch)
            assert out.equals(batch) and [r.error_code for r in results] == ["SEEN"]

        assert processor.process_batch.calls == [{}, {}]  # never given input_batch
        assert pipeline._accepts_input_batch == {id(processor): False}
