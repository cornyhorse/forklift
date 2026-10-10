"""DataQualityProcessor: pattern rules apply to text columns only."""

from __future__ import annotations

import pyarrow as pa

from forklift.processors.quality import DataQualityProcessor


class TestPatternOnNonTextColumns:
    def test_a_pattern_rule_on_a_numeric_column_reports_nothing(self):
        quality = DataQualityProcessor({"column_rules": {"code": {"pattern": "^[A-Z]+$"}}})
        batch = pa.RecordBatch.from_pydict({"code": [1, 22, 333]})

        out, results = quality.process_batch(batch)

        assert out is batch and results == []

    def test_the_same_rule_on_text_reports_the_mismatches(self):
        quality = DataQualityProcessor({"column_rules": {"code": {"pattern": "^[A-Z]+$"}}})

        _, results = quality.process_batch(pa.RecordBatch.from_pydict({"code": ["AB", "1"]}))

        assert [(r.row_index, r.column_name) for r in results] == [(1, "code")]
