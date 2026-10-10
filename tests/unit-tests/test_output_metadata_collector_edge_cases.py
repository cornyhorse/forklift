"""``OutputMetadataCollector`` with unusual data: values Python cannot represent, columns
whose values cannot be tracked, sampled quantiles and JSON-unsafe provenance."""

import json
import logging
import statistics

import pyarrow as pa
import pytest

from forklift.metadata.output_metadata_collector import OutputMetadataCollector


class _Celsius(pa.ExtensionType):
    """Minimal extension type over float64 storage (Arrow has no hash kernels for it)."""

    def __init__(self):
        super().__init__(pa.float64(), "forklift.test.celsius")

    def __arrow_ext_serialize__(self):
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return cls()


def _statistics(collector, column):
    return collector.generate_metadata(None, {})["column_statistics"][column]


class TestValuesBeyondPythonRange:
    """Arrow timestamps reach far beyond ``datetime.max``; such columns must not stop a run."""

    @pytest.mark.parametrize(
        "column",
        [
            pa.array([10**15, 1], pa.timestamp("s")),
            pa.array([2**31 - 1, 1], pa.date32()),
        ],
        ids=["timestamp", "date"],
    )
    def test_out_of_range_temporal_values_skip_value_statistics(self, column, caplog):
        collector = OutputMetadataCollector(include_value_statistics=True)

        with caplog.at_level(logging.DEBUG, logger="forklift.metadata.output_metadata_collector"):
            collector.add_batch(pa.record_batch({"when": column}))

        stats = _statistics(collector, "when")
        assert stats["non_null_count"] == 2
        assert "min_value" not in stats
        assert "max_value" not in stats
        assert stats["unique_values_count"] is None
        assert stats["distinct_count_is_lower_bound"] is True
        messages = [record.getMessage() for record in caplog.records]
        assert any(m.startswith("Skipping value statistics for column 'when'") for m in messages)
        assert any(
            m.startswith("Distinct tracking unavailable for column 'when'") for m in messages
        )


class TestUntrackableDistinctValues:
    def test_extension_column_has_no_distinct_count(self):
        collector = OutputMetadataCollector(include_value_statistics=True)
        column = pa.ExtensionArray.from_storage(_Celsius(), pa.array([1.5, 2.5, 1.5]))

        collector.add_batch(pa.record_batch({"temperature": column}))

        stats = _statistics(collector, "temperature")
        assert stats["unique_values_count"] is None
        assert stats["uniqueness_ratio"] is None
        assert stats["likely_categorical"] is None
        assert "top_values" not in stats

    def test_unhashable_dictionary_values_disable_distinct_tracking(self):
        collector = OutputMetadataCollector(include_value_statistics=True)
        column = pa.DictionaryArray.from_arrays(pa.array([0, 1, 0]), pa.array([[1], [2]]))

        collector.add_batch(pa.record_batch({"tags": column}))
        collector.add_batch(pa.record_batch({"tags": column}))

        stats = _statistics(collector, "tags")
        assert stats["non_null_count"] == 6
        assert stats["unique_values_count"] is None
        assert stats["distinct_count_is_lower_bound"] is True
        assert "top_values" not in stats


class TestAllNullColumnWithValueStatistics:
    def test_no_top_values_are_reported(self):
        collector = OutputMetadataCollector(include_value_statistics=True)

        collector.add_batch(pa.record_batch({"note": pa.array([None, None], pa.string())}))

        stats = _statistics(collector, "note")
        assert stats["non_null_count"] == 0
        assert "top_values" not in stats
        assert "top_values_unavailable" not in stats


class TestSampledQuantiles:
    def test_reservoir_keeps_its_size_across_many_small_batches(self):
        collector = OutputMetadataCollector(include_value_statistics=True, sample_size=3)

        for value in range(200):
            collector.add_batch(pa.record_batch({"n": pa.array([value])}))

        numeric = _statistics(collector, "n")["numeric_statistics"]
        assert numeric["sample_size"] == 3
        assert numeric["quantiles_are_estimated"] is True
        assert "mode" not in numeric
        assert all(0 <= value < 200 for value in numeric["quantiles"].values())
        # Mean and variance stay exact although the quantiles are sampled
        assert numeric["mean"] == 99.5
        assert numeric["variance"] == round(statistics.variance(range(200)), 4)

    def test_failing_sample_statistics_omit_numeric_statistics(self, monkeypatch):
        def failing_median(values):
            raise statistics.StatisticsError("no median")

        monkeypatch.setattr(statistics, "median", failing_median)
        collector = OutputMetadataCollector(include_value_statistics=True)

        collector.add_batch(pa.record_batch({"n": pa.array([1, 2, 3])}))

        stats = _statistics(collector, "n")
        assert "numeric_statistics" not in stats
        assert stats["min_value"] == 1
        assert stats["max_value"] == 3


class TestMetadataWithoutData:
    def test_data_quality_is_empty_before_any_batch(self):
        metadata = OutputMetadataCollector().generate_metadata(None, {"source": "none"})

        assert metadata["data_quality"] == {}
        assert metadata["column_statistics"] == {}
        assert metadata["data_summary"]["schema"] is None


class TestJsonUnsafeProvenance:
    def test_sets_and_non_finite_numbers_are_written_as_valid_json(self, tmp_path):
        collector = OutputMetadataCollector()
        collector.add_batch(pa.record_batch({"id": pa.array([1, 2])}))

        path = collector.save_metadata(
            tmp_path,
            source_info={
                "tags": {"daily"},
                "regions": frozenset({"eu"}),
                "ratio": float("nan"),
                "limit": float("inf"),
            },
        )

        written = json.loads((tmp_path / "output_metadata.json").read_text(encoding="utf-8"))
        assert path == str(tmp_path / "output_metadata.json")
        assert written["source_info"] == {
            "tags": ["daily"],
            "regions": ["eu"],
            "ratio": None,
            "limit": None,
        }
