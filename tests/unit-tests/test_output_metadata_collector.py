"""Unit tests for output metadata collector functionality."""

import json
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from forklift.metadata.output_metadata_collector import OutputMetadataCollector


class TestOutputMetadataCollectorInitialization:
    """Test initialization and configuration of OutputMetadataCollector."""

    def test_default_initialization(self):
        """Test collector with default parameters."""
        collector = OutputMetadataCollector()

        assert collector.enabled is True
        assert collector.enum_threshold == 0.1
        assert collector.uniqueness_threshold == 0.95
        assert collector.top_n_values == 10
        assert collector.quantiles == [0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
        assert collector.total_rows == 0
        assert collector.batch_count == 0
        assert collector.column_stats == {}
        assert collector.schema_info is None

    def test_custom_initialization(self):
        """Test collector with custom parameters."""
        custom_quantiles = [0.1, 0.5, 0.9]
        collector = OutputMetadataCollector(
            enabled=False,
            enum_threshold=0.2,
            uniqueness_threshold=0.8,
            top_n_values=5,
            quantiles=custom_quantiles,
        )

        assert collector.enabled is False
        assert collector.enum_threshold == 0.2
        assert collector.uniqueness_threshold == 0.8
        assert collector.top_n_values == 5
        assert collector.quantiles == custom_quantiles

    def test_disabled_collector_behavior(self):
        """Test that disabled collector doesn't process data."""
        collector = OutputMetadataCollector(enabled=False)

        # Create test batch
        batch = pa.record_batch(
            [pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["numbers", "letters"]
        )

        collector.add_batch(batch)

        assert collector.total_rows == 0
        assert collector.batch_count == 0
        assert collector.column_stats == {}


class TestDataTypeDetection:
    """Test data type detection methods."""

    def test_is_numeric_type(self):
        """Test numeric type detection."""
        collector = OutputMetadataCollector()

        assert collector._is_numeric_type(pa.int64()) is True
        assert collector._is_numeric_type(pa.int32()) is True
        assert collector._is_numeric_type(pa.float64()) is True
        assert collector._is_numeric_type(pa.float32()) is True
        assert collector._is_numeric_type(pa.string()) is False
        assert collector._is_numeric_type(pa.bool_()) is False
        assert collector._is_numeric_type(pa.date32()) is False

    def test_is_string_type(self):
        """Test string type detection."""
        collector = OutputMetadataCollector()

        assert collector._is_string_type(pa.string()) is True
        assert collector._is_string_type(pa.large_string()) is True
        assert collector._is_string_type(pa.int64()) is False
        assert collector._is_string_type(pa.float64()) is False
        assert collector._is_string_type(pa.bool_()) is False

    def test_is_temporal_type(self):
        """Test temporal type detection."""
        collector = OutputMetadataCollector()

        assert collector._is_temporal_type(pa.date32()) is True
        assert collector._is_temporal_type(pa.date64()) is True
        assert collector._is_temporal_type(pa.timestamp("s")) is True
        assert collector._is_temporal_type(pa.time32("s")) is True
        assert collector._is_temporal_type(pa.string()) is False
        assert collector._is_temporal_type(pa.int64()) is False


class TestBatchProcessing:
    """Test batch processing functionality."""

    def test_add_single_batch(self):
        """Test adding a single batch of data."""
        collector = OutputMetadataCollector()

        batch = pa.record_batch(
            [
                pa.array([1, 2, 3, None]),
                pa.array(["a", "b", "c", "d"]),
                pa.array([1.1, 2.2, None, 4.4]),
            ],
            names=["integers", "strings", "floats"],
        )

        collector.add_batch(batch)

        assert collector.total_rows == 4
        assert collector.batch_count == 1
        assert len(collector.column_stats) == 3
        assert collector.schema_info == batch.schema

    def test_add_multiple_batches(self):
        """Test adding multiple batches of data."""
        collector = OutputMetadataCollector()

        # First batch
        batch1 = pa.record_batch(
            [pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["numbers", "letters"]
        )

        # Second batch
        batch2 = pa.record_batch(
            [pa.array([4, 5, None]), pa.array(["d", "e", "f"])], names=["numbers", "letters"]
        )

        collector.add_batch(batch1)
        collector.add_batch(batch2)

        assert collector.total_rows == 6
        assert collector.batch_count == 2
        assert len(collector.column_stats) == 2

    def test_column_stats_initialization(self):
        """Test that column statistics are properly initialized."""
        collector = OutputMetadataCollector()

        batch = pa.record_batch(
            [pa.array([1, 2, 3]), pa.array(["a", "b", "c"]), pa.array([True, False, True])],
            names=["int_col", "str_col", "bool_col"],
        )

        collector.add_batch(batch)

        # Check integer column stats
        int_stats = collector.column_stats["int_col"]
        assert int_stats["data_type"] == "int64"
        assert int_stats["is_numeric"] is True
        assert int_stats["is_string"] is False
        assert int_stats["is_temporal"] is False

        # Check string column stats
        str_stats = collector.column_stats["str_col"]
        assert str_stats["data_type"] == "string"
        assert str_stats["is_numeric"] is False
        assert str_stats["is_string"] is True
        assert str_stats["is_temporal"] is False


class TestColumnStatistics:
    """Test column-level statistics collection."""

    def test_null_count_tracking(self):
        """Test null value counting."""
        collector = OutputMetadataCollector()

        batch = pa.record_batch(
            [pa.array([1, None, 3, None, 5]), pa.array(["a", "b", None, "d", None])],
            names=["numbers", "letters"],
        )

        collector.add_batch(batch)

        num_stats = collector.column_stats["numbers"]
        assert num_stats["null_count"] == 2
        assert num_stats["non_null_count"] == 3

        str_stats = collector.column_stats["letters"]
        assert str_stats["null_count"] == 2
        assert str_stats["non_null_count"] == 3

    def test_numeric_min_max_tracking(self):
        """Test min/max value tracking for numeric columns."""
        collector = OutputMetadataCollector()

        batch1 = pa.record_batch(
            [pa.array([5, 10, 15]), pa.array([1.5, 2.7, 3.9])], names=["integers", "floats"]
        )

        batch2 = pa.record_batch(
            [pa.array([1, 20, 8]), pa.array([0.5, 4.2, 2.1])], names=["integers", "floats"]
        )

        collector.add_batch(batch1)
        collector.add_batch(batch2)

        int_stats = collector.column_stats["integers"]
        assert int_stats["min_value"] == 1
        assert int_stats["max_value"] == 20

        float_stats = collector.column_stats["floats"]
        assert float_stats["min_value"] == 0.5
        assert float_stats["max_value"] == 4.2

    def test_string_length_tracking(self):
        """Test string length tracking for string columns."""
        collector = OutputMetadataCollector()

        batch = pa.record_batch([pa.array(["a", "hello", "world", "x"])], names=["strings"])

        collector.add_batch(batch)

        str_stats = collector.column_stats["strings"]
        assert str_stats["min_value"] == 1  # length of 'a' and 'x'
        assert str_stats["max_value"] == 5  # length of 'hello' and 'world'

    def test_unique_values_tracking(self):
        """Test unique value tracking."""
        collector = OutputMetadataCollector()

        batch = pa.record_batch(
            [pa.array(["a", "b", "a", "c", "b"]), pa.array([1, 2, 1, 3, 2])],
            names=["categories", "numbers"],
        )

        collector.add_batch(batch)

        cat_stats = collector.column_stats["categories"]
        assert len(cat_stats["unique_values"]) == 3
        assert cat_stats["unique_values"] == {"a", "b", "c"}

        num_stats = collector.column_stats["numbers"]
        assert len(num_stats["unique_values"]) == 3
        assert num_stats["unique_values"] == {1, 2, 3}

    def test_all_null_column_handling(self):
        """Test handling of columns with all null values."""
        collector = OutputMetadataCollector()

        batch = pa.record_batch(
            [
                pa.array([None, None, None], type=pa.int64()),
                pa.array([None, None, None], type=pa.string()),
            ],
            names=["null_ints", "null_strings"],
        )

        collector.add_batch(batch)

        null_int_stats = collector.column_stats["null_ints"]
        assert null_int_stats["null_count"] == 3
        assert null_int_stats["non_null_count"] == 0
        assert null_int_stats["min_value"] is None
        assert null_int_stats["max_value"] is None

        null_str_stats = collector.column_stats["null_strings"]
        assert null_str_stats["null_count"] == 3
        assert null_str_stats["non_null_count"] == 0


class TestNumericStatistics:
    """Test numeric statistics calculation."""

    def test_calculate_numeric_statistics_basic(self):
        """Test basic numeric statistics calculation."""
        collector = OutputMetadataCollector()
        values = [1, 2, 3, 4, 5]

        stats = collector._calculate_numeric_statistics(values)

        assert stats["mean"] == 3.0
        assert stats["median"] == 3
        assert stats["standard_deviation"] > 0
        assert "quantiles" in stats

    def test_calculate_numeric_statistics_single_value(self):
        """Test numeric statistics with single value."""
        collector = OutputMetadataCollector()
        values = [42]

        stats = collector._calculate_numeric_statistics(values)

        assert stats["mean"] == 42
        assert stats["median"] == 42
        assert stats["mode"] == 42
        assert stats["standard_deviation"] == 0
        assert stats["variance"] == 0

    def test_calculate_numeric_statistics_empty(self):
        """Test numeric statistics with empty values."""
        collector = OutputMetadataCollector()
        values = []

        stats = collector._calculate_numeric_statistics(values)

        assert stats == {}

    def test_calculate_numeric_statistics_quantiles(self):
        """Test quantile calculation."""
        collector = OutputMetadataCollector(quantiles=[0.25, 0.5, 0.75])
        values = list(range(1, 101))  # 1 to 100

        stats = collector._calculate_numeric_statistics(values)

        assert "quantiles" in stats
        assert "p25" in stats["quantiles"]
        assert "p50" in stats["quantiles"]
        assert "p75" in stats["quantiles"]


class TestMetadataGeneration:
    """Test metadata generation functionality."""

    def test_generate_metadata_basic(self):
        """Test basic metadata generation."""
        collector = OutputMetadataCollector()

        batch = pa.record_batch(
            [pa.array([1, 2, 3, None]), pa.array(["a", "b", "c", "d"])],
            names=["numbers", "letters"],
        )

        collector.add_batch(batch)

        schema = batch.schema
        source_info = {"input_path": "/test/input.csv"}

        metadata = collector.generate_metadata(schema, source_info)

        assert "generation_timestamp" in metadata
        assert metadata["source_info"] == source_info
        assert metadata["data_summary"]["total_rows"] == 4
        assert metadata["data_summary"]["total_columns"] == 2
        assert metadata["data_summary"]["batches_processed"] == 1
        assert "column_statistics" in metadata
        assert "data_quality" in metadata

    def test_generate_metadata_disabled_collector(self):
        """Test metadata generation with disabled collector."""
        collector = OutputMetadataCollector(enabled=False)

        metadata = collector.generate_metadata(None, {})

        assert metadata == {}

    def test_column_statistics_generation(self):
        """Test detailed column statistics generation."""
        collector = OutputMetadataCollector(top_n_values=3)

        batch = pa.record_batch(
            [pa.array(["a", "b", "a", "c", "a", "b"]), pa.array([1, 2, 3, None, 5, 6])],
            names=["categories", "numbers"],
        )

        collector.add_batch(batch)

        metadata = collector.generate_metadata(None, {})
        col_stats = metadata["column_statistics"]

        # Check category column
        cat_stats = col_stats["categories"]
        assert cat_stats["data_type"] == "string"
        assert cat_stats["null_count"] == 0
        assert cat_stats["non_null_count"] == 6
        assert cat_stats["unique_values_count"] == 3
        assert "top_values" in cat_stats

        # Check top values
        top_values = cat_stats["top_values"]
        assert len(top_values) <= 3
        assert top_values[0]["value"] == "a"  # Most frequent
        assert top_values[0]["count"] == 3

        # Check number column
        num_stats = col_stats["numbers"]
        assert num_stats["null_count"] == 1
        assert num_stats["non_null_count"] == 5

    def test_data_quality_metrics_generation(self):
        """Test data quality metrics generation."""
        collector = OutputMetadataCollector(
            enum_threshold=0.5,  # 50% threshold for categorical detection
            uniqueness_threshold=0.9,  # 90% threshold for too unique detection
        )

        batch = pa.record_batch(
            [
                pa.array(
                    [None, None, None, None, 1, 2, 3, 4, 5, 6]
                ),  # 40% nulls, high uniqueness (6/6 = 1.0)
                pa.array(
                    ["A", "A", "A", "B", "B", "B", "C", "C", "C", "C"]
                ),  # Low uniqueness (3/10 = 0.3) - clearly categorical
                pa.array(
                    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
                ),  # Perfect uniqueness (10/10 = 1.0) - clearly too unique
            ],
            names=["high_nulls", "categorical", "unique_values"],
        )

        collector.add_batch(batch)

        metadata = collector.generate_metadata(None, {})
        quality = metadata["data_quality"]

        assert quality["columns_with_nulls"] == 1
        assert len(quality["high_null_columns"]) == 1
        assert quality["high_null_columns"][0]["column"] == "high_nulls"
        assert len(quality["likely_categorical_columns"]) == 1
        assert quality["likely_categorical_columns"][0]["column"] == "categorical"
        assert (
            len(quality["too_unique_columns"]) == 2
        )  # Both high_nulls (6/6=1.0) and unique_values (10/10=1.0)

        # Verify the specific columns flagged as too unique
        too_unique_cols = [col["column"] for col in quality["too_unique_columns"]]
        assert "high_nulls" in too_unique_cols
        assert "unique_values" in too_unique_cols


class TestMetadataSaving:
    """Test metadata saving functionality."""

    def test_save_metadata_success(self):
        """Test successful metadata saving."""
        collector = OutputMetadataCollector()

        batch = pa.record_batch(
            [pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["numbers", "letters"]
        )

        collector.add_batch(batch)

        with tempfile.TemporaryDirectory() as temp_dir:
            result_path = collector.save_metadata(temp_dir, "test_metadata.json")

            assert result_path is not None
            assert Path(result_path).exists()

            # Verify file contents
            with open(result_path, "r") as f:
                saved_metadata = json.load(f)

            assert saved_metadata["data_summary"]["total_rows"] == 3
            assert "column_statistics" in saved_metadata

    def test_save_metadata_disabled_collector(self):
        """Test metadata saving with disabled collector."""
        collector = OutputMetadataCollector(enabled=False)

        with tempfile.TemporaryDirectory() as temp_dir:
            result_path = collector.save_metadata(temp_dir)

            assert result_path is None

    def test_save_metadata_no_data(self):
        """Test metadata saving with no collected data."""
        collector = OutputMetadataCollector()

        with tempfile.TemporaryDirectory() as temp_dir:
            result_path = collector.save_metadata(temp_dir)

            assert result_path is None

    def test_save_metadata_create_directory(self):
        """Test that save_metadata creates output directory if it doesn't exist."""
        collector = OutputMetadataCollector()

        batch = pa.record_batch([pa.array([1, 2, 3])], names=["numbers"])

        collector.add_batch(batch)

        with tempfile.TemporaryDirectory() as temp_dir:
            nested_dir = Path(temp_dir) / "nested" / "directory"
            result_path = collector.save_metadata(nested_dir)

            assert result_path is not None
            assert Path(result_path).exists()
            assert nested_dir.exists()

    @patch("builtins.open", side_effect=IOError("Disk full"))
    def test_save_metadata_io_error(self, mock_open):
        """Test metadata saving handles IO errors gracefully."""
        collector = OutputMetadataCollector()

        batch = pa.record_batch([pa.array([1, 2, 3])], names=["numbers"])

        collector.add_batch(batch)

        with tempfile.TemporaryDirectory() as temp_dir:
            result_path = collector.save_metadata(temp_dir)

            assert result_path is None


class TestCollectorReset:
    """Test collector reset functionality."""

    def test_reset_collector(self):
        """Test resetting collector to initial state."""
        collector = OutputMetadataCollector()

        # Add some data
        batch = pa.record_batch(
            [pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["numbers", "letters"]
        )

        collector.add_batch(batch)

        # Verify data was collected
        assert collector.total_rows == 3
        assert collector.batch_count == 1
        assert len(collector.column_stats) == 2
        assert collector.schema_info is not None

        # Reset collector
        collector.reset()

        # Verify reset state
        assert collector.total_rows == 0
        assert collector.batch_count == 0
        assert len(collector.column_stats) == 0
        assert collector.schema_info is None
        assert len(collector._value_counters) == 0
        assert len(collector._numeric_values) == 0


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_batch(self):
        """Test handling of empty batches."""
        collector = OutputMetadataCollector()

        # Create empty batch
        batch = pa.record_batch(
            [pa.array([], type=pa.int64()), pa.array([], type=pa.string())],
            names=["numbers", "letters"],
        )

        collector.add_batch(batch)

        assert collector.total_rows == 0
        assert collector.batch_count == 1
        assert len(collector.column_stats) == 2

    def test_large_unique_values_limit(self):
        """Test that unique values tracking is limited to prevent memory issues."""
        collector = OutputMetadataCollector()

        # Create batch with many unique values
        large_values = list(range(15000))  # More than the 10000 limit
        batch = pa.record_batch([pa.array(large_values)], names=["many_values"])

        collector.add_batch(batch)

        stats = collector.column_stats["many_values"]
        # Should be limited to prevent memory issues
        assert len(stats["unique_values"]) <= 10000

    def test_numeric_values_sampling(self):
        """Test that numeric values are sampled to prevent memory issues."""
        collector = OutputMetadataCollector()

        # Create large batch
        large_values = list(range(2000))
        batch = pa.record_batch(
            [pa.array(large_values, type=pa.float64())], names=["many_numbers"]
        )

        collector.add_batch(batch)

        # Should be limited per batch
        assert len(collector._numeric_values["many_numbers"]) <= 1000

    def test_schema_consistency_across_batches(self):
        """Test that schema is preserved across batches."""
        collector = OutputMetadataCollector()

        # First batch
        batch1 = pa.record_batch(
            [pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["numbers", "letters"]
        )

        # Second batch with same schema
        batch2 = pa.record_batch(
            [pa.array([4, 5, 6]), pa.array(["d", "e", "f"])], names=["numbers", "letters"]
        )

        collector.add_batch(batch1)
        original_schema = collector.schema_info

        collector.add_batch(batch2)

        # Schema should remain the same
        assert collector.schema_info == original_schema
        assert collector.total_rows == 6

    def test_error_handling_in_statistics(self):
        """Test graceful error handling in statistics calculation."""
        collector = OutputMetadataCollector()

        # This should not raise an exception even with problematic data
        with patch("pyarrow.compute.min", side_effect=Exception("Compute error")):
            batch = pa.record_batch([pa.array([1, 2, 3])], names=["numbers"])

            # Should not raise exception
            collector.add_batch(batch)

            # Basic stats should still be collected
            assert collector.total_rows == 3
            assert collector.column_stats["numbers"]["null_count"] == 0


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_realistic_data_processing_scenario(self):
        """Test a realistic data processing scenario with mixed data types."""
        collector = OutputMetadataCollector(
            enum_threshold=0.3,  # Changed from 0.1 to 0.3 so that 0.2 ratio is categorical
            uniqueness_threshold=0.9,
            top_n_values=5,
        )

        # Simulate processing customer data
        batch1 = pa.record_batch(
            [
                pa.array([1, 2, 3, 4, 5]),  # customer_id
                pa.array(["John", "Jane", "Bob", "Alice", "Charlie"]),  # name
                pa.array(
                    ["Premium", "Basic", "Premium", "Basic", "Premium"]
                ),  # plan_type - ratio 0.2
                pa.array([29.99, 9.99, 29.99, 9.99, 29.99]),  # monthly_fee
                pa.array([True, False, True, False, True]),  # is_active
                pa.array([None, "email", "phone", "email", None]),  # contact_method
            ],
            names=[
                "customer_id",
                "name",
                "plan_type",
                "monthly_fee",
                "is_active",
                "contact_method",
            ],
        )

        batch2 = pa.record_batch(
            [
                pa.array([6, 7, 8, 9, 10]),
                pa.array(["David", "Eve", "Frank", "Grace", "Henry"]),
                pa.array(["Basic", "Premium", "Basic", "Premium", "Basic"]),
                pa.array([9.99, 29.99, 9.99, 29.99, 9.99]),
                pa.array([False, True, True, False, True]),
                pa.array(["phone", "email", None, "phone", "email"]),
            ],
            names=[
                "customer_id",
                "name",
                "plan_type",
                "monthly_fee",
                "is_active",
                "contact_method",
            ],
        )

        collector.add_batch(batch1)
        collector.add_batch(batch2)

        # Generate metadata
        metadata = collector.generate_metadata(
            batch1.schema, {"source": "customer_database", "processing_date": "2024-01-01"}
        )

        # Verify comprehensive metadata
        assert metadata["data_summary"]["total_rows"] == 10
        assert metadata["data_summary"]["total_columns"] == 6
        assert metadata["data_summary"]["batches_processed"] == 2

        # Check column-specific insights
        col_stats = metadata["column_statistics"]

        # customer_id should be too unique
        assert col_stats["customer_id"]["uniqueness_ratio"] == 1.0

        # plan_type should be categorical
        plan_stats = col_stats["plan_type"]
        assert plan_stats["likely_categorical"] is True
        assert len(plan_stats["top_values"]) == 2  # Premium and Basic

        # contact_method has nulls
        contact_stats = col_stats["contact_method"]
        assert contact_stats["null_count"] == 3
        assert contact_stats["null_percentage"] == 30.0

        # Data quality metrics
        quality = metadata["data_quality"]
        assert quality["columns_with_nulls"] == 1  # Only contact_method has nulls
        assert quality["overall_null_percentage"] == 5.0  # 3 nulls out of 60 total values

    def test_time_series_data_scenario(self):
        """Test processing time series data with temporal columns."""
        collector = OutputMetadataCollector(
            enum_threshold=0.5  # Changed from default 0.1 to 0.5 so that device_id with ratio 0.2 is categorical
        )

        # Simulate time series data
        timestamps = pa.array(
            [
                datetime(2024, 1, 1, 12, 0),
                datetime(2024, 1, 1, 12, 5),
                datetime(2024, 1, 1, 12, 10),
                datetime(2024, 1, 1, 12, 15),
                datetime(2024, 1, 1, 12, 20),
            ],
            type=pa.timestamp("s"),
        )

        batch = pa.record_batch(
            [
                timestamps,
                pa.array([100.5, 101.2, 99.8, 102.1, 100.9]),  # sensor_value
                pa.array(
                    ["sensor_1", "sensor_1", "sensor_1", "sensor_1", "sensor_1"]
                ),  # device_id - ratio 0.2 (1 unique / 5 total)
                pa.array([25.6, 25.7, 25.5, 25.8, 25.6]),  # temperature
            ],
            names=["timestamp", "sensor_value", "device_id", "temperature"],
        )

        collector.add_batch(batch)

        metadata = collector.generate_metadata(
            batch.schema, {"data_type": "time_series", "sensor_type": "environmental"}
        )

        col_stats = metadata["column_statistics"]

        # Timestamp column should be detected as temporal
        timestamp_stats = col_stats["timestamp"]
        assert timestamp_stats["data_type"].startswith("timestamp")
        assert timestamp_stats["min_value"] is not None
        assert timestamp_stats["max_value"] is not None

        # Device ID should be categorical
        device_stats = col_stats["device_id"]
        assert device_stats["likely_categorical"] is True
        assert device_stats["unique_values_count"] == 1

        # Sensor values should have numeric statistics
        sensor_stats = col_stats["sensor_value"]
        if "numeric_statistics" in sensor_stats:
            assert "mean" in sensor_stats["numeric_statistics"]
            assert "median" in sensor_stats["numeric_statistics"]

    def test_end_to_end_workflow(self):
        """Test complete end-to-end workflow including save and reload."""
        collector = OutputMetadataCollector()

        # Process multiple batches
        for i in range(3):
            batch = pa.record_batch(
                [
                    pa.array(list(range(i * 10, (i + 1) * 10))),  # sequential IDs
                    pa.array([f"item_{j}" for j in range(i * 10, (i + 1) * 10)]),  # item names
                    pa.array([j % 3 for j in range(i * 10, (i + 1) * 10)]),  # categories (0, 1, 2)
                ],
                names=["id", "item_name", "category"],
            )

            collector.add_batch(batch)

        # Save metadata
        with tempfile.TemporaryDirectory() as temp_dir:
            metadata_path = collector.save_metadata(temp_dir, "final_metadata.json")

            assert metadata_path is not None

            # Load and verify saved metadata
            with open(metadata_path, "r") as f:
                saved_metadata = json.load(f)

            assert saved_metadata["data_summary"]["total_rows"] == 30
            assert saved_metadata["data_summary"]["total_columns"] == 3
            assert saved_metadata["data_summary"]["batches_processed"] == 3

            # Verify column statistics were preserved
            col_stats = saved_metadata["column_statistics"]
            assert "id" in col_stats
            assert "item_name" in col_stats
            assert "category" in col_stats

            # Category should be detected as categorical
            category_stats = col_stats["category"]
            assert category_stats["likely_categorical"] is True
            assert category_stats["unique_values_count"] == 3


if __name__ == "__main__":
    pytest.main([__file__])
