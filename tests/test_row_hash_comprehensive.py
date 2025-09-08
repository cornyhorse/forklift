"""Comprehensive tests for row hash processor module."""

import pytest
import pyarrow as pa
from datetime import datetime
from unittest.mock import patch, MagicMock

from forklift.processors.row_hash import (
    RowHashProcessor,
    RowHashConfig
)
from forklift.processors.base import ValidationResult


class TestRowHashConfig:
    """Test RowHashConfig dataclass."""

    def test_default_config(self):
        """Test default configuration values."""
        config = RowHashConfig()

        assert config.enabled is False
        assert config.column_name == "row_hash"
        assert config.algorithm == "sha256"
        assert config.include_columns is None
        assert config.exclude_columns == []
        assert config.null_value == "NULL"
        assert config.separator == "||"
        assert config.input_hash_enabled is False
        assert config.input_hash_column_name == "_input_hash"
        assert config.source_uri_enabled is False
        assert config.source_uri_column_name == "_source_uri"
        assert config.ingested_at_enabled is False
        assert config.ingested_at_column_name == "_ingested_at_utc"
        assert config.row_number_enabled is False
        assert config.source_row_number_column_name == "_rownum_in_source_file"
        assert config.processing_row_number_column_name == "_rownum"

    def test_custom_config(self):
        """Test custom configuration values."""
        config = RowHashConfig(
            enabled=True,
            column_name="custom_hash",
            algorithm="md5",
            include_columns=["id", "name"],
            exclude_columns=["secret"],
            null_value="<NULL>",
            separator="|",
            input_hash_enabled=True,
            source_uri_enabled=True,
            ingested_at_enabled=True,
            row_number_enabled=True
        )

        assert config.enabled is True
        assert config.column_name == "custom_hash"
        assert config.algorithm == "md5"
        assert config.include_columns == ["id", "name"]
        assert config.exclude_columns == ["secret"]
        assert config.null_value == "<NULL>"
        assert config.separator == "|"
        assert config.input_hash_enabled is True
        assert config.source_uri_enabled is True
        assert config.ingested_at_enabled is True
        assert config.row_number_enabled is True

    def test_post_init_valid_algorithm(self):
        """Test post_init with valid hash algorithm."""
        config = RowHashConfig(algorithm="sha512")
        # Should not raise exception
        assert config.algorithm == "sha512"

    def test_post_init_invalid_algorithm(self):
        """Test post_init with invalid hash algorithm."""
        with pytest.raises(ValueError, match="Unsupported hash algorithm"):
            RowHashConfig(algorithm="invalid_algo")

    def test_post_init_none_exclude_columns(self):
        """Test post_init converts None exclude_columns to empty list."""
        config = RowHashConfig(exclude_columns=None)
        assert config.exclude_columns == []

    def test_supported_algorithms(self):
        """Test all supported algorithms work."""
        algorithms = ["md5", "sha1", "sha256", "sha384", "sha512"]
        for algo in algorithms:
            config = RowHashConfig(algorithm=algo)
            assert config.algorithm == algo


class TestRowHashProcessor:
    """Test RowHashProcessor class."""

    def test_init(self):
        """Test processor initialization."""
        config = RowHashConfig(enabled=True)
        processor = RowHashProcessor(config)

        assert processor.config == config
        assert processor.row_counter == 0
        assert processor.source_uri is None
        assert processor.source_row_offset == 0

    def test_set_source_context(self):
        """Test setting source context."""
        config = RowHashConfig()
        processor = RowHashProcessor(config)

        processor.set_source_context("s3://bucket/file.csv", 100)

        assert processor.source_uri == "s3://bucket/file.csv"
        assert processor.source_row_offset == 100

    def test_process_batch_disabled(self):
        """Test processing batch with hash disabled."""
        config = RowHashConfig(enabled=False)
        processor = RowHashProcessor(config)

        batch = pa.record_batch({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })

        result_batch, validation_results = processor.process_batch(batch)

        assert result_batch == batch
        assert validation_results == []

    def test_process_batch_enabled_basic(self):
        """Test processing batch with hash enabled."""
        config = RowHashConfig(enabled=True)
        processor = RowHashProcessor(config)

        batch = pa.record_batch({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })

        result_batch, validation_results = processor.process_batch(batch)

        assert len(result_batch.columns) == len(batch.columns) + 1
        assert 'row_hash' in result_batch.schema.names
        assert validation_results == []

    def test_process_batch_with_input_hash(self):
        """Test processing batch with input hash enabled."""
        config = RowHashConfig(enabled=True, input_hash_enabled=True)
        processor = RowHashProcessor(config)

        batch = pa.record_batch({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })

        input_batch = pa.record_batch({
            'id': [1, 2, 3],
            'raw_name': ['alice', 'bob', 'charlie']
        })

        result_batch, validation_results = processor.process_batch(batch, input_batch)

        assert '_input_hash' in result_batch.schema.names
        assert 'row_hash' in result_batch.schema.names

    def test_process_batch_with_source_uri(self):
        """Test processing batch with source URI enabled."""
        config = RowHashConfig(enabled=True, source_uri_enabled=True)
        processor = RowHashProcessor(config)
        processor.set_source_context("test.csv")

        batch = pa.record_batch({
            'id': [1, 2],
            'name': ['Alice', 'Bob']
        })

        result_batch, validation_results = processor.process_batch(batch)

        assert '_source_uri' in result_batch.schema.names
        source_uri_values = result_batch.column('_source_uri').to_pylist()
        assert all(uri == "test.csv" for uri in source_uri_values)

    @patch('forklift.processors.row_hash.datetime')
    def test_process_batch_with_ingested_at(self, mock_datetime):
        """Test processing batch with ingestion timestamp enabled."""
        mock_now = datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.utcnow.return_value = mock_now

        config = RowHashConfig(enabled=True, ingested_at_enabled=True)
        processor = RowHashProcessor(config)

        batch = pa.record_batch({
            'id': [1, 2],
            'name': ['Alice', 'Bob']
        })

        result_batch, validation_results = processor.process_batch(batch)

        assert '_ingested_at_utc' in result_batch.schema.names
        timestamp_values = result_batch.column('_ingested_at_utc').to_pylist()
        assert all(ts == mock_now for ts in timestamp_values)

    def test_process_batch_with_row_numbers(self):
        """Test processing batch with row numbers enabled."""
        config = RowHashConfig(enabled=True, row_number_enabled=True)
        processor = RowHashProcessor(config)
        processor.set_source_context("test.csv", 10)

        batch = pa.record_batch({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })

        result_batch, validation_results = processor.process_batch(batch)

        assert '_rownum_in_source_file' in result_batch.schema.names
        assert '_rownum' in result_batch.schema.names

        source_row_nums = result_batch.column('_rownum_in_source_file').to_pylist()
        processing_row_nums = result_batch.column('_rownum').to_pylist()

        assert source_row_nums == [11, 12, 13]  # offset + 1-based
        assert processing_row_nums == [1, 2, 3]  # 1-based processing order

    def test_process_batch_multiple_calls_row_counter(self):
        """Test row counter increments across multiple batch calls."""
        config = RowHashConfig(enabled=True, row_number_enabled=True)
        processor = RowHashProcessor(config)

        # Process first batch
        batch1 = pa.record_batch({'id': [1, 2]})
        result1, _ = processor.process_batch(batch1)
        processing_nums1 = result1.column('_rownum').to_pylist()
        assert processing_nums1 == [1, 2]

        # Process second batch
        batch2 = pa.record_batch({'id': [3, 4, 5]})
        result2, _ = processor.process_batch(batch2)
        processing_nums2 = result2.column('_rownum').to_pylist()
        assert processing_nums2 == [3, 4, 5]

    def test_get_hash_columns_include_columns(self):
        """Test getting hash columns with include_columns specified."""
        config = RowHashConfig(include_columns=['id', 'name'])
        processor = RowHashProcessor(config)

        schema = pa.schema([
            ('id', pa.int64()),
            ('name', pa.string()),
            ('secret', pa.string())
        ])

        hash_columns = processor._get_hash_columns(schema)
        assert hash_columns == ['id', 'name']

    def test_get_hash_columns_exclude_columns(self):
        """Test getting hash columns with exclude_columns specified."""
        config = RowHashConfig(exclude_columns=['secret'])
        processor = RowHashProcessor(config)

        schema = pa.schema([
            ('id', pa.int64()),
            ('name', pa.string()),
            ('secret', pa.string())
        ])

        hash_columns = processor._get_hash_columns(schema)
        assert hash_columns == ['id', 'name']

    def test_get_hash_columns_exclude_metadata_columns(self):
        """Test getting hash columns excludes metadata columns automatically."""
        config = RowHashConfig()
        processor = RowHashProcessor(config)

        schema = pa.schema([
            ('id', pa.int64()),
            ('name', pa.string()),
            ('_source_uri', pa.string()),
            ('_ingested_at_utc', pa.timestamp('s')),
            ('row_hash', pa.string())
        ])

        hash_columns = processor._get_hash_columns(schema)
        assert hash_columns == ['id', 'name']

    def test_get_input_hash_columns(self):
        """Test getting input hash columns."""
        config = RowHashConfig()
        processor = RowHashProcessor(config)

        schema = pa.schema([
            ('id', pa.int64()),
            ('name', pa.string())
        ])

        hash_columns = processor._get_input_hash_columns(schema)
        assert hash_columns == ['id', 'name']

    def test_compute_row_hashes_basic(self):
        """Test computing row hashes for basic data."""
        config = RowHashConfig()
        processor = RowHashProcessor(config)

        batch = pa.record_batch({
            'id': [1, 2],
            'name': ['Alice', 'Bob']
        })

        hash_columns = ['id', 'name']
        hashes = processor._compute_row_hashes(batch, hash_columns)

        assert len(hashes) == 2
        assert hashes.type == pa.string()
        # Hashes should be different for different rows
        hash_values = hashes.to_pylist()
        assert hash_values[0] != hash_values[1]

    def test_compute_row_hashes_with_nulls(self):
        """Test computing row hashes with null values."""
        config = RowHashConfig(null_value="<NULL>")
        processor = RowHashProcessor(config)

        batch = pa.record_batch({
            'id': [1, 2],
            'name': ['Alice', None]
        })

        hash_columns = ['id', 'name']
        hashes = processor._compute_row_hashes(batch, hash_columns)

        assert len(hashes) == 2
        # Should handle null values without error

    def test_compute_hash_different_algorithms(self):
        """Test computing hash with different algorithms."""
        data = "test_data"

        algorithms = ["md5", "sha1", "sha256", "sha384", "sha512"]
        hashes = {}

        for algo in algorithms:
            config = RowHashConfig(algorithm=algo)
            processor = RowHashProcessor(config)
            hash_value = processor._compute_hash(data)
            hashes[algo] = hash_value

            # Verify hash length is appropriate for algorithm
            if algo == "md5":
                assert len(hash_value) == 32
            elif algo == "sha1":
                assert len(hash_value) == 40
            elif algo == "sha256":
                assert len(hash_value) == 64
            elif algo == "sha384":
                assert len(hash_value) == 96
            elif algo == "sha512":
                assert len(hash_value) == 128

        # All hashes should be different
        assert len(set(hashes.values())) == len(algorithms)

    def test_add_column(self):
        """Test adding column to batch."""
        config = RowHashConfig()
        processor = RowHashProcessor(config)

        batch = pa.record_batch({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })

        new_values = pa.array(['hash1', 'hash2', 'hash3'])
        result_batch = processor._add_column(batch, 'new_hash', new_values)

        assert 'new_hash' in result_batch.schema.names
        assert result_batch.column('new_hash').to_pylist() == ['hash1', 'hash2', 'hash3']
        assert len(result_batch.columns) == len(batch.columns) + 1

    def test_get_output_schema_disabled(self):
        """Test getting output schema with hash disabled."""
        config = RowHashConfig(enabled=False)
        processor = RowHashProcessor(config)

        input_schema = pa.schema([
            ('id', pa.int64()),
            ('name', pa.string())
        ])

        output_schema = processor.get_output_schema(input_schema)
        assert output_schema == input_schema

    def test_get_output_schema_enabled_all_features(self):
        """Test getting output schema with all features enabled."""
        config = RowHashConfig(
            enabled=True,
            input_hash_enabled=True,
            source_uri_enabled=True,
            ingested_at_enabled=True,
            row_number_enabled=True
        )
        processor = RowHashProcessor(config)

        input_schema = pa.schema([
            ('id', pa.int64()),
            ('name', pa.string())
        ])

        output_schema = processor.get_output_schema(input_schema)

        expected_columns = [
            'id', 'name',  # original columns
            'row_hash',  # output hash
            '_input_hash',  # input hash
            '_source_uri',  # source URI
            '_ingested_at_utc',  # ingestion timestamp
            '_rownum_in_source_file',  # source row number
            '_rownum'  # processing row number
        ]

        assert output_schema.names == expected_columns

    def test_compute_row_hashes_custom_separator(self):
        """Test computing row hashes with custom separator."""
        config = RowHashConfig(separator="|")
        processor = RowHashProcessor(config)

        batch = pa.record_batch({
            'id': [1],
            'name': ['Alice']
        })

        hash_columns = ['id', 'name']
        hashes = processor._compute_row_hashes(batch, hash_columns)

        # Should use custom separator in hash computation
        assert len(hashes) == 1

    def test_complex_integration_scenario(self):
        """Test complex scenario with all features enabled."""
        config = RowHashConfig(
            enabled=True,
            algorithm="sha256",
            include_columns=['id', 'name'],
            input_hash_enabled=True,
            source_uri_enabled=True,
            ingested_at_enabled=True,
            row_number_enabled=True
        )
        processor = RowHashProcessor(config)
        processor.set_source_context("test_file.csv", 0)

        output_batch = pa.record_batch({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'processed_flag': [True, True]
        })

        input_batch = pa.record_batch({
            'id': [1, 2],
            'raw_name': ['alice', 'bob']
        })

        result_batch, validation_results = processor.process_batch(output_batch, input_batch)

        # Should have all metadata columns
        assert 'row_hash' in result_batch.schema.names
        assert '_input_hash' in result_batch.schema.names
        assert '_source_uri' in result_batch.schema.names
        assert '_ingested_at_utc' in result_batch.schema.names
        assert '_rownum_in_source_file' in result_batch.schema.names
        assert '_rownum' in result_batch.schema.names

        assert validation_results == []


class TestRowHashProcessorIntegration:
    """Test row hash processor integration scenarios."""

    def test_module_imports(self):
        """Test that all components can be imported."""
        from forklift.processors.row_hash import (
            RowHashProcessor,
            RowHashConfig
        )

        assert RowHashProcessor is not None
        assert RowHashConfig is not None

    def test_module_docstring(self):
        """Test module documentation."""
        import forklift.processors.row_hash as row_hash_module

        assert row_hash_module.__doc__ is not None
        assert "Row hash processor" in row_hash_module.__doc__

    def test_processor_inheritance(self):
        """Test that processor inherits from BaseProcessor."""
        from forklift.processors.base import BaseProcessor
        from forklift.processors.row_hash import RowHashProcessor

        config = RowHashConfig()
        processor = RowHashProcessor(config)
        assert isinstance(processor, BaseProcessor)
