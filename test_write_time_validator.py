"""Tests for write-time validation processor."""

import pytest
import pyarrow as pa
from src.forklift.processors.write_time_validator import (
    WriteTimeValidator,
    WriteTimeConfig,
    create_basic_write_validator,
    create_strict_write_validator
)
from src.forklift.processors.base import ValidationResult


class TestWriteTimeConfig:
    """Test WriteTimeConfig dataclass."""

    def test_default_config(self):
        """Test default configuration values."""
        config = WriteTimeConfig()

        assert config.check_empty_tables is True
        assert config.check_schema_compliance is True
        assert config.check_duplicate_rows is True
        assert config.check_null_primary_keys is True
        assert config.primary_key_columns == []
        assert config.required_columns == []
        assert config.max_null_percentage == 50.0
        assert config.fail_on_schema_mismatch is False
        assert config.expected_schema is None
        assert config.validate_write_readiness is True

    def test_custom_config(self):
        """Test custom configuration values."""
        config = WriteTimeConfig(
            check_empty_tables=False,
            primary_key_columns=['id'],
            required_columns=['name', 'email'],
            max_null_percentage=10.0,
            fail_on_schema_mismatch=True
        )

        assert config.check_empty_tables is False
        assert config.primary_key_columns == ['id']
        assert config.required_columns == ['name', 'email']
        assert config.max_null_percentage == 10.0
        assert config.fail_on_schema_mismatch is True

    def test_post_init(self):
        """Test post_init method properly initializes None values."""
        config = WriteTimeConfig(
            primary_key_columns=None,
            required_columns=None
        )

        assert config.primary_key_columns == []
        assert config.required_columns == []


class TestWriteTimeValidator:
    """Test WriteTimeValidator class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.basic_config = WriteTimeConfig()
        self.validator = WriteTimeValidator(self.basic_config)

        # Create test schema and data
        self.schema = pa.schema([
            pa.field('id', pa.int64()),
            pa.field('name', pa.string()),
            pa.field('email', pa.string()),
            pa.field('age', pa.int32())
        ])

        self.valid_data = pa.RecordBatch.from_arrays([
            pa.array([1, 2, 3]),
            pa.array(['Alice', 'Bob', 'Charlie']),
            pa.array(['alice@example.com', 'bob@example.com', 'charlie@example.com']),
            pa.array([25, 30, 35])
        ], schema=self.schema)

    def test_init(self):
        """Test validator initialization."""
        assert self.validator.config == self.basic_config
        assert self.validator._seen_primary_keys == set()

    def test_process_batch_valid_data(self):
        """Test processing valid data."""
        batch, results = self.validator.process_batch(self.valid_data)

        assert batch == self.valid_data
        # Should have some validation results but not necessarily errors
        assert isinstance(results, list)

    def test_process_batch_exception_handling(self):
        """Test exception handling in process_batch."""
        # Create a validator with bad config that will cause exceptions
        bad_config = WriteTimeConfig(expected_schema="not_a_schema")  # Invalid type
        validator = WriteTimeValidator(bad_config)

        batch, results = validator.process_batch(self.valid_data)

        assert batch == self.valid_data
        assert len(results) >= 1
        assert any(r.error_code == "WRITE_VALIDATION_ERROR" for r in results)

    def test_validate_not_empty_success(self):
        """Test validation passes for non-empty batch."""
        results = self.validator._validate_not_empty(self.valid_data)
        assert len(results) == 0

    def test_validate_not_empty_failure(self):
        """Test validation fails for empty batch."""
        empty_batch = pa.RecordBatch.from_arrays([], schema=pa.schema([]))
        results = self.validator._validate_not_empty(empty_batch)

        assert len(results) == 1
        assert results[0].is_valid is False
        assert results[0].error_code == "EMPTY_TABLE"
        assert "empty table" in results[0].error_message.lower()

    def test_validate_schema_compliance_no_expected_schema(self):
        """Test schema compliance when no expected schema is set."""
        results = self.validator._validate_schema_compliance(self.valid_data)
        assert len(results) == 0

    def test_validate_schema_compliance_matching_schema(self):
        """Test schema compliance with matching schema."""
        config = WriteTimeConfig(expected_schema=self.schema)
        validator = WriteTimeValidator(config)

        results = validator._validate_schema_compliance(self.valid_data)
        assert len(results) == 0

    def test_validate_schema_compliance_mismatch_warning(self):
        """Test schema compliance with mismatched schema (warning mode)."""
        different_schema = pa.schema([pa.field('different', pa.string())])
        config = WriteTimeConfig(
            expected_schema=different_schema,
            fail_on_schema_mismatch=False
        )
        validator = WriteTimeValidator(config)

        results = validator._validate_schema_compliance(self.valid_data)

        assert len(results) == 1
        assert results[0].is_valid is True  # Warning, not error
        assert results[0].error_code == "SCHEMA_WARNING"

    def test_validate_schema_compliance_mismatch_error(self):
        """Test schema compliance with mismatched schema (error mode)."""
        different_schema = pa.schema([pa.field('different', pa.string())])
        config = WriteTimeConfig(
            expected_schema=different_schema,
            fail_on_schema_mismatch=True
        )
        validator = WriteTimeValidator(config)

        results = validator._validate_schema_compliance(self.valid_data)

        assert len(results) == 1
        assert results[0].is_valid is False
        assert results[0].error_code == "SCHEMA_MISMATCH"

    def test_validate_required_columns_no_requirements(self):
        """Test required columns validation with no requirements."""
        results = self.validator._validate_required_columns(self.valid_data)
        assert len(results) == 0

    def test_validate_required_columns_all_present(self):
        """Test required columns validation when all are present."""
        config = WriteTimeConfig(required_columns=['id', 'name'])
        validator = WriteTimeValidator(config)

        results = validator._validate_required_columns(self.valid_data)
        assert len(results) == 0

    def test_validate_required_columns_missing(self):
        """Test required columns validation with missing columns."""
        config = WriteTimeConfig(required_columns=['id', 'name', 'missing_column'])
        validator = WriteTimeValidator(config)

        results = validator._validate_required_columns(self.valid_data)

        assert len(results) == 1
        assert results[0].is_valid is False
        assert results[0].error_code == "MISSING_REQUIRED_COLUMNS"
        assert "missing_column" in results[0].error_message

    def test_validate_null_percentages_empty_batch(self):
        """Test null percentage validation on empty batch."""
        empty_batch = pa.RecordBatch.from_arrays([], schema=pa.schema([]))
        results = self.validator._validate_null_percentages(empty_batch)
        assert len(results) == 0

    def test_validate_null_percentages_acceptable(self):
        """Test null percentage validation with acceptable nulls."""
        # Create data with some nulls but within threshold
        data_with_nulls = pa.RecordBatch.from_arrays([
            pa.array([1, 2, None]),  # 33% nulls, below 50% threshold
            pa.array(['Alice', 'Bob', 'Charlie'])
        ], schema=pa.schema([
            pa.field('id', pa.int64()),
            pa.field('name', pa.string())
        ]))

        results = self.validator._validate_null_percentages(data_with_nulls)
        assert len(results) == 0

    def test_validate_null_percentages_excessive(self):
        """Test null percentage validation with excessive nulls."""
        config = WriteTimeConfig(max_null_percentage=10.0)
        validator = WriteTimeValidator(config)

        # Create data with many nulls
        data_with_nulls = pa.RecordBatch.from_arrays([
            pa.array([1, None, None]),  # 67% nulls, above 10% threshold
            pa.array(['Alice', 'Bob', 'Charlie'])
        ], schema=pa.schema([
            pa.field('id', pa.int64()),
            pa.field('name', pa.string())
        ]))

        results = validator._validate_null_percentages(data_with_nulls)

        assert len(results) == 1
        assert results[0].is_valid is False
        assert results[0].error_code == "EXCESSIVE_NULLS"
        assert results[0].column_name == "id"

    def test_validate_primary_key_nulls_missing_column(self):
        """Test primary key null validation with missing column."""
        config = WriteTimeConfig(primary_key_columns=['missing_column'])
        validator = WriteTimeValidator(config)

        results = validator._validate_primary_key_nulls(self.valid_data)

        assert len(results) == 1
        assert results[0].is_valid is False
        assert results[0].error_code == "MISSING_PRIMARY_KEY_COLUMN"
        assert results[0].column_name == "missing_column"

    def test_validate_primary_key_nulls_no_nulls(self):
        """Test primary key null validation with no nulls."""
        config = WriteTimeConfig(primary_key_columns=['id'])
        validator = WriteTimeValidator(config)

        results = validator._validate_primary_key_nulls(self.valid_data)
        assert len(results) == 0

    def test_validate_primary_key_nulls_with_nulls(self):
        """Test primary key null validation with nulls present."""
        config = WriteTimeConfig(primary_key_columns=['id'])
        validator = WriteTimeValidator(config)

        # Create data with null in primary key
        data_with_null_pk = pa.RecordBatch.from_arrays([
            pa.array([1, None, 3]),
            pa.array(['Alice', 'Bob', 'Charlie'])
        ], schema=pa.schema([
            pa.field('id', pa.int64()),
            pa.field('name', pa.string())
        ]))

        results = validator._validate_primary_key_nulls(data_with_null_pk)

        assert len(results) == 1
        assert results[0].is_valid is False
        assert results[0].error_code == "NULL_PRIMARY_KEY"
        assert results[0].column_name == "id"

    def test_validate_duplicate_rows_no_primary_key(self):
        """Test duplicate row validation with no primary key configured."""
        results = self.validator._validate_duplicate_rows(self.valid_data)
        assert len(results) == 0

    def test_validate_duplicate_rows_empty_batch(self):
        """Test duplicate row validation on empty batch."""
        config = WriteTimeConfig(primary_key_columns=['id'])
        validator = WriteTimeValidator(config)

        empty_batch = pa.RecordBatch.from_arrays([], schema=pa.schema([]))
        results = validator._validate_duplicate_rows(empty_batch)
        assert len(results) == 0

    def test_validate_duplicate_rows_missing_pk_columns(self):
        """Test duplicate row validation with missing primary key columns."""
        config = WriteTimeConfig(primary_key_columns=['missing_column'])
        validator = WriteTimeValidator(config)

        results = validator._validate_duplicate_rows(self.valid_data)

        assert len(results) == 1
        assert results[0].is_valid is False
        assert results[0].error_code == "MISSING_PRIMARY_KEY_COLUMNS"

    def test_validate_duplicate_rows_no_duplicates(self):
        """Test duplicate row validation with no duplicates."""
        config = WriteTimeConfig(primary_key_columns=['id'])
        validator = WriteTimeValidator(config)

        results = validator._validate_duplicate_rows(self.valid_data)
        assert len(results) == 0

    def test_validate_duplicate_rows_with_duplicates(self):
        """Test duplicate row validation with duplicates."""
        config = WriteTimeConfig(primary_key_columns=['id'])
        validator = WriteTimeValidator(config)

        # Create data with duplicate primary keys
        data_with_duplicates = pa.RecordBatch.from_arrays([
            pa.array([1, 2, 1]),  # Duplicate id=1
            pa.array(['Alice', 'Bob', 'Alice2'])
        ], schema=pa.schema([
            pa.field('id', pa.int64()),
            pa.field('name', pa.string())
        ]))

        results = validator._validate_duplicate_rows(data_with_duplicates)

        assert len(results) == 1
        assert results[0].is_valid is False
        assert results[0].error_code == "DUPLICATE_PRIMARY_KEYS"

    def test_validate_duplicate_rows_across_batches(self):
        """Test duplicate row validation across multiple batches."""
        config = WriteTimeConfig(primary_key_columns=['id'])
        validator = WriteTimeValidator(config)

        # Process first batch
        batch1 = pa.RecordBatch.from_arrays([
            pa.array([1, 2]),
            pa.array(['Alice', 'Bob'])
        ], schema=pa.schema([
            pa.field('id', pa.int64()),
            pa.field('name', pa.string())
        ]))

        results1 = validator._validate_duplicate_rows(batch1)
        assert len(results1) == 0  # No duplicates in first batch

        # Process second batch with duplicate from first batch
        batch2 = pa.RecordBatch.from_arrays([
            pa.array([1, 3]),  # id=1 is duplicate from batch1
            pa.array(['Alice2', 'Charlie'])
        ], schema=pa.schema([
            pa.field('id', pa.int64()),
            pa.field('name', pa.string())
        ]))

        results2 = validator._validate_duplicate_rows(batch2)

        assert len(results2) == 1
        assert results2[0].is_valid is False
        assert results2[0].error_code == "DUPLICATE_PRIMARY_KEYS"

    def test_validate_write_readiness_good_data(self):
        """Test write readiness validation with good data."""
        results = self.validator._validate_write_readiness(self.valid_data)
        assert len(results) == 0

    def test_validate_write_readiness_null_type(self):
        """Test write readiness validation with null type column."""
        # Create schema with null type
        null_schema = pa.schema([
            pa.field('id', pa.int64()),
            pa.field('null_col', pa.null())
        ])

        null_batch = pa.RecordBatch.from_arrays([
            pa.array([1, 2, 3]),
            pa.array([None, None, None])
        ], schema=null_schema)

        results = self.validator._validate_write_readiness(null_batch)

        assert len(results) == 1
        assert results[0].is_valid is False
        assert results[0].error_code == "NULL_TYPE_COLUMN"
        assert results[0].column_name == "null_col"

    def test_validate_write_readiness_large_strings(self):
        """Test write readiness validation with very large strings."""
        # Create data with very large string
        large_string = "x" * 1000001  # Larger than 1MB limit

        large_string_batch = pa.RecordBatch.from_arrays([
            pa.array([1]),
            pa.array([large_string])
        ], schema=pa.schema([
            pa.field('id', pa.int64()),
            pa.field('large_text', pa.string())
        ]))

        results = self.validator._validate_write_readiness(large_string_batch)

        assert len(results) == 1
        assert results[0].is_valid is False
        assert results[0].error_code == "LARGE_STRING_VALUES"
        assert results[0].column_name == "large_text"

    def test_reset_state(self):
        """Test resetting validator state."""
        # Add some primary keys to internal state
        self.validator._seen_primary_keys.add((1,))
        self.validator._seen_primary_keys.add((2,))

        assert len(self.validator._seen_primary_keys) == 2

        self.validator.reset_state()

        assert len(self.validator._seen_primary_keys) == 0


class TestFactoryFunctions:
    """Test factory functions for creating validators."""

    def test_create_basic_write_validator_no_pk(self):
        """Test creating basic validator without primary key."""
        validator = create_basic_write_validator()

        assert isinstance(validator, WriteTimeValidator)
        assert validator.config.check_empty_tables is True
        assert validator.config.check_duplicate_rows is False
        assert validator.config.check_null_primary_keys is False
        assert validator.config.primary_key_columns == []
        assert validator.config.max_null_percentage == 90.0

    def test_create_basic_write_validator_with_pk(self):
        """Test creating basic validator with primary key."""
        validator = create_basic_write_validator(['id'])

        assert isinstance(validator, WriteTimeValidator)
        assert validator.config.check_duplicate_rows is True
        assert validator.config.check_null_primary_keys is True
        assert validator.config.primary_key_columns == ['id']

    def test_create_strict_write_validator(self):
        """Test creating strict validator."""
        expected_schema = pa.schema([pa.field('id', pa.int64())])

        validator = create_strict_write_validator(
            primary_key_columns=['id'],
            required_columns=['id', 'name'],
            expected_schema=expected_schema
        )

        assert isinstance(validator, WriteTimeValidator)
        assert validator.config.check_empty_tables is True
        assert validator.config.check_schema_compliance is True
        assert validator.config.check_duplicate_rows is True
        assert validator.config.check_null_primary_keys is True
        assert validator.config.primary_key_columns == ['id']
        assert validator.config.required_columns == ['id', 'name']
        assert validator.config.max_null_percentage == 10.0
        assert validator.config.fail_on_schema_mismatch is True
        assert validator.config.expected_schema == expected_schema
        assert validator.config.validate_write_readiness is True


if __name__ == "__main__":
    pytest.main([__file__])
