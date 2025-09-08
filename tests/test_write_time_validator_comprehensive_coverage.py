"""Comprehensive tests for write time validator module coverage improvement."""

import pytest
import pyarrow as pa
from unittest.mock import patch, MagicMock

from forklift.processors.write_time_validator import (
    WriteTimeValidator,
    WriteTimeConfig,
    create_basic_write_validator,
    create_strict_write_validator
)
from forklift.processors.base import ValidationResult


class TestWriteTimeValidatorComprehensive:
    """Comprehensive tests for WriteTimeValidator to achieve 100% coverage."""

    def setup_method(self):
        """Set up test fixtures."""
        self.schema = pa.schema([
            ('id', pa.int64()),
            ('name', pa.string()),
            ('age', pa.int32())
        ])

    def test_validate_write_readiness_null_type_column(self):
        """Test write readiness validation with null type column."""
        config = WriteTimeConfig()
        validator = WriteTimeValidator(config)

        # Create batch with null type column
        null_schema = pa.schema([('null_col', pa.null())])
        batch = pa.record_batch([pa.array([None, None])], schema=null_schema)

        result_batch, validation_results = validator.process_batch(batch)

        # Should detect null type column issue
        null_type_errors = [r for r in validation_results if r.error_code == "NULL_TYPE_COLUMN"]
        assert len(null_type_errors) > 0

    def test_validate_write_readiness_large_strings(self):
        """Test write readiness validation with very large strings."""
        config = WriteTimeConfig()
        validator = WriteTimeValidator(config)

        # Create batch with very large string
        large_string = "x" * 2000000  # 2MB string
        batch = pa.record_batch({
            'large_text': [large_string, 'normal']
        })

        result_batch, validation_results = validator.process_batch(batch)

        # Should detect large string issue
        large_string_errors = [r for r in validation_results if r.error_code == "LARGE_STRING_VALUES"]
        assert len(large_string_errors) > 0

    def test_validate_write_readiness_string_length_exception(self):
        """Test write readiness validation when string length computation fails."""
        config = WriteTimeConfig()
        validator = WriteTimeValidator(config)

        # Create batch with string column
        batch = pa.record_batch({
            'text': ['hello', 'world']
        })

        # Mock pc.utf8_length to raise exception
        with patch('forklift.processors.write_time_validator.pc.utf8_length') as mock_utf8_length:
            mock_utf8_length.side_effect = Exception("Computation failed")

            result_batch, validation_results = validator.process_batch(batch)

            # Should handle exception gracefully and continue
            assert result_batch == batch

    def test_process_batch_exception_handling(self):
        """Test process_batch exception handling."""
        config = WriteTimeConfig(check_empty_tables=True)
        validator = WriteTimeValidator(config)

        # Mock _validate_not_empty to raise exception
        with patch.object(validator, '_validate_not_empty') as mock_validate:
            mock_validate.side_effect = Exception("Validation failed")

            batch = pa.record_batch({'id': [1, 2, 3]})
            result_batch, validation_results = validator.process_batch(batch)

            # Should catch exception and return error result
            assert len(validation_results) > 0
            assert any("Write validation error" in r.error_message for r in validation_results)

    def test_validate_schema_compliance_with_mismatch_strict(self):
        """Test schema compliance with mismatch in strict mode."""
        expected_schema = pa.schema([('id', pa.int64()), ('name', pa.string())])
        config = WriteTimeConfig(
            expected_schema=expected_schema,
            fail_on_schema_mismatch=True
        )
        validator = WriteTimeValidator(config)

        # Create batch with different schema
        batch = pa.record_batch({
            'id': [1, 2],
            'different_name': ['a', 'b']
        })

        result_batch, validation_results = validator.process_batch(batch)

        # Should fail with schema mismatch
        schema_errors = [r for r in validation_results if r.error_code == "SCHEMA_MISMATCH"]
        assert len(schema_errors) > 0
        assert not schema_errors[0].is_valid

    def test_validate_schema_compliance_with_mismatch_lenient(self):
        """Test schema compliance with mismatch in lenient mode."""
        expected_schema = pa.schema([('id', pa.int64()), ('name', pa.string())])
        config = WriteTimeConfig(
            expected_schema=expected_schema,
            fail_on_schema_mismatch=False
        )
        validator = WriteTimeValidator(config)

        # Create batch with different schema
        batch = pa.record_batch({
            'id': [1, 2],
            'different_name': ['a', 'b']
        })

        result_batch, validation_results = validator.process_batch(batch)

        # Should warn about schema differences
        schema_warnings = [r for r in validation_results if r.error_code == "SCHEMA_WARNING"]
        assert len(schema_warnings) > 0
        assert schema_warnings[0].is_valid

    def test_validate_schema_compliance_type_error(self):
        """Test schema compliance when expected_schema is not a Schema object."""
        config = WriteTimeConfig(expected_schema="not_a_schema")
        validator = WriteTimeValidator(config)

        batch = pa.record_batch({'id': [1, 2]})

        # Should handle TypeError gracefully
        result_batch, validation_results = validator.process_batch(batch)
        assert result_batch == batch

    def test_validate_duplicate_rows_missing_pk_columns(self):
        """Test duplicate row validation when primary key columns are missing."""
        config = WriteTimeConfig(
            check_duplicate_rows=True,
            primary_key_columns=['missing_col']
        )
        validator = WriteTimeValidator(config)

        batch = pa.record_batch({'id': [1, 2, 3]})

        result_batch, validation_results = validator.process_batch(batch)

        # Should detect missing primary key columns
        missing_pk_errors = [r for r in validation_results if r.error_code == "MISSING_PRIMARY_KEY_COLUMNS"]
        assert len(missing_pk_errors) > 0

    def test_validate_duplicate_rows_with_nulls(self):
        """Test duplicate row validation with null values in primary key."""
        config = WriteTimeConfig(
            check_duplicate_rows=True,
            primary_key_columns=['id']
        )
        validator = WriteTimeValidator(config)

        # Create batch with null values in primary key
        batch = pa.record_batch({
            'id': [1, None, 1, 2]  # Duplicate 1 and a null
        })

        result_batch, validation_results = validator.process_batch(batch)

        # Should detect duplicates
        duplicate_errors = [r for r in validation_results if r.error_code == "DUPLICATE_PRIMARY_KEYS"]
        assert len(duplicate_errors) > 0

    def test_validate_duplicate_rows_across_batches(self):
        """Test duplicate row validation across multiple batches."""
        config = WriteTimeConfig(
            check_duplicate_rows=True,
            primary_key_columns=['id']
        )
        validator = WriteTimeValidator(config)

        # Process first batch
        batch1 = pa.record_batch({'id': [1, 2, 3]})
        validator.process_batch(batch1)

        # Process second batch with duplicate
        batch2 = pa.record_batch({'id': [4, 1, 5]})  # 1 is duplicate from first batch
        result_batch, validation_results = validator.process_batch(batch2)

        # Should detect cross-batch duplicate
        duplicate_errors = [r for r in validation_results if r.error_code == "DUPLICATE_PRIMARY_KEYS"]
        assert len(duplicate_errors) > 0

    def test_validate_duplicate_rows_many_duplicates(self):
        """Test duplicate row validation with many duplicates (truncation)."""
        config = WriteTimeConfig(
            check_duplicate_rows=True,
            primary_key_columns=['id']
        )
        validator = WriteTimeValidator(config)

        # Create batch with many duplicates
        duplicate_ids = [1] * 20  # 20 duplicates
        batch = pa.record_batch({'id': duplicate_ids})

        result_batch, validation_results = validator.process_batch(batch)

        # Should detect duplicates and truncate error message
        duplicate_errors = [r for r in validation_results if r.error_code == "DUPLICATE_PRIMARY_KEYS"]
        assert len(duplicate_errors) > 0
        assert "..." in duplicate_errors[0].error_message

    def test_reset_state(self):
        """Test resetting validator state."""
        config = WriteTimeConfig(
            check_duplicate_rows=True,
            primary_key_columns=['id']
        )
        validator = WriteTimeValidator(config)

        # Process batch to populate seen keys
        batch = pa.record_batch({'id': [1, 2, 3]})
        validator.process_batch(batch)

        assert len(validator._seen_primary_keys) > 0

        # Reset state
        validator.reset_state()

        assert len(validator._seen_primary_keys) == 0

    def test_create_basic_write_validator_with_pk(self):
        """Test creating basic validator with primary key."""
        validator = create_basic_write_validator(['user_id', 'record_id'])

        assert isinstance(validator, WriteTimeValidator)
        assert validator.config.check_empty_tables is True
        assert validator.config.check_duplicate_rows is True
        assert validator.config.check_null_primary_keys is True
        assert validator.config.primary_key_columns == ['user_id', 'record_id']
        assert validator.config.max_null_percentage == 90.0

    def test_create_basic_write_validator_without_pk(self):
        """Test creating basic validator without primary key."""
        validator = create_basic_write_validator()

        assert isinstance(validator, WriteTimeValidator)
        assert validator.config.check_duplicate_rows is False
        assert validator.config.check_null_primary_keys is False
        assert validator.config.primary_key_columns == []

    def test_create_strict_write_validator_with_all_options(self):
        """Test creating strict validator with all options."""
        schema = pa.schema([('id', pa.int64()), ('name', pa.string())])

        validator = create_strict_write_validator(
            primary_key_columns=['id'],
            required_columns=['id', 'name'],
            expected_schema=schema
        )

        assert isinstance(validator, WriteTimeValidator)
        assert validator.config.check_empty_tables is True
        assert validator.config.check_duplicate_rows is True
        assert validator.config.check_null_primary_keys is True
        assert validator.config.check_null_percentages is True
        assert validator.config.primary_key_columns == ['id']
        assert validator.config.required_columns == ['id', 'name']
        assert validator.config.expected_schema == schema
        assert validator.config.fail_on_schema_mismatch is True
        assert validator.config.max_null_percentage == 10.0

    def test_create_strict_write_validator_minimal(self):
        """Test creating strict validator with minimal options."""
        validator = create_strict_write_validator()

        assert isinstance(validator, WriteTimeValidator)
        assert validator.config.check_duplicate_rows is False
        assert validator.config.check_null_primary_keys is False
        assert validator.config.primary_key_columns == []
        assert validator.config.required_columns is None
        assert validator.config.expected_schema is None
        assert validator.config.fail_on_schema_mismatch is False

    def test_write_time_config_post_init_none_pk(self):
        """Test WriteTimeConfig post_init with None primary_key_columns."""
        config = WriteTimeConfig(primary_key_columns=None)

        # post_init should convert None to empty list
        assert config.primary_key_columns == []

    def test_write_time_config_post_init_existing_pk(self):
        """Test WriteTimeConfig post_init with existing primary_key_columns."""
        config = WriteTimeConfig(primary_key_columns=['id', 'uuid'])

        # post_init should preserve existing list
        assert config.primary_key_columns == ['id', 'uuid']

    def test_validate_null_percentages_empty_batch(self):
        """Test null percentage validation with empty batch."""
        config = WriteTimeConfig(check_null_percentages=True)
        validator = WriteTimeValidator(config)

        # Create empty batch
        empty_batch = pa.record_batch({'id': pa.array([], type=pa.int64())})

        result_batch, validation_results = validator.process_batch(empty_batch)

        # Should not produce null percentage errors for empty batch
        null_errors = [r for r in validation_results if r.error_code == "EXCESSIVE_NULLS"]
        assert len(null_errors) == 0

    def test_validate_primary_key_nulls_missing_column(self):
        """Test primary key null validation with missing column."""
        config = WriteTimeConfig(
            check_null_primary_keys=True,
            primary_key_columns=['missing_column']
        )
        validator = WriteTimeValidator(config)

        batch = pa.record_batch({'id': [1, 2, 3]})

        result_batch, validation_results = validator.process_batch(batch)

        # Should detect missing primary key column
        missing_errors = [r for r in validation_results if r.error_code == "MISSING_PRIMARY_KEY_COLUMN"]
        assert len(missing_errors) > 0
        assert missing_errors[0].column_name == 'missing_column'

    def test_module_all_exports(self):
        """Test that __all__ contains all expected exports."""
        from forklift.processors.write_time_validator import __all__

        expected_exports = [
            "WriteTimeValidator",
            "WriteTimeConfig",
            "create_basic_write_validator",
            "create_strict_write_validator"
        ]

        assert set(__all__) == set(expected_exports)


class TestWriteTimeValidatorEdgeCases:
    """Test edge cases and error conditions."""

    def test_all_validation_types_together(self):
        """Test all validation types working together."""
        schema = pa.schema([('id', pa.int64()), ('name', pa.string())])
        config = WriteTimeConfig(
            expected_schema=schema,
            fail_on_schema_mismatch=True,
            required_columns=['id', 'name'],
            check_empty_tables=True,
            check_duplicate_rows=True,
            check_null_primary_keys=True,
            check_null_percentages=True,
            primary_key_columns=['id'],
            max_null_percentage=50.0,
            min_row_count=2
        )
        validator = WriteTimeValidator(config)

        # Create batch that violates multiple rules
        batch = pa.record_batch({
            'id': [1, 1, None],  # Duplicate and null in PK
            'name': [None, 'Alice', None]  # High null percentage
        }, schema=schema)

        result_batch, validation_results = validator.process_batch(batch)

        # Should detect multiple types of violations
        error_codes = [r.error_code for r in validation_results]
        assert "DUPLICATE_PRIMARY_KEYS" in error_codes
        assert "NULL_PRIMARY_KEY" in error_codes
        assert "EXCESSIVE_NULLS" in error_codes

    def test_validator_with_complex_schema(self):
        """Test validator with complex schema types."""
        complex_schema = pa.schema([
            ('id', pa.int64()),
            ('data', pa.list_(pa.string())),
            ('metadata', pa.struct([('key', pa.string()), ('value', pa.int32())]))
        ])

        config = WriteTimeConfig(expected_schema=complex_schema)
        validator = WriteTimeValidator(config)

        batch = pa.record_batch([
            pa.array([1, 2, 3]),
            pa.array([['a', 'b'], ['c'], ['d', 'e', 'f']]),
            pa.array([{'key': 'test', 'value': 42}, {'key': 'test2', 'value': 43}, {'key': 'test3', 'value': 44}])
        ], schema=complex_schema)

        result_batch, validation_results = validator.process_batch(batch)

        # Should handle complex types without errors
        assert result_batch == batch
