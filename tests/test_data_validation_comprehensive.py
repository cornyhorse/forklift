"""Comprehensive unit tests for data_validation.py"""

import re
from datetime import date, datetime
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from forklift.processors.base import ValidationResult
from forklift.processors.data_validation import (BadRowsConfig,
                                                 DataValidationProcessor,
                                                 DateValidation,
                                                 EnumValidation,
                                                 FieldValidationRule,
                                                 RangeValidation,
                                                 StringValidation,
                                                 ValidationConfig)


class TestRangeValidation:
    """Test RangeValidation dataclass."""

    def test_range_validation_defaults(self):
        """Test RangeValidation with default values."""
        range_val = RangeValidation()
        assert range_val.min_value is None
        assert range_val.max_value is None
        assert range_val.inclusive is True

    def test_range_validation_custom_values(self):
        """Test RangeValidation with custom values."""
        range_val = RangeValidation(min_value=10, max_value=100, inclusive=False)
        assert range_val.min_value == 10
        assert range_val.max_value == 100
        assert range_val.inclusive is False

    def test_range_validation_string_values(self):
        """Test RangeValidation with string values."""
        range_val = RangeValidation(min_value="10.5", max_value="99.9")
        assert range_val.min_value == "10.5"
        assert range_val.max_value == "99.9"


class TestStringValidation:
    """Test StringValidation dataclass."""

    def test_string_validation_defaults(self):
        """Test StringValidation with default values."""
        string_val = StringValidation()
        assert string_val.min_length is None
        assert string_val.max_length is None
        assert string_val.pattern is None
        assert string_val.allow_empty is True

    def test_string_validation_custom_values(self):
        """Test StringValidation with custom values."""
        string_val = StringValidation(
            min_length=5, max_length=50, pattern=r"^[a-zA-Z]+$", allow_empty=False
        )
        assert string_val.min_length == 5
        assert string_val.max_length == 50
        assert string_val.pattern == r"^[a-zA-Z]+$"
        assert string_val.allow_empty is False


class TestEnumValidation:
    """Test EnumValidation dataclass."""

    def test_enum_validation_required_values(self):
        """Test EnumValidation with required allowed_values."""
        enum_val = EnumValidation(allowed_values=["A", "B", "C"])
        assert enum_val.allowed_values == ["A", "B", "C"]
        assert enum_val.case_sensitive is True

    def test_enum_validation_case_insensitive(self):
        """Test EnumValidation with case insensitive option."""
        enum_val = EnumValidation(allowed_values=["red", "green", "blue"], case_sensitive=False)
        assert enum_val.allowed_values == ["red", "green", "blue"]
        assert enum_val.case_sensitive is False


class TestDateValidation:
    """Test DateValidation dataclass."""

    def test_date_validation_defaults(self):
        """Test DateValidation with default values."""
        date_val = DateValidation()
        assert date_val.min_date is None
        assert date_val.max_date is None
        assert date_val.formats is None

    def test_date_validation_custom_values(self):
        """Test DateValidation with custom values."""
        date_val = DateValidation(
            min_date="2020-01-01", max_date="2030-12-31", formats=["%Y-%m-%d", "%m/%d/%Y"]
        )
        assert date_val.min_date == "2020-01-01"
        assert date_val.max_date == "2030-12-31"
        assert date_val.formats == ["%Y-%m-%d", "%m/%d/%Y"]


class TestFieldValidationRule:
    """Test FieldValidationRule dataclass."""

    def test_field_validation_rule_minimal(self):
        """Test FieldValidationRule with minimal configuration."""
        rule = FieldValidationRule(field_name="test_field")
        assert rule.field_name == "test_field"
        assert rule.required is False
        assert rule.unique is False
        assert rule.range_validation is None
        assert rule.string_validation is None
        assert rule.enum_validation is None
        assert rule.date_validation is None
        assert rule.on_violation == {}

    def test_field_validation_rule_full_config(self):
        """Test FieldValidationRule with full configuration."""
        range_val = RangeValidation(min_value=0, max_value=100)
        string_val = StringValidation(min_length=1, max_length=50)
        enum_val = EnumValidation(allowed_values=["A", "B", "C"])
        date_val = DateValidation(min_date="2020-01-01")

        rule = FieldValidationRule(
            field_name="full_field",
            required=True,
            unique=True,
            range_validation=range_val,
            string_validation=string_val,
            enum_validation=enum_val,
            date_validation=date_val,
            on_violation={"action": "reject"},
        )

        assert rule.field_name == "full_field"
        assert rule.required is True
        assert rule.unique is True
        assert rule.range_validation == range_val
        assert rule.string_validation == string_val
        assert rule.enum_validation == enum_val
        assert rule.date_validation == date_val
        assert rule.on_violation == {"action": "reject"}

    def test_field_validation_rule_post_init(self):
        """Test FieldValidationRule __post_init__ method."""
        rule = FieldValidationRule(field_name="test", on_violation=None)
        assert rule.on_violation == {}


class TestBadRowsConfig:
    """Test BadRowsConfig dataclass."""

    def test_bad_rows_config_defaults(self):
        """Test BadRowsConfig with default values."""
        config = BadRowsConfig()
        assert config.enabled is True
        assert config.output_path == "bad_rows"
        assert config.file_format == "parquet"
        assert config.include_original_row is True
        assert config.include_validation_errors is True
        assert config.max_bad_rows_percent == 10.0
        assert config.fail_on_exceed_threshold is True

    def test_bad_rows_config_custom_values(self):
        """Test BadRowsConfig with custom values."""
        config = BadRowsConfig(
            enabled=False,
            output_path="/custom/path",
            file_format="json",
            include_original_row=False,
            include_validation_errors=False,
            max_bad_rows_percent=5.0,
            fail_on_exceed_threshold=False,
        )
        assert config.enabled is False
        assert config.output_path == "/custom/path"
        assert config.file_format == "json"
        assert config.include_original_row is False
        assert config.include_validation_errors is False
        assert config.max_bad_rows_percent == 5.0
        assert config.fail_on_exceed_threshold is False


class TestValidationConfig:
    """Test ValidationConfig dataclass."""

    def test_validation_config_minimal(self):
        """Test ValidationConfig with minimal configuration."""
        rule = FieldValidationRule(field_name="test")
        bad_rows_config = BadRowsConfig()

        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        assert config.field_validations == [rule]
        assert config.bad_rows_config == bad_rows_config
        assert config.uniqueness_strategy == "first_wins"

    def test_validation_config_custom_strategy(self):
        """Test ValidationConfig with custom uniqueness strategy."""
        rule = FieldValidationRule(field_name="test")
        bad_rows_config = BadRowsConfig()

        config = ValidationConfig(
            field_validations=[rule],
            bad_rows_config=bad_rows_config,
            uniqueness_strategy="last_wins",
        )

        assert config.uniqueness_strategy == "last_wins"

    def test_validation_config_invalid_strategy(self):
        """Test ValidationConfig with invalid uniqueness strategy."""
        rule = FieldValidationRule(field_name="test")
        bad_rows_config = BadRowsConfig()

        with pytest.raises(ValueError, match="Invalid uniqueness strategy"):
            ValidationConfig(
                field_validations=[rule],
                bad_rows_config=bad_rows_config,
                uniqueness_strategy="invalid_strategy",
            )

    def test_validation_config_all_strategies(self):
        """Test all valid uniqueness strategies."""
        rule = FieldValidationRule(field_name="test")
        bad_rows_config = BadRowsConfig()

        valid_strategies = ["first_wins", "last_wins", "fail_on_duplicate", "mark_all_duplicates"]

        for strategy in valid_strategies:
            config = ValidationConfig(
                field_validations=[rule],
                bad_rows_config=bad_rows_config,
                uniqueness_strategy=strategy,
            )
            assert config.uniqueness_strategy == strategy


class TestDataValidationProcessor:
    """Test DataValidationProcessor class."""

    def test_data_validation_processor_init(self):
        """Test DataValidationProcessor initialization."""
        rule1 = FieldValidationRule(field_name="field1", unique=True)
        rule2 = FieldValidationRule(field_name="field2", required=True)
        bad_rows_config = BadRowsConfig()

        config = ValidationConfig(field_validations=[rule1, rule2], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        assert processor.config == config
        assert len(processor.unique_value_tracker) == 1
        assert "field1" in processor.unique_value_tracker
        assert "field2" not in processor.unique_value_tracker
        assert processor.bad_rows == []
        assert processor.total_rows_processed == 0

    def test_process_batch_all_valid_rows(self):
        """Test processing batch with all valid rows."""
        rule = FieldValidationRule(field_name="age", required=True)
        bad_rows_config = BadRowsConfig()
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        # Create test batch
        data = {"age": [25, 30, 35], "name": ["Alice", "Bob", "Charlie"]}
        batch = pa.RecordBatch.from_pydict(data)

        clean_batch, validation_results = processor.process_batch(batch)

        assert len(validation_results) == 0
        assert clean_batch.num_rows == 3
        assert processor.total_rows_processed == 3
        assert len(processor.bad_rows) == 0

    def test_process_batch_with_validation_errors(self):
        """Test processing batch with validation errors."""
        rule = FieldValidationRule(field_name="age", required=True)
        bad_rows_config = BadRowsConfig(
            max_bad_rows_percent=50.0
        )  # Set high threshold to avoid threshold errors
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        # Create test batch with null value
        data = {"age": [25, None, 35], "name": ["Alice", "Bob", "Charlie"]}
        batch = pa.RecordBatch.from_pydict(data)

        clean_batch, validation_results = processor.process_batch(batch)

        assert len(validation_results) == 1
        assert validation_results[0].is_valid is False
        assert "required but is null/empty" in validation_results[0].error_message
        assert validation_results[0].row_index == 1
        assert clean_batch.num_rows == 2
        assert len(processor.bad_rows) == 1

    def test_process_batch_all_bad_rows(self):
        """Test processing batch where all rows are bad."""
        rule = FieldValidationRule(field_name="age", required=True)
        bad_rows_config = BadRowsConfig(
            fail_on_exceed_threshold=False
        )  # Disable threshold checking
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        # Create test batch with all null values
        data = {"age": [None, None, None], "name": ["Alice", "Bob", "Charlie"]}
        batch = pa.RecordBatch.from_pydict(data)

        clean_batch, validation_results = processor.process_batch(batch)

        assert len(validation_results) == 3
        assert clean_batch.num_rows == 0
        assert len(processor.bad_rows) == 3

    def test_process_batch_bad_rows_threshold_exceeded(self):
        """Test processing batch where bad rows exceed threshold."""
        rule = FieldValidationRule(field_name="age", required=True)
        bad_rows_config = BadRowsConfig(max_bad_rows_percent=20.0, fail_on_exceed_threshold=True)
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        # Create test batch with 50% bad rows (exceeds 20% threshold)
        data = {"age": [25, None, 35, None], "name": ["Alice", "Bob", "Charlie", "Dave"]}
        batch = pa.RecordBatch.from_pydict(data)

        clean_batch, validation_results = processor.process_batch(batch)

        # Should have validation errors for the bad rows plus threshold exceeded error
        threshold_errors = [r for r in validation_results if "exceed threshold" in r.error_message]
        assert len(threshold_errors) == 1
        assert "Bad rows (50.0%) exceed threshold (20.0%)" in threshold_errors[0].error_message

    def test_process_batch_exception_handling(self):
        """Test exception handling in process_batch."""
        rule = FieldValidationRule(field_name="age", required=True)
        bad_rows_config = BadRowsConfig()
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        # Mock _validate_row to raise an exception
        with patch.object(processor, "_validate_row", side_effect=Exception("Test error")):
            data = {"age": [25], "name": ["Alice"]}
            batch = pa.RecordBatch.from_pydict(data)

            clean_batch, validation_results = processor.process_batch(batch)

            assert len(validation_results) == 1
            assert validation_results[0].is_valid is False
            assert "Validation processing failed" in validation_results[0].error_message
            assert validation_results[0].error_code == "VALIDATION_PROCESSOR_ERROR"

    def test_validate_row_required_field_missing(self):
        """Test _validate_row with required field missing."""
        rule = FieldValidationRule(field_name="age", required=True)
        bad_rows_config = BadRowsConfig()
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        data = {"age": [None], "name": ["Alice"]}
        batch = pa.RecordBatch.from_pydict(data)

        is_valid, errors = processor._validate_row(batch, 0)

        assert is_valid is False
        assert len(errors) == 1
        assert "required but is null/empty" in errors[0]

    def test_validate_row_unique_field_duplicate(self):
        """Test _validate_row with unique field having duplicates."""
        rule = FieldValidationRule(field_name="email", unique=True)
        bad_rows_config = BadRowsConfig()
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        data = {"email": ["alice@test.com", "alice@test.com"], "name": ["Alice", "Alice2"]}
        batch = pa.RecordBatch.from_pydict(data)

        # First row should be valid
        is_valid1, errors1 = processor._validate_row(batch, 0)
        assert is_valid1 is True
        assert len(errors1) == 0

        # Second row should be invalid (duplicate)
        is_valid2, errors2 = processor._validate_row(batch, 1)
        assert is_valid2 is False
        assert len(errors2) == 1
        assert "is not unique" in errors2[0]

    def test_validate_row_unique_field_fail_on_duplicate(self):
        """Test _validate_row with fail_on_duplicate strategy."""
        rule = FieldValidationRule(field_name="email", unique=True)
        bad_rows_config = BadRowsConfig()
        config = ValidationConfig(
            field_validations=[rule],
            bad_rows_config=bad_rows_config,
            uniqueness_strategy="fail_on_duplicate",
        )

        processor = DataValidationProcessor(config)

        data = {"email": ["alice@test.com", "alice@test.com"], "name": ["Alice", "Alice2"]}
        batch = pa.RecordBatch.from_pydict(data)

        # First row should be valid
        is_valid1, errors1 = processor._validate_row(batch, 0)
        assert is_valid1 is True

        # Second row should be invalid with different error message
        is_valid2, errors2 = processor._validate_row(batch, 1)
        assert is_valid2 is False
        assert "violates uniqueness constraint" in errors2[0]

    def test_validate_row_field_not_in_schema(self):
        """Test _validate_row when validation field is not in batch schema."""
        rule = FieldValidationRule(field_name="missing_field", required=True)
        bad_rows_config = BadRowsConfig()
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        data = {"age": [25], "name": ["Alice"]}
        batch = pa.RecordBatch.from_pydict(data)

        is_valid, errors = processor._validate_row(batch, 0)

        # Should be valid because field is not in schema (skipped)
        assert is_valid is True
        assert len(errors) == 0

    def test_is_null_or_empty(self):
        """Test _is_null_or_empty method."""
        rule = FieldValidationRule(field_name="test")
        bad_rows_config = BadRowsConfig()
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        assert processor._is_null_or_empty(None) is True
        assert processor._is_null_or_empty("") is True
        assert processor._is_null_or_empty("   ") is True
        assert processor._is_null_or_empty("value") is False
        assert processor._is_null_or_empty(0) is False
        assert processor._is_null_or_empty(False) is False

    def test_validate_range_numeric_values(self):
        """Test _validate_range with numeric values."""
        rule = FieldValidationRule(field_name="test")
        bad_rows_config = BadRowsConfig()
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        range_val = RangeValidation(min_value=10, max_value=100, inclusive=True)

        # Valid values
        assert processor._validate_range("test", 50, range_val) is None
        assert processor._validate_range("test", 10, range_val) is None
        assert processor._validate_range("test", 100, range_val) is None

        # Invalid values
        error = processor._validate_range("test", 5, range_val)
        assert error is not None
        assert "below minimum" in error

        error = processor._validate_range("test", 150, range_val)
        assert error is not None
        assert "above maximum" in error

    def test_validate_range_non_inclusive(self):
        """Test _validate_range with non-inclusive bounds."""
        rule = FieldValidationRule(field_name="test")
        bad_rows_config = BadRowsConfig()
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        range_val = RangeValidation(min_value=10, max_value=100, inclusive=False)

        # Values equal to bounds should be invalid
        error = processor._validate_range("test", 10, range_val)
        assert error is not None
        assert "not greater than" in error

        error = processor._validate_range("test", 100, range_val)
        assert error is not None
        assert "not less than" in error

    def test_validate_range_string_conversion(self):
        """Test _validate_range with string to numeric conversion."""
        rule = FieldValidationRule(field_name="test")
        bad_rows_config = BadRowsConfig()
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        range_val = RangeValidation(min_value="10", max_value="100")

        # Valid string numbers
        assert processor._validate_range("test", "50", range_val) is None
        assert processor._validate_range("test", "10.5", range_val) is None

        # Invalid string that can't be converted
        error = processor._validate_range("test", "not_a_number", range_val)
        assert error is not None
        assert "cannot be converted to numeric" in error

    def test_validate_range_exception_handling(self):
        """Test _validate_range exception handling."""
        rule = FieldValidationRule(field_name="test")
        bad_rows_config = BadRowsConfig()
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        # Create a range validation that will cause an exception
        range_val = RangeValidation(min_value=10, max_value=100)

        # Mock a comparison that raises an exception
        with patch("builtins.float", side_effect=Exception("Conversion error")):
            error = processor._validate_range("test", "10.5", range_val)
            assert error is not None
            assert "range validation error" in error

    def test_validate_string_length_checks(self):
        """Test _validate_string with length checks."""
        rule = FieldValidationRule(field_name="test")
        bad_rows_config = BadRowsConfig()
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        string_val = StringValidation(min_length=5, max_length=10)

        # Valid length
        assert processor._validate_string("test", "hello", string_val) is None
        assert processor._validate_string("test", "12345", string_val) is None
        assert processor._validate_string("test", "1234567890", string_val) is None

        # Too short
        error = processor._validate_string("test", "hi", string_val)
        assert error is not None
        assert "below minimum" in error

        # Too long
        error = processor._validate_string("test", "this_is_too_long", string_val)
        assert error is not None
        assert "exceeds maximum" in error

    def test_validate_string_pattern_matching(self):
        """Test _validate_string with pattern matching."""
        rule = FieldValidationRule(field_name="test")
        bad_rows_config = BadRowsConfig()
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        string_val = StringValidation(pattern=r"^[a-zA-Z]+$")

        # Valid pattern
        assert processor._validate_string("test", "hello", string_val) is None
        assert processor._validate_string("test", "HelloWorld", string_val) is None

        # Invalid pattern
        error = processor._validate_string("test", "hello123", string_val)
        assert error is not None
        assert "does not match required pattern" in error

    def test_validate_string_allow_empty(self):
        """Test _validate_string with allow_empty option."""
        rule = FieldValidationRule(field_name="test")
        bad_rows_config = BadRowsConfig()
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        string_val = StringValidation(allow_empty=False)

        # Non-empty should be valid
        assert processor._validate_string("test", "hello", string_val) is None

        # Empty should be invalid
        error = processor._validate_string("test", "", string_val)
        assert error is not None
        assert "cannot be empty" in error

        error = processor._validate_string("test", "   ", string_val)
        assert error is not None
        assert "cannot be empty" in error

    def test_validate_string_pattern_error(self):
        """Test _validate_string with invalid regex pattern."""
        rule = FieldValidationRule(field_name="test")
        bad_rows_config = BadRowsConfig()
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        string_val = StringValidation(pattern="[invalid_regex")

        error = processor._validate_string("test", "hello", string_val)
        assert error is not None
        assert "pattern validation error" in error

    def test_validate_string_non_string_value(self):
        """Test _validate_string with non-string value."""
        rule = FieldValidationRule(field_name="test")
        bad_rows_config = BadRowsConfig()
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        string_val = StringValidation(min_length=1, max_length=5)

        # Non-string value should be converted to string
        assert processor._validate_string("test", 123, string_val) is None
        assert processor._validate_string("test", True, string_val) is None

    def test_validate_enum_case_sensitive(self):
        """Test _validate_enum with case sensitive matching."""
        rule = FieldValidationRule(field_name="test")
        bad_rows_config = BadRowsConfig()
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        enum_val = EnumValidation(allowed_values=["red", "green", "blue"], case_sensitive=True)

        # Valid values
        assert processor._validate_enum("test", "red", enum_val) is None
        assert processor._validate_enum("test", "green", enum_val) is None

        # Invalid values
        error = processor._validate_enum("test", "Red", enum_val)
        assert error is not None
        assert "not in allowed values" in error

        error = processor._validate_enum("test", "yellow", enum_val)
        assert error is not None
        assert "not in allowed values" in error

    def test_validate_enum_case_insensitive(self):
        """Test _validate_enum with case insensitive matching."""
        rule = FieldValidationRule(field_name="test")
        bad_rows_config = BadRowsConfig()
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        enum_val = EnumValidation(allowed_values=["red", "green", "blue"], case_sensitive=False)

        # Valid values (different cases)
        assert processor._validate_enum("test", "Red", enum_val) is None
        assert processor._validate_enum("test", "GREEN", enum_val) is None
        assert processor._validate_enum("test", "BLue", enum_val) is None

        # Invalid value
        error = processor._validate_enum("test", "yellow", enum_val)
        assert error is not None
        assert "not in allowed values" in error

    def test_validate_date_string_format(self):
        """Test _validate_date with string date format."""
        rule = FieldValidationRule(field_name="test")
        bad_rows_config = BadRowsConfig()
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        date_val = DateValidation(min_date="2020-01-01", max_date="2030-12-31")

        # Valid dates
        assert processor._validate_date("test", "2025-06-15", date_val) is None
        assert processor._validate_date("test", "2020-01-01", date_val) is None
        assert processor._validate_date("test", "2030-12-31", date_val) is None

        # Invalid date format
        error = processor._validate_date("test", "15/06/2025", date_val)
        assert error is not None
        assert "not a valid date" in error

        # Date too early
        error = processor._validate_date("test", "2019-12-31", date_val)
        assert error is not None
        assert "before minimum" in error

        # Date too late
        error = processor._validate_date("test", "2031-01-01", date_val)
        assert error is not None
        assert "after maximum" in error

    def test_validate_date_datetime_objects(self):
        """Test _validate_date with datetime objects."""
        rule = FieldValidationRule(field_name="test")
        bad_rows_config = BadRowsConfig()
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        date_val = DateValidation(min_date="2020-01-01", max_date="2030-12-31")

        # Valid datetime object
        test_date = datetime(2025, 6, 15)
        assert processor._validate_date("test", test_date, date_val) is None

        # Valid date object
        test_date = date(2025, 6, 15)
        assert processor._validate_date("test", test_date, date_val) is None

    def test_validate_date_invalid_type(self):
        """Test _validate_date with invalid value type."""
        rule = FieldValidationRule(field_name="test")
        bad_rows_config = BadRowsConfig()
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        date_val = DateValidation()

        error = processor._validate_date("test", 123, date_val)
        assert error is not None
        assert "not a valid date type" in error

    def test_handle_bad_row_enabled(self):
        """Test _handle_bad_row when bad rows handling is enabled."""
        rule = FieldValidationRule(field_name="test")
        bad_rows_config = BadRowsConfig(
            enabled=True, include_validation_errors=True, include_original_row=True
        )
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        data = {"age": [25], "name": ["Alice"]}
        batch = pa.RecordBatch.from_pydict(data)
        errors = ["Field 'test' is required"]

        processor._handle_bad_row(batch, 0, errors)

        assert len(processor.bad_rows) == 1
        bad_row = processor.bad_rows[0]
        assert bad_row["age"] == 25
        assert bad_row["name"] == "Alice"
        assert bad_row["_validation_errors"] == "Field 'test' is required"
        assert bad_row["_error_count"] == 1
        assert "_processed_timestamp" in bad_row

    def test_handle_bad_row_disabled(self):
        """Test _handle_bad_row when bad rows handling is disabled."""
        rule = FieldValidationRule(field_name="test")
        bad_rows_config = BadRowsConfig(enabled=False)
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        data = {"age": [25], "name": ["Alice"]}
        batch = pa.RecordBatch.from_pydict(data)
        errors = ["Field 'test' is required"]

        processor._handle_bad_row(batch, 0, errors)

        assert len(processor.bad_rows) == 0

    def test_handle_bad_row_no_validation_errors(self):
        """Test _handle_bad_row without including validation errors."""
        rule = FieldValidationRule(field_name="test")
        bad_rows_config = BadRowsConfig(enabled=True, include_validation_errors=False)
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        data = {"age": [25], "name": ["Alice"]}
        batch = pa.RecordBatch.from_pydict(data)
        errors = ["Field 'test' is required"]

        processor._handle_bad_row(batch, 0, errors)

        assert len(processor.bad_rows) == 1
        bad_row = processor.bad_rows[0]
        assert bad_row["age"] == 25
        assert bad_row["name"] == "Alice"
        assert "_validation_errors" not in bad_row
        assert "_error_count" not in bad_row
        assert "_processed_timestamp" not in bad_row

    def test_get_bad_rows_batch_no_bad_rows(self):
        """Test get_bad_rows_batch when there are no bad rows."""
        rule = FieldValidationRule(field_name="test")
        bad_rows_config = BadRowsConfig()
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        result = processor.get_bad_rows_batch()
        assert result is None

    def test_get_bad_rows_batch_with_bad_rows(self):
        """Test get_bad_rows_batch with bad rows present."""
        rule = FieldValidationRule(field_name="age", required=True)
        bad_rows_config = BadRowsConfig(include_validation_errors=True)
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        # Add some bad rows
        processor.bad_rows = [
            {
                "age": None,
                "name": "Alice",
                "_validation_errors": "Field 'age' is required",
                "_error_count": 1,
                "_processed_timestamp": "2023-01-01T00:00:00",
            },
            {
                "age": 25,
                "name": "Bob",
                "_validation_errors": "Some error",
                "_error_count": 1,
                "_processed_timestamp": "2023-01-01T00:00:01",
            },
        ]

        result = processor.get_bad_rows_batch()

        assert result is not None
        assert result.num_rows == 2
        assert result.num_columns == 5  # age, name, + 3 error fields

        # Check schema
        expected_fields = [
            "age",
            "name",
            "_validation_errors",
            "_error_count",
            "_processed_timestamp",
        ]
        actual_fields = [field.name for field in result.schema]
        assert set(actual_fields) == set(expected_fields)

    def test_get_bad_rows_batch_type_inference(self):
        """Test get_bad_rows_batch type inference for different data types."""
        rule = FieldValidationRule(field_name="test")
        bad_rows_config = BadRowsConfig(include_validation_errors=False)
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        # Add bad rows with different data types
        processor.bad_rows = [
            {"bool_field": True, "int_field": 42, "float_field": 3.14, "string_field": "hello"}
        ]

        result = processor.get_bad_rows_batch()

        assert result is not None
        schema = result.schema
        assert schema.field("bool_field").type == pa.bool_()
        assert schema.field("int_field").type == pa.int64()
        assert schema.field("float_field").type == pa.float64()
        assert schema.field("string_field").type == pa.string()

    def test_get_validation_summary(self):
        """Test get_validation_summary method."""
        rule1 = FieldValidationRule(field_name="email", unique=True)
        rule2 = FieldValidationRule(field_name="age", required=True)
        bad_rows_config = BadRowsConfig()
        config = ValidationConfig(field_validations=[rule1, rule2], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        # Simulate processing
        processor.total_rows_processed = 100
        processor.bad_rows = [{"error": "test"}] * 5  # 5 bad rows
        processor.unique_value_tracker["email"] = {
            "alice@test.com",
            "bob@test.com",
            "charlie@test.com",
        }

        summary = processor.get_validation_summary()

        assert summary["total_rows_processed"] == 100
        assert summary["bad_rows_count"] == 5
        assert summary["bad_rows_percent"] == 5.0
        assert summary["unique_fields_tracked"] == ["email"]
        assert summary["unique_values_counts"] == {"email": 3}

    def test_get_validation_summary_no_rows(self):
        """Test get_validation_summary with no processed rows."""
        rule = FieldValidationRule(field_name="test")
        bad_rows_config = BadRowsConfig()
        config = ValidationConfig(field_validations=[rule], bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        summary = processor.get_validation_summary()

        assert summary["total_rows_processed"] == 0
        assert summary["bad_rows_count"] == 0
        assert summary["bad_rows_percent"] == 0.0

    def test_validation_with_all_validation_types(self):
        """Test validation with all types of validation rules."""
        range_val = RangeValidation(min_value=18, max_value=100)
        string_val = StringValidation(min_length=2, max_length=50, pattern=r"^[a-zA-Z\s]+$")
        enum_val = EnumValidation(allowed_values=["M", "F", "Other"])
        date_val = DateValidation(min_date="1900-01-01", max_date="2023-12-31")

        rules = [
            FieldValidationRule(field_name="age", required=True, range_validation=range_val),
            FieldValidationRule(field_name="name", required=True, string_validation=string_val),
            FieldValidationRule(field_name="gender", enum_validation=enum_val),
            FieldValidationRule(field_name="birth_date", date_validation=date_val),
            FieldValidationRule(field_name="email", unique=True),
        ]

        bad_rows_config = BadRowsConfig()
        config = ValidationConfig(field_validations=rules, bad_rows_config=bad_rows_config)

        processor = DataValidationProcessor(config)

        # Test data with various validation scenarios
        data = {
            "age": [25, 150, None, 17, 30],  # valid, too high, missing (required), too low, valid
            "name": [
                "John Doe",
                "X",
                "Jane Smith",
                "John123",
                "Alice",
            ],  # valid, too short, valid, invalid pattern, valid
            "gender": ["M", "F", "Invalid", "Other", "M"],  # valid, valid, invalid, valid, valid
            "birth_date": [
                "1990-01-01",
                "1800-01-01",
                "2025-01-01",
                "1995-05-15",
                "2000-12-31",
            ],  # valid, too early, too late, valid, valid
            "email": [
                "john@test.com",
                "jane@test.com",
                "alice@test.com",
                "bob@test.com",
                "john@test.com",
            ],  # valid, valid, valid, valid, duplicate
        }
        batch = pa.RecordBatch.from_pydict(data)

        clean_batch, validation_results = processor.process_batch(batch)

        # Should have validation errors for rows with invalid data
        assert len(validation_results) > 0
        assert clean_batch.num_rows < batch.num_rows

        # Check that we have various types of validation errors
        error_messages = [r.error_message for r in validation_results]
        error_types = set()
        for msg in error_messages:
            if "required" in msg:
                error_types.add("required")
            elif "range" in msg or "below" in msg or "above" in msg:
                error_types.add("range")
            elif "length" in msg or "pattern" in msg:
                error_types.add("string")
            elif "allowed values" in msg:
                error_types.add("enum")
            elif "date" in msg or "before" in msg or "after" in msg:
                error_types.add("date")
            elif "unique" in msg or "duplicate" in msg:
                error_types.add("unique")

        # We should have detected multiple types of validation errors
        assert len(error_types) > 1


class TestDataValidationIntegration:
    """Integration tests for data validation functionality."""

    def test_end_to_end_validation_workflow(self):
        """Test complete validation workflow from configuration to results."""
        # Create comprehensive validation configuration
        rules = [
            FieldValidationRule(
                field_name="user_id",
                required=True,
                unique=True,
                range_validation=RangeValidation(min_value=1, max_value=999999),
            ),
            FieldValidationRule(
                field_name="username",
                required=True,
                string_validation=StringValidation(
                    min_length=3, max_length=20, pattern=r"^[a-zA-Z0-9_]+$", allow_empty=False
                ),
            ),
            FieldValidationRule(
                field_name="status",
                enum_validation=EnumValidation(
                    allowed_values=["active", "inactive", "pending"], case_sensitive=False
                ),
            ),
            FieldValidationRule(
                field_name="created_date",
                date_validation=DateValidation(min_date="2020-01-01", max_date="2030-12-31"),
            ),
        ]

        bad_rows_config = BadRowsConfig(
            enabled=True,
            include_validation_errors=True,
            max_bad_rows_percent=25.0,
            fail_on_exceed_threshold=True,
        )

        config = ValidationConfig(
            field_validations=rules,
            bad_rows_config=bad_rows_config,
            uniqueness_strategy="first_wins",
        )

        processor = DataValidationProcessor(config)

        # Create test data with various validation scenarios
        test_data = {
            "user_id": [
                1,
                2,
                None,
                1000000,
                3,
                2,
            ],  # valid, valid, missing, too high, valid, duplicate
            "username": [
                "alice",
                "bob_123",
                "",
                "x",
                "charlie",
                "invalid@user",
            ],  # valid, valid, empty, too short, valid, invalid chars
            "status": [
                "ACTIVE",
                "inactive",
                "unknown",
                "Pending",
                "active",
                "",
            ],  # valid, valid, invalid, valid, valid, empty (None validation)
            "created_date": [
                "2022-01-01",
                "2019-12-31",
                "invalid",
                "2025-06-15",
                "2021-03-15",
                "2035-01-01",
            ],  # valid, too early, invalid, valid, valid, too late
            "email": [
                "alice@test.com",
                "bob@test.com",
                "charlie@test.com",
                "dave@test.com",
                "eve@test.com",
                "frank@test.com",
            ],  # additional field not validated
        }

        batch = pa.RecordBatch.from_pydict(test_data)

        clean_batch, validation_results = processor.process_batch(batch)

        # Verify results
        assert len(validation_results) > 0  # Should have validation errors
        assert clean_batch.num_rows < batch.num_rows  # Some rows should be filtered out
        assert len(processor.bad_rows) > 0  # Bad rows should be captured

        # Get summary
        summary = processor.get_validation_summary()
        assert summary["total_rows_processed"] == 6
        assert summary["bad_rows_count"] > 0
        assert summary["bad_rows_percent"] > 0
        assert "user_id" in summary["unique_fields_tracked"]

        # Get bad rows batch
        bad_rows_batch = processor.get_bad_rows_batch()
        assert bad_rows_batch is not None
        assert bad_rows_batch.num_rows == len(processor.bad_rows)

        # Verify error fields are included
        schema_names = [field.name for field in bad_rows_batch.schema]
        assert "_validation_errors" in schema_names
        assert "_error_count" in schema_names
        assert "_processed_timestamp" in schema_names
