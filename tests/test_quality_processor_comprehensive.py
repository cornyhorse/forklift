"""Comprehensive tests for quality processor module."""

import pytest
import pyarrow as pa
from forklift.processors.quality import DataQualityProcessor
from forklift.processors.base import ValidationResult


class TestDataQualityProcessor:
    """Test DataQualityProcessor class."""

    def test_init(self):
        """Test processor initialization."""
        rules = {
            "column_rules": {
                "name": {"min_length": 1, "max_length": 50},
                "email": {"pattern": r"^[^@]+@[^@]+\.[^@]+$"}
            }
        }
        processor = DataQualityProcessor(rules)
        assert processor.rules == rules

    def test_process_batch_no_rules(self):
        """Test processing batch with no rules."""
        processor = DataQualityProcessor({})

        batch = pa.record_batch({
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        })

        result_batch, validation_results = processor.process_batch(batch)

        assert result_batch == batch
        assert validation_results == []

    def test_process_batch_with_column_rules(self):
        """Test processing batch with column rules."""
        rules = {
            "column_rules": {
                "name": {"min_length": 2, "max_length": 10},
                "email": {"pattern": r"^[^@]+@[^@]+\.[^@]+$"}
            }
        }
        processor = DataQualityProcessor(rules)

        batch = pa.record_batch({
            'name': ['A', 'Bob', 'VeryLongNameThatExceedsLimit'],
            'email': ['invalid', 'bob@test.com', 'charlie@example.org']
        })

        result_batch, validation_results = processor.process_batch(batch)

        assert result_batch == batch
        assert len(validation_results) > 0

        # Check for string length violations
        length_violations = [r for r in validation_results if "length" in r.error_message.lower()]
        assert len(length_violations) > 0

        # Check for pattern violations
        pattern_violations = [r for r in validation_results if "pattern" in r.error_message.lower()]
        assert len(pattern_violations) > 0

    def test_apply_column_rules_string_length(self):
        """Test applying string length rules to a column."""
        rules = {"min_length": 3, "max_length": 8}
        processor = DataQualityProcessor({})
        validation_results = []

        column = pa.array(['ab', 'abc', 'verylongstring', 'good'])

        processor._apply_column_rules(column, rules, "test_column", validation_results)

        # Should have violations for 'ab' (too short) and 'verylongstring' (too long)
        assert len(validation_results) == 2

    def test_apply_column_rules_pattern(self):
        """Test applying pattern rules to a column."""
        rules = {"pattern": r"^[A-Z][a-z]+$"}  # Capitalized words
        processor = DataQualityProcessor({})
        validation_results = []

        column = pa.array(['Alice', 'bob', 'CHARLIE', 'David'])

        processor._apply_column_rules(column, rules, "name_column", validation_results)

        # Should have violations for 'bob' and 'CHARLIE'
        assert len(validation_results) == 2

    def test_apply_column_rules_numeric_range(self):
        """Test applying numeric range rules to a column."""
        rules = {"min_value": 18, "max_value": 65}
        processor = DataQualityProcessor({})
        validation_results = []

        column = pa.array([16, 25, 45, 70])

        processor._apply_column_rules(column, rules, "age_column", validation_results)

        # Should have violations for 16 (too low) and 70 (too high)
        assert len(validation_results) == 2

    def test_validate_string_length_min_length(self):
        """Test string length validation for minimum length."""
        rules = {"min_length": 5}
        processor = DataQualityProcessor({})
        validation_results = []

        column = pa.array(['abc', 'abcde', 'abcdef'])

        processor._validate_string_length(column, rules, "test_column", validation_results)

        # Should have one violation for 'abc'
        assert len(validation_results) == 1
        assert validation_results[0].error_code == "STRING_TOO_SHORT"

    def test_validate_string_length_max_length(self):
        """Test string length validation for maximum length."""
        rules = {"max_length": 5}
        processor = DataQualityProcessor({})
        validation_results = []

        column = pa.array(['abc', 'abcde', 'abcdef'])

        processor._validate_string_length(column, rules, "test_column", validation_results)

        # Should have one violation for 'abcdef'
        assert len(validation_results) == 1
        assert validation_results[0].error_code == "STRING_TOO_LONG"

    def test_validate_string_length_both_limits(self):
        """Test string length validation for both min and max length."""
        rules = {"min_length": 3, "max_length": 5}
        processor = DataQualityProcessor({})
        validation_results = []

        column = pa.array(['ab', 'abc', 'abcde', 'abcdef'])

        processor._validate_string_length(column, rules, "test_column", validation_results)

        # Should have violations for 'ab' and 'abcdef'
        assert len(validation_results) == 2

    def test_validate_string_length_with_nulls(self):
        """Test string length validation with null values."""
        rules = {"min_length": 3}
        processor = DataQualityProcessor({})
        validation_results = []

        column = pa.array(['abc', None, 'ab'])

        processor._validate_string_length(column, rules, "test_column", validation_results)

        # Should have one violation for 'ab', nulls should be skipped
        assert len(validation_results) == 1

    def test_validate_pattern_valid_pattern(self):
        """Test pattern validation with valid pattern."""
        processor = DataQualityProcessor({})
        validation_results = []

        column = pa.array(['test@email.com', 'user@domain.org', 'invalid-email'])
        pattern = r'^[^@]+@[^@]+\.[^@]+$'

        processor._validate_pattern(column, pattern, "email_column", validation_results)

        # Should have one violation for 'invalid-email'
        assert len(validation_results) == 1
        assert validation_results[0].error_code == "PATTERN_MISMATCH"

    def test_validate_pattern_with_nulls(self):
        """Test pattern validation with null values."""
        processor = DataQualityProcessor({})
        validation_results = []

        column = pa.array(['valid@email.com', None, 'invalid'])
        pattern = r'^[^@]+@[^@]+\.[^@]+$'

        processor._validate_pattern(column, pattern, "email_column", validation_results)

        # Should have one violation for 'invalid', nulls should be skipped
        assert len(validation_results) == 1

    def test_validate_pattern_invalid_regex(self):
        """Test pattern validation with invalid regex."""
        processor = DataQualityProcessor({})
        validation_results = []

        column = pa.array(['test'])
        invalid_pattern = r'[unclosed'

        processor._validate_pattern(column, invalid_pattern, "test_column", validation_results)

        # Should have a validation error for invalid regex
        assert len(validation_results) == 1
        assert "regex" in validation_results[0].error_message.lower()

    def test_validate_numeric_range_min_value(self):
        """Test numeric range validation for minimum value."""
        rules = {"min_value": 10}
        processor = DataQualityProcessor({})
        validation_results = []

        column = pa.array([5, 10, 15])

        processor._validate_numeric_range(column, rules, "number_column", validation_results)

        # Should have one violation for 5
        assert len(validation_results) == 1
        assert validation_results[0].error_code == "VALUE_TOO_LOW"

    def test_validate_numeric_range_max_value(self):
        """Test numeric range validation for maximum value."""
        rules = {"max_value": 10}
        processor = DataQualityProcessor({})
        validation_results = []

        column = pa.array([5, 10, 15])

        processor._validate_numeric_range(column, rules, "number_column", validation_results)

        # Should have one violation for 15
        assert len(validation_results) == 1
        assert validation_results[0].error_code == "VALUE_TOO_HIGH"

    def test_validate_numeric_range_both_limits(self):
        """Test numeric range validation for both min and max values."""
        rules = {"min_value": 10, "max_value": 20}
        processor = DataQualityProcessor({})
        validation_results = []

        column = pa.array([5, 15, 25])

        processor._validate_numeric_range(column, rules, "number_column", validation_results)

        # Should have violations for 5 and 25
        assert len(validation_results) == 2

    def test_validate_numeric_range_with_nulls(self):
        """Test numeric range validation with null values."""
        rules = {"min_value": 10}
        processor = DataQualityProcessor({})
        validation_results = []

        column = pa.array([5, None, 15])

        processor._validate_numeric_range(column, rules, "number_column", validation_results)

        # Should have one violation for 5, nulls should be skipped
        assert len(validation_results) == 1

    def test_process_batch_missing_column(self):
        """Test processing batch when rule references missing column."""
        rules = {
            "column_rules": {
                "missing_column": {"min_length": 1}
            }
        }
        processor = DataQualityProcessor(rules)

        batch = pa.record_batch({
            'existing_column': ['value1', 'value2']
        })

        result_batch, validation_results = processor.process_batch(batch)

        # Should not error, just skip the missing column
        assert result_batch == batch
        assert validation_results == []

    def test_complex_quality_rules(self):
        """Test complex quality rules scenario."""
        rules = {
            "column_rules": {
                "name": {"min_length": 2, "max_length": 20, "pattern": r"^[A-Za-z\s]+$"},
                "age": {"min_value": 0, "max_value": 150},
                "email": {"pattern": r"^[^@]+@[^@]+\.[^@]+$"}
            }
        }
        processor = DataQualityProcessor(rules)

        batch = pa.record_batch({
            'name': ['A', 'John Doe', 'Jane123', 'VeryVeryVeryLongNameThatExceedsLimit'],
            'age': [-5, 25, 45, 200],
            'email': ['invalid', 'john@test.com', 'jane@example.org', 'bad-email']
        })

        result_batch, validation_results = processor.process_batch(batch)

        assert result_batch == batch
        assert len(validation_results) > 0

        # Verify different types of violations are caught
        error_codes = [r.error_code for r in validation_results]
        assert "STRING_TOO_SHORT" in error_codes  # 'A'
        assert "STRING_TOO_LONG" in error_codes   # long name
        assert "PATTERN_MISMATCH" in error_codes  # 'Jane123', invalid emails
        assert "VALUE_TOO_LOW" in error_codes     # -5
        assert "VALUE_TOO_HIGH" in error_codes    # 200


class TestDataQualityProcessorIntegration:
    """Test data quality processor integration scenarios."""

    def test_module_imports(self):
        """Test that all components can be imported."""
        from forklift.processors.quality import DataQualityProcessor

        assert DataQualityProcessor is not None
        assert callable(DataQualityProcessor)

    def test_module_docstring(self):
        """Test module documentation."""
        import forklift.processors.quality as quality_module

        assert quality_module.__doc__ is not None
        assert "Data quality processor" in quality_module.__doc__

    def test_processor_inheritance(self):
        """Test that processor inherits from BaseProcessor."""
        from forklift.processors.base import BaseProcessor
        from forklift.processors.quality import DataQualityProcessor

        processor = DataQualityProcessor({})
        assert isinstance(processor, BaseProcessor)
