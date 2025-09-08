"""Comprehensive tests for constraint validator processor module."""

import pytest
import pyarrow as pa
from unittest.mock import patch, MagicMock
from datetime import date, datetime

from forklift.processors.constraint_validator import (
    ConstraintValidator,
    ConstraintConfig,
    ConstraintViolation,
    ErrorMode,
    create_constraint_config_from_schema
)
from forklift.processors.base import ValidationResult


class TestConstraintViolation:
    """Test ConstraintViolation dataclass."""

    def test_constraint_violation_creation(self):
        """Test creating constraint violation."""
        violation = ConstraintViolation(
            column_name="age",
            constraint_type="minimum",
            value=15,
            message="Age must be at least 18"
        )

        assert violation.column_name == "age"
        assert violation.constraint_type == "minimum"
        assert violation.value == 15
        assert violation.message == "Age must be at least 18"

    def test_constraint_violation_with_optional_fields(self):
        """Test creating constraint violation with optional fields."""
        violation = ConstraintViolation(
            column_name="email",
            constraint_type="pattern",
            value="invalid-email",
            message="Invalid email format",
            row_number=5,
            expected_value="valid@email.com"
        )

        assert violation.row_number == 5
        assert violation.expected_value == "valid@email.com"


class TestErrorMode:
    """Test ErrorMode enum."""

    def test_error_mode_values(self):
        """Test ErrorMode enum values."""
        assert ErrorMode.STRICT.value == "strict"
        assert ErrorMode.LENIENT.value == "lenient"
        assert ErrorMode.IGNORE.value == "ignore"


class TestConstraintConfig:
    """Test ConstraintConfig dataclass."""

    def test_default_config(self):
        """Test default constraint configuration."""
        config = ConstraintConfig()

        assert config.field_constraints == {}
        assert config.error_mode == ErrorMode.STRICT
        assert config.max_violations_per_field == 100
        assert config.collect_all_violations is True

    def test_custom_config(self):
        """Test custom constraint configuration."""
        field_constraints = {
            "age": {"minimum": 18, "maximum": 100},
            "name": {"minLength": 1, "maxLength": 50}
        }

        config = ConstraintConfig(
            field_constraints=field_constraints,
            error_mode=ErrorMode.LENIENT,
            max_violations_per_field=50,
            collect_all_violations=False
        )

        assert config.field_constraints == field_constraints
        assert config.error_mode == ErrorMode.LENIENT
        assert config.max_violations_per_field == 50
        assert config.collect_all_violations is False


class TestConstraintValidator:
    """Test ConstraintValidator class."""

    def test_init(self):
        """Test validator initialization."""
        config = ConstraintConfig()
        validator = ConstraintValidator(config)

        assert validator.config == config
        assert validator.violation_counts == {}

    def test_process_batch_no_constraints(self):
        """Test processing batch with no constraints."""
        config = ConstraintConfig()
        validator = ConstraintValidator(config)

        batch = pa.record_batch({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })

        result_batch, validation_results = validator.process_batch(batch)

        assert result_batch == batch
        assert validation_results == []

    def test_process_batch_with_minimum_constraint(self):
        """Test processing batch with minimum value constraint."""
        config = ConstraintConfig(
            field_constraints={
                "age": {"minimum": 18}
            }
        )
        validator = ConstraintValidator(config)

        batch = pa.record_batch({
            'age': [16, 25, 17, 30],  # 16 and 17 violate minimum
            'name': ['Alice', 'Bob', 'Charlie', 'David']
        })

        result_batch, validation_results = validator.process_batch(batch)

        # Should have violations for ages 16 and 17
        assert len(validation_results) == 2
        for result in validation_results:
            assert "minimum" in result.error_message.lower()
            assert result.error_code == "CONSTRAINT_VIOLATION"

    def test_process_batch_with_maximum_constraint(self):
        """Test processing batch with maximum value constraint."""
        config = ConstraintConfig(
            field_constraints={
                "score": {"maximum": 100}
            }
        )
        validator = ConstraintValidator(config)

        batch = pa.record_batch({
            'score': [95, 105, 85, 110],  # 105 and 110 violate maximum
            'name': ['Alice', 'Bob', 'Charlie', 'David']
        })

        result_batch, validation_results = validator.process_batch(batch)

        # Should have violations for scores 105 and 110
        assert len(validation_results) == 2

    def test_process_batch_with_min_length_constraint(self):
        """Test processing batch with minimum length constraint."""
        config = ConstraintConfig(
            field_constraints={
                "name": {"minLength": 3}
            }
        )
        validator = ConstraintValidator(config)

        batch = pa.record_batch({
            'name': ['Al', 'Bob', 'X', 'Alice'],  # 'Al' and 'X' violate minLength
            'id': [1, 2, 3, 4]
        })

        result_batch, validation_results = validator.process_batch(batch)

        # Should have violations for 'Al' and 'X'
        assert len(validation_results) == 2
        for result in validation_results:
            assert "minlength" in result.error_message.lower()

    def test_process_batch_with_max_length_constraint(self):
        """Test processing batch with maximum length constraint."""
        config = ConstraintConfig(
            field_constraints={
                "code": {"maxLength": 5}
            }
        )
        validator = ConstraintValidator(config)

        batch = pa.record_batch({
            'code': ['ABC', 'DEFGH', 'TOOLONG', 'XY'],  # 'TOOLONG' violates maxLength
            'id': [1, 2, 3, 4]
        })

        result_batch, validation_results = validator.process_batch(batch)

        # Should have violation for 'TOOLONG'
        assert len(validation_results) == 1
        assert "maxlength" in validation_results[0].error_message.lower()

    def test_process_batch_with_pattern_constraint(self):
        """Test processing batch with pattern constraint."""
        config = ConstraintConfig(
            field_constraints={
                "email": {"pattern": r"^[^@]+@[^@]+\.[^@]+$"}
            }
        )
        validator = ConstraintValidator(config)

        batch = pa.record_batch({
            'email': ['valid@test.com', 'invalid-email', 'another@valid.org', 'bad'],
            'id': [1, 2, 3, 4]
        })

        result_batch, validation_results = validator.process_batch(batch)

        # Should have violations for 'invalid-email' and 'bad'
        assert len(validation_results) == 2
        for result in validation_results:
            assert "pattern" in result.error_message.lower()

    def test_process_batch_with_enum_constraint(self):
        """Test processing batch with enum constraint."""
        config = ConstraintConfig(
            field_constraints={
                "status": {"enum": ["active", "inactive", "pending"]}
            }
        )
        validator = ConstraintValidator(config)

        batch = pa.record_batch({
            'status': ['active', 'invalid', 'pending', 'unknown'],
            'id': [1, 2, 3, 4]
        })

        result_batch, validation_results = validator.process_batch(batch)

        # Should have violations for 'invalid' and 'unknown'
        assert len(validation_results) == 2
        for result in validation_results:
            assert "enum" in result.error_message.lower()

    def test_process_batch_with_required_constraint(self):
        """Test processing batch with required constraint."""
        config = ConstraintConfig(
            field_constraints={
                "name": {"required": True}
            }
        )
        validator = ConstraintValidator(config)

        batch = pa.record_batch({
            'name': ['Alice', None, 'Charlie', None],
            'id': [1, 2, 3, 4]
        })

        result_batch, validation_results = validator.process_batch(batch)

        # Should have violations for the two None values
        assert len(validation_results) == 2
        for result in validation_results:
            assert "required" in result.error_message.lower()

    def test_process_batch_multiple_constraints_same_field(self):
        """Test processing batch with multiple constraints on same field."""
        config = ConstraintConfig(
            field_constraints={
                "age": {"minimum": 18, "maximum": 65, "required": True}
            }
        )
        validator = ConstraintValidator(config)

        batch = pa.record_batch({
            'age': [16, 25, 70, None],  # Multiple violations
            'id': [1, 2, 3, 4]
        })

        result_batch, validation_results = validator.process_batch(batch)

        # Should have violations for 16 (min), 70 (max), and None (required)
        assert len(validation_results) == 3

    def test_process_batch_lenient_mode(self):
        """Test processing batch in lenient error mode."""
        config = ConstraintConfig(
            field_constraints={
                "age": {"minimum": 18}
            },
            error_mode=ErrorMode.LENIENT
        )
        validator = ConstraintValidator(config)

        batch = pa.record_batch({
            'age': [16, 25, 17],
            'id': [1, 2, 3]
        })

        result_batch, validation_results = validator.process_batch(batch)

        # Should have warnings instead of errors
        assert len(validation_results) == 2
        for result in validation_results:
            assert result.is_valid is True  # Warnings in lenient mode

    def test_process_batch_ignore_mode(self):
        """Test processing batch in ignore error mode."""
        config = ConstraintConfig(
            field_constraints={
                "age": {"minimum": 18}
            },
            error_mode=ErrorMode.IGNORE
        )
        validator = ConstraintValidator(config)

        batch = pa.record_batch({
            'age': [16, 25, 17],
            'id': [1, 2, 3]
        })

        result_batch, validation_results = validator.process_batch(batch)

        # Should have no validation results in ignore mode
        assert validation_results == []

    def test_process_batch_max_violations_limit(self):
        """Test processing batch with max violations limit."""
        config = ConstraintConfig(
            field_constraints={
                "age": {"minimum": 18}
            },
            max_violations_per_field=2
        )
        validator = ConstraintValidator(config)

        batch = pa.record_batch({
            'age': [16, 17, 15, 14, 13],  # 5 violations, but limit is 2
            'id': [1, 2, 3, 4, 5]
        })

        result_batch, validation_results = validator.process_batch(batch)

        # Should only have 2 violations due to limit
        assert len(validation_results) == 2

    def test_process_batch_with_nulls_in_non_required_field(self):
        """Test processing batch with nulls in non-required field."""
        config = ConstraintConfig(
            field_constraints={
                "age": {"minimum": 18}  # Not required, so nulls should be skipped
            }
        )
        validator = ConstraintValidator(config)

        batch = pa.record_batch({
            'age': [None, 25, None, 30],
            'id': [1, 2, 3, 4]
        })

        result_batch, validation_results = validator.process_batch(batch)

        # Should have no violations (nulls are skipped for non-required fields)
        assert validation_results == []

    def test_get_violation_summary(self):
        """Test getting violation summary."""
        config = ConstraintConfig(
            field_constraints={
                "age": {"minimum": 18}
            }
        )
        validator = ConstraintValidator(config)

        # Process a batch to generate violations
        batch = pa.record_batch({
            'age': [16, 25, 17],
            'id': [1, 2, 3]
        })
        validator.process_batch(batch)

        summary = validator.get_violation_summary()

        assert isinstance(summary, dict)
        assert "total_violations" in summary
        assert "violations_by_field" in summary
        assert "violations_by_constraint" in summary
        assert summary["total_violations"] == 2

    def test_constraint_violation_with_date_values(self):
        """Test constraint validation with date values."""
        config = ConstraintConfig(
            field_constraints={
                "birth_date": {"minimum": "1900-01-01", "maximum": "2023-12-31"}
            }
        )
        validator = ConstraintValidator(config)

        batch = pa.record_batch({
            'birth_date': [date(1899, 12, 31), date(1990, 5, 15), date(2024, 1, 1)],
            'id': [1, 2, 3]
        })

        result_batch, validation_results = validator.process_batch(batch)

        # Should have violations for dates outside range
        assert len(validation_results) == 2

    def test_missing_column_handling(self):
        """Test handling of constraints for missing columns."""
        config = ConstraintConfig(
            field_constraints={
                "missing_column": {"minimum": 10}
            }
        )
        validator = ConstraintValidator(config)

        batch = pa.record_batch({
            'existing_column': [1, 2, 3]
        })

        result_batch, validation_results = validator.process_batch(batch)

        # Should handle missing columns gracefully
        assert result_batch == batch
        assert validation_results == []


class TestCreateConstraintConfigFromSchema:
    """Test create_constraint_config_from_schema function."""

    def test_create_config_from_schema_basic(self):
        """Test creating constraint config from basic schema."""
        schema_dict = {
            "type": "object",
            "properties": {
                "age": {
                    "type": "integer",
                    "minimum": 18,
                    "maximum": 65
                },
                "name": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 100
                }
            },
            "required": ["name"]
        }

        config = create_constraint_config_from_schema(schema_dict)

        assert isinstance(config, ConstraintConfig)
        assert "age" in config.field_constraints
        assert "name" in config.field_constraints
        assert config.field_constraints["age"]["minimum"] == 18
        assert config.field_constraints["age"]["maximum"] == 65
        assert config.field_constraints["name"]["minLength"] == 1
        assert config.field_constraints["name"]["maxLength"] == 100
        assert config.field_constraints["name"]["required"] is True

    def test_create_config_from_schema_with_patterns(self):
        """Test creating constraint config from schema with patterns."""
        schema_dict = {
            "type": "object",
            "properties": {
                "email": {
                    "type": "string",
                    "pattern": r"^[^@]+@[^@]+\.[^@]+$"
                },
                "phone": {
                    "type": "string",
                    "pattern": r"^\d{3}-\d{3}-\d{4}$"
                }
            }
        }

        config = create_constraint_config_from_schema(schema_dict)

        assert config.field_constraints["email"]["pattern"] == r"^[^@]+@[^@]+\.[^@]+$"
        assert config.field_constraints["phone"]["pattern"] == r"^\d{3}-\d{3}-\d{4}$"

    def test_create_config_from_schema_with_enums(self):
        """Test creating constraint config from schema with enums."""
        schema_dict = {
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "enum": ["active", "inactive", "pending"]
                },
                "priority": {
                    "type": "integer",
                    "enum": [1, 2, 3, 4, 5]
                }
            }
        }

        config = create_constraint_config_from_schema(schema_dict)

        assert config.field_constraints["status"]["enum"] == ["active", "inactive", "pending"]
        assert config.field_constraints["priority"]["enum"] == [1, 2, 3, 4, 5]

    def test_create_config_from_empty_schema(self):
        """Test creating constraint config from empty schema."""
        schema_dict = {}

        config = create_constraint_config_from_schema(schema_dict)

        assert isinstance(config, ConstraintConfig)
        assert config.field_constraints == {}

    def test_create_config_from_none_schema(self):
        """Test creating constraint config from None schema."""
        config = create_constraint_config_from_schema(None)

        assert isinstance(config, ConstraintConfig)
        assert config.field_constraints == {}


class TestConstraintValidatorIntegration:
    """Test constraint validator integration scenarios."""

    def test_real_world_user_data_validation(self):
        """Test real-world user data validation scenario."""
        config = ConstraintConfig(
            field_constraints={
                "user_id": {"required": True, "minimum": 1},
                "email": {"required": True, "pattern": r"^[^@]+@[^@]+\.[^@]+$"},
                "age": {"minimum": 13, "maximum": 120},
                "status": {"enum": ["active", "inactive", "suspended"]},
                "username": {"minLength": 3, "maxLength": 20}
            }
        )
        validator = ConstraintValidator(config)

        batch = pa.record_batch({
            'user_id': [1, 0, 3, None],  # 0 and None violate constraints
            'email': ['user1@test.com', 'invalid', 'user3@test.com', 'user4@test.com'],
            'age': [25, 150, 30, 12],  # 150 and 12 violate constraints
            'status': ['active', 'banned', 'inactive', 'active'],  # 'banned' violates enum
            'username': ['abc', 'x', 'validuser', 'verylongusernamethatisinvalid']  # 'x' and long name violate
        })

        result_batch, validation_results = validator.process_batch(batch)

        # Should detect multiple constraint violations
        assert len(validation_results) > 5

        # Check specific violation types
        violation_types = [r.error_message for r in validation_results]
        assert any("minimum" in msg.lower() for msg in violation_types)
        assert any("maximum" in msg.lower() for msg in violation_types)
        assert any("required" in msg.lower() for msg in violation_types)
        assert any("pattern" in msg.lower() for msg in violation_types)
        assert any("enum" in msg.lower() for msg in violation_types)
        assert any("minlength" in msg.lower() for msg in violation_types)
        assert any("maxlength" in msg.lower() for msg in violation_types)

    def test_module_imports(self):
        """Test that all components can be imported."""
        from forklift.processors.constraint_validator import (
            ConstraintValidator,
            ConstraintConfig,
            ConstraintViolation,
            ErrorMode,
            create_constraint_config_from_schema
        )

        assert ConstraintValidator is not None
        assert ConstraintConfig is not None
        assert ConstraintViolation is not None
        assert ErrorMode is not None
        assert callable(create_constraint_config_from_schema)

    def test_module_docstring(self):
        """Test module documentation."""
        import forklift.processors.constraint_validator as constraint_module

        assert constraint_module.__doc__ is not None

    def test_processor_inheritance(self):
        """Test that processor inherits from BaseProcessor."""
        from forklift.processors.base import BaseProcessor
        from forklift.processors.constraint_validator import ConstraintValidator

        config = ConstraintConfig()
        validator = ConstraintValidator(config)
        assert isinstance(validator, BaseProcessor)
