"""Tests for constraint validation functionality."""

import pytest
import pyarrow as pa
from unittest.mock import Mock, patch

from forklift.processors.constraint_validator import (
    ErrorMode,
    ConstraintConfig,
    ConstraintViolation,
    ConstraintValidator,
    create_constraint_config_from_schema
)


class TestErrorMode:
    """Test the ErrorMode enum."""

    def test_error_mode_values(self):
        """Test that ErrorMode has correct values."""
        assert ErrorMode.FAIL_FAST.value == "fail_fast"
        assert ErrorMode.FAIL_COMPLETE.value == "fail_complete"
        assert ErrorMode.BAD_ROWS.value == "bad_rows"


class TestConstraintConfig:
    """Test the ConstraintConfig dataclass."""

    def test_constraint_config_defaults(self):
        """Test default values for ConstraintConfig."""
        config = ConstraintConfig()

        assert config.error_mode == ErrorMode.BAD_ROWS
        assert config.check_constraints == {}
        assert config.unique_constraints == []
        assert config.foreign_key_constraints == {}

    def test_constraint_config_with_none_values(self):
        """Test that None values are properly initialized in __post_init__."""
        config = ConstraintConfig(
            check_constraints=None,
            unique_constraints=None,
            foreign_key_constraints=None
        )

        assert config.check_constraints == {}
        assert config.unique_constraints == []
        assert config.foreign_key_constraints == {}

    def test_constraint_config_with_custom_values(self):
        """Test ConstraintConfig with custom values."""
        check_constraints = {"test_constraint": {"column": "test", "min": 0}}
        unique_constraints = ["id", "email"]
        foreign_key_constraints = {"fk_test": {"table": "other", "column": "id"}}

        config = ConstraintConfig(
            error_mode=ErrorMode.FAIL_FAST,
            check_constraints=check_constraints,
            unique_constraints=unique_constraints,
            foreign_key_constraints=foreign_key_constraints
        )

        assert config.error_mode == ErrorMode.FAIL_FAST
        assert config.check_constraints == check_constraints
        assert config.unique_constraints == unique_constraints
        assert config.foreign_key_constraints == foreign_key_constraints


class TestConstraintViolation:
    """Test the ConstraintViolation dataclass."""

    def test_constraint_violation_creation(self):
        """Test creating a ConstraintViolation."""
        violation = ConstraintViolation(
            violation_type="range",
            error_message="Value out of range",
            columns=["age"],
            values=[150],
            constraint_name="age_range",
            row_index=5
        )

        assert violation.violation_type == "range"
        assert violation.error_message == "Value out of range"
        assert violation.columns == ["age"]
        assert violation.values == [150]
        assert violation.constraint_name == "age_range"
        assert violation.row_index == 5

    def test_constraint_violation_optional_row_index(self):
        """Test ConstraintViolation with optional row_index."""
        violation = ConstraintViolation(
            violation_type="unique",
            error_message="Duplicate value",
            columns=["email"],
            values=["test@example.com"],
            constraint_name="email_unique"
        )

        assert violation.row_index is None


class TestConstraintValidator:
    """Test the ConstraintValidator class."""

    def test_constraint_validator_initialization(self):
        """Test ConstraintValidator initialization."""
        config = ConstraintConfig()
        validator = ConstraintValidator(config)

        assert validator.config == config
        assert validator.violations == []

    def test_process_batch_basic(self):
        """Test basic batch processing."""
        config = ConstraintConfig()
        validator = ConstraintValidator(config)

        # Create a simple batch
        schema = pa.schema([
            pa.field('id', pa.int64()),
            pa.field('name', pa.string())
        ])
        batch = pa.record_batch([
            [1, 2, 3],
            ['Alice', 'Bob', 'Charlie']
        ], schema=schema)

        result_batch, validation_results = validator.process_batch(batch)

        assert result_batch == batch
        assert validation_results == []
        assert validator.violations == []

    def test_get_all_violations_empty(self):
        """Test get_all_violations when no violations exist."""
        config = ConstraintConfig()
        validator = ConstraintValidator(config)

        violations = validator.get_all_violations()
        assert violations == []

    def test_get_all_violations_with_data(self):
        """Test get_all_violations returns a copy of violations."""
        config = ConstraintConfig()
        validator = ConstraintValidator(config)

        # Manually add some violations for testing
        violation = ConstraintViolation(
            violation_type="test",
            error_message="Test violation",
            columns=["test_col"],
            values=["test_val"],
            constraint_name="test_constraint"
        )
        validator.violations.append(violation)

        violations = validator.get_all_violations()
        assert len(violations) == 1
        assert violations[0] == violation

        # Verify it's a copy
        violations.append(ConstraintViolation(
            violation_type="test2",
            error_message="Test violation 2",
            columns=["test_col2"],
            values=["test_val2"],
            constraint_name="test_constraint2"
        ))
        assert len(validator.violations) == 1  # Original should be unchanged

    def test_finalize_no_violations(self):
        """Test finalize with no violations."""
        config = ConstraintConfig(error_mode=ErrorMode.FAIL_FAST)
        validator = ConstraintValidator(config)

        # Should not raise any exception
        validator.finalize()

    def test_finalize_with_violations_bad_rows_mode(self):
        """Test finalize with violations in BAD_ROWS mode."""
        config = ConstraintConfig(error_mode=ErrorMode.BAD_ROWS)
        validator = ConstraintValidator(config)

        # Add a violation
        validator.violations.append(ConstraintViolation(
            violation_type="test",
            error_message="Test violation",
            columns=["test_col"],
            values=["test_val"],
            constraint_name="test_constraint"
        ))

        # Should not raise exception in BAD_ROWS mode
        validator.finalize()

    def test_finalize_with_violations_fail_fast_mode(self):
        """Test finalize with violations in FAIL_FAST mode."""
        config = ConstraintConfig(error_mode=ErrorMode.FAIL_FAST)
        validator = ConstraintValidator(config)

        # Add violations
        validator.violations.extend([
            ConstraintViolation(
                violation_type="test1",
                error_message="Test violation 1",
                columns=["test_col1"],
                values=["test_val1"],
                constraint_name="test_constraint1"
            ),
            ConstraintViolation(
                violation_type="test2",
                error_message="Test violation 2",
                columns=["test_col2"],
                values=["test_val2"],
                constraint_name="test_constraint2"
            )
        ])

        with pytest.raises(ValueError, match="Constraint validation failed with 2 violations"):
            validator.finalize()

    def test_finalize_with_violations_fail_complete_mode(self):
        """Test finalize with violations in FAIL_COMPLETE mode."""
        config = ConstraintConfig(error_mode=ErrorMode.FAIL_COMPLETE)
        validator = ConstraintValidator(config)

        # Add a violation
        validator.violations.append(ConstraintViolation(
            violation_type="test",
            error_message="Test violation",
            columns=["test_col"],
            values=["test_val"],
            constraint_name="test_constraint"
        ))

        with pytest.raises(ValueError, match="Constraint validation failed with 1 violations"):
            validator.finalize()


class TestCreateConstraintConfigFromSchema:
    """Test the create_constraint_config_from_schema function."""

    def test_create_config_empty_schema(self):
        """Test creating config from empty schema."""
        schema_dict = {}
        config = create_constraint_config_from_schema(schema_dict)

        assert config.error_mode == ErrorMode.BAD_ROWS
        assert config.check_constraints == {}
        assert config.unique_constraints == []
        assert config.foreign_key_constraints == {}

    def test_create_config_with_error_mode(self):
        """Test creating config with specified error mode."""
        schema_dict = {
            "x-constraintHandling": {
                "errorMode": "fail_fast"
            }
        }
        config = create_constraint_config_from_schema(schema_dict)

        assert config.error_mode == ErrorMode.FAIL_FAST

    def test_create_config_with_invalid_error_mode(self):
        """Test creating config with invalid error mode falls back to BAD_ROWS."""
        schema_dict = {
            "x-constraintHandling": {
                "errorMode": "invalid_mode"
            }
        }
        config = create_constraint_config_from_schema(schema_dict)

        assert config.error_mode == ErrorMode.BAD_ROWS

    def test_create_config_with_range_constraints(self):
        """Test creating config with minimum/maximum constraints."""
        schema_dict = {
            "properties": {
                "age": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 120
                },
                "score": {
                    "type": "number",
                    "minimum": 0.0
                },
                "name": {
                    "type": "string"
                }
            }
        }
        config = create_constraint_config_from_schema(schema_dict)

        assert "age_range" in config.check_constraints
        assert config.check_constraints["age_range"]["column"] == "age"
        assert config.check_constraints["age_range"]["min"] == 0
        assert config.check_constraints["age_range"]["max"] == 120

        assert "score_range" in config.check_constraints
        assert config.check_constraints["score_range"]["column"] == "score"
        assert config.check_constraints["score_range"]["min"] == 0.0
        assert config.check_constraints["score_range"]["max"] is None

    def test_create_config_with_unique_constraints(self):
        """Test creating config with unique constraints."""
        schema_dict = {
            "properties": {
                "id": {
                    "type": "integer",
                    "x-unique": True
                },
                "email": {
                    "type": "string",
                    "x-unique": True
                },
                "name": {
                    "type": "string",
                    "x-unique": False
                },
                "description": {
                    "type": "string"
                }
            }
        }
        config = create_constraint_config_from_schema(schema_dict)

        assert "id" in config.unique_constraints
        assert "email" in config.unique_constraints
        assert "name" not in config.unique_constraints
        assert "description" not in config.unique_constraints
        assert len(config.unique_constraints) == 2

    def test_create_config_complex_schema(self):
        """Test creating config with complex schema containing multiple constraint types."""
        schema_dict = {
            "x-constraintHandling": {
                "errorMode": "fail_complete"
            },
            "properties": {
                "id": {
                    "type": "integer",
                    "minimum": 1,
                    "x-unique": True
                },
                "age": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 150
                },
                "email": {
                    "type": "string",
                    "x-unique": True
                },
                "salary": {
                    "type": "number",
                    "minimum": 0
                },
                "name": {
                    "type": "string"
                }
            }
        }
        config = create_constraint_config_from_schema(schema_dict)

        # Check error mode
        assert config.error_mode == ErrorMode.FAIL_COMPLETE

        # Check range constraints
        assert "id_range" in config.check_constraints
        assert "age_range" in config.check_constraints
        assert "salary_range" in config.check_constraints
        assert len(config.check_constraints) == 3

        # Check unique constraints
        assert "id" in config.unique_constraints
        assert "email" in config.unique_constraints
        assert len(config.unique_constraints) == 2

        # Check foreign key constraints (should be empty)
        assert config.foreign_key_constraints == {}

    def test_create_config_no_properties(self):
        """Test creating config from schema without properties."""
        schema_dict = {
            "x-constraintHandling": {
                "errorMode": "fail_fast"
            },
            "type": "object"
        }
        config = create_constraint_config_from_schema(schema_dict)

        assert config.error_mode == ErrorMode.FAIL_FAST
        assert config.check_constraints == {}
        assert config.unique_constraints == []
        assert config.foreign_key_constraints == {}

    def test_create_config_missing_constraint_handling(self):
        """Test creating config when x-constraintHandling is missing."""
        schema_dict = {
            "properties": {
                "test_field": {
                    "type": "string"
                }
            }
        }
        config = create_constraint_config_from_schema(schema_dict)

        assert config.error_mode == ErrorMode.BAD_ROWS

    def test_create_config_constraint_handling_without_error_mode(self):
        """Test creating config when x-constraintHandling exists but errorMode is missing."""
        schema_dict = {
            "x-constraintHandling": {
                "someOtherSetting": "value"
            },
            "properties": {
                "test_field": {
                    "type": "string"
                }
            }
        }
        config = create_constraint_config_from_schema(schema_dict)

        assert config.error_mode == ErrorMode.BAD_ROWS


class TestIntegration:
    """Integration tests for constraint validation components."""

    def test_end_to_end_constraint_validation_workflow(self):
        """Test complete workflow from schema to validation."""
        # Define a schema with constraints
        schema_dict = {
            "x-constraintHandling": {
                "errorMode": "fail_fast"
            },
            "properties": {
                "id": {
                    "type": "integer",
                    "minimum": 1,
                    "x-unique": True
                },
                "age": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 120
                }
            }
        }

        # Create config from schema
        config = create_constraint_config_from_schema(schema_dict)

        # Create validator
        validator = ConstraintValidator(config)

        # Create test batch
        schema = pa.schema([
            pa.field('id', pa.int64()),
            pa.field('age', pa.int64())
        ])
        batch = pa.record_batch([
            [1, 2, 3],
            [25, 30, 35]
        ], schema=schema)

        # Process batch
        result_batch, validation_results = validator.process_batch(batch)

        # Verify results
        assert result_batch == batch
        assert validation_results == []
        assert validator.get_all_violations() == []

        # Finalize should not raise (no violations)
        validator.finalize()
