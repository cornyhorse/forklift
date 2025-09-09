"""Comprehensive unit tests for validation_factory.py"""

import pytest
from unittest.mock import Mock, patch
import pyarrow as pa

from forklift.processors.validation_factory import (
    ValidationFactory,
    ValidatorType,
    ValidationFactoryConfig
)
from forklift.processors.constraint_validator import ConstraintValidator, ErrorMode
from forklift.processors.schema_validator import SchemaValidator


class TestValidatorType:
    """Test ValidatorType enum."""

    def test_validator_type_values(self):
        """Test that ValidatorType enum has expected values."""
        assert ValidatorType.SCHEMA.value == "schema"
        assert ValidatorType.CONSTRAINT.value == "constraint"
        assert ValidatorType.DATA.value == "data"
        assert ValidatorType.WRITE_TIME.value == "write_time"

    def test_validator_type_from_string(self):
        """Test creating ValidatorType from string."""
        assert ValidatorType("schema") == ValidatorType.SCHEMA
        assert ValidatorType("constraint") == ValidatorType.CONSTRAINT
        assert ValidatorType("data") == ValidatorType.DATA
        assert ValidatorType("write_time") == ValidatorType.WRITE_TIME

    def test_validator_type_invalid_string(self):
        """Test that invalid string raises ValueError."""
        with pytest.raises(ValueError):
            ValidatorType("invalid")


class TestValidationFactoryConfig:
    """Test ValidationFactoryConfig dataclass."""

    def test_validation_factory_config_creation(self):
        """Test creating ValidationFactoryConfig."""
        config = ValidationFactoryConfig(
            validator_type=ValidatorType.SCHEMA,
            config={"schema": {"id": "int64"}},
            strict_mode=False
        )

        assert config.validator_type == ValidatorType.SCHEMA
        assert config.config == {"schema": {"id": "int64"}}
        assert config.strict_mode is False

    def test_validation_factory_config_defaults(self):
        """Test ValidationFactoryConfig defaults."""
        config = ValidationFactoryConfig(
            validator_type=ValidatorType.SCHEMA,
            config={}
        )

        assert config.strict_mode is True


class TestValidationFactory:
    """Test ValidationFactory class."""

    def test_get_supported_validators(self):
        """Test getting list of supported validators."""
        supported = ValidationFactory.get_supported_validators()
        expected = ["schema", "constraint", "data", "write_time"]

        assert sorted(supported) == sorted(expected)
        assert len(supported) == 4

    def test_create_validator_invalid_type_string(self):
        """Test creating validator with invalid type string."""
        with pytest.raises(ValueError, match="Unsupported validator type: invalid"):
            ValidationFactory.create_validator("invalid")

    def test_create_validator_invalid_type_enum(self):
        """Test creating validator with invalid type (should not happen in practice)."""
        # Test with a simple invalid string instead of trying to patch enum
        with pytest.raises(ValueError, match="Unsupported validator type"):
            ValidationFactory.create_validator("invalid_enum_type")

    @patch('forklift.processors.validation_factory.SchemaValidator')
    def test_create_schema_validator_with_pyarrow_schema(self, mock_schema_validator):
        """Test creating schema validator with PyArrow schema."""
        schema = pa.schema([pa.field("id", pa.int64())])
        config = {"schema": schema, "strict_mode": False}

        mock_instance = Mock(spec=SchemaValidator)
        mock_schema_validator.return_value = mock_instance

        result = ValidationFactory.create_validator(ValidatorType.SCHEMA, config)

        mock_schema_validator.assert_called_once_with(schema, False)
        assert result == mock_instance

    @patch('forklift.processors.validation_factory.SchemaValidator')
    def test_create_schema_validator_with_dict_schema(self, mock_schema_validator):
        """Test creating schema validator with dict schema."""
        schema_dict = {"id": "int64", "name": "string"}
        config = {"schema": schema_dict}

        mock_instance = Mock(spec=SchemaValidator)
        mock_schema_validator.return_value = mock_instance

        result = ValidationFactory.create_validator(ValidatorType.SCHEMA, config)

        # Verify SchemaValidator was called with converted schema
        call_args = mock_schema_validator.call_args
        assert call_args[0][1] is True  # strict_mode default
        # Verify the schema was converted to PyArrow schema
        created_schema = call_args[0][0]
        assert isinstance(created_schema, pa.Schema)
        assert len(created_schema) == 2

    @patch('forklift.processors.validation_factory.SchemaValidator')
    def test_create_schema_validator_kwargs(self, mock_schema_validator):
        """Test creating schema validator with kwargs."""
        schema = pa.schema([pa.field("id", pa.int64())])

        mock_instance = Mock(spec=SchemaValidator)
        mock_schema_validator.return_value = mock_instance

        result = ValidationFactory.create_validator(
            ValidatorType.SCHEMA,
            schema=schema,
            strict_mode=False
        )

        mock_schema_validator.assert_called_once_with(schema, False)

    def test_create_schema_validator_missing_schema(self):
        """Test creating schema validator without schema raises TypeError."""
        with pytest.raises(TypeError, match="Schema validator requires 'schema' parameter"):
            ValidationFactory.create_validator(ValidatorType.SCHEMA, {})

    @patch('forklift.processors.validation_factory.ConstraintValidator')
    def test_create_constraint_validator_default_config(self, mock_constraint_validator):
        """Test creating constraint validator with default config."""
        mock_instance = Mock(spec=ConstraintValidator)
        mock_constraint_validator.return_value = mock_instance

        result = ValidationFactory.create_validator(ValidatorType.CONSTRAINT, {})

        # Verify ConstraintValidator was called with proper config
        call_args = mock_constraint_validator.call_args[0][0]
        assert call_args.error_mode == ErrorMode.BAD_ROWS
        assert call_args.check_constraints == {}
        assert call_args.unique_constraints == []
        assert call_args.foreign_key_constraints == {}

    @patch('forklift.processors.validation_factory.ConstraintValidator')
    def test_create_constraint_validator_custom_config(self, mock_constraint_validator):
        """Test creating constraint validator with custom config."""
        config = {
            "error_mode": "fail_fast",
            "check_constraints": {"age": "age > 0"},
            "unique_constraints": ["email"],
            "foreign_key_constraints": {"dept_id": "departments.id"}
        }

        mock_instance = Mock(spec=ConstraintValidator)
        mock_constraint_validator.return_value = mock_instance

        result = ValidationFactory.create_validator(ValidatorType.CONSTRAINT, config)

        call_args = mock_constraint_validator.call_args[0][0]
        assert call_args.error_mode == ErrorMode.FAIL_FAST
        assert call_args.check_constraints == {"age": "age > 0"}
        assert call_args.unique_constraints == ["email"]
        assert call_args.foreign_key_constraints == {"dept_id": "departments.id"}

    @patch('forklift.processors.validation_factory.ConstraintValidator')
    def test_create_constraint_validator_error_mode_enum(self, mock_constraint_validator):
        """Test creating constraint validator with ErrorMode enum."""
        config = {"error_mode": ErrorMode.FAIL_COMPLETE}

        mock_instance = Mock(spec=ConstraintValidator)
        mock_constraint_validator.return_value = mock_instance

        result = ValidationFactory.create_validator(ValidatorType.CONSTRAINT, config)

        call_args = mock_constraint_validator.call_args[0][0]
        assert call_args.error_mode == ErrorMode.FAIL_COMPLETE

    @patch('forklift.processors.validation_factory.DataValidationProcessor')
    @patch('forklift.processors.validation_factory.BadRowsConfig')
    def test_create_data_validator_default_config(self, mock_bad_rows_config, mock_data_validator):
        """Test creating data validator with default config."""
        mock_bad_rows_instance = Mock()
        mock_bad_rows_config.return_value = mock_bad_rows_instance
        mock_instance = Mock()
        mock_data_validator.return_value = mock_instance

        result = ValidationFactory.create_validator(ValidatorType.DATA, {})

        # Verify DataValidationProcessor was called with proper config
        call_args = mock_data_validator.call_args[0][0]
        assert call_args.field_validations == []
        assert call_args.bad_rows_config == mock_bad_rows_instance
        assert call_args.uniqueness_strategy == "first_wins"

    @patch('forklift.processors.validation_factory.DataValidationProcessor')
    @patch('forklift.processors.validation_factory.FieldValidationRule')
    @patch('forklift.processors.validation_factory.BadRowsConfig')
    def test_create_data_validator_dict_rules(self, mock_bad_rows_config, mock_field_rule, mock_data_validator):
        """Test creating data validator with dict validation rules."""
        rule_dict = {"field_name": "email", "required": True}
        config = {"field_validations": [rule_dict]}

        mock_rule_instance = Mock()
        mock_field_rule.return_value = mock_rule_instance
        mock_bad_rows_instance = Mock()
        mock_bad_rows_config.return_value = mock_bad_rows_instance
        mock_validator_instance = Mock()
        mock_data_validator.return_value = mock_validator_instance

        result = ValidationFactory.create_validator(ValidatorType.DATA, config)

        mock_field_rule.assert_called_once_with(**rule_dict)

    @patch('forklift.processors.validation_factory.WriteTimeValidator')
    def test_create_write_time_validator_default_config(self, mock_write_validator):
        """Test creating write time validator with default config."""
        mock_instance = Mock()
        mock_write_validator.return_value = mock_instance

        result = ValidationFactory.create_validator(ValidatorType.WRITE_TIME, {})

        # Verify WriteTimeValidator was called with proper config
        call_args = mock_write_validator.call_args[0][0]
        assert call_args.fail_on_schema_mismatch is False
        assert call_args.check_empty_tables is True
        assert call_args.primary_key_columns == []

    @patch('forklift.processors.validation_factory.WriteTimeValidator')
    def test_create_write_time_validator_custom_config(self, mock_write_validator):
        """Test creating write time validator with custom config."""
        config = {
            "expected_schema": pa.schema([pa.field("id", pa.int64())]),
            "fail_on_schema_mismatch": True,
            "required_columns": ["id", "name"],
            "check_duplicate_rows": True,
            "primary_key_columns": ["id"],
            "max_null_percentage": 25.0
        }

        mock_instance = Mock()
        mock_write_validator.return_value = mock_instance

        result = ValidationFactory.create_validator(ValidatorType.WRITE_TIME, config)

        call_args = mock_write_validator.call_args[0][0]
        assert call_args.fail_on_schema_mismatch is True
        assert call_args.required_columns == ["id", "name"]
        assert call_args.check_duplicate_rows is True
        assert call_args.primary_key_columns == ["id"]
        assert call_args.max_null_percentage == 25.0

    def test_create_validator_string_type(self):
        """Test creating validator with string type."""
        with patch('forklift.processors.validation_factory.ConstraintValidator') as mock_validator:
            mock_instance = Mock()
            mock_validator.return_value = mock_instance

            result = ValidationFactory.create_validator("constraint", {})

            assert result == mock_instance

    @patch('forklift.processors.validation_factory.SchemaValidator')
    @patch('forklift.processors.validation_factory.ConstraintValidator')
    def test_create_validators_from_config(self, mock_constraint_validator, mock_schema_validator):
        """Test creating multiple validators from config list."""
        schema = pa.schema([pa.field("id", pa.int64())])

        configs = [
            ValidationFactoryConfig(
                validator_type=ValidatorType.SCHEMA,
                config={"schema": schema},
                strict_mode=False
            ),
            ValidationFactoryConfig(
                validator_type=ValidatorType.CONSTRAINT,
                config={"error_mode": "fail_fast"}
            )
        ]

        mock_schema_instance = Mock()
        mock_constraint_instance = Mock()
        mock_schema_validator.return_value = mock_schema_instance
        mock_constraint_validator.return_value = mock_constraint_instance

        validators = ValidationFactory.create_validators_from_config(configs)

        assert len(validators) == 2
        assert validators[0] == mock_schema_instance
        assert validators[1] == mock_constraint_instance

    def test_create_validators_from_config_empty(self):
        """Test creating validators from empty config list."""
        validators = ValidationFactory.create_validators_from_config([])
        assert validators == []

    def test_validate_config_schema_valid(self):
        """Test validating valid schema config."""
        config = {"schema": pa.schema([pa.field("id", pa.int64())])}
        assert ValidationFactory.validate_config(ValidatorType.SCHEMA, config) is True

    def test_validate_config_schema_dict_valid(self):
        """Test validating valid schema config with dict."""
        config = {"schema": {"id": "int64"}}
        assert ValidationFactory.validate_config(ValidatorType.SCHEMA, config) is True

    def test_validate_config_schema_invalid(self):
        """Test validating invalid schema config."""
        config = {}
        with pytest.raises(ValueError, match="Schema validator requires 'schema' parameter"):
            ValidationFactory.validate_config(ValidatorType.SCHEMA, config)

    def test_validate_config_constraint_valid(self):
        """Test validating constraint config (always valid)."""
        config = {"error_mode": "fail_fast"}
        assert ValidationFactory.validate_config(ValidatorType.CONSTRAINT, config) is True

    def test_validate_config_data_valid(self):
        """Test validating data config (always valid)."""
        config = {"validation_rules": []}
        assert ValidationFactory.validate_config(ValidatorType.DATA, config) is True

    def test_validate_config_write_time_valid(self):
        """Test validating write time config (always valid)."""
        config = {"write_mode": "append"}
        assert ValidationFactory.validate_config(ValidatorType.WRITE_TIME, config) is True

    def test_validate_config_string_type(self):
        """Test validating config with string validator type."""
        config = {"schema": pa.schema([pa.field("id", pa.int64())])}
        assert ValidationFactory.validate_config("schema", config) is True


    def test_validate_config_unknown_type_simple(self):
        """Test validating config with simple unknown validator type."""
        # Create a mock type that doesn't exist in the enum
        class MockType:
            def __init__(self, value):
                self.value = value

        unknown_type = MockType("unknown")

        with pytest.raises(ValueError, match="Unknown validator type"):
            ValidationFactory.validate_config(unknown_type, {})


class TestValidationFactoryIntegration:
    """Integration tests for ValidationFactory."""

    def test_schema_validator_integration(self):
        """Test creating and using a schema validator."""
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string())
        ])

        validator = ValidationFactory.create_validator(
            ValidatorType.SCHEMA,
            {"schema": schema}
        )

        assert isinstance(validator, SchemaValidator)
        assert validator.schema == schema
        assert validator.strict_mode is True

    def test_constraint_validator_integration(self):
        """Test creating and using a constraint validator."""
        config = {
            "error_mode": "bad_rows",
            "unique_constraints": ["email"]
        }

        validator = ValidationFactory.create_validator(
            ValidatorType.CONSTRAINT,
            config
        )

        assert isinstance(validator, ConstraintValidator)
        assert validator.config.error_mode == ErrorMode.BAD_ROWS
        assert validator.config.unique_constraints == ["email"]

    def test_factory_config_workflow(self):
        """Test complete workflow with ValidationFactoryConfig."""
        schema = pa.schema([pa.field("id", pa.int64())])

        configs = [
            ValidationFactoryConfig(
                validator_type=ValidatorType.SCHEMA,
                config={"schema": schema},
                strict_mode=True
            )
        ]

        validators = ValidationFactory.create_validators_from_config(configs)

        assert len(validators) == 1
        assert isinstance(validators[0], SchemaValidator)

    def test_error_handling_cascade(self):
        """Test error handling cascades properly through factory."""
        # Test missing required parameter
        with pytest.raises(TypeError):
            ValidationFactory.create_validator(ValidatorType.SCHEMA, {})

        # Test invalid validator type
        with pytest.raises(ValueError):
            ValidationFactory.create_validator("invalid_type", {})

    def test_config_parameter_precedence(self):
        """Test that config parameters take precedence over kwargs."""
        schema1 = pa.schema([pa.field("id1", pa.int64())])
        schema2 = pa.schema([pa.field("id2", pa.int64())])

        validator = ValidationFactory.create_validator(
            ValidatorType.SCHEMA,
            config={"schema": schema1, "strict_mode": False},
            schema=schema2,  # This should be ignored
            strict_mode=True  # This should be ignored
        )

        assert isinstance(validator, SchemaValidator)
        assert validator.schema == schema1
        assert validator.strict_mode is False


class TestValidationFactoryEdgeCases:
    """Test edge cases and error conditions."""

    def test_none_config(self):
        """Test handling None config."""
        with patch('forklift.processors.validation_factory.ConstraintValidator') as mock_validator:
            mock_instance = Mock()
            mock_validator.return_value = mock_instance

            result = ValidationFactory.create_validator(ValidatorType.CONSTRAINT, None)

            assert result == mock_instance

    def test_empty_config(self):
        """Test handling empty config."""
        with patch('forklift.processors.validation_factory.ConstraintValidator') as mock_validator:
            mock_instance = Mock()
            mock_validator.return_value = mock_instance

            result = ValidationFactory.create_validator(ValidatorType.CONSTRAINT, {})

            assert result == mock_instance

    def test_schema_dict_with_complex_types(self):
        """Test schema creation with complex PyArrow types."""
        schema_dict = {
            "id": pa.int64(),
            "scores": pa.list_(pa.float64()),
            "metadata": pa.map_(pa.string(), pa.string())
        }

        validator = ValidationFactory.create_validator(
            ValidatorType.SCHEMA,
            {"schema": schema_dict}
        )

        assert isinstance(validator, SchemaValidator)
        created_schema = validator.schema
        assert len(created_schema) == 3
        assert created_schema.field("scores").type == pa.list_(pa.float64())

    @patch('forklift.processors.validation_factory.DataValidationProcessor')
    @patch('forklift.processors.validation_factory.BadRowsConfig')
    def test_data_validator_empty_rules(self, mock_bad_rows_config, mock_data_validator):
        """Test data validator with empty validation rules."""
        config = {"field_validations": []}

        mock_bad_rows_instance = Mock()
        mock_bad_rows_config.return_value = mock_bad_rows_instance
        mock_instance = Mock()
        mock_data_validator.return_value = mock_instance

        result = ValidationFactory.create_validator(ValidatorType.DATA, config)

        call_args = mock_data_validator.call_args[0][0]
        assert call_args.field_validations == []

    @patch('forklift.processors.validation_factory.DataValidationProcessor')
    @patch('forklift.processors.validation_factory.BadRowsConfig')
    def test_data_validator_custom_bad_rows_config(self, mock_bad_rows_config, mock_data_validator):
        """Test data validator with custom bad rows config."""
        config = {
            "bad_rows_config": {
                "enabled": False,
                "output_path": "custom_bad_rows",
                "max_bad_rows_percent": 5.0
            }
        }

        mock_bad_rows_instance = Mock()
        mock_bad_rows_config.return_value = mock_bad_rows_instance
        mock_instance = Mock()
        mock_data_validator.return_value = mock_instance

        result = ValidationFactory.create_validator(ValidatorType.DATA, config)

        # Verify BadRowsConfig was called with custom config
        mock_bad_rows_config.assert_called_once_with(
            enabled=False,
            output_path="custom_bad_rows",
            max_bad_rows_percent=5.0
        )

    def test_write_time_validator_all_checks_enabled(self):
        """Test write time validator with all validation checks enabled."""
        config = {
            "check_empty_tables": True,
            "check_duplicate_rows": True,
            "check_null_primary_keys": True,
            "check_null_percentages": True,
            "primary_key_columns": ["id", "code"],
            "max_null_percentage": 10.0,
            "min_row_count": 5
        }

        with patch('forklift.processors.validation_factory.WriteTimeValidator') as mock_validator:
            mock_instance = Mock()
            mock_validator.return_value = mock_instance

            result = ValidationFactory.create_validator(ValidatorType.WRITE_TIME, config)

            call_args = mock_validator.call_args[0][0]
            assert call_args.check_empty_tables is True
            assert call_args.check_duplicate_rows is True
            assert call_args.check_null_primary_keys is True
            assert call_args.check_null_percentages is True
            assert call_args.primary_key_columns == ["id", "code"]
            assert call_args.max_null_percentage == 10.0
            assert call_args.min_row_count == 5
