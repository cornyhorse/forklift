"""Comprehensive tests for validation factory module."""

import pytest
import pyarrow as pa
from unittest.mock import patch, MagicMock

from forklift.processors.validation_factory import (
    ValidationFactory,
    ValidatorType,
    ValidationFactoryConfig,
    create_validation_processor_from_schema
)
from forklift.processors.base import BaseProcessor


class TestValidatorType:
    """Test ValidatorType enum."""

    def test_validator_type_values(self):
        """Test that all validator types have correct values."""
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
        """Test creating ValidatorType from invalid string."""
        with pytest.raises(ValueError):
            ValidatorType("invalid")


class TestValidationFactoryConfig:
    """Test ValidationFactoryConfig dataclass."""

    def test_default_config(self):
        """Test default configuration values."""
        config = ValidationFactoryConfig(
            validator_type=ValidatorType.SCHEMA,
            config={"test": "value"}
        )

        assert config.validator_type == ValidatorType.SCHEMA
        assert config.config == {"test": "value"}
        assert config.strict_mode is True

    def test_custom_config(self):
        """Test custom configuration values."""
        config = ValidationFactoryConfig(
            validator_type=ValidatorType.CONSTRAINT,
            config={"rules": []},
            strict_mode=False
        )

        assert config.validator_type == ValidatorType.CONSTRAINT
        assert config.config == {"rules": []}
        assert config.strict_mode is False


class TestValidationFactory:
    """Test ValidationFactory class."""

    @patch('forklift.processors.validation_factory.SchemaValidator')
    def test_create_schema_validator(self, mock_schema_validator):
        """Test creating schema validator."""
        schema = pa.schema([('id', pa.int64())])
        config = {"schema": schema}

        mock_instance = MagicMock()
        mock_schema_validator.return_value = mock_instance

        result = ValidationFactory.create_validator(ValidatorType.SCHEMA, config)

        assert result == mock_instance
        mock_schema_validator.assert_called_once()

    @patch('forklift.processors.validation_factory.ConstraintValidator')
    def test_create_constraint_validator(self, mock_constraint_validator):
        """Test creating constraint validator."""
        config = {"rules": {"field1": {"type": "string"}}}

        mock_instance = MagicMock()
        mock_constraint_validator.return_value = mock_instance

        result = ValidationFactory.create_validator(ValidatorType.CONSTRAINT, config)

        assert result == mock_instance
        mock_constraint_validator.assert_called_once()

    @patch('forklift.processors.validation_factory.DataValidationProcessor')
    def test_create_data_validator(self, mock_data_validator):
        """Test creating data validation processor."""
        config = {"validation_rules": []}

        mock_instance = MagicMock()
        mock_data_validator.return_value = mock_instance

        result = ValidationFactory.create_validator(ValidatorType.DATA, config)

        assert result == mock_instance
        mock_data_validator.assert_called_once()

    @patch('forklift.processors.validation_factory.WriteTimeValidator')
    def test_create_write_time_validator(self, mock_write_time_validator):
        """Test creating write time validator."""
        config = {"check_empty_tables": True}

        mock_instance = MagicMock()
        mock_write_time_validator.return_value = mock_instance

        result = ValidationFactory.create_validator(ValidatorType.WRITE_TIME, config)

        assert result == mock_instance
        mock_write_time_validator.assert_called_once()

    def test_create_validator_string_type(self):
        """Test creating validator with string type."""
        with patch('forklift.processors.validation_factory.SchemaValidator') as mock_validator:
            mock_instance = MagicMock()
            mock_validator.return_value = mock_instance

            schema = pa.schema([('id', pa.int64())])
            result = ValidationFactory.create_validator("schema", {"schema": schema})

            assert result == mock_instance

    def test_create_validator_invalid_string_type(self):
        """Test creating validator with invalid string type."""
        with pytest.raises(ValueError, match="Unsupported validator type: invalid"):
            ValidationFactory.create_validator("invalid", {})

    def test_create_validator_invalid_enum_type(self):
        """Test creating validator with invalid enum type."""
        # Create a mock enum value that's not in ValidatorType
        class MockValidatorType:
            pass

        invalid_type = MockValidatorType()
        with pytest.raises(ValueError, match="Unsupported validator type"):
            ValidationFactory.create_validator(invalid_type, {})

    def test_create_validator_no_config(self):
        """Test creating validator with no config."""
        with patch('forklift.processors.validation_factory.SchemaValidator') as mock_validator:
            mock_instance = MagicMock()
            mock_validator.return_value = mock_instance

            result = ValidationFactory.create_validator(ValidatorType.SCHEMA)
            assert result == mock_instance

    @patch('forklift.processors.validation_factory.SchemaValidator')
    def test_create_validator_with_kwargs(self, mock_schema_validator):
        """Test creating validator with kwargs."""
        mock_instance = MagicMock()
        mock_schema_validator.return_value = mock_instance

        schema = pa.schema([('id', pa.int64())])
        result = ValidationFactory.create_validator(
            ValidatorType.SCHEMA,
            config={},
            schema=schema,
            strict_mode=False
        )

        assert result == mock_instance
        mock_schema_validator.assert_called_once()

    @patch('forklift.processors.validation_factory.ValidationFactory.create_validator')
    def test_create_validators_from_config(self, mock_create_validator):
        """Test creating multiple validators from config list."""
        mock_validator1 = MagicMock()
        mock_validator2 = MagicMock()
        mock_create_validator.side_effect = [mock_validator1, mock_validator2]

        configs = [
            ValidationFactoryConfig(ValidatorType.SCHEMA, {"schema": "test1"}),
            ValidationFactoryConfig(ValidatorType.CONSTRAINT, {"rules": "test2"})
        ]

        result = ValidationFactory.create_validators_from_config(configs)

        assert len(result) == 2
        assert result[0] == mock_validator1
        assert result[1] == mock_validator2
        assert mock_create_validator.call_count == 2

    def test_create_validators_from_config_empty_list(self):
        """Test creating validators from empty config list."""
        result = ValidationFactory.create_validators_from_config([])
        assert result == []

    def test_get_supported_validators(self):
        """Test getting list of supported validators."""
        supported = ValidationFactory.get_supported_validators()

        assert isinstance(supported, list)
        assert "schema" in supported
        assert "constraint" in supported
        assert "data" in supported
        assert "write_time" in supported
        assert len(supported) == 4

    def test_validate_config_schema_valid(self):
        """Test validating schema validator config."""
        schema = pa.schema([('id', pa.int64())])
        config = {"schema": schema}

        result = ValidationFactory.validate_config(ValidatorType.SCHEMA, config)
        assert result is True

    def test_validate_config_schema_missing_schema(self):
        """Test validating schema validator config without schema."""
        config = {}

        result = ValidationFactory.validate_config(ValidatorType.SCHEMA, config)
        assert result is False

    def test_validate_config_constraint_valid(self):
        """Test validating constraint validator config."""
        config = {"rules": {"field1": {"type": "string"}}}

        result = ValidationFactory.validate_config(ValidatorType.CONSTRAINT, config)
        assert result is True

    def test_validate_config_constraint_missing_rules(self):
        """Test validating constraint validator config without rules."""
        config = {}

        result = ValidationFactory.validate_config(ValidatorType.CONSTRAINT, config)
        assert result is False

    def test_validate_config_data_valid(self):
        """Test validating data validator config."""
        config = {"validation_rules": []}

        result = ValidationFactory.validate_config(ValidatorType.DATA, config)
        assert result is True

    def test_validate_config_data_missing_rules(self):
        """Test validating data validator config without rules."""
        config = {}

        result = ValidationFactory.validate_config(ValidatorType.DATA, config)
        assert result is False

    def test_validate_config_write_time_valid(self):
        """Test validating write time validator config."""
        config = {"check_empty_tables": True}

        result = ValidationFactory.validate_config(ValidatorType.WRITE_TIME, config)
        assert result is True

    def test_validate_config_write_time_empty(self):
        """Test validating write time validator with empty config."""
        config = {}

        # Write time validator should accept empty config
        result = ValidationFactory.validate_config(ValidatorType.WRITE_TIME, config)
        assert result is True

    def test_validate_config_string_type(self):
        """Test validating config with string validator type."""
        schema = pa.schema([('id', pa.int64())])
        config = {"schema": schema}

        result = ValidationFactory.validate_config("schema", config)
        assert result is True

    def test_validate_config_invalid_type(self):
        """Test validating config with invalid validator type."""
        with pytest.raises(ValueError, match="Unsupported validator type"):
            ValidationFactory.validate_config("invalid", {})


class TestCreateValidationProcessorFromSchema:
    """Test create_validation_processor_from_schema function."""

    @patch('forklift.processors.validation_factory.ValidationFactory.create_validator')
    def test_create_from_schema_with_validation(self, mock_create_validator):
        """Test creating processor from schema with validation config."""
        mock_processor = MagicMock()
        mock_create_validator.return_value = mock_processor

        schema_config = {
            "validation": {
                "type": "data",
                "rules": [{"field": "test", "required": True}]
            }
        }

        result = create_validation_processor_from_schema(schema_config)

        assert result == mock_processor
        mock_create_validator.assert_called_once()

    def test_create_from_schema_no_validation(self):
        """Test creating processor from schema without validation config."""
        schema_config = {"properties": {"field1": {"type": "string"}}}

        result = create_validation_processor_from_schema(schema_config)

        assert result is None

    def test_create_from_schema_none_config(self):
        """Test creating processor from None schema config."""
        result = create_validation_processor_from_schema(None)

        assert result is None

    def test_create_from_schema_empty_config(self):
        """Test creating processor from empty schema config."""
        result = create_validation_processor_from_schema({})

        assert result is None

    @patch('forklift.processors.validation_factory.ValidationFactory.create_validator')
    def test_create_from_schema_invalid_type(self, mock_create_validator):
        """Test creating processor from schema with invalid validator type."""
        mock_create_validator.side_effect = ValueError("Invalid type")

        schema_config = {
            "validation": {
                "type": "invalid",
                "rules": []
            }
        }

        # Should handle the error gracefully and return None
        result = create_validation_processor_from_schema(schema_config)

        assert result is None


class TestValidationFactoryIntegration:
    """Test validation factory integration scenarios."""

    def test_factory_workflow(self):
        """Test complete factory workflow."""
        # Test creating multiple validators
        configs = [
            ValidationFactoryConfig(ValidatorType.SCHEMA, {"schema": pa.schema([('id', pa.int64())])}),
            ValidationFactoryConfig(ValidatorType.CONSTRAINT, {"rules": {"id": {"type": "integer"}}})
        ]

        with patch('forklift.processors.validation_factory.SchemaValidator'), \
             patch('forklift.processors.validation_factory.ConstraintValidator'):

            validators = ValidationFactory.create_validators_from_config(configs)
            assert len(validators) == 2

    def test_module_imports(self):
        """Test that all components can be imported."""
        from forklift.processors.validation_factory import (
            ValidationFactory,
            ValidatorType,
            ValidationFactoryConfig,
            create_validation_processor_from_schema
        )

        assert ValidationFactory is not None
        assert ValidatorType is not None
        assert ValidationFactoryConfig is not None
        assert callable(create_validation_processor_from_schema)

    def test_module_docstring(self):
        """Test module documentation."""
        import forklift.processors.validation_factory as factory_module

        assert factory_module.__doc__ is not None
        assert "Factory for creating validation processors" in factory_module.__doc__

    def test_supported_validators_complete(self):
        """Test that all ValidatorType enum values are supported."""
        supported = ValidationFactory.get_supported_validators()
        enum_values = [e.value for e in ValidatorType]

        assert set(supported) == set(enum_values)
