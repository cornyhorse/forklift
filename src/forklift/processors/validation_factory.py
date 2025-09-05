"""Factory for creating validation processors."""

from __future__ import annotations
from typing import Dict, Any, Optional, Union, List
from dataclasses import dataclass
from enum import Enum
import pyarrow as pa

from .base import BaseProcessor
from .constraint_validator import ConstraintValidator, ConstraintConfig, ErrorMode
from .schema_validator import SchemaValidator
from .data_validation import DataValidationProcessor, FieldValidationRule, ValidationConfig, BadRowsConfig
from .write_time_validator import WriteTimeValidator, WriteTimeConfig


class ValidatorType(Enum):
    """Supported validator types."""
    SCHEMA = "schema"
    CONSTRAINT = "constraint"
    DATA = "data"
    WRITE_TIME = "write_time"


@dataclass
class ValidationFactoryConfig:
    """Configuration for the validation factory."""
    validator_type: ValidatorType
    config: Dict[str, Any]
    strict_mode: bool = True


class ValidationFactory:
    """Factory class for creating validation processors.

    This factory provides a unified interface for creating different types
    of validation processors based on configuration parameters.
    """

    @staticmethod
    def create_validator(
        validator_type: Union[ValidatorType, str],
        config: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> BaseProcessor:
        """Create a validator based on type and configuration.

        Args:
            validator_type: Type of validator to create
            config: Configuration dictionary for the validator
            **kwargs: Additional keyword arguments for the validator

        Returns:
            Configured validator instance

        Raises:
            ValueError: If validator type is not supported
            TypeError: If required configuration is missing
        """
        if isinstance(validator_type, str):
            try:
                validator_type = ValidatorType(validator_type)
            except ValueError:
                raise ValueError(f"Unsupported validator type: {validator_type}")

        config = config or {}

        if validator_type == ValidatorType.SCHEMA:
            return ValidationFactory._create_schema_validator(config, **kwargs)
        elif validator_type == ValidatorType.CONSTRAINT:
            return ValidationFactory._create_constraint_validator(config, **kwargs)
        elif validator_type == ValidatorType.DATA:
            return ValidationFactory._create_data_validator(config, **kwargs)
        elif validator_type == ValidatorType.WRITE_TIME:
            return ValidationFactory._create_write_time_validator(config, **kwargs)
        else:
            raise ValueError(f"Unsupported validator type: {validator_type}")

    @staticmethod
    def _create_schema_validator(config: Dict[str, Any], **kwargs) -> SchemaValidator:
        """Create a schema validator."""
        schema = config.get('schema') or kwargs.get('schema')
        if schema is None:
            raise TypeError("Schema validator requires 'schema' parameter")

        if isinstance(schema, dict):
            # Convert dict to PyArrow schema if needed
            fields = []
            for name, type_info in schema.items():
                if isinstance(type_info, str):
                    pa_type = getattr(pa, type_info)()
                else:
                    pa_type = type_info
                fields.append(pa.field(name, pa_type))
            schema = pa.schema(fields)

        strict_mode = config.get('strict_mode', kwargs.get('strict_mode', True))

        # Use the old interface that tests expect: SchemaValidator(schema, strict_mode)
        # This will map to our new constructor as: SchemaValidator(schema, config=None, strict_mode=strict_mode)
        return SchemaValidator(schema, strict_mode)

    @staticmethod
    def _create_constraint_validator(config: Dict[str, Any], **kwargs) -> ConstraintValidator:
        """Create a constraint validator."""
        error_mode_str = config.get('error_mode', kwargs.get('error_mode', 'bad_rows'))
        if isinstance(error_mode_str, str):
            error_mode = ErrorMode(error_mode_str)
        else:
            error_mode = error_mode_str

        constraint_config = ConstraintConfig(
            error_mode=error_mode,
            check_constraints=config.get('check_constraints', kwargs.get('check_constraints', {})),
            unique_constraints=config.get('unique_constraints', kwargs.get('unique_constraints', [])),
            foreign_key_constraints=config.get('foreign_key_constraints', kwargs.get('foreign_key_constraints', {}))
        )

        return ConstraintValidator(constraint_config)

    @staticmethod
    def _create_data_validator(config: Dict[str, Any], **kwargs) -> DataValidationProcessor:
        """Create a data validator."""
        validation_rules = config.get('field_validations', kwargs.get('field_validations', []))

        # Convert dict rules to FieldValidationRule objects if needed
        if validation_rules and isinstance(validation_rules[0], dict):
            rule_objects = []
            for rule_dict in validation_rules:
                rule = FieldValidationRule(**rule_dict)
                rule_objects.append(rule)
            validation_rules = rule_objects

        # Create bad rows config
        bad_rows_config_dict = config.get('bad_rows_config', kwargs.get('bad_rows_config', {}))
        bad_rows_config = BadRowsConfig(**bad_rows_config_dict) if bad_rows_config_dict else BadRowsConfig()

        validation_config = ValidationConfig(
            field_validations=validation_rules,
            bad_rows_config=bad_rows_config,
            uniqueness_strategy=config.get('uniqueness_strategy', kwargs.get('uniqueness_strategy', 'first_wins'))
        )

        return DataValidationProcessor(validation_config)

    @staticmethod
    def _create_write_time_validator(config: Dict[str, Any], **kwargs) -> WriteTimeValidator:
        """Create a write time validator."""
        write_config = WriteTimeConfig(
            expected_schema=config.get('expected_schema', kwargs.get('expected_schema')),
            fail_on_schema_mismatch=config.get('fail_on_schema_mismatch', kwargs.get('fail_on_schema_mismatch', False)),
            required_columns=config.get('required_columns', kwargs.get('required_columns')),
            check_empty_tables=config.get('check_empty_tables', kwargs.get('check_empty_tables', True)),
            check_duplicate_rows=config.get('check_duplicate_rows', kwargs.get('check_duplicate_rows', False)),
            check_null_primary_keys=config.get('check_null_primary_keys', kwargs.get('check_null_primary_keys', False)),
            check_null_percentages=config.get('check_null_percentages', kwargs.get('check_null_percentages', False)),
            primary_key_columns=config.get('primary_key_columns', kwargs.get('primary_key_columns', [])),
            max_null_percentage=config.get('max_null_percentage', kwargs.get('max_null_percentage', 50.0)),
            min_row_count=config.get('min_row_count', kwargs.get('min_row_count', 1))
        )

        return WriteTimeValidator(write_config)

    @staticmethod
    def create_validators_from_config(configs: List[ValidationFactoryConfig]) -> List[BaseProcessor]:
        """Create multiple validators from a list of configurations.

        Args:
            configs: List of validation factory configurations

        Returns:
            List of configured validator instances
        """
        validators = []
        for config in configs:
            validator = ValidationFactory.create_validator(
                config.validator_type,
                config.config,
                strict_mode=config.strict_mode
            )
            validators.append(validator)

        return validators

    @staticmethod
    def get_supported_validators() -> List[str]:
        """Get list of supported validator types.

        Returns:
            List of supported validator type names
        """
        return [validator_type.value for validator_type in ValidatorType]

    @staticmethod
    def validate_config(validator_type: Union[ValidatorType, str], config: Dict[str, Any]) -> bool:
        """Validate configuration for a specific validator type.

        Args:
            validator_type: Type of validator to validate config for
            config: Configuration to validate

        Returns:
            True if configuration is valid

        Raises:
            ValueError: If configuration is invalid
        """
        if isinstance(validator_type, str):
            validator_type = ValidatorType(validator_type)

        if validator_type == ValidatorType.SCHEMA:
            if 'schema' not in config and not isinstance(config.get('schema'), (pa.Schema, dict)):
                raise ValueError("Schema validator requires 'schema' parameter")
        elif validator_type == ValidatorType.CONSTRAINT:
            # Constraint validator has optional parameters, so any config is valid
            pass
        elif validator_type == ValidatorType.DATA:
            # Data validator has optional parameters, so any config is valid
            pass
        elif validator_type == ValidatorType.WRITE_TIME:
            # Write time validator has optional parameters, so any config is valid
            pass
        else:
            raise ValueError(f"Unknown validator type: {validator_type}")

        return True


def create_validation_processor_from_schema(schema_config: Optional[Dict[str, Any]]) -> Optional[BaseProcessor]:
    """Create a validation processor from schema configuration.

    This function provides backward compatibility for existing tests that expect
    a simple factory function interface.

    Args:
        schema_config: Schema configuration dictionary

    Returns:
        Validation processor instance or None if config is empty/None
    """
    if not schema_config:
        return None

    # Determine validator type from config
    validator_type = schema_config.get('type', 'schema')

    try:
        return ValidationFactory.create_validator(validator_type, schema_config)
    except (ValueError, TypeError):
        # Return None for invalid configurations to match expected behavior
        return None
