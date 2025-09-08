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
        constraint_config = ConstraintConfig()

        # Map common configuration keys
        if 'error_mode' in config:
            try:
                constraint_config.error_mode = ErrorMode(config['error_mode'])
            except ValueError:
                constraint_config.error_mode = ErrorMode.BAD_ROWS

        if 'field_constraints' in config:
            constraint_config.field_constraints = config['field_constraints']

        if 'check_constraints' in config:
            constraint_config.check_constraints = config['check_constraints']

        if 'unique_constraints' in config:
            constraint_config.unique_constraints = config['unique_constraints']

        if 'max_violations' in config:
            constraint_config.max_violations = config['max_violations']

        return ConstraintValidator(constraint_config)

    @staticmethod
    def _create_data_validator(config: Dict[str, Any], **kwargs) -> DataValidationProcessor:
        """Create a data validation processor."""
        validation_config = ValidationConfig()

        # Map configuration
        if 'rules' in config:
            validation_config.rules = config['rules']
        elif 'field_rules' in config:
            validation_config.rules = config['field_rules']

        if 'bad_rows_config' in config:
            validation_config.bad_rows_config = BadRowsConfig(**config['bad_rows_config'])

        return DataValidationProcessor(validation_config)

    @staticmethod
    def _create_write_time_validator(config: Dict[str, Any], **kwargs) -> WriteTimeValidator:
        """Create a write time validator."""
        write_time_config = WriteTimeConfig()

        # Map configuration
        if 'timezone' in config:
            write_time_config.timezone = config['timezone']

        if 'format' in config:
            write_time_config.format = config['format']

        return WriteTimeValidator(write_time_config)

    @staticmethod
    def validate_config(validator_type: Union[ValidatorType, str], config: Dict[str, Any]) -> bool:
        """Validate configuration for a given validator type.

        Args:
            validator_type: Type of validator to validate config for
            config: Configuration dictionary to validate

        Returns:
            True if configuration is valid

        Raises:
            ValueError: If configuration is invalid
        """
        if isinstance(validator_type, str):
            try:
                validator_type = ValidatorType(validator_type)
            except ValueError:
                raise ValueError(f"Unsupported validator type: {validator_type}")

        if validator_type == ValidatorType.SCHEMA:
            if 'schema' not in config:
                raise ValueError("Schema validator requires 'schema' parameter")

        elif validator_type == ValidatorType.CONSTRAINT:
            # Constraint validator config is optional, but validate if present
            if 'rules' in config and not config['rules']:
                return False

        elif validator_type == ValidatorType.DATA:
            # Data validator config is optional, but validate if present
            if 'rules' in config and not config['rules']:
                return False

        elif validator_type == ValidatorType.WRITE_TIME:
            # Write time validator config is always valid
            pass
        else:
            raise ValueError(f"Unsupported validator type: {validator_type}")

        return True

    @staticmethod
    def create_from_schema_file(
        schema_file_path: str,
        validator_type: Union[ValidatorType, str] = ValidatorType.SCHEMA,
        **kwargs
    ) -> BaseProcessor:
        """Create a validator from a schema file.

        Args:
            schema_file_path: Path to the schema file
            validator_type: Type of validator to create
            **kwargs: Additional configuration

        Returns:
            Configured validator instance
        """
        import json

        try:
            with open(schema_file_path, 'r') as f:
                schema_dict = json.load(f)
        except FileNotFoundError:
            raise ValueError(f"Schema file not found: {schema_file_path}")
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON in schema file: {schema_file_path}")

        config = {'schema': schema_dict}
        config.update(kwargs)

        return ValidationFactory.create_validator(validator_type, config)


# Add missing factory function that tests expect as a module-level function
def create_validation_processor_from_schema(schema_dict: Dict[str, Any], **kwargs):
    """Create a validation processor from a schema dictionary.

    Args:
        schema_dict: Schema dictionary containing validation rules
        **kwargs: Additional configuration options

    Returns:
        Configured validation processor
    """
    # This is a compatibility wrapper - tests can use the ValidationFactory directly
    return ValidationFactory.create_validator("schema", {"schema": schema_dict}, **kwargs)
