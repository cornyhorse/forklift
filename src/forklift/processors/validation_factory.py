"""Factory functions for creating validation processors from schema configurations."""

from __future__ import annotations
from typing import Dict, Any, Optional, List
import pyarrow as pa

from .data_validation import (
    DataValidationProcessor,
    ValidationConfig,
    FieldValidationRule,
    BadRowsConfig,
    RangeValidation,
    StringValidation,
    EnumValidation,
    DateValidation
)


def create_validation_processor_from_schema(
    schema_config: Dict[str, Any]
) -> Optional[DataValidationProcessor]:
    """Create a DataValidationProcessor from schema configuration.

    Args:
        schema_config: Dictionary containing the x-validation configuration

    Returns:
        DataValidationProcessor instance or None if no configuration found
    """
    if not schema_config:
        return None

    # Parse bad rows configuration
    bad_rows_config_dict = schema_config.get("badRowsHandling", {})
    bad_rows_config = BadRowsConfig(
        enabled=bad_rows_config_dict.get("enabled", True),
        output_path=bad_rows_config_dict.get("outputPath", "bad_rows"),
        file_format=bad_rows_config_dict.get("fileFormat", "parquet"),
        include_original_row=bad_rows_config_dict.get("includeOriginalRow", True),
        include_validation_errors=bad_rows_config_dict.get("includeValidationErrors", True),
        max_bad_rows_percent=bad_rows_config_dict.get("maxBadRowsPercent", 10.0),
        fail_on_exceed_threshold=bad_rows_config_dict.get("failOnExceedThreshold", True)
    )

    # Parse uniqueness handling strategy
    uniqueness_config = schema_config.get("uniquenessHandling", {})
    uniqueness_strategy = uniqueness_config.get("strategy", "first_wins")

    # Parse field validations
    field_validations = []
    field_validations_dict = schema_config.get("fieldValidations", {})

    for field_name, field_config in field_validations_dict.items():
        # Parse range validation
        range_validation = None
        if "range" in field_config:
            range_config = field_config["range"]
            range_validation = RangeValidation(
                min_value=range_config.get("min"),
                max_value=range_config.get("max"),
                inclusive=range_config.get("inclusive", True)
            )

        # Parse string validation
        string_validation = None
        if "stringValidation" in field_config:
            string_config = field_config["stringValidation"]
            string_validation = StringValidation(
                min_length=string_config.get("minLength"),
                max_length=string_config.get("maxLength"),
                pattern=string_config.get("pattern"),
                allow_empty=string_config.get("allowEmpty", True)
            )

        # Parse enum validation
        enum_validation = None
        if "enumValidation" in field_config:
            enum_config = field_config["enumValidation"]
            enum_validation = EnumValidation(
                allowed_values=enum_config.get("allowedValues", []),
                case_sensitive=enum_config.get("caseSensitive", True)
            )

        # Parse date validation
        date_validation = None
        if "dateValidation" in field_config:
            date_config = field_config["dateValidation"]
            date_validation = DateValidation(
                min_date=date_config.get("minDate"),
                max_date=date_config.get("maxDate"),
                formats=date_config.get("format")
            )

        # Create field validation rule
        field_rule = FieldValidationRule(
            field_name=field_name,
            required=field_config.get("required", False),
            unique=field_config.get("unique", False),
            range_validation=range_validation,
            string_validation=string_validation,
            enum_validation=enum_validation,
            date_validation=date_validation,
            on_violation=field_config.get("onViolation", {})
        )

        field_validations.append(field_rule)

    # Create validation configuration
    config = ValidationConfig(
        field_validations=field_validations,
        bad_rows_config=bad_rows_config,
        uniqueness_strategy=uniqueness_strategy
    )

    return DataValidationProcessor(config)


def get_validation_config_from_schema_file(schema_path: str) -> Optional[Dict[str, Any]]:
    """Extract validation configuration from a schema file.

    Args:
        schema_path: Path to schema JSON file

    Returns:
        Validation configuration dictionary or None
    """
    import json

    try:
        with open(schema_path, 'r') as f:
            schema_data = json.load(f)

        return schema_data.get("x-validation")
    except Exception:
        return None


def create_default_validation_rules(field_names: List[str]) -> List[FieldValidationRule]:
    """Create default validation rules for common field patterns.

    Args:
        field_names: List of field names to create rules for

    Returns:
        List of default validation rules
    """
    rules = []

    for field_name in field_names:
        field_lower = field_name.lower()

        # ID fields
        if 'id' in field_lower:
            rules.append(FieldValidationRule(
                field_name=field_name,
                required=True,
                unique=True,
                range_validation=RangeValidation(min_value=1, max_value=999999999)
            ))

        # Email fields
        elif 'email' in field_lower:
            rules.append(FieldValidationRule(
                field_name=field_name,
                required=True,
                unique=True,
                string_validation=StringValidation(
                    max_length=254,
                    pattern=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
                )
            ))

        # Age fields
        elif 'age' in field_lower:
            rules.append(FieldValidationRule(
                field_name=field_name,
                required=False,
                unique=False,
                range_validation=RangeValidation(min_value=0, max_value=150)
            ))

        # Name fields
        elif 'name' in field_lower:
            rules.append(FieldValidationRule(
                field_name=field_name,
                required=True,
                unique=False,
                string_validation=StringValidation(
                    min_length=1,
                    max_length=100,
                    allow_empty=False
                )
            ))

        # Salary/money fields
        elif any(term in field_lower for term in ['salary', 'wage', 'income', 'amount', 'price']):
            rules.append(FieldValidationRule(
                field_name=field_name,
                required=False,
                unique=False,
                range_validation=RangeValidation(min_value=0, max_value=10000000)
            ))

        # Phone fields
        elif 'phone' in field_lower:
            rules.append(FieldValidationRule(
                field_name=field_name,
                required=False,
                unique=False,
                string_validation=StringValidation(
                    pattern=r"^\+?[1-9]\d{1,14}$"
                )
            ))

    return rules
