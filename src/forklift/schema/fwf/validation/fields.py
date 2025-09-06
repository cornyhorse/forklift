"""Field validation functionality."""

from __future__ import annotations
from typing import Any, Dict, List, Union

from .parquet_types import ParquetTypeValidator


class FieldValidator:
    """Validates field configurations in FWF schemas."""

    @staticmethod
    def validate_traditional_fields(fields: List[Dict[str, Any]]) -> List[str]:
        """Validate traditional field configurations.

        Args:
            fields: List of field dictionaries to validate

        Returns:
            List of validation error messages
        """
        errors = []
        positions_used = set()

        if not fields:
            errors.append("x-fwf.fields array is required and cannot be empty")
            return errors

        for i, field in enumerate(fields):
            if not isinstance(field, dict):
                errors.append(f"Field {i} must be a dictionary")
                continue

            errors.extend(FieldValidator._validate_single_field(field, i, positions_used))

        return errors

    @staticmethod
    def validate_conditional_fields(conditional_schemas: Dict[str, Any]) -> List[str]:
        """Validate conditional schema field configurations.

        Args:
            conditional_schemas: The conditional schemas configuration

        Returns:
            List of validation error messages
        """
        errors = []

        # Validate flag column
        flag_column = conditional_schemas.get("flagColumn")
        if not flag_column:
            errors.append("conditionalSchemas.flagColumn is required")
        else:
            errors.extend(FieldValidator._validate_single_field(flag_column, "flagColumn", set()))

        # Validate schema variants
        schema_variants = conditional_schemas.get("schemas", [])
        if not schema_variants:
            errors.append("conditionalSchemas.schemas array is required and cannot be empty")
            return errors

        for variant_index, variant in enumerate(schema_variants):
            if not isinstance(variant, dict):
                errors.append(f"Schema variant {variant_index} must be a dictionary")
                continue

            if not variant.get("flagValue"):
                errors.append(f"Schema variant {variant_index} missing required 'flagValue'")

            fields = variant.get("fields", [])
            if not fields:
                errors.append(f"Schema variant {variant_index} missing required 'fields' array")
                continue

            positions_used = set()
            for j, field in enumerate(fields):
                if not isinstance(field, dict):
                    errors.append(f"Schema variant {variant_index} field {j} must be a dictionary")
                    continue

                errors.extend(FieldValidator._validate_single_field(field, f"variant {variant_index} field {j}", positions_used))

        return errors

    @staticmethod
    def _validate_single_field(field: Dict[str, Any], field_id: Union[int, str], positions_used: set) -> List[str]:
        """Validate a single field configuration.

        Args:
            field: The field dictionary to validate
            field_id: Identifier for the field (for error messages)
            positions_used: Set of positions already used by other fields

        Returns:
            List of validation error messages
        """
        errors = []

        # Validate required fields
        name = field.get("name")
        if not name:
            errors.append(f"Field {field_id} missing required 'name'")

        start = field.get("start")
        length = field.get("length")

        if start is None:
            errors.append(f"Field {field_id} missing required 'start' position")
        elif not isinstance(start, int) or start < 1:
            errors.append(f"Field {field_id} start position must be a positive integer")

        if length is None:
            errors.append(f"Field {field_id} missing required 'length'")
        elif not isinstance(length, int) or length < 1:
            errors.append(f"Field {field_id} length must be a positive integer")

        # Check for overlapping positions within the same schema
        if isinstance(start, int) and isinstance(length, int):
            field_positions = set(range(start, start + length))
            if positions_used & field_positions:
                errors.append(f"Field {field_id} overlaps with previous field positions")
            positions_used.update(field_positions)

        # Validate field type
        field_type = field.get("type")
        if field_type:
            valid_types = {"string", "integer", "number", "boolean"}
            if field_type not in valid_types:
                errors.append(f"Field {field_id} invalid type '{field_type}'")

        # Validate Parquet type
        parquet_type = field.get("parquetType")
        if parquet_type and not ParquetTypeValidator.is_valid_parquet_type(parquet_type):
            errors.append(f"Field {field_id} invalid Parquet type '{parquet_type}'")

        # Validate alignment
        alignment = field.get("alignment")
        if alignment and alignment not in {"left", "right", "center"}:
            errors.append(f"Field {field_id} invalid alignment '{alignment}', must be 'left', 'right', or 'center'")

        # Validate padding character
        pad_char = field.get("padChar")
        if pad_char and (not isinstance(pad_char, str) or len(pad_char) != 1):
            errors.append(f"Field {field_id} padChar must be a single character")

        return errors
