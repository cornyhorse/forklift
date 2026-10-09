"""Field validation functionality."""

from __future__ import annotations

from typing import Any, Dict, List, Union

from ...validation_utils import resolve_json_types
from .parquet_types import ParquetTypeValidator

# Upper bound for a record's width. Overlap detection materialises the character positions of every
# field, so an absurd start/length must be rejected before it can exhaust memory.
MAX_RECORD_WIDTH = 1_000_000

_FIELD_TYPES = {"string", "integer", "number", "boolean"}


class FieldValidator:
    """Validates field configurations in FWF schemas."""

    @staticmethod
    def validate_traditional_fields(
        fields: List[Dict[str, Any]], allow_duplicate_names: bool = False
    ) -> List[str]:
        """Validate traditional field configurations.

        Args:
            fields: List of field dictionaries to validate
            allow_duplicate_names: Accept repeated field names (only sensible when the schema
                configures ``case.dedupeNames`` to disambiguate them)

        Returns:
            List of validation error messages
        """
        errors = []
        positions_used = set()
        names_seen = set()

        if not fields:
            errors.append("x-fwf.fields array is required and cannot be empty")
            return errors
        if not isinstance(fields, list):
            errors.append("x-fwf.fields must be an array")
            return errors

        for i, field in enumerate(fields):
            if not isinstance(field, dict):
                errors.append(f"Field {i} must be a dictionary")
                continue

            errors.extend(FieldValidator._validate_single_field(field, i, positions_used))
            if not allow_duplicate_names:
                errors.extend(FieldValidator._check_duplicate_name(field, i, names_seen))

        return errors

    @staticmethod
    def validate_conditional_fields(
        conditional_schemas: Dict[str, Any], allow_duplicate_names: bool = False
    ) -> List[str]:
        """Validate conditional schema field configurations.

        Args:
            conditional_schemas: The conditional schemas configuration
            allow_duplicate_names: Accept repeated field names within a variant (only sensible
                when the schema configures ``case.dedupeNames``)

        Returns:
            List of validation error messages
        """
        errors = []

        # Validate flag column
        flag_column = conditional_schemas.get("flagColumn")
        flag_name = None
        flag_position = None
        flag_positions: set = set()
        if not flag_column:
            errors.append("conditionalSchemas.flagColumn is required")
        elif not isinstance(flag_column, dict):
            errors.append("conditionalSchemas.flagColumn must be an object")
        else:
            flag_errors = FieldValidator._validate_single_field(flag_column, "flagColumn", set())
            errors.extend(flag_errors)
            if not flag_errors:
                flag_name = flag_column.get("name")
                flag_position = (flag_column["start"], flag_column["length"])
                flag_positions = set(range(flag_position[0], sum(flag_position)))

        # Validate schema variants
        schema_variants = conditional_schemas.get("schemas", [])
        if not schema_variants:
            errors.append("conditionalSchemas.schemas array is required and cannot be empty")
            return errors
        if not isinstance(schema_variants, list):
            errors.append("conditionalSchemas.schemas must be an array")
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
            if not isinstance(fields, list):
                errors.append(f"Schema variant {variant_index} 'fields' must be an array")
                continue

            # The flag column occupies its positions in every variant: other fields must not
            # overlap it (it is part of each record's layout), so it seeds the variant's set.
            positions_used = set(flag_positions)
            names_seen: set = set()
            for j, field in enumerate(fields):
                if not isinstance(field, dict):
                    errors.append(f"Schema variant {variant_index} field {j} must be a dictionary")
                    continue

                field_id = f"variant {variant_index} field {j}"
                if flag_name and field.get("name") == flag_name:
                    # A variant may repeat the flag column, but only exactly where it is defined
                    errors.extend(FieldValidator._validate_single_field(field, field_id, set()))
                    if (field.get("start"), field.get("length")) != flag_position:
                        errors.append(
                            f"Field {field_id} redefines flag column '{flag_name}'"
                            " at a different position"
                        )
                else:
                    errors.extend(
                        FieldValidator._validate_single_field(field, field_id, positions_used)
                    )
                if not allow_duplicate_names:
                    errors.extend(
                        FieldValidator._check_duplicate_name(field, field_id, names_seen)
                    )

        return errors

    @staticmethod
    def _check_duplicate_name(
        field: Dict[str, Any], field_id: Union[int, str], names_seen: set
    ) -> List[str]:
        """Report a field whose name was already used by an earlier field of the same layout."""
        name = field.get("name")
        if not isinstance(name, str) or not name:
            return []
        if name in names_seen:
            return [f"Field {field_id} duplicate name '{name}'"]
        names_seen.add(name)
        return []

    @staticmethod
    def _validate_single_field(
        field: Dict[str, Any], field_id: Union[int, str], positions_used: set
    ) -> List[str]:
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
        elif not isinstance(name, str):
            errors.append(f"Field {field_id} name must be a string")

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
            if start + length > MAX_RECORD_WIDTH:
                errors.append(
                    f"Field {field_id} extends beyond the maximum record width"
                    f" of {MAX_RECORD_WIDTH} characters"
                )
            else:
                field_positions = set(range(start, start + length))
                if positions_used & field_positions:
                    errors.append(f"Field {field_id} overlaps with previous field positions")
                positions_used.update(field_positions)

        # Validate field type (string, or a nullable array such as ["string", "null"])
        if field.get("type"):
            _, invalid_types, problems = resolve_json_types(field, _FIELD_TYPES)
            for field_type in invalid_types:
                errors.append(f"Field {field_id} invalid type '{field_type}'")
            for problem in problems:
                errors.append(f"Field {field_id}: {problem}")

        # Validate Parquet type
        parquet_type = field.get("parquetType")
        if parquet_type and not ParquetTypeValidator.is_valid_parquet_type(parquet_type):
            errors.append(f"Field {field_id} invalid Parquet type '{parquet_type}'")

        # Validate alignment
        alignment = field.get("alignment")
        if alignment and not (
            isinstance(alignment, str) and alignment in {"left", "right", "center"}
        ):
            errors.append(
                f"Field {field_id} invalid alignment '{alignment}'"
                f", must be 'left', 'right', or 'center'"
            )

        # Validate padding character
        pad_char = field.get("padChar")
        if pad_char and (not isinstance(pad_char, str) or len(pad_char) != 1):
            errors.append(f"Field {field_id} padChar must be a single character")

        return errors
