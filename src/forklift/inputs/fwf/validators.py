"""Field validation and configuration validation for FWF processing."""

from __future__ import annotations
from typing import List

from ..config import FwfInputConfig, FwfFieldSpec, FwfConditionalSchema


class FwfFieldValidator:
    """Handles validation of individual FWF field specifications."""

    @staticmethod
    def validate_field_spec(field: FwfFieldSpec) -> None:
        """Validate a single field specification.

        Args:
            field: Field specification to validate

        Raises:
            ValueError: If field specification is invalid
        """
        # Validate field name
        if not field.name or field.name == "":
            raise ValueError("Field name cannot be empty")

        # Validate start position
        if field.start < 0:
            raise ValueError("start position cannot be negative")
        if field.start == 0:
            raise ValueError("start position must be greater than 0 (1-based indexing)")

        # Validate length
        if field.length <= 0:
            raise ValueError("length must be greater than 0")

        # Validate parquet type
        FwfFieldValidator.validate_data_type(field.parquet_type)

    @staticmethod
    def validate_data_type(data_type: str) -> None:
        """Validate a data type string.

        Args:
            data_type: Data type to validate

        Raises:
            ValueError: If data type is invalid
        """
        valid_types = {
            "string", "int8", "int16", "int32", "int64",
            "uint8", "uint16", "uint32", "uint64",
            "float32", "float64", "bool", "date32", "date64",
            "timestamp", "binary"
        }

        # Handle decimal types with precision/scale
        if data_type.startswith("decimal"):
            return

        # Handle timestamp types with units
        if data_type.startswith("timestamp[") and data_type.endswith("]"):
            return

        # Handle duration types with units
        if data_type.startswith("duration[") and data_type.endswith("]"):
            return

        # Handle list types
        if data_type.startswith("list<") and data_type.endswith(">"):
            return

        if data_type not in valid_types:
            raise ValueError(f"Invalid data type: {data_type}")

    @staticmethod
    def validate_position_range(start: int, length: int) -> None:
        """Validate position range parameters.

        Args:
            start: Start position (1-based)
            length: Field length

        Raises:
            ValueError: If position range is invalid
        """
        if start < 0:
            raise ValueError("Start position cannot be negative")
        if start == 0:
            raise ValueError("Start position must be greater than 0 (1-based indexing)")
        if length <= 0:
            raise ValueError("Length must be greater than 0")


class FwfSchemaValidator:
    """Handles validation of FWF schema specifications."""

    @staticmethod
    def validate_schema(schema: FwfConditionalSchema) -> None:
        """Validate a conditional schema.

        Args:
            schema: Schema to validate

        Raises:
            ValueError: If schema is invalid
        """
        # Validate fields exist
        if not schema.fields or len(schema.fields) == 0:
            raise ValueError("Schema must have at least one field")

        # Validate individual fields
        for field in schema.fields:
            FwfFieldValidator.validate_field_spec(field)

        # Validate field positions don't overlap
        FwfSchemaValidator.validate_field_positions(schema.fields)

        # Validate field names are unique
        FwfSchemaValidator.validate_field_names(schema.fields)

    @staticmethod
    def validate_field_positions(fields: List[FwfFieldSpec]) -> None:
        """Validate that fields don't overlap in position.

        Args:
            fields: List of field specifications to validate

        Raises:
            ValueError: If fields overlap
        """
        for i, field1 in enumerate(fields):
            field1_end = field1.start + field1.length - 1
            for j, field2 in enumerate(fields[i + 1:], i + 1):
                field2_end = field2.start + field2.length - 1

                # Check for overlap
                if (field1.start <= field2.start <= field1_end or
                    field2.start <= field1.start <= field2_end):
                    raise ValueError(
                        f"Field positions overlap: '{field1.name}' (positions {field1.start}-{field1_end}) "
                        f"overlaps with '{field2.name}' (positions {field2.start}-{field2_end})"
                    )

    @staticmethod
    def validate_field_names(fields: List[FwfFieldSpec]) -> None:
        """Validate that field names are unique.

        Args:
            fields: List of field specifications to validate

        Raises:
            ValueError: If duplicate field names exist
        """
        seen_names = set()
        for field in fields:
            if field.name in seen_names:
                raise ValueError(f"Duplicate field name: '{field.name}'")
            seen_names.add(field.name)


class FwfConfigValidator:
    """Handles validation of FWF configuration."""

    @staticmethod
    def validate_config(config: FwfInputConfig) -> None:
        """Validate the FWF configuration.

        Args:
            config: Configuration to validate

        Raises:
            ValueError: If configuration is invalid
        """
        # Must have either fields or conditional schemas
        if not config.fields and not config.conditional_schemas:
            raise ValueError("Either fields or conditional_schemas must be specified")

        # If using conditional schemas, must have flag column
        if config.conditional_schemas and not config.flag_column:
            raise ValueError("Flag column must be specified when using conditional schemas")

        # Validate field overlaps for simple fields
        if config.fields:
            FwfSchemaValidator.validate_field_positions(config.fields)

        # Validate conditional schema fields
        if config.conditional_schemas:
            for schema in config.conditional_schemas:
                FwfSchemaValidator.validate_schema(schema)

    @staticmethod
    def _validate_field_overlaps(fields: List[FwfFieldSpec]) -> None:
        """Validate that fields don't overlap.

        Args:
            fields: List of field specifications to validate

        Raises:
            ValueError: If fields overlap
        """
        # Delegate to the schema validator
        FwfSchemaValidator.validate_field_positions(fields)
