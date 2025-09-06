"""Field validation and configuration validation for FWF processing."""

from __future__ import annotations
from typing import List

from ..config import FwfInputConfig, FwfFieldSpec, FwfConditionalSchema


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
            FwfConfigValidator._validate_field_overlaps(config.fields)

        # Validate conditional schema fields
        if config.conditional_schemas:
            for schema in config.conditional_schemas:
                FwfConfigValidator._validate_field_overlaps(schema.fields)

    @staticmethod
    def _validate_field_overlaps(fields: List[FwfFieldSpec]) -> None:
        """Validate that fields don't overlap.

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
                        f"Field '{field1.name}' (positions {field1.start}-{field1_end}) "
                        f"overlaps with field '{field2.name}' (positions {field2.start}-{field2_end})"
                    )
