"""Column mapping processor for transforming column names in PyArrow data."""

from __future__ import annotations
from typing import List, Tuple, Dict, Optional, Callable
from dataclasses import dataclass
import re

import pyarrow as pa

from .base import BaseProcessor, ValidationResult


@dataclass
class ColumnMappingConfig:
    """Configuration for column mapping operations.

    Attributes:
        explicit_mappings: Direct column name mappings (source -> target)
        naming_convention: Apply standard naming convention ('snake_case', 'camelCase', 'PascalCase', 'lowercase', 'UPPERCASE')
        custom_transform: Custom function to transform column names
        case_sensitive: Whether mappings are case sensitive
        allow_unmapped: Whether to keep columns that don't have explicit mappings
        drop_unmapped: Whether to drop columns that don't have mappings (overrides allow_unmapped)
    """
    explicit_mappings: Optional[Dict[str, str]] = None
    naming_convention: Optional[str] = None
    custom_transform: Optional[Callable[[str], str]] = None
    case_sensitive: bool = True
    allow_unmapped: bool = True
    drop_unmapped: bool = False

    def __post_init__(self):
        if self.explicit_mappings is None:
            self.explicit_mappings = {}

        valid_conventions = {'snake_case', 'camelCase', 'PascalCase', 'lowercase', 'UPPERCASE'}
        if self.naming_convention and self.naming_convention not in valid_conventions:
            raise ValueError(f"naming_convention must be one of {valid_conventions}, got: {self.naming_convention}")


class ColumnMapper(BaseProcessor):
    """Maps column names according to specified configuration.

    This processor allows you to:
    - Map specific columns to new names (e.g., "A" -> "StateID")
    - Apply naming conventions (e.g., "StateID" -> "state_id")
    - Use custom transformation functions
    - Handle case sensitivity

    Examples:
        # Basic column mapping
        config = ColumnMappingConfig(
            explicit_mappings={"A": "StateID", "B": "CountyCode"}
        )

        # Apply PostgreSQL snake_case convention
        config = ColumnMappingConfig(
            naming_convention='snake_case'
        )

        # Combined: explicit mappings + naming convention
        config = ColumnMappingConfig(
            explicit_mappings={"A": "StateID"},
            naming_convention='snake_case'  # StateID -> state_id
        )
    """

    def __init__(self, config: ColumnMappingConfig):
        """Initialize the column mapper.

        Args:
            config: Column mapping configuration
        """
        self.config = config

    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process a batch by mapping column names.

        Args:
            batch: PyArrow RecordBatch to process

        Returns:
            Tuple of (mapped_batch, validation_results)
        """
        validation_results = []

        try:
            # Get current column names
            current_columns = batch.schema.names

            # Apply column mappings
            new_column_names = []
            columns_to_keep = []

            for i, col_name in enumerate(current_columns):
                mapped_name = self._map_column_name(col_name)

                if mapped_name is None:
                    # Column should be dropped
                    continue

                new_column_names.append(mapped_name)
                columns_to_keep.append(i)

            # Create new batch with mapped columns
            if columns_to_keep:
                # Select only the columns we want to keep
                arrays = [batch.column(i) for i in columns_to_keep]

                # Create new schema with mapped names
                new_fields = []
                for i, col_idx in enumerate(columns_to_keep):
                    old_field = batch.schema.field(col_idx)
                    new_field = pa.field(new_column_names[i], old_field.type, old_field.nullable, old_field.metadata)
                    new_fields.append(new_field)

                new_schema = pa.schema(new_fields)
                new_batch = pa.RecordBatch.from_arrays(arrays, schema=new_schema)
            else:
                # No columns to keep - create empty batch
                new_schema = pa.schema([])
                new_batch = pa.RecordBatch.from_arrays([], schema=new_schema)

                # Only add validation error if there were originally columns that got dropped
                # An empty input batch should not generate a validation error
                if len(current_columns) > 0:
                    validation_results.append(ValidationResult(
                        is_valid=False,
                        error_message="All columns were dropped during mapping",
                        error_code="ALL_COLUMNS_DROPPED"
                    ))

            return new_batch, validation_results

        except Exception as e:
            validation_results.append(ValidationResult(
                is_valid=False,
                error_message=f"Column mapping failed: {str(e)}",
                error_code="MAPPING_ERROR"
            ))
            return batch, validation_results

    def _map_column_name(self, column_name: str) -> Optional[str]:
        """Map a single column name according to configuration.

        Args:
            column_name: Original column name

        Returns:
            Mapped column name, or None if column should be dropped
        """
        # Step 1: Check explicit mappings
        mapped_name = self._apply_explicit_mapping(column_name)

        # Step 2: Apply naming convention if specified
        if self.config.naming_convention:
            mapped_name = self._apply_naming_convention(mapped_name)

        # Step 3: Apply custom transform if specified
        if self.config.custom_transform:
            mapped_name = self.config.custom_transform(mapped_name)

        # Step 4: Check if we should keep unmapped columns
        if mapped_name == column_name and not self.config.allow_unmapped and self.config.drop_unmapped:
            return None

        return mapped_name

    def _apply_explicit_mapping(self, column_name: str) -> str:
        """Apply explicit column mappings.

        Args:
            column_name: Original column name

        Returns:
            Mapped column name
        """
        if not self.config.explicit_mappings:
            return column_name

        # Handle case sensitivity
        if self.config.case_sensitive:
            return self.config.explicit_mappings.get(column_name, column_name)
        else:
            # Case-insensitive lookup
            for source, target in self.config.explicit_mappings.items():
                if source.lower() == column_name.lower():
                    return target
            return column_name

    def _apply_naming_convention(self, column_name: str) -> str:
        """Apply naming convention transformation.

        Args:
            column_name: Column name to transform

        Returns:
            Transformed column name
        """
        if not self.config.naming_convention:
            return column_name

        if self.config.naming_convention == 'snake_case':
            return self._to_snake_case(column_name)
        elif self.config.naming_convention == 'camelCase':
            return self._to_camel_case(column_name)
        elif self.config.naming_convention == 'PascalCase':
            return self._to_pascal_case(column_name)
        elif self.config.naming_convention == 'lowercase':
            return column_name.lower()
        elif self.config.naming_convention == 'UPPERCASE':
            return column_name.upper()

        return column_name

    def _to_snake_case(self, name: str) -> str:
        """Convert name to snake_case.

        Examples:
            StateID -> state_id
            firstName -> first_name
            XMLParser -> xml_parser
        """
        # Insert underscore before uppercase letters that follow lowercase letters
        s1 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
        # Insert underscore before uppercase letters that are followed by lowercase letters
        s2 = re.sub('([A-Z])([A-Z][a-z])', r'\1_\2', s1)
        return s2.lower()

    def _to_camel_case(self, name: str) -> str:
        """Convert name to camelCase.

        Examples:
            state_id -> stateId
            StateID -> stateID
        """
        components = re.split('[_\\s-]+', name)
        if not components:
            return name

        # First component stays lowercase, rest are capitalized
        result = components[0].lower()
        for component in components[1:]:
            if component:
                result += component.capitalize()

        return result

    def _to_pascal_case(self, name: str) -> str:
        """Convert name to PascalCase.

        Examples:
            state_id -> StateId
            firstName -> FirstName
        """
        components = re.split('[_\\s-]+', name)
        return ''.join(component.capitalize() for component in components if component)


def create_postgres_mapper() -> ColumnMapper:
    """Create a column mapper configured for PostgreSQL naming conventions.

    PostgreSQL conventionally uses snake_case for column names.

    Returns:
        ColumnMapper configured for PostgreSQL conventions
    """
    config = ColumnMappingConfig(
        naming_convention='snake_case',
        case_sensitive=False  # PostgreSQL is case-insensitive by default
    )
    return ColumnMapper(config)


def create_custom_mapper(mappings: Dict[str, str], postgres_style: bool = True) -> ColumnMapper:
    """Create a column mapper with custom mappings and optional PostgreSQL style.

    Args:
        mappings: Dictionary of source -> target column name mappings
        postgres_style: Whether to also apply PostgreSQL snake_case convention

    Returns:
        ColumnMapper with the specified configuration
    """
    config = ColumnMappingConfig(
        explicit_mappings=mappings,
        naming_convention='snake_case' if postgres_style else None,
        case_sensitive=False
    )
    return ColumnMapper(config)
