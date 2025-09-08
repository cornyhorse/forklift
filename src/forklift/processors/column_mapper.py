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
        # Start with the original name
        mapped_name = column_name

        # Step 1: Apply explicit mappings first
        if self.config.explicit_mappings:
            if self.config.case_sensitive:
                if mapped_name in self.config.explicit_mappings:
                    mapped_name = self.config.explicit_mappings[mapped_name]
            else:
                # Case-insensitive mapping
                for source, target in self.config.explicit_mappings.items():
                    if mapped_name.lower() == source.lower():
                        mapped_name = target
                        break

        # Step 2: Apply naming convention
        if self.config.naming_convention:
            mapped_name = self.apply_naming_convention(mapped_name, self.config.naming_convention)

        # Step 3: Apply custom transform if provided
        if self.config.custom_transform:
            mapped_name = self.config.custom_transform(mapped_name)

        # Step 4: Check if we should drop unmapped columns
        if self.config.drop_unmapped and mapped_name == column_name:
            # Column wasn't mapped and we should drop unmapped columns
            if not self.config.explicit_mappings or column_name not in self.config.explicit_mappings:
                return None

        return mapped_name

    def apply_naming_convention(self, name: str, convention: str) -> str:
        """Apply a naming convention to a column name.

        Args:
            name: Column name to transform
            convention: Naming convention to apply

        Returns:
            Transformed column name
        """
        if convention == 'snake_case':
            return self.to_snake_case(name)
        elif convention == 'camelCase':
            return self.to_camel_case(name)
        elif convention == 'PascalCase':
            return self.to_pascal_case(name)
        elif convention == 'lowercase':
            return name.lower()
        elif convention == 'UPPERCASE':
            return name.upper()
        else:
            return name

    def to_snake_case(self, name: str) -> str:
        """Convert string to snake_case."""
        # Handle empty string
        if not name:
            return name

        # Replace spaces and hyphens with underscores
        s1 = re.sub(r'[\s\-]+', '_', name)

        # Insert underscore before uppercase letters that follow lowercase letters
        s1 = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1)

        # Convert to lowercase
        result = s1.lower()

        # Clean up multiple underscores
        result = re.sub(r'_+', '_', result)

        # Remove leading/trailing underscores
        result = result.strip('_')

        return result

    def to_camel_case(self, name: str) -> str:
        """Convert string to camelCase."""
        if not name:
            return name

        # Handle leading underscores specially
        leading_underscores = ''
        stripped_name = name.lstrip('_')
        if len(stripped_name) < len(name):
            # There were leading underscores, but we want to handle them specially
            # For camelCase, leading underscores should typically be preserved
            pass

        # Split on non-alphanumeric characters
        components = re.split(r'[^a-zA-Z0-9]+', stripped_name)

        # Filter out empty components
        components = [comp for comp in components if comp]

        if not components:
            return name.lower()

        # First component stays lowercase, rest get title case
        result = components[0].lower()
        for component in components[1:]:
            if component:
                result += component.capitalize()

        return result

    def to_pascal_case(self, name: str) -> str:
        """Convert string to PascalCase."""
        if not name:
            return name

        # Split on non-alphanumeric characters
        components = re.split(r'[^a-zA-Z0-9]+', name)

        # Filter out empty components and capitalize each
        components = [comp.capitalize() for comp in components if comp]

        return ''.join(components)

    def get_column_mapping(self, input_schema: pa.Schema) -> Dict[str, str]:
        """Get the column mapping that would be applied to a schema.

        Args:
            input_schema: Input PyArrow schema

        Returns:
            Dictionary mapping original column names to new names
        """
        mapping = {}
        for field in input_schema:
            mapped_name = self._map_column_name(field.name)
            if mapped_name is not None:
                mapping[field.name] = mapped_name
        return mapping

    def preview_mapping(self, column_names: List[str]) -> Dict[str, Optional[str]]:
        """Preview the column mapping without processing data.

        Args:
            column_names: List of column names to preview

        Returns:
            Dictionary mapping original names to new names (None means dropped)
        """
        mapping = {}
        for name in column_names:
            mapping[name] = self._map_column_name(name)
        return mapping


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
