"""Column mapping processor for transforming column names in PyArrow data."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import pyarrow as pa

from .base import BaseProcessor, ValidationResult

#: Valid values of ``ColumnMappingConfig.naming_convention``.
NAMING_CONVENTIONS = ("snake_case", "camelCase", "PascalCase", "lowercase", "UPPERCASE")


@dataclass
class ColumnMappingConfig:
    """Configuration for column mapping operations.

    Attributes:
        explicit_mappings: Direct column name mappings (source -> target)
        naming_convention: Apply standard naming convention
               ('snake_case', 'camelCase', 'PascalCase', 'lowercase', 'UPPERCASE')
        custom_transform: Custom function to transform column names
        case_sensitive: Whether mappings are case sensitive
        allow_unmapped: Whether to keep columns that don't have explicit mappings
        drop_unmapped: Whether to drop columns that don't have an explicit mapping
            (overrides allow_unmapped). A column counts as mapped when it matches an entry of
            ``explicit_mappings`` - even an identity mapping such as ``"A": "A"`` - and is
            unmapped otherwise, whether or not a naming convention would rename it.
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

        valid_conventions = set(NAMING_CONVENTIONS)
        if self.naming_convention and self.naming_convention not in valid_conventions:
            raise ValueError(
                f"naming_convention must be one of {valid_conventions}"
                f", got: {self.naming_convention}"
            )


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

    def process_batch(
        self, batch: pa.RecordBatch
    ) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process a batch by mapping column names.

        Args:
            batch: PyArrow RecordBatch to process

        Returns:
            Tuple of (mapped_batch, validation_results)
        """
        validation_results = []

        # Get current column names and work out the output names. A mapping that produces two
        # columns with the same name is a configuration error and always raises.
        current_columns = batch.schema.names
        mapped_names = self._mapped_names(current_columns)
        new_column_names = [name for name in mapped_names if name is not None]
        columns_to_keep = [i for i, name in enumerate(mapped_names) if name is not None]

        try:
            # Create new batch with mapped columns
            if columns_to_keep:
                # Select only the columns we want to keep
                arrays = [batch.column(i) for i in columns_to_keep]

                # Create new schema with mapped names
                new_fields = []
                for i, col_idx in enumerate(columns_to_keep):
                    old_field = batch.schema.field(col_idx)
                    new_field = pa.field(
                        new_column_names[i], old_field.type, old_field.nullable, old_field.metadata
                    )
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
                    validation_results.append(
                        ValidationResult(
                            is_valid=False,
                            error_message="All columns were dropped during mapping",
                            error_code="ALL_COLUMNS_DROPPED",
                        )
                    )

            return new_batch, validation_results

        except Exception as e:
            validation_results.append(
                ValidationResult(
                    is_valid=False,
                    error_message=f"Column mapping failed: {str(e)}",
                    error_code="MAPPING_ERROR",
                )
            )
            return batch, validation_results

    def output_names(self, names: Sequence[str]) -> Dict[str, Optional[str]]:
        """Output name of each input column, without touching any data.

        This is exactly the renaming ``process_batch`` applies to a batch whose columns are
        ``names`` (explicit mappings, naming convention, custom transform, ``drop_unmapped``).

        Args:
            names: Input column names, in batch order.

        Returns:
            ``{input name: output name}``; the output name is ``None`` for a column that
            ``process_batch`` drops.

        Raises:
            ValueError: If two columns would get the same output name (as ``process_batch``
                does), or if ``custom_transform`` returns something that is not a usable name.
        """
        names = list(names)
        return dict(zip(names, self._mapped_names(names)))

    def _mapped_names(self, names: Sequence[str]) -> List[Optional[str]]:
        """Output name (``None`` = dropped) per input name; raises on duplicate output names."""
        mapped = [self._map_column_name(name) for name in names]
        kept = [(name, out) for name, out in zip(names, mapped) if out is not None]
        self._check_output_names([name for name, _ in kept], [out for _, out in kept])
        return mapped

    def _map_column_name(self, column_name: str) -> Optional[str]:
        """Map a single column name according to configuration.

        Args:
            column_name: Original column name

        Returns:
            Mapped column name, or None if column should be dropped

        Raises:
            ValueError: If a custom transform returns something that is not a usable name.
        """
        # Step 1: Check explicit mappings
        mapped_name = self._apply_explicit_mapping(column_name)

        # Step 2: Apply naming convention if specified
        if self.config.naming_convention:
            mapped_name = self._apply_naming_convention(mapped_name)

        # Step 3: Apply custom transform if specified
        if self.config.custom_transform:
            mapped_name = self.config.custom_transform(mapped_name)
            if not isinstance(mapped_name, str) or mapped_name == "":
                raise ValueError(
                    f"custom_transform must return a non-empty string "
                    f"(column '{column_name}' gave {type(mapped_name).__name__})"
                )

        # Step 4: Drop columns without an explicit mapping if requested
        if self.config.drop_unmapped and not self._is_explicitly_mapped(column_name):
            return None

        return mapped_name

    def _is_explicitly_mapped(self, column_name: str) -> bool:
        """Whether ``column_name`` matches an entry of ``explicit_mappings``."""
        mappings = self.config.explicit_mappings
        if not mappings:
            return False
        if self.config.case_sensitive:
            return column_name in mappings
        lowered = column_name.lower()
        return any(source.lower() == lowered for source in mappings)

    @staticmethod
    def _check_output_names(source_names: List[str], output_names: List[str]) -> None:
        """Raise ``ValueError`` if two source columns map to the same output name."""
        sources_by_output: Dict[str, List[str]] = {}
        for source, output in zip(source_names, output_names):
            sources_by_output.setdefault(output, []).append(source)

        collisions = {out: srcs for out, srcs in sources_by_output.items() if len(srcs) > 1}
        if collisions:
            details = "; ".join(
                f"'{output}' <- {sorted(sources)}" for output, sources in collisions.items()
            )
            raise ValueError(f"Column mapping creates duplicate output column names: {details}")

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

        if self.config.naming_convention == "snake_case":
            return self._to_snake_case(column_name)
        elif self.config.naming_convention == "camelCase":
            return self._to_camel_case(column_name)
        elif self.config.naming_convention == "PascalCase":
            return self._to_pascal_case(column_name)
        elif self.config.naming_convention == "lowercase":
            return column_name.lower()
        elif self.config.naming_convention == "UPPERCASE":
            return column_name.upper()

        return column_name

    @staticmethod
    def _split_words(name: str) -> List[str]:
        """Split a column name into lower-case words.

        Separators are anything that is not a letter or digit; camelCase boundaries are
        ``aB``/``1B`` and the end of an acronym (``XMLParser`` -> ``xml``, ``parser``).
        """
        words: List[str] = []
        for chunk in re.split(r"[^0-9A-Za-z]+", name):
            if not chunk:
                continue
            chunk = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", chunk)
            chunk = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", chunk)
            words.extend(word.lower() for word in chunk.split(" ") if word)
        return words

    def _to_snake_case(self, name: str) -> str:
        """Convert name to snake_case.

        Leading and trailing underscores are kept (``_rownum`` stays ``_rownum``).

        Examples:
            StateID -> state_id
            firstName -> first_name
            XMLParser -> xml_parser
            First Name -> first_name
        """
        words = self._split_words(name)
        if not words:
            return name.lower()
        leading = len(name) - len(name.lstrip("_"))
        trailing = len(name) - len(name.rstrip("_")) if name.strip("_") else 0
        return "_" * leading + "_".join(words) + "_" * trailing

    def _to_camel_case(self, name: str) -> str:
        """Convert name to camelCase.

        Examples:
            state_id -> stateId
            StateID -> stateId
            firstName -> firstName
            First Name -> firstName
        """
        words = self._split_words(name)
        if not words:
            return name
        return words[0] + "".join(word.capitalize() for word in words[1:])

    def _to_pascal_case(self, name: str) -> str:
        """Convert name to PascalCase.

        Examples:
            state_id -> StateId
            firstName -> FirstName
            First Name -> FirstName
        """
        return "".join(word.capitalize() for word in self._split_words(name))


def create_postgres_mapper() -> ColumnMapper:
    """Create a column mapper configured for PostgreSQL naming conventions.

    PostgreSQL conventionally uses snake_case for column names.

    Returns:
        ColumnMapper configured for PostgreSQL conventions
    """
    config = ColumnMappingConfig(
        naming_convention="snake_case",
        case_sensitive=False,  # PostgreSQL is case-insensitive by default
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
        naming_convention="snake_case" if postgres_style else None,
        case_sensitive=False,
    )
    return ColumnMapper(config)
