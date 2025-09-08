"""Core CSV schema importer class."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from .exceptions import SchemaValidationError
from .validator import SchemaValidator
from .utils import SchemaDataExtractor
from .type_validator import ParquetTypeValidator


class CsvSchemaImporter:
    """Parse a Forklift CSV schema JSON file/dict and expose derived options.

    The schema is expected to follow the internal extension structure present in
    ``schema-standards/20250826-csv.json`` (``x-csv`` root key extension). This class
    performs comprehensive validation to ensure schemas conform to the standard
    and provides complete Parquet data type mapping support.

    Provided conveniences:
      * Access to the raw schema dict (``.schema``)
      * Extraction of Forklift CSV extension (``.csv_ext``)
      * Comprehensive schema validation with detailed error reporting
      * Parquet data type mapping and validation
      * Derivation of reader options for PyArrow CSV processing
      * Column name standardization + dedupe helpers if case rules configured
    """

    def __init__(self, schema: Union[str, Path, Dict[str, Any]], validate: bool = True):
        if isinstance(schema, (str, Path)):
            with open(schema, "r", encoding="utf-8") as f:
                self.schema: Dict[str, Any] = json.load(f)
        elif isinstance(schema, dict):
            self.schema = schema
        else:
            raise TypeError("schema must be path-like or dict")

        # Initialize data extractor for convenient access to schema data
        self._extractor = SchemaDataExtractor(self.schema)

        # Extract core schema components
        self.csv_ext: Dict[str, Any] = self._extractor.get_csv_extension()
        self.field_map: Dict[str, Any] = self._extractor.get_field_map()
        self.required: List[str] = self._extractor.get_required_fields()
        self.additional_properties: bool = self._extractor.get_additional_properties()

        # Extract case configuration
        self.standardize_names: Optional[str] = self._extractor.standardize_names
        self.dedupe_names: Optional[str] = self._extractor.dedupe_names

        # Validate schema if requested
        self.validation_errors: List[str] = []
        if validate:
            self.validate_schema()

    def validate_schema(self) -> None:
        """Perform comprehensive schema validation and collect all errors."""
        validator = SchemaValidator(self.schema)
        errors = validator.validate_all()

        self.validation_errors = errors
        if errors:
            error_msg = "Schema validation failed with the following errors:\n" + "\n".join(f"  - {err}" for err in errors)
            raise SchemaValidationError(error_msg)

    def _is_valid_parquet_type(self, parquet_type: str) -> bool:
        """Check if a Parquet type is valid.

        This method is maintained for backward compatibility with existing tests.
        The actual implementation is delegated to ParquetTypeValidator.
        """
        return ParquetTypeValidator.is_valid_parquet_type(parquet_type)

    def _validate_json_schema_structure(self) -> List[str]:
        """Validate basic JSON Schema structure.

        This method is maintained for backward compatibility with existing tests.
        The actual implementation is delegated to SchemaValidator, but we need to
        handle the case where field_map might be modified after instantiation.
        """
        validator = SchemaValidator(self.schema)
        errors = validator.validate_json_schema_structure()

        # Additional check for the current field_map state (for test compatibility)
        if not isinstance(self.field_map, dict):
            if "Properties must be a dictionary" not in errors:
                errors.append("Properties must be a dictionary")

        return errors

    def _validate_parquet_types(self) -> List[str]:
        """Validate Parquet type mappings.

        This method is maintained for backward compatibility with existing tests.
        The actual implementation is delegated to SchemaValidator.
        """
        validator = SchemaValidator(self.schema)
        return validator.validate_parquet_types()

    def _validate_csv_extension(self) -> List[str]:
        """Validate CSV extension.

        This method is maintained for backward compatibility with existing tests.
        The actual implementation is delegated to SchemaValidator.
        """
        validator = SchemaValidator(self.schema)
        return validator.validate_csv_extension()

    def _validate_properties(self) -> List[str]:
        """Validate properties.

        This method is maintained for backward compatibility with existing tests.
        The actual implementation is delegated to SchemaValidator.
        """
        validator = SchemaValidator(self.schema)
        return validator.validate_properties()

    def get_field_map(self) -> Dict[str, Any]:
        """Get the field mapping from the schema."""
        return self._extractor.get_field_map()

    def get_csv_extension(self) -> Dict[str, Any]:
        """Get the CSV-specific extension configuration."""
        return self._extractor.get_csv_extension()

    def get_parquet_type_mapping(self) -> Dict[str, str]:
        """Get the Parquet type mapping for all fields."""
        return self._extractor.get_parquet_type_mapping()

    def get_encoding_priority(self) -> List[str]:
        """Get the encoding detection priority list."""
        return self._extractor.get_encoding_priority()

    def get_delimiter(self) -> str:
        """Get the delimiter configuration."""
        return self._extractor.get_delimiter()

    def get_null_values(self, column_name: Optional[str] = None) -> List[str]:
        """Get null values for a specific column or global defaults."""
        return self._extractor.get_null_values(column_name)

    def standardize_column_names(self, column_names: List[str]) -> List[str]:
        """Apply column name standardization if configured."""
        return self._extractor.standardize_column_names(column_names)

    def as_dict(self) -> Dict[str, Any]:
        """Get the raw schema dictionary for backward compatibility."""
        return self.schema

    def get_calculated_columns_config(self) -> Optional[Dict[str, Any]]:
        """Extract calculated columns configuration from schema."""
        return self._extractor.get_calculated_columns_config()

    def get_row_hash_config(self) -> Optional[Dict[str, Any]]:
        """Extract row hash configuration from schema."""
        return self._extractor.get_row_hash_config()

    def has_calculated_columns(self) -> bool:
        """Check if schema defines calculated columns."""
        return self._extractor.has_calculated_columns()

    def get_partition_columns(self) -> List[str]:
        """Get partition columns from calculated columns configuration."""
        return self._extractor.get_partition_columns()

    def get_index_columns(self) -> List[str]:
        """Get index columns from calculated columns configuration."""
        return self._extractor.get_index_columns()
