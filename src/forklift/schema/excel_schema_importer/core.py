"""Core Excel schema importer class."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from .exceptions import SchemaValidationError
from .validator import SchemaValidator
from .utils import SchemaDataExtractor
from .type_validator import ParquetTypeValidator


class ExcelSchemaImporter:
    """Parse a Forklift Excel schema JSON file/dict and expose derived options.

    The schema is expected to follow the internal extension structure present in
    ``schema-standards/20250826-excel.json`` (``x-excel`` root key extension). This class
    performs comprehensive validation to ensure schemas conform to the standard
    and provides complete Parquet data type mapping support.

    Provided conveniences:
      * Access to the raw schema dict (``.schema``)
      * Extraction of Forklift Excel extension (``.excel_ext``)
      * Comprehensive schema validation with detailed error reporting
      * Sheet selection and column mapping validation
      * Parquet data type mapping and validation
      * Excel-specific configuration validation (date systems, cell positioning)
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
        self.excel_ext: Dict[str, Any] = self._extractor.get_excel_extension()
        self.field_map: Dict[str, Any] = self._extractor.get_field_map()
        self.required: List[str] = self._extractor.get_required_fields()
        self.additional_properties: bool = self._extractor.get_additional_properties()

        # Extract Excel-specific configurations
        self.sheets: List[Dict[str, Any]] = self._extractor.get_sheets()
        self.nulls: Dict[str, Any] = self.excel_ext.get("nulls", {})
        self.values_only: bool = self._extractor.get_values_only()
        self.date_system: str = self._extractor.get_date_system()

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

    def get_field_map(self) -> Dict[str, Any]:
        """Get the field mapping from the schema."""
        return self._extractor.get_field_map()

    def get_excel_extension(self) -> Dict[str, Any]:
        """Get the Excel-specific extension configuration."""
        return self._extractor.get_excel_extension()

    def get_sheets(self) -> List[Dict[str, Any]]:
        """Get the sheet configurations."""
        return self._extractor.get_sheets()

    def get_null_values(self, column_name: Optional[str] = None) -> List[str]:
        """Get null values for a specific column or global defaults."""
        return self._extractor.get_null_values(column_name)

    def get_date_system(self) -> str:
        """Get the Excel date system (1900 or 1904)."""
        return self._extractor.get_date_system()

    def get_values_only(self) -> bool:
        """Get the values-only flag for Excel reading."""
        return self._extractor.get_values_only()

    def get_column_mapping(self, sheet_name: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
        """Get column mapping for a specific sheet or the first sheet."""
        return self._extractor.get_column_mapping(sheet_name)

    def as_dict(self) -> Dict[str, Any]:
        """Get the raw schema dictionary for backward compatibility."""
        return self.schema
