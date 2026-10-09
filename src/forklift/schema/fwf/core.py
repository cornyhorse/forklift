"""Core FWF schema importer - refactored main class."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from ..validation_utils import required_names, validate_required
from .conditional import ConditionalSchemaManager, VariantManager
from .exceptions import SchemaValidationError
from .fields import FieldMapper, FieldParser, PositionCalculator
from .utils import ParquetMappingUtils
from .validation import (
    CompatibilityValidator,
    FieldValidator,
    FwfExtensionValidator,
    JsonSchemaValidator,
)


class FwfSchemaImporter:
    """Parse a Forklift FWF schema JSON file/dict and expose derived options.

    The schema is expected to follow the internal extension structure present in
    ``schema-standards/20250826-fwf.json`` (``x-fwf`` root key extension). This class
    performs comprehensive validation to ensure schemas conform to the standard
    and provides complete Parquet data type mapping support.

    Provided conveniences:
      * Access to the raw schema dict (``.schema``)
      * Extraction of Forklift FWF extension (``.fwf_ext``)
      * Comprehensive schema validation with detailed error reporting
      * Fixed-width field position and length validation
      * Parquet data type mapping and validation
      * FWF-specific configuration validation (alignment, padding, trimming)
    """

    def __init__(self, schema: Union[str, Path, Dict[str, Any]], validate: bool = True):
        if isinstance(schema, (str, Path)):
            with open(schema, "r", encoding="utf-8") as f:
                self.schema: Dict[str, Any] = json.load(f)
            if not isinstance(self.schema, dict):
                raise SchemaValidationError(f"{schema}: schema root must be a JSON object")
        elif isinstance(schema, dict):
            self.schema = schema
        else:
            raise TypeError("schema must be path-like or dict")

        # Extract core schema components (malformed values are replaced by empty ones here and
        # reported by validation)
        fwf_ext = self.schema.get("x-fwf", {})
        self.fwf_ext: Dict[str, Any] = fwf_ext if isinstance(fwf_ext, dict) else {}
        self.field_map: Dict[str, Any] = self.schema.get("properties", {})
        self.required: List[str] = required_names(self.schema.get("required", []))
        self.additional_properties: bool = bool(self.schema.get("additionalProperties", True))

        # Extract FWF-specific configurations
        self.fields: List[Dict[str, Any]] = self.fwf_ext.get("fields", [])
        self.encoding: str = self.fwf_ext.get("encoding", "utf-8")
        self.trim: Dict[str, bool] = self.fwf_ext.get("trim", {})
        self.nulls: Dict[str, Any] = self.fwf_ext.get("nulls", {})
        self.header_rows: int = self.fwf_ext.get("headerRows", 0)
        self.footer_rows: int = self.fwf_ext.get("footerRows", 0)

        # Extract conditional schema configurations
        conditional_schemas = self.fwf_ext.get("conditionalSchemas", {})
        self.conditional_schemas: Dict[str, Any] = (
            conditional_schemas if isinstance(conditional_schemas, dict) else {}
        )
        self.has_conditional_schemas: bool = bool(self.conditional_schemas)

        # Initialize conditional schema manager if needed
        self._conditional_manager: Optional[ConditionalSchemaManager] = None
        self._variant_manager: Optional[VariantManager] = None
        if self.has_conditional_schemas:
            self._conditional_manager = ConditionalSchemaManager(self.conditional_schemas)
            self._variant_manager = VariantManager(
                self._conditional_manager.get_schema_variants(),
                self._conditional_manager.get_flag_column_info(),
            )

        # Extract case configuration
        case_cfg = (
            self.fwf_ext.get("case", {}) if isinstance(self.fwf_ext.get("case", {}), dict) else {}
        )
        self.standardize_names: Optional[str] = case_cfg.get("standardizeNames")
        self.dedupe_names: Optional[str] = case_cfg.get("dedupeNames")

        # Validate schema if requested
        self.validation_errors: List[str] = []
        if validate:
            self.validate_schema()

    def validate_schema(self) -> None:
        """Perform comprehensive schema validation and collect all errors."""
        errors = []

        # Validate basic JSON Schema structure
        errors.extend(JsonSchemaValidator.validate(self.schema))

        # Validate FWF-specific extension
        if "x-fwf" in self.schema and not isinstance(self.schema["x-fwf"], dict):
            errors.append("x-fwf must be an object")
        else:
            errors.extend(FwfExtensionValidator.validate(self.fwf_ext))

        # Validate field configurations
        errors.extend(self._validate_fields())

        # Validate Parquet type mappings
        errors.extend(self._validate_parquet_types())

        # Validate properties and data types
        errors.extend(self._validate_properties())

        # Validate the required list against the known properties/fields
        errors.extend(validate_required(self.schema.get("required"), self._known_field_names()))

        self.validation_errors = errors
        if errors:
            error_msg = "Schema validation failed with the following errors:\n" + "\n".join(
                f"  - {err}" for err in errors
            )
            raise SchemaValidationError(error_msg)

    def _known_field_names(self) -> List[str]:
        """Names a ``required`` entry may refer to: properties and the x-fwf fields."""
        names = list(self.field_map) if isinstance(self.field_map, dict) else []
        field_lists = [self.fields]
        if self.has_conditional_schemas:
            flag_column = self.conditional_schemas.get("flagColumn")
            field_lists.append([flag_column])
            variants = self.conditional_schemas.get("schemas")
            for variant in variants if isinstance(variants, list) else []:
                if isinstance(variant, dict):
                    field_lists.append(variant.get("fields"))
        for field_list in field_lists:
            for field in field_list if isinstance(field_list, list) else []:
                if isinstance(field, dict) and isinstance(field.get("name"), str):
                    names.append(field["name"])
        return names

    def _validate_fields(self) -> List[str]:
        """Validate field configurations."""
        # Repeated names are only acceptable when case.dedupeNames will disambiguate them
        options = (
            {"allow_duplicate_names": True} if self.dedupe_names in ("suffix", "prefix") else {}
        )
        if self.has_conditional_schemas:
            return FieldValidator.validate_conditional_fields(self.conditional_schemas, **options)
        else:
            return FieldValidator.validate_traditional_fields(self.fields, **options)

    def _validate_parquet_types(self) -> List[str]:
        """Validate Parquet type mappings in field configurations."""
        if self.has_conditional_schemas:
            return ParquetMappingUtils.validate_parquet_types_in_variants(
                self._conditional_manager.get_schema_variants()
            )
        else:
            return ParquetMappingUtils.validate_parquet_types_in_fields(self.fields)

    def _validate_properties(self) -> List[str]:
        """Validate schema properties and compatibility."""
        errors = []

        if self.has_conditional_schemas:
            # Validate compatibility between variants
            errors.extend(
                CompatibilityValidator.validate_schema_compatibility(
                    self._conditional_manager.get_schema_variants()
                )
            )

        return errors

    # Core accessor methods
    def get_field_map(self) -> Dict[str, Any]:
        """Get the field mapping from the schema."""
        return self.field_map

    def get_fwf_extension(self) -> Dict[str, Any]:
        """Get the FWF-specific extension configuration."""
        return self.fwf_ext

    def get_fields(self) -> List[Dict[str, Any]]:
        """Get the field configurations with positions and lengths."""
        return self.fields

    def get_encoding(self) -> str:
        """Get the file encoding."""
        return self.encoding

    def get_null_values(self, column_name: Optional[str] = None) -> List[str]:
        """Get null values for a specific column or global defaults."""
        return FieldParser.get_null_values(column_name, self.nulls)

    def get_field_positions(self) -> List[tuple[int, int]]:
        """Get field positions as (start, end) tuples for parsing."""
        return PositionCalculator.get_field_positions(self.fields)

    def get_column_names(self) -> List[str]:
        """Get column names in field order."""
        return FieldParser.get_column_names(self.fields, self.standardize_names, self.dedupe_names)

    def should_trim_field(self, field_name: str) -> bool:
        """Check if a field should be trimmed."""
        return FieldParser.should_trim_field(field_name, self.trim)

    def as_dict(self) -> Dict[str, Any]:
        """Get the raw schema dictionary for backward compatibility."""
        return self.schema

    # Conditional schema methods
    def has_conditional_schema_support(self) -> bool:
        """Check if this schema supports conditional schemas."""
        return self.has_conditional_schemas

    def get_flag_column_info(self) -> Optional[Dict[str, Any]]:
        """Get the flag column configuration."""
        if self._conditional_manager:
            return self._conditional_manager.get_flag_column_info()
        return None

    def get_schema_variants(self) -> List[Dict[str, Any]]:
        """Get all schema variants."""
        if self._conditional_manager:
            return self._conditional_manager.get_schema_variants()
        return []

    def get_variant_by_flag_value(self, flag_value: str) -> Optional[Dict[str, Any]]:
        """Get a specific schema variant by flag value."""
        if self._conditional_manager:
            return self._conditional_manager.get_variant_by_flag_value(flag_value)
        return None

    def get_all_possible_fields(self) -> Dict[str, Dict[str, Any]]:
        """Get all possible fields from all schema variants combined."""
        return FieldMapper.get_all_possible_fields(
            self.has_conditional_schemas,
            self.fields,
            self.get_flag_column_info(),
            self.get_schema_variants(),
        )

    def get_unified_parquet_schema(self) -> Dict[str, str]:
        """Get a unified Parquet schema that accommodates all variants."""
        all_fields = self.get_all_possible_fields()
        return FieldMapper.get_unified_parquet_schema(
            all_fields, self.get_flag_column_info(), self.get_schema_variants()
        )

    def get_fields_for_flag_value(self, flag_value: str) -> List[Dict[str, Any]]:
        """Get field definitions for a specific flag value."""
        if self._conditional_manager:
            return self._conditional_manager.get_fields_for_flag_value(flag_value)
        return self.fields

    def get_field_positions_for_flag_value(self, flag_value: str) -> List[tuple[int, int]]:
        """Get field positions for a specific flag value."""
        if self._variant_manager:
            return self._variant_manager.get_field_positions_for_flag_value(flag_value)
        return self.get_field_positions()

    def get_column_names_for_flag_value(self, flag_value: str) -> List[str]:
        """Get column names for a specific flag value."""
        if self._variant_manager:
            return self._variant_manager.get_column_names_for_flag_value(
                flag_value, self.standardize_names, self.dedupe_names
            )
        return self.get_column_names()

    def get_all_possible_flag_values(self) -> List[str]:
        """Get all possible flag values defined in the schema."""
        if self._conditional_manager:
            return self._conditional_manager.get_all_possible_flag_values()
        return []

    def validate_flag_value(self, flag_value: str) -> bool:
        """Check if a flag value is valid according to the schema."""
        if self._conditional_manager:
            return self._conditional_manager.validate_flag_value(flag_value)
        return False

    def get_record_mapping_for_row(self, row_data: str) -> Optional[Dict[str, Any]]:
        """Get the appropriate field mapping for a given row based on its flag value."""
        if self._conditional_manager:
            return self._conditional_manager.get_record_mapping_for_row(row_data)
        return None
