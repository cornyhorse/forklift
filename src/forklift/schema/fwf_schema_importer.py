from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Union

from ..utils.column_name_utilities import standardize_postgres_column_name, dedupe_column_names


class SchemaValidationError(Exception):
    """Raised when schema validation fails."""
    pass


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

    # Define supported Parquet data types
    SUPPORTED_PARQUET_TYPES = {
        "int8", "int16", "int32", "int64",
        "uint8", "uint16", "uint32", "uint64",
        "float32", "double", "bool", "string", "binary",
        "date32", "date64", "timestamp[s]", "timestamp[ms]",
        "timestamp[us]", "timestamp[ns]", "duration[s]", "duration[ms]",
        "duration[us]", "duration[ns]", "decimal128(10,2)",
        "list<string>", "struct", "dictionary<values=string, indices=int32>"
    }

    def __init__(self, schema: Union[str, Path, Dict[str, Any]], validate: bool = True):
        if isinstance(schema, (str, Path)):
            with open(schema, "r", encoding="utf-8") as f:
                self.schema: Dict[str, Any] = json.load(f)
        elif isinstance(schema, dict):
            self.schema = schema
        else:
            raise TypeError("schema must be path-like or dict")

        # Extract core schema components
        self.fwf_ext: Dict[str, Any] = self.schema.get("x-fwf", {})
        self.field_map: Dict[str, Any] = self.schema.get("properties", {})
        self.required: List[str] = list(self.schema.get("required", []))
        self.additional_properties: bool = bool(self.schema.get("additionalProperties", True))

        # Extract FWF-specific configurations
        self.fields: List[Dict[str, Any]] = self.fwf_ext.get("fields", [])
        self.encoding: str = self.fwf_ext.get("encoding", "utf-8")
        self.trim: Dict[str, bool] = self.fwf_ext.get("trim", {})
        self.nulls: Dict[str, Any] = self.fwf_ext.get("nulls", {})
        self.header_rows: int = self.fwf_ext.get("headerRows", 0)
        self.footer_rows: int = self.fwf_ext.get("footerRows", 0)

        # Extract conditional schema configurations (new feature)
        self.conditional_schemas: Dict[str, Any] = self.fwf_ext.get("conditionalSchemas", {})
        self.has_conditional_schemas: bool = bool(self.conditional_schemas)
        self.flag_column: Optional[Dict[str, Any]] = self.conditional_schemas.get("flagColumn") if self.has_conditional_schemas else None
        self.schema_variants: List[Dict[str, Any]] = self.conditional_schemas.get("schemas", []) if self.has_conditional_schemas else []

        # Extract case configuration
        case_cfg = self.fwf_ext.get("case", {}) if isinstance(self.fwf_ext.get("case", {}), dict) else {}
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
        errors.extend(self._validate_json_schema_structure())

        # Validate FWF-specific extension
        errors.extend(self._validate_fwf_extension())

        # Validate field configurations
        errors.extend(self._validate_fields())

        # Validate Parquet type mappings
        errors.extend(self._validate_parquet_types())

        # Validate properties and data types
        errors.extend(self._validate_properties())

        self.validation_errors = errors
        if errors:
            error_msg = "Schema validation failed with the following errors:\n" + "\n".join(f"  - {err}" for err in errors)
            raise SchemaValidationError(error_msg)

    def _validate_json_schema_structure(self) -> List[str]:
        """Validate basic JSON Schema 2020-12 structure."""
        errors = []

        # Required JSON Schema fields
        if not self.schema.get("$schema"):
            errors.append("Missing required '$schema' field")
        elif self.schema["$schema"] != "https://json-schema.org/draft/2020-12/schema":
            errors.append("Schema must reference JSON Schema 2020-12 standard")

        if not self.schema.get("$id"):
            errors.append("Missing required '$id' field")
        elif not self.schema["$id"].startswith("https://github.com/cornyhorse/forklift/schema-standards/"):
            errors.append("Schema $id must follow the standard GitHub URL pattern")

        if not self.schema.get("title"):
            errors.append("Missing required 'title' field")

        if self.schema.get("type") != "object":
            errors.append("Schema type must be 'object'")

        if not isinstance(self.field_map, dict):
            errors.append("Properties must be a dictionary")

        return errors

    def _validate_fwf_extension(self) -> List[str]:
        """Validate x-fwf extension structure and values."""
        errors = []

        if not self.fwf_ext:
            errors.append("Missing required 'x-fwf' extension")
            return errors

        # Validate encoding
        valid_encodings = {"utf-8", "utf-8-sig", "latin-1", "cp1252", "ascii"}
        if self.encoding not in valid_encodings:
            errors.append(f"Invalid encoding '{self.encoding}', must be one of {valid_encodings}")

        # Validate header and footer rows
        if not isinstance(self.header_rows, int) or self.header_rows < 0:
            errors.append("headerRows must be a non-negative integer")

        if not isinstance(self.footer_rows, int) or self.footer_rows < 0:
            errors.append("footerRows must be a non-negative integer")

        # Validate trim configuration
        if self.trim:
            if not isinstance(self.trim, dict):
                errors.append("trim configuration must be a dictionary")
            else:
                for field_name, should_trim in self.trim.items():
                    if not isinstance(should_trim, bool):
                        errors.append(f"trim.{field_name} must be a boolean")

        # Validate nulls configuration
        if self.nulls:
            if "global" in self.nulls and not isinstance(self.nulls["global"], list):
                errors.append("x-fwf.nulls.global must be a list")
            if "perColumn" in self.nulls and not isinstance(self.nulls["perColumn"], dict):
                errors.append("x-fwf.nulls.perColumn must be a dictionary")

        # Validate case configuration
        case_cfg = self.fwf_ext.get("case")
        if case_cfg and isinstance(case_cfg, dict):
            standardize = case_cfg.get("standardizeNames")
            if standardize and standardize not in {"postgres", "snake_case", "camelCase"}:
                errors.append(f"Invalid standardizeNames value '{standardize}'")

            dedupe = case_cfg.get("dedupeNames")
            if dedupe and dedupe not in {"suffix", "prefix", "error"}:
                errors.append(f"Invalid dedupeNames value '{dedupe}'")

        return errors

    def _validate_fields(self) -> List[str]:
        """Validate fixed-width field configurations."""
        errors = []

        # Handle conditional schemas or traditional fields
        if self.has_conditional_schemas:
            errors.extend(self._validate_conditional_schemas())
        else:
            if not self.fields:
                errors.append("x-fwf.fields array is required and cannot be empty")
                return errors
            errors.extend(self._validate_traditional_fields())

        return errors

    def _validate_traditional_fields(self) -> List[str]:
        """Validate traditional field configurations."""
        errors = []
        positions_used = set()
        current_position = 1

        for i, field in enumerate(self.fields):
            if not isinstance(field, dict):
                errors.append(f"Field {i} configuration must be a dictionary")
                continue

            errors.extend(self._validate_single_field(field, i, positions_used))

            # Update position tracking
            start = field.get("start")
            length = field.get("length")
            if isinstance(start, int) and isinstance(length, int):
                current_position = max(current_position, start + length)

        return errors

    def _validate_conditional_schemas(self) -> List[str]:
        """Validate conditional schema configurations."""
        errors = []

        # Validate flag column
        if not self.flag_column:
            errors.append("Conditional schemas require a flagColumn definition")
            return errors

        errors.extend(self._validate_flag_column())

        # Validate schema variants
        if not self.schema_variants:
            errors.append("Conditional schemas require at least one schema variant")
            return errors

        errors.extend(self._validate_schema_variants())

        # Validate compatibility between schema variants
        errors.extend(self._validate_schema_compatibility())

        return errors

    def _validate_flag_column(self) -> List[str]:
        """Validate the flag column configuration."""
        errors = []

        required_fields = ["name", "start", "length"]
        for field in required_fields:
            if field not in self.flag_column:
                errors.append(f"Flag column missing required field '{field}'")

        if "start" in self.flag_column:
            start = self.flag_column["start"]
            if not isinstance(start, int) or start < 1:
                errors.append("Flag column start position must be a positive integer")

        if "length" in self.flag_column:
            length = self.flag_column["length"]
            if not isinstance(length, int) or length < 1:
                errors.append("Flag column length must be a positive integer")

        parquet_type = self.flag_column.get("parquetType")
        if parquet_type and not self._is_valid_parquet_type(parquet_type):
            errors.append(f"Flag column invalid Parquet type '{parquet_type}'")

        return errors

    def _validate_schema_variants(self) -> List[str]:
        """Validate individual schema variants."""
        errors = []
        flag_values_seen = set()

        for i, variant in enumerate(self.schema_variants):
            if not isinstance(variant, dict):
                errors.append(f"Schema variant {i} must be a dictionary")
                continue

            # Validate required fields
            flag_value = variant.get("flagValue")
            if not flag_value:
                errors.append(f"Schema variant {i} missing required 'flagValue'")
            elif flag_value in flag_values_seen:
                errors.append(f"Schema variant {i} duplicate flagValue '{flag_value}'")
            else:
                flag_values_seen.add(flag_value)

            fields = variant.get("fields")
            if not fields:
                errors.append(f"Schema variant {i} missing required 'fields'")
            elif not isinstance(fields, list):
                errors.append(f"Schema variant {i} fields must be a list")
            else:
                errors.extend(self._validate_variant_fields(fields, i))

        return errors

    def _validate_variant_fields(self, fields: List[Dict[str, Any]], variant_index: int) -> List[str]:
        """Validate fields within a schema variant."""
        errors = []
        positions_used = set()

        for j, field in enumerate(fields):
            if not isinstance(field, dict):
                errors.append(f"Schema variant {variant_index} field {j} must be a dictionary")
                continue

            errors.extend(self._validate_single_field(field, f"variant {variant_index} field {j}", positions_used))

        return errors

    def _validate_single_field(self, field: Dict[str, Any], field_id: Union[int, str], positions_used: set) -> List[str]:
        """Validate a single field configuration."""
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
        if parquet_type and not self._is_valid_parquet_type(parquet_type):
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

    def _validate_schema_compatibility(self) -> List[str]:
        """Validate compatibility between different schema variants."""
        errors = []

        # Collect all fields from all variants
        all_fields = {}  # field_name -> list of field definitions

        for i, variant in enumerate(self.schema_variants):
            fields = variant.get("fields", [])
            for field in fields:
                field_name = field.get("name")
                if field_name:
                    if field_name not in all_fields:
                        all_fields[field_name] = []
                    all_fields[field_name].append((i, field))

        # Check for compatibility issues
        for field_name, field_defs in all_fields.items():
            if len(field_defs) > 1:  # Field appears in multiple variants
                errors.extend(self._validate_field_compatibility(field_name, field_defs))

        return errors

    def _validate_field_compatibility(self, field_name: str, field_defs: List[tuple[int, Dict[str, Any]]]) -> List[str]:
        """Validate compatibility of a field across multiple schema variants."""
        errors = []

        # Get Parquet types for this field across variants
        parquet_types = set()
        position_ranges = []

        for variant_idx, field_def in field_defs:
            parquet_type = field_def.get("parquetType")
            if parquet_type:
                parquet_types.add(parquet_type)

            start = field_def.get("start")
            length = field_def.get("length")
            if isinstance(start, int) and isinstance(length, int):
                position_ranges.append((variant_idx, start, start + length - 1))

        # Check Parquet type compatibility
        if len(parquet_types) > 1:
            compatible = self._are_parquet_types_compatible(list(parquet_types))
            if not compatible:
                errors.append(f"Field '{field_name}' has incompatible Parquet types across variants: {list(parquet_types)}")

        # Check for overlapping positions that might cause issues
        for i, (v1_idx, v1_start, v1_end) in enumerate(position_ranges):
            for v2_idx, v2_start, v2_end in position_ranges[i+1:]:
                # Check if positions overlap in a way that could cause data corruption
                if self._positions_overlap_incompatibly(v1_start, v1_end, v2_start, v2_end):
                    parquet_type_1 = next((field_def.get("parquetType") for variant_idx, field_def in field_defs if variant_idx == v1_idx), None)
                    parquet_type_2 = next((field_def.get("parquetType") for variant_idx, field_def in field_defs if variant_idx == v2_idx), None)

                    if parquet_type_1 and parquet_type_2 and not self._are_parquet_types_compatible([parquet_type_1, parquet_type_2]):
                        errors.append(f"Field '{field_name}' has incompatible overlapping positions and types between variants {v1_idx} and {v2_idx}")

        return errors

    def _are_parquet_types_compatible(self, types: List[str]) -> bool:
        """Check if different Parquet types are compatible for the same logical field."""
        if len(set(types)) == 1:
            return True

        # Define compatibility groups
        numeric_types = {"int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64", "float32", "double"}
        temporal_types = {"date32", "date64", "timestamp[s]", "timestamp[ms]", "timestamp[us]", "timestamp[ns]"}
        duration_types = {"duration[s]", "duration[ms]", "duration[us]", "duration[ns]"}
        string_types = {"string", "binary"}

        # Check if all types belong to the same compatibility group
        types_set = set(types)

        if types_set.issubset(numeric_types):
            return True
        elif types_set.issubset(temporal_types):
            return True
        elif types_set.issubset(duration_types):
            return True
        elif types_set.issubset(string_types):
            return True

        # Special cases for decimal types
        decimal_types = [t for t in types if t.startswith("decimal128")]
        if len(decimal_types) == len(types):
            return True  # All decimal types are compatible

        return False

    def _positions_overlap_incompatibly(self, start1: int, end1: int, start2: int, end2: int) -> bool:
        """Check if two position ranges overlap in an incompatible way."""
        # Check for any overlap
        return not (end1 < start2 or end2 < start1)

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
        global_nulls = self.nulls.get("global", [""])

        if column_name:
            per_column = self.nulls.get("perColumn", {})
            return per_column.get(column_name, global_nulls)

        return global_nulls

    def get_field_positions(self) -> List[tuple[int, int]]:
        """Get field positions as (start, end) tuples for parsing."""
        positions = []
        for field in self.fields:
            start = field.get("start", 1)
            length = field.get("length", 1)
            end = start + length - 1
            positions.append((start - 1, end))  # Convert to 0-based indexing
        return positions

    def get_column_names(self) -> List[str]:
        """Get column names in field order."""
        names = []
        for field in self.fields:
            name = field.get("name", "")
            names.append(name)

        if self.standardize_names or self.dedupe_names:
            return self.standardize_column_names(names)

        return names

    def should_trim_field(self, field_name: str) -> bool:
        """Check if a field should be trimmed."""
        return self.trim.get(field_name, True)  # Default to trim

    def standardize_column_names(self, column_names: List[str]) -> List[str]:
        """Apply column name standardization if configured."""
        if not self.standardize_names:
            return column_names

        if self.standardize_names == "postgres":
            standardized = [standardize_postgres_column_name(name) for name in column_names]
        else:
            # Add other standardization methods as needed
            standardized = column_names

        if self.dedupe_names:
            return dedupe_column_names(standardized, self.dedupe_names)

        return standardized

    def as_dict(self) -> Dict[str, Any]:
        """Get the raw schema dictionary for backward compatibility."""
        return self.schema

    # New methods for conditional schema support

    def has_conditional_schema_support(self) -> bool:
        """Check if this schema supports conditional schemas."""
        return self.has_conditional_schemas

    def get_flag_column_info(self) -> Optional[Dict[str, Any]]:
        """Get the flag column configuration."""
        return self.flag_column

    def get_schema_variants(self) -> List[Dict[str, Any]]:
        """Get all schema variants."""
        return self.schema_variants

    def get_variant_by_flag_value(self, flag_value: str) -> Optional[Dict[str, Any]]:
        """Get a specific schema variant by flag value."""
        for variant in self.schema_variants:
            if variant.get("flagValue") == flag_value:
                return variant
        return None

    def get_all_possible_fields(self) -> Dict[str, Dict[str, Any]]:
        """Get all possible fields from all schema variants combined."""
        if not self.has_conditional_schemas:
            # Return traditional fields
            all_fields = {}
            for field in self.fields:
                field_name = field.get("name")
                if field_name:
                    all_fields[field_name] = field
            return all_fields

        # Combine fields from all variants
        all_fields = {}

        # Add flag column first
        if self.flag_column and self.flag_column.get("name"):
            all_fields[self.flag_column["name"]] = self.flag_column

        # Add fields from all variants
        for variant in self.schema_variants:
            fields = variant.get("fields", [])
            for field in fields:
                field_name = field.get("name")
                if field_name and field_name not in all_fields:
                    # Store the field with additional metadata about which variants contain it
                    all_fields[field_name] = {
                        **field,
                        "_appears_in_variants": [variant.get("flagValue")]
                    }
                elif field_name and field_name in all_fields:
                    # Add to existing field's variant list
                    if "_appears_in_variants" not in all_fields[field_name]:
                        all_fields[field_name]["_appears_in_variants"] = []
                    all_fields[field_name]["_appears_in_variants"].append(variant.get("flagValue"))

        return all_fields

    def get_unified_parquet_schema(self) -> Dict[str, str]:
        """Get a unified Parquet schema that accommodates all variants."""
        unified_schema = {}
        all_fields = self.get_all_possible_fields()

        for field_name, field_info in all_fields.items():
            if field_name == self.flag_column.get("name") if self.flag_column else None:
                # Flag column
                unified_schema[field_name] = field_info.get("parquetType", "string")
            else:
                # Determine the best unified type for this field
                variants_with_field = field_info.get("_appears_in_variants", [])
                if len(variants_with_field) == len(self.schema_variants):
                    # Field appears in all variants, use its Parquet type
                    unified_schema[field_name] = field_info.get("parquetType", "string")
                else:
                    # Field doesn't appear in all variants, make it nullable
                    base_type = field_info.get("parquetType", "string")
                    unified_schema[field_name] = base_type  # Parquet handles nullability

        return unified_schema

    def get_fields_for_flag_value(self, flag_value: str) -> List[Dict[str, Any]]:
        """Get field definitions for a specific flag value."""
        if not self.has_conditional_schemas:
            return self.fields

        variant = self.get_variant_by_flag_value(flag_value)
        if variant:
            return variant.get("fields", [])
        return []

    def get_field_positions_for_flag_value(self, flag_value: str) -> List[tuple[int, int]]:
        """Get field positions for a specific flag value."""
        if not self.has_conditional_schemas:
            return self.get_field_positions()

        fields = self.get_fields_for_flag_value(flag_value)
        positions = []

        # Add flag column position first
        if self.flag_column:
            flag_start = self.flag_column.get("start", 1)
            flag_length = self.flag_column.get("length", 1)
            positions.append((flag_start - 1, flag_start + flag_length - 1))

        # Add variant-specific field positions
        for field in fields:
            start = field.get("start", 1)
            length = field.get("length", 1)
            end = start + length - 1
            positions.append((start - 1, end))  # Convert to 0-based indexing

        return positions

    def get_column_names_for_flag_value(self, flag_value: str) -> List[str]:
        """Get column names for a specific flag value."""
        if not self.has_conditional_schemas:
            return self.get_column_names()

        names = []

        # Add flag column name first
        if self.flag_column and self.flag_column.get("name"):
            names.append(self.flag_column["name"])

        # Add variant-specific field names
        fields = self.get_fields_for_flag_value(flag_value)
        for field in fields:
            name = field.get("name", "")
            if name and name != self.flag_column.get("name"):  # Avoid duplicating flag column
                names.append(name)

        if self.standardize_names or self.dedupe_names:
            return self.standardize_column_names(names)

        return names

    def get_all_possible_flag_values(self) -> List[str]:
        """Get all possible flag values defined in the schema."""
        if not self.has_conditional_schemas:
            return []

        return [variant.get("flagValue") for variant in self.schema_variants if variant.get("flagValue")]

    def validate_flag_value(self, flag_value: str) -> bool:
        """Check if a flag value is valid according to the schema."""
        return flag_value in self.get_all_possible_flag_values()

    def get_record_mapping_for_row(self, row_data: str) -> Optional[Dict[str, Any]]:
        """Get the appropriate field mapping for a given row based on its flag value."""
        if not self.has_conditional_schemas or not self.flag_column:
            return None

        # Extract flag value from the row
        flag_start = self.flag_column.get("start", 1) - 1  # Convert to 0-based
        flag_length = self.flag_column.get("length", 1)

        if len(row_data) > flag_start + flag_length:
            flag_value = row_data[flag_start:flag_start + flag_length].strip()

            # Find the appropriate variant
            variant = self.get_variant_by_flag_value(flag_value)
            if variant:
                return {
                    "flagValue": flag_value,
                    "variant": variant,
                    "fields": variant.get("fields", []),
                    "columnNames": self.get_column_names_for_flag_value(flag_value),
                    "fieldPositions": self.get_field_positions_for_flag_value(flag_value)
                }

        return None

    def _validate_parquet_types(self) -> List[str]:
        """Validate Parquet type mappings in field configurations."""
        errors = []

        if self.has_conditional_schemas:
            # Validate Parquet types in conditional schemas
            for i, variant in enumerate(self.schema_variants):
                fields = variant.get("fields", [])
                for j, field in enumerate(fields):
                    if isinstance(field, dict):
                        parquet_type = field.get("parquetType")
                        if parquet_type and not self._is_valid_parquet_type(parquet_type):
                            errors.append(f"Variant {i} field {j} invalid Parquet type '{parquet_type}'")
        else:
            # Validate traditional fields
            for i, field in enumerate(self.fields):
                if isinstance(field, dict):
                    parquet_type = field.get("parquetType")
                    if parquet_type and not self._is_valid_parquet_type(parquet_type):
                        errors.append(f"Field {i} invalid Parquet type '{parquet_type}'")

        return errors

    def _is_valid_parquet_type(self, parquet_type: str) -> bool:
        """Check if a Parquet type is valid."""
        if parquet_type in self.SUPPORTED_PARQUET_TYPES:
            return True

        # Check for parameterized types like decimal128(precision,scale)
        if parquet_type.startswith("decimal128(") and parquet_type.endswith(")"):
            return True

        # Check for timestamp with timezone
        if parquet_type.startswith("timestamp[") and parquet_type.endswith("]"):
            return True

        # Check for duration types
        if parquet_type.startswith("duration[") and parquet_type.endswith("]"):
            return True

        # Check for list types
        if parquet_type.startswith("list<") and parquet_type.endswith(">"):
            return True

        # Check for dictionary types
        if parquet_type.startswith("dictionary<") and parquet_type.endswith(">"):
            return True

        return False
