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

        if not self.fields:
            errors.append("x-fwf.fields array is required and cannot be empty")
            return errors

        positions_used = set()
        current_position = 1

        for i, field in enumerate(self.fields):
            if not isinstance(field, dict):
                errors.append(f"Field {i} configuration must be a dictionary")
                continue

            # Validate required fields
            name = field.get("name")
            if not name:
                errors.append(f"Field {i} missing required 'name'")

            start = field.get("start")
            length = field.get("length")

            if start is None:
                errors.append(f"Field {i} missing required 'start' position")
            elif not isinstance(start, int) or start < 1:
                errors.append(f"Field {i} start position must be a positive integer")

            if length is None:
                errors.append(f"Field {i} missing required 'length'")
            elif not isinstance(length, int) or length < 1:
                errors.append(f"Field {i} length must be a positive integer")

            # Check for overlapping positions
            if isinstance(start, int) and isinstance(length, int):
                field_positions = set(range(start, start + length))
                if positions_used & field_positions:
                    errors.append(f"Field {i} overlaps with previous field positions")
                positions_used.update(field_positions)

                # Check for gaps (optional warning)
                if start > current_position:
                    # This could be a warning rather than error
                    pass
                current_position = max(current_position, start + length)

            # Validate field type
            field_type = field.get("type")
            if field_type:
                valid_types = {"string", "integer", "number", "boolean"}
                if field_type not in valid_types:
                    errors.append(f"Field {i} invalid type '{field_type}'")

            # Validate Parquet type
            parquet_type = field.get("parquetType")
            if parquet_type and not self._is_valid_parquet_type(parquet_type):
                errors.append(f"Field {i} invalid Parquet type '{parquet_type}'")

            # Validate alignment
            alignment = field.get("alignment")
            if alignment and alignment not in {"left", "right", "center"}:
                errors.append(f"Field {i} invalid alignment '{alignment}', must be 'left', 'right', or 'center'")

            # Validate padding character
            pad_char = field.get("padChar")
            if pad_char and (not isinstance(pad_char, str) or len(pad_char) != 1):
                errors.append(f"Field {i} padChar must be a single character")

        return errors

    def _validate_parquet_types(self) -> List[str]:
        """Validate Parquet type mappings in field configurations."""
        errors = []

        for i, field in enumerate(self.fields):
            if isinstance(field, dict):
                parquet_type = field.get("parquetType")
                if parquet_type and not self._is_valid_parquet_type(parquet_type):
                    errors.append(f"Field {i} invalid Parquet type '{parquet_type}'")

        return errors

    def _validate_properties(self) -> List[str]:
        """Validate field properties and their constraints."""
        errors = []

        for field_name, field_def in self.field_map.items():
            if not isinstance(field_def, dict):
                errors.append(f"Field '{field_name}' definition must be a dictionary")
                continue

            field_type = field_def.get("type")
            valid_types = {"string", "integer", "number", "boolean", "array", "object"}
            if field_type not in valid_types:
                errors.append(f"Invalid type '{field_type}' for field '{field_name}'")

            # Validate constraints based on type
            if field_type == "integer":
                minimum = field_def.get("minimum")
                maximum = field_def.get("maximum")
                if minimum is not None and not isinstance(minimum, (int, float)):
                    errors.append(f"Invalid minimum value for integer field '{field_name}'")
                if maximum is not None and not isinstance(maximum, (int, float)):
                    errors.append(f"Invalid maximum value for integer field '{field_name}'")

            elif field_type == "string":
                min_length = field_def.get("minLength")
                max_length = field_def.get("maxLength")
                pattern = field_def.get("pattern")

                if min_length is not None and (not isinstance(min_length, int) or min_length < 0):
                    errors.append(f"Invalid minLength for string field '{field_name}'")
                if max_length is not None and (not isinstance(max_length, int) or max_length < 0):
                    errors.append(f"Invalid maxLength for string field '{field_name}'")
                if pattern is not None:
                    try:
                        re.compile(pattern)
                    except re.error:
                        errors.append(f"Invalid regex pattern for field '{field_name}'")

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


__all__ = ["FwfSchemaImporter", "SchemaValidationError"]
