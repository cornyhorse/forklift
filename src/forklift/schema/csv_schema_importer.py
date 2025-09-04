from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Union

from ..utils.column_name_utilities import standardize_postgres_column_name, dedupe_column_names


class SchemaValidationError(Exception):
    """Raised when schema validation fails."""
    pass


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
        self.csv_ext: Dict[str, Any] = self.schema.get("x-csv", {})
        self.field_map: Dict[str, Any] = self.schema.get("properties", {})
        self.required: List[str] = list(self.schema.get("required", []))
        self.additional_properties: bool = bool(self.schema.get("additionalProperties", True))

        # Extract case configuration
        case_cfg = self.csv_ext.get("case", {}) if isinstance(self.csv_ext.get("case", {}), dict) else {}
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

        # Validate CSV-specific extension
        errors.extend(self._validate_csv_extension())

        # Validate Parquet type mappings
        errors.extend(self._validate_parquet_types())

        # Validate properties
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

    def _validate_csv_extension(self) -> List[str]:
        """Validate x-csv extension structure and values."""
        errors = []

        if not self.csv_ext:
            errors.append("Missing required 'x-csv' extension")
            return errors

        # Validate encoding priority
        encoding_priority = self.csv_ext.get("encodingPriority")
        if encoding_priority and not isinstance(encoding_priority, list):
            errors.append("x-csv.encodingPriority must be a list")
        elif isinstance(encoding_priority, list):
            valid_encodings = {"utf-8", "utf-8-sig", "latin-1", "cp1252"}
            for enc in encoding_priority:
                if enc not in valid_encodings:
                    errors.append(f"Invalid encoding '{enc}' in encodingPriority")

        # Validate delimiter
        delimiter = self.csv_ext.get("delimiter")
        if delimiter and delimiter not in ["auto", ",", ";", "\t", "|"]:
            if not isinstance(delimiter, str) or len(delimiter) > 5:
                errors.append("Invalid delimiter specification")

        # Validate quote char
        quote_char = self.csv_ext.get("quotechar")
        if quote_char and (not isinstance(quote_char, str) or len(quote_char) != 1):
            errors.append("quotechar must be a single character")

        # Validate escape char
        escape_char = self.csv_ext.get("escapechar")
        if escape_char and (not isinstance(escape_char, str) or len(escape_char) != 1):
            errors.append("escapechar must be a single character")

        # Validate nulls configuration
        nulls = self.csv_ext.get("nulls")
        if nulls and isinstance(nulls, dict):
            if "global" in nulls and not isinstance(nulls["global"], list):
                errors.append("x-csv.nulls.global must be a list")
            if "perColumn" in nulls and not isinstance(nulls["perColumn"], dict):
                errors.append("x-csv.nulls.perColumn must be a dictionary")

        # Validate header configuration
        header = self.csv_ext.get("header")
        if header and isinstance(header, dict):
            mode = header.get("mode")
            valid_modes = {"present", "absent", "auto", "stability_scan"}
            if mode and mode not in valid_modes:
                errors.append(f"Invalid header mode '{mode}', must be one of {valid_modes}")

            # Validate keywords for stability_scan mode
            if mode == "stability_scan":
                keywords = header.get("keywords")
                if not keywords or not isinstance(keywords, list):
                    errors.append("stability_scan mode requires 'keywords' list")

        # Validate footer configuration
        footer = self.csv_ext.get("footer")
        if footer and isinstance(footer, dict):
            mode = footer.get("mode")
            if mode and mode not in {"regex", "blank_line"}:
                errors.append(f"Invalid footer mode '{mode}', must be 'regex' or 'blank_line'")
            if mode == "regex" and not footer.get("pattern"):
                errors.append("Footer mode 'regex' requires a pattern")

        # Validate case configuration
        case_cfg = self.csv_ext.get("case")
        if case_cfg and isinstance(case_cfg, dict):
            standardize = case_cfg.get("standardizeNames")
            if standardize and standardize not in {"postgres", "snake_case", "camelCase"}:
                errors.append(f"Invalid standardizeNames value '{standardize}'")

            dedupe = case_cfg.get("dedupeNames")
            if dedupe and dedupe not in {"suffix", "prefix", "error"}:
                errors.append(f"Invalid dedupeNames value '{dedupe}'")

        return errors

    def _validate_parquet_types(self) -> List[str]:
        """Validate Parquet type mappings in the schema."""
        errors = []

        parquet_mapping = self.csv_ext.get("parquetTypeMapping", {})
        if parquet_mapping:
            for field_name, parquet_type in parquet_mapping.items():
                if field_name not in self.field_map:
                    errors.append(f"Parquet type mapping for unknown field '{field_name}'")

                if not self._is_valid_parquet_type(parquet_type):
                    errors.append(f"Invalid Parquet type '{parquet_type}' for field '{field_name}'")

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

            elif field_type == "array":
                items = field_def.get("items")
                if items and not isinstance(items, dict):
                    errors.append(f"Array field '{field_name}' items must be an object")

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

    def get_csv_extension(self) -> Dict[str, Any]:
        """Get the CSV-specific extension configuration."""
        return self.csv_ext

    def get_parquet_type_mapping(self) -> Dict[str, str]:
        """Get the Parquet type mapping for all fields."""
        return self.csv_ext.get("parquetTypeMapping", {})

    def get_encoding_priority(self) -> List[str]:
        """Get the encoding detection priority list."""
        return self.csv_ext.get("encodingPriority", ["utf-8"])

    def get_delimiter(self) -> str:
        """Get the delimiter configuration."""
        return self.csv_ext.get("delimiter", ",")

    def get_null_values(self, column_name: Optional[str] = None) -> List[str]:
        """Get null values for a specific column or global defaults."""
        nulls = self.csv_ext.get("nulls", {})
        global_nulls = nulls.get("global", [""])

        if column_name:
            per_column = nulls.get("perColumn", {})
            return per_column.get(column_name, global_nulls)

        return global_nulls

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

    def get_calculated_columns_config(self) -> Optional[Dict[str, Any]]:
        """Extract calculated columns configuration from schema.

        Returns:
            Dictionary containing calculated columns configuration or None if not present
        """
        return self.schema.get("x-calculatedColumns")

    def get_row_hash_config(self) -> Optional[Dict[str, Any]]:
        """Extract row hash configuration from schema.

        Returns:
            Dictionary containing row hash configuration or None if not present
        """
        return self.schema.get("x-rowHash")

    def has_calculated_columns(self) -> bool:
        """Check if schema defines calculated columns.

        Returns:
            True if calculated columns are defined, False otherwise
        """
        calc_cols = self.get_calculated_columns_config()
        if not calc_cols:
            return False

        return (
            bool(calc_cols.get("constants")) or
            bool(calc_cols.get("expressions")) or
            bool(calc_cols.get("calculated"))
        )

    def get_partition_columns(self) -> List[str]:
        """Get partition columns from calculated columns configuration.

        Returns:
            List of column names to be used for partitioning
        """
        calc_cols = self.get_calculated_columns_config()
        if calc_cols:
            return calc_cols.get("partitionColumns", [])
        return []

    def get_index_columns(self) -> List[str]:
        """Get index columns from calculated columns configuration.

        Returns:
            List of column names to be used for indexing
        """
        calc_cols = self.get_calculated_columns_config()
        if calc_cols:
            return calc_cols.get("indexColumns", [])
        return []


__all__ = ["CsvSchemaImporter", "SchemaValidationError"]
