"""Schema validation logic for CSV schema imports."""

import re
from typing import Any, Dict, List

from .type_validator import ParquetTypeValidator


class SchemaValidator:
    """Handles comprehensive validation of CSV schema structures."""

    def __init__(self, schema: Dict[str, Any]):
        self.schema = schema
        self.csv_ext = schema.get("x-csv", {})
        self.field_map = schema.get("properties", {})

    def validate_all(self) -> List[str]:
        """Perform comprehensive schema validation and collect all errors."""
        errors = []

        # Validate basic JSON Schema structure
        errors.extend(self.validate_json_schema_structure())

        # Validate CSV-specific extension
        errors.extend(self.validate_csv_extension())

        # Validate Parquet type mappings
        errors.extend(self.validate_parquet_types())

        # Validate properties
        errors.extend(self.validate_properties())

        return errors

    def validate_json_schema_structure(self) -> List[str]:
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

    def validate_csv_extension(self) -> List[str]:
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

    def validate_parquet_types(self) -> List[str]:
        """Validate Parquet type mappings in the schema."""
        errors = []

        parquet_mapping = self.csv_ext.get("parquetTypeMapping", {})
        if parquet_mapping:
            for field_name, parquet_type in parquet_mapping.items():
                if field_name not in self.field_map:
                    errors.append(f"Parquet type mapping for unknown field '{field_name}'")

                if not ParquetTypeValidator.is_valid_parquet_type(parquet_type):
                    errors.append(f"Invalid Parquet type '{parquet_type}' for field '{field_name}'")

        return errors

    def validate_properties(self) -> List[str]:
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
