"""Schema validation logic for Excel schema imports."""

import re
from typing import Any, Dict, List

from .type_validator import ParquetTypeValidator


class SchemaValidator:
    """Handles comprehensive validation of Excel schema structures."""

    def __init__(self, schema: Dict[str, Any]):
        self.schema = schema
        self.excel_ext = schema.get("x-excel", {})
        self.field_map = schema.get("properties", {})
        self.sheets = self.excel_ext.get("sheets", [])
        self.nulls = self.excel_ext.get("nulls", {})
        self.date_system = self.excel_ext.get("dateSystem", "1900")
        self.values_only = self.excel_ext.get("valuesOnly", True)

    def validate_all(self) -> List[str]:
        """Perform comprehensive schema validation and collect all errors."""
        errors = []

        # Validate basic JSON Schema structure
        errors.extend(self.validate_json_schema_structure())

        # Validate Excel-specific extension
        errors.extend(self.validate_excel_extension())

        # Validate sheet configurations
        errors.extend(self.validate_sheets())

        # Validate Parquet type mappings
        errors.extend(self.validate_parquet_types())

        # Validate properties and data types
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

    def validate_excel_extension(self) -> List[str]:
        """Validate x-excel extension structure and values."""
        errors = []

        if not self.excel_ext:
            errors.append("Missing required 'x-excel' extension")
            return errors

        # Validate date system
        if self.date_system not in ["1900", "1904"]:
            errors.append(f"Invalid dateSystem '{self.date_system}', must be '1900' or '1904'")

        # Validate valuesOnly flag
        if not isinstance(self.values_only, bool):
            errors.append("valuesOnly must be a boolean")

        # Validate nulls configuration
        if self.nulls:
            if "global" in self.nulls and not isinstance(self.nulls["global"], list):
                errors.append("x-excel.nulls.global must be a list")
            if "perColumn" in self.nulls and not isinstance(self.nulls["perColumn"], dict):
                errors.append("x-excel.nulls.perColumn must be a dictionary")

        return errors

    def validate_sheets(self) -> List[str]:
        """Validate sheet configurations."""
        errors = []

        if not self.sheets:
            errors.append("x-excel.sheets array is required and cannot be empty")
            return errors

        for i, sheet in enumerate(self.sheets):
            if not isinstance(sheet, dict):
                errors.append(f"Sheet {i} configuration must be a dictionary")
                continue

            # Validate sheet selection
            select = sheet.get("select")
            if select is None:
                errors.append(f"Sheet {i} missing required 'select' configuration")
            elif isinstance(select, dict):
                if not any(key in select for key in ["name", "index", "regex"]):
                    errors.append(f"Sheet {i} select must have 'name', 'index', or 'regex'")
            else:
                errors.append(f"Sheet {i} select must be a dictionary")

            # Validate columns
            columns = sheet.get("columns")
            if columns:
                if not isinstance(columns, list):
                    errors.append(f"Sheet {i} columns must be a list")
                else:
                    errors.extend(self.validate_sheet_columns(columns, i))

            # Validate header configuration
            header = sheet.get("header")
            if header and isinstance(header, dict):
                row = header.get("row")
                if row and not isinstance(row, int):
                    errors.append(f"Sheet {i} header.row must be an integer")
                mode = header.get("mode")
                if mode and mode not in ["present", "absent", "auto"]:
                    errors.append(f"Sheet {i} invalid header mode '{mode}'")

            # Validate data start row
            data_start = sheet.get("dataStartRow")
            if data_start and not isinstance(data_start, int):
                errors.append(f"Sheet {i} dataStartRow must be an integer")

        return errors

    def validate_sheet_columns(self, columns: List[Dict[str, Any]], sheet_index: int) -> List[str]:
        """Validate column configurations for a sheet."""
        errors = []
        positions_used = set()

        for j, column in enumerate(columns):
            if not isinstance(column, dict):
                errors.append(f"Sheet {sheet_index} column {j} must be a dictionary")
                continue

            # Validate required fields
            name = column.get("name")
            if not name:
                errors.append(f"Sheet {sheet_index} column {j} missing required 'name'")

            position = column.get("position")
            if position is None:
                errors.append(f"Sheet {sheet_index} column {j} missing required 'position'")
            elif isinstance(position, str):
                # Validate Excel column notation (A, B, AA, etc.)
                if not re.match(r'^[A-Z]+$', position):
                    errors.append(f"Sheet {sheet_index} column {j} invalid position '{position}'")
                elif position in positions_used:
                    errors.append(f"Sheet {sheet_index} column {j} duplicate position '{position}'")
                else:
                    positions_used.add(position)
            elif isinstance(position, int):
                if position < 1:
                    errors.append(f"Sheet {sheet_index} column {j} position must be >= 1")
                elif position in positions_used:
                    errors.append(f"Sheet {sheet_index} column {j} duplicate position {position}")
                else:
                    positions_used.add(position)

            # Validate column type
            col_type = column.get("type")
            if col_type:
                valid_types = {"string", "integer", "number", "boolean", "array", "object"}
                if col_type not in valid_types:
                    errors.append(f"Sheet {sheet_index} column {j} invalid type '{col_type}'")

            # Validate Parquet type
            parquet_type = column.get("parquetType")
            if parquet_type and not ParquetTypeValidator.is_valid_parquet_type(parquet_type):
                errors.append(f"Sheet {sheet_index} column {j} invalid Parquet type '{parquet_type}'")

            # Validate format
            format_val = column.get("format")
            if format_val and col_type == "string":
                valid_formats = {"date", "date-time", "email", "uri", "uuid"}
                if format_val not in valid_formats:
                    errors.append(f"Sheet {sheet_index} column {j} invalid format '{format_val}'")

        return errors

    def validate_parquet_types(self) -> List[str]:
        """Validate Parquet type mappings in sheet columns."""
        errors = []

        for i, sheet in enumerate(self.sheets):
            if not isinstance(sheet, dict):
                continue  # Skip invalid sheets, will be caught by sheet validation
            columns = sheet.get("columns", [])
            for j, column in enumerate(columns):
                if isinstance(column, dict):
                    parquet_type = column.get("parquetType")
                    if parquet_type and not ParquetTypeValidator.is_valid_parquet_type(parquet_type):
                        errors.append(f"Sheet {i} column {j} invalid Parquet type '{parquet_type}'")

        return errors

    def validate_properties(self) -> List[str]:
        """Validate field properties and their constraints."""
        errors = []

        if not isinstance(self.field_map, dict):
            return errors  # This will be caught by JSON schema structure validation

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
