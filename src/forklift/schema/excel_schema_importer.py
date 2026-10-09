from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from .types.data_types import is_valid_parquet_type
from .validation_utils import (
    bounds_inverted,
    regex_error,
    required_names,
    resolve_json_types,
    validate_required,
)


class SchemaValidationError(Exception):
    """Raised when schema validation fails."""

    pass


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

    # Define supported Parquet data types
    SUPPORTED_PARQUET_TYPES = {
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "float32",
        "double",
        "bool",
        "string",
        "binary",
        "date32",
        "date64",
        "timestamp[s]",
        "timestamp[ms]",
        "timestamp[us]",
        "timestamp[ns]",
        "duration[s]",
        "duration[ms]",
        "duration[us]",
        "duration[ns]",
        "decimal128(10,2)",
        "list<string>",
        "struct",
        "dictionary<values=string, indices=int32>",
    }

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

        # Extract core schema components (malformed values are kept so validation can report
        # them; the accessors below never assume more than what was validated)
        excel_ext = self.schema.get("x-excel", {})
        self.excel_ext: Dict[str, Any] = excel_ext if isinstance(excel_ext, dict) else {}
        self.field_map: Dict[str, Any] = self.schema.get("properties", {})
        self.required: List[str] = required_names(self.schema.get("required", []))
        self.additional_properties: bool = bool(self.schema.get("additionalProperties", True))

        # Extract Excel-specific configurations. Both the explicit ``sheets`` list and the single
        # ``sheet`` form emitted by the schema generator are accepted.
        self.sheets: List[Dict[str, Any]]
        self.sheets, self._sheet_errors = self._resolve_sheets(self.excel_ext)
        nulls = self.excel_ext.get("nulls", {})
        self.nulls: Dict[str, Any] = nulls if isinstance(nulls, dict) else {}
        self.values_only: bool = self.excel_ext.get("valuesOnly", True)
        self.date_system: str = self.excel_ext.get("dateSystem", "1900")

        # Validate schema if requested
        self.validation_errors: List[str] = []
        if validate:
            self.validate_schema()

    @staticmethod
    def _resolve_sheets(excel_ext: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Normalise ``x-excel.sheets`` / ``x-excel.sheet`` into a list of sheet definitions.

        The generator writes ``"sheet": <name or 0-based index>`` plus sheet-level ``header`` keys
        directly under ``x-excel``; that is expanded to a one-element ``sheets`` list.
        """
        has_sheets = "sheets" in excel_ext
        has_sheet = "sheet" in excel_ext
        if has_sheets and has_sheet:
            return [], ["x-excel must specify either 'sheets' or 'sheet', not both"]
        if has_sheets:
            sheets = excel_ext["sheets"]
            if not isinstance(sheets, list):
                return [], ["x-excel.sheets must be an array"]
            return sheets, []
        if not has_sheet:
            return [], []

        sheet = excel_ext["sheet"]
        if isinstance(sheet, bool):
            select: Any = None
        elif isinstance(sheet, str) and sheet:
            select = {"name": sheet}
        elif isinstance(sheet, int):
            select = {"index": sheet}
        elif isinstance(sheet, dict):
            select = sheet
        else:
            select = None
        if select is None:
            return [], ["x-excel.sheet must be a sheet name, a 0-based index or a select object"]

        definition: Dict[str, Any] = {"select": select}
        for key in ("header", "columns", "dataStartRow"):
            if key in excel_ext:
                definition[key] = excel_ext[key]
        return [definition], []

    def validate_schema(self) -> None:
        """Perform comprehensive schema validation and collect all errors."""
        errors = []

        # Validate basic JSON Schema structure
        errors.extend(self._validate_json_schema_structure())

        # Validate Excel-specific extension
        errors.extend(self._validate_excel_extension())

        # Validate sheet configurations
        errors.extend(self._validate_sheets())

        # Validate Parquet type mappings
        errors.extend(self._validate_parquet_types())

        # Validate properties and data types
        errors.extend(self._validate_properties())

        self.validation_errors = errors
        if errors:
            error_msg = "Schema validation failed with the following errors:\n" + "\n".join(
                f"  - {err}" for err in errors
            )
            raise SchemaValidationError(error_msg)

    def _validate_json_schema_structure(self) -> List[str]:
        """Validate basic JSON Schema 2020-12 structure."""
        errors = []

        # Required JSON Schema fields
        if not self.schema.get("$schema"):
            errors.append("Missing required '$schema' field")
        elif self.schema["$schema"] != "https://json-schema.org/draft/2020-12/schema":
            errors.append("Schema must reference JSON Schema 2020-12 standard")

        schema_id = self.schema.get("$id")
        if not schema_id:
            errors.append("Missing required '$id' field")
        elif not isinstance(schema_id, str):
            errors.append("'$id' must be a string")
        elif not schema_id.startswith("https://github.com/cornyhorse/forklift/schema-standards/"):
            errors.append("Schema $id must follow the standard GitHub URL pattern")

        if not self.schema.get("title"):
            errors.append("Missing required 'title' field")

        if self.schema.get("type") != "object":
            errors.append("Schema type must be 'object'")

        if not isinstance(self.field_map, dict):
            errors.append("Properties must be a dictionary")

        known_fields = set(self.field_map) if isinstance(self.field_map, dict) else set()
        known_fields.update(self._sheet_column_names())
        errors.extend(validate_required(self.schema.get("required"), known_fields))

        return errors

    def _sheet_column_names(self) -> List[str]:
        """Names of the columns declared in the sheet definitions (valid ``required`` targets)."""
        names = []
        for sheet in self.sheets:
            columns = sheet.get("columns") if isinstance(sheet, dict) else None
            for column in columns if isinstance(columns, list) else []:
                if isinstance(column, dict) and isinstance(column.get("name"), str):
                    names.append(column["name"])
        return names

    def _validate_excel_extension(self) -> List[str]:
        """Validate x-excel extension structure and values."""
        errors = []

        if "x-excel" in self.schema and not isinstance(self.schema["x-excel"], dict):
            errors.append("x-excel must be an object")
            return errors

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
        if self.excel_ext.get("nulls") is not None and not isinstance(
            self.excel_ext.get("nulls"), dict
        ):
            errors.append("x-excel.nulls must be an object")
        elif self.nulls:
            if "global" in self.nulls and not isinstance(self.nulls["global"], list):
                errors.append("x-excel.nulls.global must be a list")
            if "perColumn" in self.nulls and not isinstance(self.nulls["perColumn"], dict):
                errors.append("x-excel.nulls.perColumn must be a dictionary")

        return errors

    def _validate_sheets(self) -> List[str]:
        """Validate sheet configurations."""
        errors = list(self._sheet_errors)
        if errors:
            return errors

        if not self.sheets:
            errors.append(
                "x-excel.sheets array is required and cannot be empty (or give a single 'sheet')"
            )
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
                errors.extend(self._validate_sheet_select(select, i))
            else:
                errors.append(f"Sheet {i} select must be a dictionary")

            # Validate columns
            columns = sheet.get("columns")
            if columns:
                if not isinstance(columns, list):
                    errors.append(f"Sheet {i} columns must be a list")
                else:
                    errors.extend(self._validate_sheet_columns(columns, i))

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

            # Validate footer configuration
            footer = sheet.get("footer")
            if footer is not None and not isinstance(footer, dict):
                errors.append(f"Sheet {i} footer must be an object")
            elif footer and footer.get("pattern") is not None:
                reason = regex_error(footer["pattern"])
                if reason:
                    errors.append(f"Sheet {i} footer.pattern {reason}")

        return errors

    @staticmethod
    def _validate_sheet_select(select: Dict[str, Any], sheet_index: int) -> List[str]:
        """Validate the value types of a sheet ``select`` (name / 0-based index / regex)."""
        errors = []
        if "name" in select and not (isinstance(select["name"], str) and select["name"]):
            errors.append(f"Sheet {sheet_index} select.name must be a non-empty string")
        if "index" in select and (
            isinstance(select["index"], bool)
            or not isinstance(select["index"], int)
            or select["index"] < 0
        ):
            errors.append(f"Sheet {sheet_index} select.index must be a non-negative integer")
        if "regex" in select:
            reason = regex_error(select["regex"])
            if reason:
                errors.append(f"Sheet {sheet_index} select.regex {reason}")
        return errors

    def _validate_sheet_columns(
        self, columns: List[Dict[str, Any]], sheet_index: int
    ) -> List[str]:
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
            elif not isinstance(name, str):
                errors.append(f"Sheet {sheet_index} column {j} name must be a string")

            position = column.get("position")
            if position is None:
                errors.append(f"Sheet {sheet_index} column {j} missing required 'position'")
            elif isinstance(position, str):
                # Validate Excel column notation (A, B, AA, etc.)
                if not re.match(r"^[A-Z]+$", position):
                    errors.append(f"Sheet {sheet_index} column {j} invalid position '{position}'")
                elif position in positions_used:
                    errors.append(
                        f"Sheet {sheet_index} column {j} duplicate position '{position}'"
                    )
                else:
                    positions_used.add(position)
            elif isinstance(position, int):
                if position < 1:
                    errors.append(f"Sheet {sheet_index} column {j} position must be >= 1")
                elif position in positions_used:
                    errors.append(f"Sheet {sheet_index} column {j} duplicate position {position}")
                else:
                    positions_used.add(position)

            # Validate column type (string, nullable array such as ["string", "null"], anyOf)
            col_types: List[str] = []
            if column.get("type") or "anyOf" in column or "oneOf" in column:
                col_types, invalid_types, problems = resolve_json_types(column)
                for col_type in invalid_types:
                    errors.append(f"Sheet {sheet_index} column {j} invalid type '{col_type}'")
                for problem in problems:
                    errors.append(f"Sheet {sheet_index} column {j}: {problem}")

            # Validate Parquet type
            parquet_type = column.get("parquetType")
            if parquet_type and not self._is_valid_parquet_type(parquet_type):
                errors.append(
                    f"Sheet {sheet_index} column {j} invalid Parquet type '{parquet_type}'"
                )

            # Validate format
            format_val = column.get("format")
            if format_val and "string" in col_types:
                valid_formats = {"date", "date-time", "email", "uri", "uuid"}
                if not (isinstance(format_val, str) and format_val in valid_formats):
                    errors.append(f"Sheet {sheet_index} column {j} invalid format '{format_val}'")

        return errors

    def _validate_parquet_types(self) -> List[str]:
        """Validate Parquet type mappings in sheet columns."""
        errors = []

        for i, sheet in enumerate(self.sheets):
            if not isinstance(sheet, dict):
                continue  # Skip invalid sheets, will be caught by sheet validation
            columns = sheet.get("columns", [])
            if not isinstance(columns, list):
                continue  # reported by sheet validation
            for j, column in enumerate(columns):
                if isinstance(column, dict):
                    parquet_type = column.get("parquetType")
                    if parquet_type and not self._is_valid_parquet_type(parquet_type):
                        errors.append(
                            f"Sheet {i} column {j} invalid Parquet type '{parquet_type}'"
                        )

        return errors

    def _validate_properties(self) -> List[str]:
        """Validate field properties and their constraints."""
        errors: List[str] = []

        if not isinstance(self.field_map, dict):
            return errors  # This will be caught by JSON schema structure validation

        for field_name, field_def in self.field_map.items():
            if not isinstance(field_def, dict):
                errors.append(f"Field '{field_name}' definition must be a dictionary")
                continue

            # "type" may be a string, a nullable array (["string", "null"]) or an anyOf/oneOf union
            _, invalid_types, problems = resolve_json_types(field_def)
            for field_type in invalid_types:
                errors.append(f"Invalid type '{field_type}' for field '{field_name}'")
            for problem in problems:
                errors.append(f"Field '{field_name}': {problem}")

            errors.extend(self._validate_constraints(field_name, field_def))

        return errors

    def _validate_constraints(self, field_name: str, field_def: Dict[str, Any]) -> List[str]:
        """Validate the type-specific constraints of a property (and of its union branches)."""
        errors: List[str] = []
        types, _, _ = resolve_json_types(field_def)

        for kind in ("integer", "number"):
            if kind not in types:
                continue
            minimum = field_def.get("minimum")
            maximum = field_def.get("maximum")
            if minimum is not None and not isinstance(minimum, (int, float)):
                errors.append(f"Invalid minimum value for {kind} field '{field_name}'")
            if maximum is not None and not isinstance(maximum, (int, float)):
                errors.append(f"Invalid maximum value for {kind} field '{field_name}'")
            if bounds_inverted(minimum, maximum):
                errors.append(f"minimum must not exceed maximum for {kind} field '{field_name}'")

        if "string" in types:
            min_length = field_def.get("minLength")
            max_length = field_def.get("maxLength")
            pattern = field_def.get("pattern")

            if min_length is not None and (not isinstance(min_length, int) or min_length < 0):
                errors.append(f"Invalid minLength for string field '{field_name}'")
            if max_length is not None and (not isinstance(max_length, int) or max_length < 0):
                errors.append(f"Invalid maxLength for string field '{field_name}'")
            if (
                isinstance(min_length, int)
                and isinstance(max_length, int)
                and min_length > max_length
            ):
                errors.append(
                    f"minLength must not exceed maxLength for string field '{field_name}'"
                )
            if pattern is not None and regex_error(pattern):
                errors.append(f"Invalid regex pattern for field '{field_name}'")

        if "array" in types:
            items = field_def.get("items")
            if items and not isinstance(items, dict):
                errors.append(f"Array field '{field_name}' items must be an object")

        for key in ("anyOf", "oneOf"):
            branches = field_def.get(key)
            if isinstance(branches, list):
                for branch in branches:
                    if isinstance(branch, dict):
                        errors.extend(self._validate_constraints(field_name, branch))

        return errors

    def _is_valid_parquet_type(self, parquet_type: Any) -> bool:
        """Check if a Parquet type is valid (strict: units and parameters are parsed)."""
        return is_valid_parquet_type(parquet_type)

    def get_field_map(self) -> Dict[str, Any]:
        """Get the field mapping from the schema."""
        return self.field_map

    def get_excel_extension(self) -> Dict[str, Any]:
        """Get the Excel-specific extension configuration."""
        return self.excel_ext

    def get_sheets(self) -> List[Dict[str, Any]]:
        """Get the sheet configurations."""
        return self.sheets

    def get_null_values(self, column_name: Optional[str] = None) -> List[str]:
        """Get null values for a specific column or global defaults."""
        global_nulls = self.nulls.get("global", [""])

        if column_name:
            per_column = self.nulls.get("perColumn", {})
            return per_column.get(column_name, global_nulls)

        return global_nulls

    def get_date_system(self) -> str:
        """Get the Excel date system (1900 or 1904)."""
        return self.date_system

    def get_values_only(self) -> bool:
        """Get the values-only flag for Excel reading."""
        return self.values_only

    def get_column_mapping(self, sheet_name: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
        """Get column mapping for a specific sheet or the first sheet."""
        target_sheet = None

        if sheet_name:
            for sheet in self.sheets:
                select = sheet.get("select", {})
                if select.get("name") == sheet_name:
                    target_sheet = sheet
                    break
        else:
            target_sheet = self.sheets[0] if self.sheets else None

        if not target_sheet:
            return {}

        columns = target_sheet.get("columns", [])
        mapping = {}

        for column in columns:
            if isinstance(column, dict) and column.get("name"):
                mapping[column["name"]] = column

        return mapping

    def as_dict(self) -> Dict[str, Any]:
        """Get the raw schema dictionary for backward compatibility."""
        return self.schema


__all__ = ["ExcelSchemaImporter", "SchemaValidationError"]
