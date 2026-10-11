"""Schema processing and validation logic."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import pyarrow as pa

from ...io import UnifiedIOHandler
from ..config import ImportConfig
from .type_conversion import ColumnConverter, NullPolicy, csv_target_type, parse_arrow_type


def _json_kind(value: Any) -> str:
    """What a JSON value is, for a message (never the value itself)."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true/false"
    if isinstance(value, (int, float)):
        return "a number"
    if isinstance(value, str):
        return "a string"
    return "a list" if isinstance(value, list) else "an object"


def check_schema_shape(document: Any) -> None:
    """The shape every reader of a schema relies on, checked once with clear messages: a JSON
    object whose ``properties`` maps column names to objects and whose ``required`` lists
    column names. Everything else is left to the schema tooling.

    Raises:
        ValueError: naming the key that has the wrong shape and what it should be
    """
    if not isinstance(document, dict):
        raise ValueError(f"A schema must be a JSON object, not {_json_kind(document)}")
    properties = document.get("properties", {})
    if not isinstance(properties, dict):
        raise ValueError(
            "The schema's 'properties' must be an object that maps each column name to its "
            f"definition, not {_json_kind(properties)}"
        )
    for name, definition in properties.items():
        if not isinstance(definition, dict):
            raise ValueError(
                f"The definition of column {name!r} in the schema's 'properties' must be an "
                f"object (use {{}} for a column without rules), not {_json_kind(definition)}"
            )
    required = document.get("required", [])
    if not isinstance(required, list) or not all(isinstance(name, str) for name in required):
        raise ValueError(
            "The schema's 'required' must be a list of column names, for example "
            '["id", "email"]'
        )


class SchemaProcessor:
    """Handles schema loading, conversion, and validation operations."""

    def __init__(self, config: ImportConfig, io_handler: UnifiedIOHandler):
        """Initialize schema processor with configuration.

        Args:
            config: Import configuration with schema settings
            io_handler: Unified I/O handler for file operations
        """
        self.config = config
        self.io_handler = io_handler
        self.schema: Optional[pa.Schema] = None
        self.schema_dict: Optional[Dict[str, Any]] = None
        self._csv_importer = None

    def load_schema(self) -> Optional[pa.Schema]:
        """Load and parse schema from file.

        Returns:
            PyArrow schema object if schema file provided, None otherwise

        Raises:
            FileNotFoundError: If schema file path is provided but file doesn't exist
        """
        if not self.config.schema_file:
            return None

        # Use unified I/O handler to support S3 schema files
        if not self.io_handler.exists(self.config.schema_file):
            raise FileNotFoundError(f"Schema file not found: {self.config.schema_file}")

        with self.io_handler.open_for_read(self.config.schema_file, encoding="utf-8") as f:
            self.schema_dict = json.load(f)
        check_schema_shape(self.schema_dict)

        # Convert JSON schema to PyArrow schema
        self.schema = self._json_schema_to_pyarrow(self.schema_dict)

        # Read-only view for per-column type mapping and null markers. Imported lazily because
        # the schema package pulls in the schema generator. Structural validation is left to
        # the schema tooling: the engine accepts any JSON schema with "properties".
        from ...schema.csv_schema_importer import CsvSchemaImporter

        self._csv_importer = CsvSchemaImporter(self.schema_dict, validate=False)
        return self.schema

    def _json_schema_to_pyarrow(self, schema_dict: Dict[str, Any]) -> pa.Schema:
        """Convert JSON schema to PyArrow schema.

        Args:
            schema_dict: Dictionary containing JSON schema definition

        Returns:
            PyArrow schema object with fields and types
        """
        properties = schema_dict.get("properties", {})
        fields = []

        for field_name, field_def in properties.items():
            field_type = self._json_type_to_pyarrow(field_def)
            nullable = field_name not in schema_dict.get("required", [])
            fields.append(pa.field(field_name, field_type, nullable=nullable))

        return pa.schema(fields)

    def _json_type_to_pyarrow(self, field_def: Dict[str, Any]) -> pa.DataType:
        """Convert JSON schema field definition to PyArrow data type.

        Args:
            field_def: Dictionary containing field type definition

        Returns:
            PyArrow data type corresponding to the JSON schema type
        """
        json_type = field_def.get("type", "string")
        if isinstance(json_type, list):
            # JSON schema allows ["integer", "null"]; the non-null entry decides the type
            json_type = next((t for t in json_type if t != "null"), "string")
        format_hint = field_def.get("format", "")

        type_mapping = {
            "string": pa.string(),
            "integer": pa.int64(),
            "number": pa.float64(),
            "boolean": pa.bool_(),
            "null": pa.null(),
        }

        if json_type == "string" and format_hint == "date":
            return pa.date32()
        elif json_type == "string" and format_hint == "date-time":
            return pa.timestamp("us")

        return type_mapping.get(json_type, pa.string())

    def get_column_types(self) -> Dict[str, pa.DataType]:
        """Target Arrow type for every schema column that CSV text can be converted to.

        ``x-csv.parquetTypeMapping`` wins over the JSON ``type``/``format``. Nested types
        (arrays/objects) and types that cannot be built from text stay ``string``.

        Returns:
            Column name to Arrow type; empty when no schema is loaded
        """
        if not self.schema_dict or self._csv_importer is None:
            return {}

        mapping = self._csv_importer.get_parquet_type_mapping() or {}
        column_types: Dict[str, pa.DataType] = {}
        for name, field_def in self._csv_importer.get_field_map().items():
            arrow_type = parse_arrow_type(mapping[name]) if name in mapping else None
            if arrow_type is None:
                arrow_type = self._json_type_to_pyarrow(
                    field_def if isinstance(field_def, dict) else {}
                )
            column_types[name] = csv_target_type(arrow_type)
        return column_types

    def get_null_policy(self) -> NullPolicy:
        """Null markers from ``x-csv.nulls`` (an unconfigured policy when absent)."""
        if self._csv_importer is None:
            return NullPolicy()
        nulls = self._csv_importer.get_csv_extension().get("nulls")
        if not isinstance(nulls, dict) or not nulls:
            return NullPolicy()
        per_column = nulls.get("perColumn")
        return NullPolicy(
            global_values=self._csv_importer.get_null_values(),
            per_column={
                name: values
                for name, values in (per_column.items() if isinstance(per_column, dict) else [])
                if isinstance(values, list)
            },
        )

    def get_required_columns(self) -> List[str]:
        """Names of the schema's required columns."""
        if not self.schema_dict:
            return []
        return list(self.schema_dict.get("required", []))

    def build_converter(self) -> ColumnConverter:
        """Column converter implementing the loaded schema (a no-op one without schema)."""
        return ColumnConverter(self.get_column_types(), self.get_null_policy())

    def get_column_names_from_schema(self) -> Optional[list[str]]:
        """Get column names from loaded schema.

        Returns:
            List of column names if schema is loaded, None otherwise
        """
        if self.schema:
            return [field.name for field in self.schema]
        return None

    def has_row_hash_config(self) -> bool:
        """Check if schema has row hash configuration.

        Returns:
            True if schema contains row hash configuration
        """
        return self.schema_dict is not None and "x-rowHash" in self.schema_dict

    def get_row_hash_config(self) -> Optional[Dict[str, Any]]:
        """Get row hash configuration from schema.

        Returns:
            Row hash configuration dictionary or None
        """
        if self.schema_dict:
            return self.schema_dict.get("x-rowHash")
        return None

    def get_metadata_config(self) -> Dict[str, Any]:
        """Get metadata generation configuration from schema.

        Returns:
            Metadata configuration dictionary
        """
        if self.schema_dict:
            return self.schema_dict.get("x-metadata-generation", {})
        return {}
