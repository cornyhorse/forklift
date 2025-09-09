"""Schema processing and validation logic."""

from __future__ import annotations
import json
from typing import Any, Dict, Optional

import pyarrow as pa

from ..config import ImportConfig
from ...io import UnifiedIOHandler


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

        with self.io_handler.open_for_read(self.config.schema_file, encoding='utf-8') as f:
            self.schema_dict = json.load(f)

        # Convert JSON schema to PyArrow schema
        self.schema = self._json_schema_to_pyarrow(self.schema_dict)
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
        return (self.schema_dict is not None and
                'x-rowHash' in self.schema_dict)

    def get_row_hash_config(self) -> Optional[Dict[str, Any]]:
        """Get row hash configuration from schema.

        Returns:
            Row hash configuration dictionary or None
        """
        if self.schema_dict:
            return self.schema_dict.get('x-rowHash')
        return None

    def get_metadata_config(self) -> Dict[str, Any]:
        """Get metadata generation configuration from schema.

        Returns:
            Metadata configuration dictionary
        """
        if self.schema_dict:
            return self.schema_dict.get('x-metadata-generation', {})
        return {}
