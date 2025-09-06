"""Schema formatting utilities."""

import json
from typing import Any, Dict, Optional
from datetime import datetime


class SchemaFormatter:
    """Handles formatting and output of schema objects."""

    @staticmethod
    def format_schema_json(schema: Dict[str, Any], indent: int = 2) -> str:
        """Format schema as JSON string.

        Args:
            schema: Schema dictionary
            indent: JSON indentation level

        Returns:
            str: Formatted JSON string
        """
        return json.dumps(schema, indent=indent, default=str)

    @staticmethod
    def add_generation_metadata(schema: Dict[str, Any], source_file: str, rows_analyzed: int) -> Dict[str, Any]:
        """Add generation metadata to schema.

        Args:
            schema: Schema dictionary to modify
            source_file: Source file path
            rows_analyzed: Number of rows analyzed

        Returns:
            Dict: Schema with added metadata
        """
        schema["x-generation"] = {
            "generated_at": datetime.now().isoformat(),
            "source_file": str(source_file),
            "rows_analyzed": rows_analyzed,
            "generator_version": "1.0.0"
        }
        return schema

    @staticmethod
    def create_base_schema(file_type: str) -> Dict[str, Any]:
        """Create base schema structure.

        Args:
            file_type: Type of file (csv, excel, parquet)

        Returns:
            Dict: Base schema structure
        """
        return {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": f"https://github.com/cornyhorse/forklift/schema-standards/{datetime.now().strftime('%Y%m%d')}-{file_type}.json",
            "title": f"Forklift {file_type.upper()} Schema - Generated",
            "description": f"Auto-generated schema for {file_type.upper()} file processing with Forklift",
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": False
        }
