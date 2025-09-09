"""JSON Schema validation functionality."""

from __future__ import annotations
from typing import Any, Dict, List


class JsonSchemaValidator:
    """Validates basic JSON Schema 2020-12 structure."""

    @staticmethod
    def validate(schema: Dict[str, Any]) -> List[str]:
        """Validate basic JSON Schema 2020-12 structure.

        Args:
            schema: The schema dictionary to validate

        Returns:
            List of validation error messages
        """
        errors = []

        # Required JSON Schema fields
        if not schema.get("$schema"):
            errors.append("Missing required '$schema' field")
        elif schema["$schema"] != "https://json-schema.org/draft/2020-12/schema":
            errors.append("Schema must reference JSON Schema 2020-12 standard")

        if not schema.get("$id"):
            errors.append("Missing required '$id' field")
        elif not schema["$id"].startswith("https://github.com/cornyhorse/forklift/schema-standards/"):
            errors.append("Schema $id must follow the standard GitHub URL pattern")

        if not schema.get("title"):
            errors.append("Missing required 'title' field")

        if schema.get("type") != "object":
            errors.append("Schema type must be 'object'")

        properties = schema.get("properties", {})
        if not isinstance(properties, dict):
            errors.append("Properties must be a dictionary")

        return errors
