"""Schema validation utilities."""

from typing import Dict, Any, List, Tuple
import pyarrow as pa
from ..utils.helpers import SchemaValidationError


class SchemaValidator:
    """Validates schema structures and data compatibility."""

    @staticmethod
    def validate_schema_structure(schema: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate that a schema has the required structure.

        Args:
            schema: Schema dictionary to validate

        Returns:
            Tuple[bool, List[str]]: (is_valid, list_of_errors)
        """
        errors = []

        # Check required top-level fields
        required_fields = ["$schema", "type", "properties"]
        for field in required_fields:
            if field not in schema:
                errors.append(f"Missing required field: {field}")

        # Validate properties structure
        if "properties" in schema:
            if not isinstance(schema["properties"], dict):
                errors.append("Properties must be a dictionary")
            else:
                # Validate each property
                for prop_name, prop_def in schema["properties"].items():
                    if not isinstance(prop_def, dict):
                        errors.append(f"Property '{prop_name}' must be a dictionary")
                    elif "type" not in prop_def:
                        errors.append(f"Property '{prop_name}' missing type definition")

        # Validate required fields list
        if "required" in schema:
            if not isinstance(schema["required"], list):
                errors.append("Required field must be a list")
            else:
                properties = schema.get("properties", {})
                for req_field in schema["required"]:
                    if req_field not in properties:
                        errors.append(f"Required field '{req_field}' not defined in properties")

        return len(errors) == 0, errors

    @staticmethod
    def validate_data_compatibility(schema: Dict[str, Any], table: pa.Table) -> Tuple[bool, List[str]]:
        """Validate that data is compatible with schema.

        Args:
            schema: Schema dictionary
            table: PyArrow table to validate

        Returns:
            Tuple[bool, List[str]]: (is_compatible, list_of_issues)
        """
        issues = []

        if "properties" not in schema:
            return False, ["Schema has no properties defined"]

        schema_properties = schema["properties"]
        table_columns = set(table.schema.names)
        schema_columns = set(schema_properties.keys())

        # Check for missing columns
        missing_in_data = schema_columns - table_columns
        if missing_in_data:
            issues.append(f"Columns missing in data: {', '.join(missing_in_data)}")

        # Check for extra columns
        extra_in_data = table_columns - schema_columns
        if extra_in_data:
            issues.append(f"Extra columns in data: {', '.join(extra_in_data)}")

        # Check required fields
        required_fields = schema.get("required", [])
        for req_field in required_fields:
            if req_field in table_columns:
                column_index = table.schema.get_field_index(req_field)
                column_data = table.column(column_index)
                if column_data.null_count > 0:
                    issues.append(f"Required field '{req_field}' has null values")

        return len(issues) == 0, issues

    @staticmethod
    def validate_transformation_config(transform_config: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate transformation configuration structure.

        Args:
            transform_config: Transformation configuration

        Returns:
            Tuple[bool, List[str]]: (is_valid, list_of_errors)
        """
        errors = []

        if not isinstance(transform_config, dict):
            return False, ["Transformation config must be a dictionary"]

        # Validate global settings if present
        if "global_settings" in transform_config:
            global_settings = transform_config["global_settings"]
            if not isinstance(global_settings, dict):
                errors.append("global_settings must be a dictionary")

        # Validate column transformations if present
        if "column_transformations" in transform_config:
            col_transforms = transform_config["column_transformations"]
            if not isinstance(col_transforms, dict):
                errors.append("column_transformations must be a dictionary")
            else:
                for col_name, transforms in col_transforms.items():
                    if not isinstance(transforms, dict):
                        errors.append(f"Transformations for column '{col_name}' must be a dictionary")
                    else:
                        for transform_name, transform_def in transforms.items():
                            if not isinstance(transform_def, dict):
                                errors.append(f"Transform '{transform_name}' for column '{col_name}' must be a dictionary")
                            elif "enabled" not in transform_def:
                                errors.append(f"Transform '{transform_name}' for column '{col_name}' missing 'enabled' field")

        return len(errors) == 0, errors
