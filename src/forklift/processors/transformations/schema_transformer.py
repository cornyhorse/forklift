"""Schema-driven transformation processor."""

from __future__ import annotations
from typing import Dict, List, Tuple, Callable, Any

import pyarrow as pa

from ..base import BaseProcessor, ValidationResult
from ...utils.transformations import (
    DataTransformer,
    create_transformation_from_config
)


class SchemaBasedTransformer(BaseProcessor):
    """Schema-driven data transformer that applies transformations based on x-transformations schema extension.

    This processor reads transformation configurations from the schema's x-transformations
    extension and applies them automatically during data processing.
    """

    def __init__(self, schema: Dict[str, Any]):
        """Initialize the schema-based transformer.

        Args:
            schema: Complete schema dictionary with x-transformations extension
        """
        self.schema = schema
        self.transformer = DataTransformer()
        self.column_transformations = self._parse_transformation_config()

    def _parse_transformation_config(self) -> Dict[str, List[Callable]]:
        """Parse x-transformations schema extension into callable transformations.

        Returns:
            Dictionary mapping column names to lists of transformation functions
        """
        transformations = {}

        # First, auto-detect special types from schema properties
        self._add_special_type_transformations(transformations)

        # Then, get explicit x-transformations section from schema
        x_transformations = self.schema.get("x-transformations", {})
        column_configs = x_transformations.get("column_transformations", {})

        for column_name, column_config in column_configs.items():
            column_transforms = transformations.get(column_name, [])

            # Process each transformation type for this column
            for transform_type, config in column_config.items():
                if isinstance(config, dict) and config.get("enabled", False):
                    try:
                        transform_func = create_transformation_from_config(transform_type, config)
                        column_transforms.append(transform_func)
                    except ValueError as e:
                        # Log warning but continue processing
                        print(f"Warning: Could not create transformation {transform_type} for column {column_name}: {e}")

            if column_transforms:
                transformations[column_name] = column_transforms

        return transformations

    def _add_special_type_transformations(self, transformations: Dict[str, List[Callable]]) -> None:
        """Automatically add transformations for fields with x-special-type markers.

        Scans schema properties for fields with x-special-type and adds appropriate
        transformation functions automatically.

        Args:
            transformations: Dictionary to add special type transformations to
        """
        # Get the properties section from the schema
        properties = self.schema.get("properties", {})

        for field_name, field_definition in properties.items():
            if isinstance(field_definition, dict):
                special_type = field_definition.get("x-special-type")

                if special_type == "ssn":
                    # Auto-configure SSN formatting
                    from ...utils.transformations import SSNConfig
                    ssn_config = SSNConfig(
                        format_with_dashes=True,
                        zero_pad=True,
                        validate=True,
                        allow_invalid=False
                    )
                    transform_func = lambda col: self.transformer.apply_ssn_formatting(col, ssn_config)
                    transformations.setdefault(field_name, []).append(transform_func)

                elif special_type in ["zip-permissive", "zip-5", "zip-9"]:
                    # Auto-configure ZIP code formatting
                    from ...utils.transformations import ZipCodeConfig
                    zip_config = ZipCodeConfig(
                        zip_type=special_type,
                        format_with_dash=True,
                        zero_pad=True,
                        validate=True,
                        allow_invalid=False
                    )
                    transform_func = lambda col: self.transformer.apply_zip_code_formatting(col, zip_config)
                    transformations.setdefault(field_name, []).append(transform_func)

                elif special_type == "phone":
                    # Auto-configure phone number formatting
                    from ...utils.transformations import PhoneNumberConfig
                    phone_config = PhoneNumberConfig(
                        format_style="us-standard",
                        use_parentheses=True,
                        use_dashes=True,
                        validate=True,
                        allow_invalid=False
                    )
                    transform_func = lambda col: self.transformer.apply_phone_number_formatting(col, phone_config)
                    transformations.setdefault(field_name, []).append(transform_func)

                elif special_type == "email":
                    # Auto-configure email formatting
                    from ...utils.transformations import EmailConfig
                    email_config = EmailConfig(
                        normalize_case=True,
                        validate_format=True,
                        allow_invalid=False,
                        strip_whitespace=True,
                        normalize_domain=True
                    )
                    transform_func = lambda col: self.transformer.apply_email_formatting(col, email_config)
                    transformations.setdefault(field_name, []).append(transform_func)

                elif special_type in ["ipv4", "ipv6", "ip"]:
                    # Auto-configure IP address formatting
                    from ...utils.transformations import IPAddressConfig
                    if special_type == "ipv4":
                        ip_version = "ipv4"
                    elif special_type == "ipv6":
                        ip_version = "ipv6"
                    else:  # "ip"
                        ip_version = "both"

                    ip_config = IPAddressConfig(
                        ip_version=ip_version,
                        normalize_ipv6=True,
                        validate=True,
                        allow_invalid=False,
                        compress_ipv6=True
                    )
                    transform_func = lambda col: self.transformer.apply_ip_address_formatting(col, ip_config)
                    transformations.setdefault(field_name, []).append(transform_func)

                elif special_type == "mac-address":
                    # Auto-configure MAC address formatting
                    from ...utils.transformations import MACAddressConfig
                    mac_config = MACAddressConfig(
                        format_style="colon",
                        case_style="lower",
                        validate=True,
                        allow_invalid=False,
                        zero_pad=True
                    )
                    transform_func = lambda col: self.transformer.apply_mac_address_formatting(col, mac_config)
                    transformations.setdefault(field_name, []).append(transform_func)

    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Apply schema-based transformations to batch columns.

        Args:
            batch: PyArrow RecordBatch to transform

        Returns:
            Tuple of (transformed_batch, validation_results)
        """
        validation_results = []

        # Apply transformations for each configured column
        for column_name, transforms in self.column_transformations.items():
            if column_name in batch.schema.names:
                column_index = batch.schema.get_field_index(column_name)
                column = batch.column(column_index)

                try:
                    # Apply all transformations in sequence
                    transformed_column = column
                    for transform in transforms:
                        transformed_column = transform(transformed_column)

                    # Update the batch with transformed column
                    batch = batch.set_column(column_index, column_name, transformed_column)

                except Exception as e:
                    validation_results.append(ValidationResult(
                        is_valid=False,
                        error_message=f"Schema-based transformation failed for column '{column_name}': {str(e)}",
                        error_code="SCHEMA_TRANSFORMATION_ERROR",
                        column_name=column_name
                    ))

        return batch, validation_results
