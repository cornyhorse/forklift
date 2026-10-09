"""Schema-driven transformation processor."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Tuple

import pyarrow as pa
import pyarrow.compute as pc

from ...utils.transformations import DataTransformer, create_transformation_from_config
from .._columns import column_index
from ..base import BaseProcessor, ValidationResult


class SchemaBasedTransformer(BaseProcessor):
    """Schema-driven data transformer that applies transformations
    based on x-transformations schema extension.

    This processor reads transformation configurations from the schema's x-transformations
    extension and applies them automatically during data processing.

    Per column, the explicit ``x-transformations`` steps run first, in the order they are
    configured, followed by the automatic ``x-special-type`` formatting/validation step - so an
    explicit ``regex_replace`` can strip a prefix such as ``"SSN: "`` before the value is
    validated. A value that is not a valid ``x-special-type`` value becomes NULL and is reported
    as an ``INVALID_SPECIAL_VALUE`` result with its row index.

    Configuration errors and failing transformations raise (``ValueError``); a batch is never
    returned with a column left untransformed.
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

        # Explicit x-transformations first ...
        x_transformations = self.schema.get("x-transformations", {})
        column_configs = x_transformations.get("column_transformations", {})

        for column_name, column_config in column_configs.items():
            column_transforms = transformations.get(column_name, [])

            # Process each transformation type for this column
            for transform_type, config in column_config.items():
                if isinstance(config, dict) and config.get("enabled", False):
                    try:
                        transform_func = create_transformation_from_config(transform_type, config)
                    except ValueError as e:
                        raise ValueError(
                            f"Could not create transformation '{transform_type}' "
                            f"for column '{column_name}': {e}"
                        ) from e
                    column_transforms.append(transform_func)

            if column_transforms:
                transformations[column_name] = column_transforms

        # ... then the automatic steps for fields with an x-special-type
        self._add_special_type_transformations(transformations)

        return transformations

    def _add_special_type_transformations(
        self, transformations: Dict[str, List[Callable]]
    ) -> None:
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
                        format_with_dashes=True, zero_pad=True, validate=True, allow_invalid=False
                    )

                    def create_ssn_transform(config):
                        def transform_func(col):
                            return self.transformer.apply_ssn_formatting(col, config)

                        return transform_func

                    transform_func = create_ssn_transform(ssn_config)
                    self._append_special_step(
                        transformations, field_name, special_type, transform_func
                    )

                elif special_type in ["zip-permissive", "zip-5", "zip-9"]:
                    # Auto-configure ZIP code formatting
                    from ...utils.transformations import ZipCodeConfig

                    zip_config = ZipCodeConfig(
                        zip_type=special_type,
                        format_with_dash=True,
                        zero_pad=True,
                        validate=True,
                        allow_invalid=False,
                    )

                    def create_zip_transform(config):
                        def transform_func(col):
                            return self.transformer.apply_zip_code_formatting(col, config)

                        return transform_func

                    transform_func = create_zip_transform(zip_config)
                    self._append_special_step(
                        transformations, field_name, special_type, transform_func
                    )

                elif special_type == "phone":
                    # Auto-configure phone number formatting
                    from ...utils.transformations import PhoneNumberConfig

                    phone_config = PhoneNumberConfig(
                        format_style="us-standard",
                        use_parentheses=True,
                        use_dashes=True,
                        validate=True,
                        allow_invalid=False,
                    )

                    def create_phone_transform(config):
                        def transform_func(col):
                            return self.transformer.apply_phone_number_formatting(col, config)

                        return transform_func

                    transform_func = create_phone_transform(phone_config)
                    self._append_special_step(
                        transformations, field_name, special_type, transform_func
                    )

                elif special_type == "email":
                    # Auto-configure email formatting
                    from ...utils.transformations import EmailConfig

                    email_config = EmailConfig(
                        normalize_case=True,
                        validate_format=True,
                        allow_invalid=False,
                        strip_whitespace=True,
                        normalize_domain=True,
                    )

                    def create_email_transform(config):
                        def transform_func(col):
                            return self.transformer.apply_email_formatting(col, config)

                        return transform_func

                    transform_func = create_email_transform(email_config)
                    self._append_special_step(
                        transformations, field_name, special_type, transform_func
                    )

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
                        compress_ipv6=True,
                    )

                    def create_ip_transform(config):
                        def transform_func(col):
                            return self.transformer.apply_ip_address_formatting(col, config)

                        return transform_func

                    transform_func = create_ip_transform(ip_config)
                    self._append_special_step(
                        transformations, field_name, special_type, transform_func
                    )

                elif special_type == "mac-address":
                    # Auto-configure MAC address formatting
                    from ...utils.transformations import MACAddressConfig

                    mac_config = MACAddressConfig(
                        format_style="colon",
                        case_style="lower",
                        validate=True,
                        allow_invalid=False,
                        zero_pad=True,
                    )

                    def create_mac_transform(config):
                        def transform_func(col):
                            return self.transformer.apply_mac_address_formatting(col, config)

                        return transform_func

                    transform_func = create_mac_transform(mac_config)
                    self._append_special_step(
                        transformations, field_name, special_type, transform_func
                    )

    @staticmethod
    def _append_special_step(
        transformations: Dict[str, List[Callable]],
        field_name: str,
        special_type: str,
        transform_func: Callable,
    ) -> None:
        """Add an automatic x-special-type step (tagged so invalid values can be reported)."""
        transform_func.special_type = special_type
        transformations.setdefault(field_name, []).append(transform_func)

    def process_batch(
        self, batch: pa.RecordBatch
    ) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Apply schema-based transformations to batch columns.

        Args:
            batch: PyArrow RecordBatch to transform

        Returns:
            Tuple of (transformed_batch, validation_results). ``validation_results`` has one
            ``INVALID_SPECIAL_VALUE`` entry per value that an ``x-special-type`` step rejected
            (the value is NULL in the returned batch).

        Raises:
            ValueError: If a transformation fails or a configured column name is ambiguous.
        """
        validation_results = []

        # Apply transformations for each configured column
        for column_name, transforms in self.column_transformations.items():
            col_idx = column_index(batch.schema, column_name)
            if col_idx is None:
                continue
            column = batch.column(col_idx)

            try:
                # Apply all transformations in sequence
                transformed_column = column
                for transform in transforms:
                    result_column = transform(transformed_column)

                    special_type = getattr(transform, "special_type", None)
                    if special_type is not None:
                        validation_results.extend(
                            self._invalid_special_values(
                                transformed_column, result_column, column_name, special_type
                            )
                        )
                    transformed_column = result_column

                # Update the batch with transformed column
                batch = batch.set_column(col_idx, column_name, transformed_column)

            except Exception as e:
                # Fail closed: never pass the column on untransformed
                raise ValueError(
                    f"Schema-based transformation failed for column '{column_name}' "
                    f"({type(e).__name__})"
                ) from e

        return batch, validation_results

    @staticmethod
    def _invalid_special_values(
        before: pa.Array, after: pa.Array, column_name: str, special_type: str
    ) -> List[ValidationResult]:
        """Results for rows that were non-null before an x-special-type step and null after."""
        rejected = pc.and_(pc.is_valid(before), pc.is_null(after))
        return [
            ValidationResult(
                is_valid=False,
                error_message=f"Column '{column_name}' has a value that is not a valid "
                f"'{special_type}'",
                error_code="INVALID_SPECIAL_VALUE",
                row_index=row_idx,
                column_name=column_name,
            )
            for row_idx, flag in enumerate(rejected.to_pylist())
            if flag
        ]
