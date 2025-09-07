#!/usr/bin/env python3
"""
Script to fix import errors after transformations refactoring.
This script will recreate all the files in the transformations package with proper content.
"""

import os
import sys

# Add the src directory to the path
sys.path.insert(0, '/Users/matt/PycharmProjects/forklift/src')

def create_transformations_package():
    """Recreate the transformations package with proper content."""

    base_dir = '/Users/matt/PycharmProjects/forklift/src/forklift/processors/transformations'

    # Ensure directory exists
    os.makedirs(base_dir, exist_ok=True)

    # Create __init__.py
    init_content = '''"""Transformation processors package.

This package provides data transformation capabilities including:
- Basic column transformations
- Schema-driven transformations
- Common transformation functions
- Factory functions for creating transformations
"""

from .column_transformer import ColumnTransformer
from .schema_transformer import SchemaBasedTransformer
from .common import (
    trim_whitespace,
    uppercase,
    lowercase
)
from .factories import (
    apply_money_conversion,
    apply_numeric_cleaning,
    apply_regex_replace,
    apply_string_replace,
    apply_html_xml_cleaning,
    apply_string_padding,
    apply_string_trimming
)

# Re-export utilities for backward compatibility
from ...utils.transformations import (
    DataTransformer,
    create_transformation_from_config,
    # Configuration classes
    MoneyTypeConfig,
    NumericCleaningConfig,
    RegexReplaceConfig,
    StringReplaceConfig,
    HTMLXMLConfig,
    StringPaddingConfig,
    DateTimeTransformConfig,
    StringCleaningConfig
)

__all__ = [
    'ColumnTransformer',
    'SchemaBasedTransformer',
    'trim_whitespace',
    'uppercase',
    'lowercase',
    'apply_money_conversion',
    'apply_numeric_cleaning',
    'apply_regex_replace',
    'apply_string_replace',
    'apply_html_xml_cleaning',
    'apply_string_padding',
    'apply_string_trimming',
    'DataTransformer',
    'create_transformation_from_config',
    # Configuration classes
    'MoneyTypeConfig',
    'NumericCleaningConfig',
    'RegexReplaceConfig',
    'StringReplaceConfig',
    'HTMLXMLConfig',
    'StringPaddingConfig',
    'DateTimeTransformConfig',
    'StringCleaningConfig'
]'''

    # Create common.py
    common_content = '''"""Common transformation functions for basic string operations."""

import pyarrow as pa
import pyarrow.compute as pc


def trim_whitespace(column: pa.Array) -> pa.Array:
    """Remove leading and trailing whitespace from string column.

    Args:
        column: PyArrow Array containing string data

    Returns:
        PyArrow Array with whitespace trimmed from string values
    """
    if pa.types.is_string(column.type):
        return pc.utf8_trim_whitespace(column)
    return column


def uppercase(column: pa.Array) -> pa.Array:
    """Convert string column to uppercase.

    Args:
        column: PyArrow Array containing string data

    Returns:
        PyArrow Array with string values converted to uppercase
    """
    if pa.types.is_string(column.type):
        return pc.utf8_upper(column)
    return column


def lowercase(column: pa.Array) -> pa.Array:
    """Convert string column to lowercase.

    Args:
        column: PyArrow Array containing string data

    Returns:
        PyArrow Array with string values converted to lowercase
    """
    if pa.types.is_string(column.type):
        return pc.utf8_lower(column)
    return column'''

    # Create column_transformer.py
    column_transformer_content = '''"""Basic column transformation processor."""

from __future__ import annotations
from typing import Dict, List, Tuple, Callable

import pyarrow as pa

from ..base import BaseProcessor, ValidationResult


class ColumnTransformer(BaseProcessor):
    """Transforms column data (standardization, cleaning, etc.).

    This processor applies configurable transformations to column data,
    such as trimming whitespace, changing case, or applying custom
    transformation functions.

    Args:
        transformations: Dictionary mapping column names to lists of transformation functions

    Attributes:
        transformations: Dictionary of column transformations to apply
    """

    def __init__(self, transformations: Dict[str, List[Callable]]):
        """Initialize the column transformer.

        Args:
            transformations: Dictionary where keys are column names and values are
                           lists of transformation functions to apply in order
        """
        self.transformations = transformations

    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Apply transformations to batch columns.

        Applies all configured transformations to their respective columns,
        returning the transformed batch along with any errors encountered.

        Args:
            batch: PyArrow RecordBatch to transform

        Returns:
            Tuple of (transformed_batch, validation_results) where transformed_batch
            contains the data with transformations applied and validation_results
            contains any transformation errors
        """
        validation_results = []

        # Apply transformations to each configured column
        for column_name, transforms in self.transformations.items():
            if column_name in batch.schema.names:
                column_index = batch.schema.get_field_index(column_name)
                column = batch.column(column_index)

                try:
                    transformed_column = self._apply_transforms(column, transforms)
                    batch = batch.set_column(column_index, column_name, transformed_column)
                except Exception as e:
                    validation_results.append(ValidationResult(
                        is_valid=False,
                        error_message=f"Transformation failed for column '{column_name}': {str(e)}",
                        error_code="TRANSFORMATION_ERROR",
                        column_name=column_name
                    ))

        return batch, validation_results

    def _apply_transforms(self, column: pa.Array, transforms: List[Callable]) -> pa.Array:
        """Apply a list of transformations to a column.

        Applies transformation functions in sequence to the column data.

        Args:
            column: PyArrow Array to transform
            transforms: List of transformation functions to apply

        Returns:
            PyArrow Array with transformations applied
        """
        result = column
        for transform in transforms:
            result = transform(result)
        return result'''

    # Create schema_transformer.py
    schema_transformer_content = '''"""Schema-driven transformation processor."""

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

        return batch, validation_results'''

    # Create factories.py
    factories_content = '''"""Factory functions for creating enhanced transformation functions."""

from __future__ import annotations
from typing import List, Callable, Optional

import pyarrow as pa

from ...utils.transformations import (
    DataTransformer,
    MoneyTypeConfig,
    NumericCleaningConfig,
    RegexReplaceConfig,
    StringReplaceConfig,
    HTMLXMLConfig,
    StringPaddingConfig
)


def apply_money_conversion(currency_symbols: List[str] = None,
                          thousands_separator: str = ",",
                          decimal_separator: str = ".",
                          parentheses_negative: bool = True) -> Callable[[pa.Array], pa.Array]:
    """Create a money conversion transformation function.

    Args:
        currency_symbols: List of currency symbols to remove
        thousands_separator: Thousands separator character
        decimal_separator: Decimal separator character
        parentheses_negative: Whether to treat parentheses as negative

    Returns:
        Transformation function for money conversion
    """
    config = MoneyTypeConfig(
        currency_symbols=currency_symbols,
        thousands_separator=thousands_separator,
        decimal_separator=decimal_separator,
        parentheses_negative=parentheses_negative
    )
    transformer = DataTransformer()
    return lambda column: transformer.apply_money_conversion(column, config)


def apply_numeric_cleaning(thousands_separator: str = ",",
                          decimal_separator: str = ".",
                          allow_nan: bool = True,
                          target_type: str = "double") -> Callable[[pa.Array], pa.Array]:
    """Create a numeric cleaning transformation function.

    Args:
        thousands_separator: Thousands separator to remove
        decimal_separator: Decimal separator to normalize
        allow_nan: Whether to allow NaN values instead of errors
        target_type: Target numeric type (int64, double, etc.)

    Returns:
        Transformation function for numeric cleaning
    """
    config = NumericCleaningConfig(
        thousands_separator=thousands_separator,
        decimal_separator=decimal_separator,
        allow_nan=allow_nan
    )
    transformer = DataTransformer()
    return lambda column: transformer.apply_numeric_cleaning(column, config, target_type)


def apply_regex_replace(pattern: str,
                       replacement: str,
                       flags: int = 0) -> Callable[[pa.Array], pa.Array]:
    """Create a regex replace transformation function.

    Args:
        pattern: Regex pattern to match
        replacement: Replacement string
        flags: Regex flags (re.IGNORECASE, etc.)

    Returns:
        Transformation function for regex replacement
    """
    config = RegexReplaceConfig(
        pattern=pattern,
        replacement=replacement,
        flags=flags
    )
    transformer = DataTransformer()
    return lambda column: transformer.apply_regex_replace(column, config)


def apply_string_replace(old: str,
                        new: str,
                        count: int = -1) -> Callable[[pa.Array], pa.Array]:
    """Create a string replace transformation function.

    Args:
        old: String to replace
        new: Replacement string
        count: Number of replacements (-1 for all)

    Returns:
        Transformation function for string replacement
    """
    config = StringReplaceConfig(
        old=old,
        new=new,
        count=count
    )
    transformer = DataTransformer()
    return lambda column: transformer.apply_string_replace(column, config)


def apply_html_xml_cleaning(strip_tags: bool = True,
                           decode_entities: bool = True,
                           preserve_whitespace: bool = False) -> Callable[[pa.Array], pa.Array]:
    """Create an HTML/XML cleaning transformation function.

    Args:
        strip_tags: Whether to remove HTML/XML tags
        decode_entities: Whether to decode HTML entities
        preserve_whitespace: Whether to preserve whitespace formatting

    Returns:
        Transformation function for HTML/XML cleaning
    """
    config = HTMLXMLConfig(
        strip_tags=strip_tags,
        decode_entities=decode_entities,
        preserve_whitespace=preserve_whitespace
    )
    transformer = DataTransformer()
    return lambda column: transformer.apply_html_xml_cleaning(column, config)


def apply_string_padding(width: int,
                        fillchar: str = " ",
                        side: str = "left") -> Callable[[pa.Array], pa.Array]:
    """Create a string padding transformation function.

    Args:
        width: Target width for padding
        fillchar: Character to use for padding
        side: Which side to pad ("left", "right", "both")

    Returns:
        Transformation function for string padding
    """
    config = StringPaddingConfig(
        width=width,
        fillchar=fillchar,
        side=side
    )
    transformer = DataTransformer()
    return lambda column: transformer.apply_string_padding(column, config)


def apply_string_trimming(side: str = "both",
                         chars: Optional[str] = None) -> Callable[[pa.Array], pa.Array]:
    """Create a string trimming transformation function.

    Args:
        side: Which side to trim ("left", "right", "both")
        chars: Characters to trim (None for whitespace)

    Returns:
        Transformation function for string trimming
    """
    transformer = DataTransformer()
    return lambda column: transformer.apply_string_trimming(column, side, chars)'''

    # Write all files
    files_to_create = [
        ('__init__.py', init_content),
        ('common.py', common_content),
        ('column_transformer.py', column_transformer_content),
        ('schema_transformer.py', schema_transformer_content),
        ('factories.py', factories_content),
    ]

    for filename, content in files_to_create:
        filepath = os.path.join(base_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"Created {filepath}")

def test_imports():
    """Test that all imports work correctly."""
    try:
        from src.forklift.processors.transformations import (
            ColumnTransformer, SchemaBasedTransformer,
            trim_whitespace, uppercase, lowercase,
            apply_money_conversion, apply_numeric_cleaning
        )
        print("✅ All imports successful!")
        return True
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False

if __name__ == "__main__":
    create_transformations_package()
    print("Transformations package files recreated successfully!")

    print("\nTesting imports...")
    if test_imports():
        print("🎉 Refactoring completed successfully!")
    else:
        print("⚠️  There may still be import issues to resolve.")
