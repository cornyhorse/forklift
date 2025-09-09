"""Tests for FWF validation package initialization."""

import pytest
from unittest.mock import Mock

from forklift.schema.fwf.validation import (
    JsonSchemaValidator,
    FwfExtensionValidator,
    FieldValidator,
    ParquetTypeValidator,
    CompatibilityValidator
)


class TestFwfValidationInit:
    """Test cases for the FWF validation package __init__ module."""

    def test_json_schema_validator_import(self):
        """Test that JsonSchemaValidator can be imported."""
        assert JsonSchemaValidator is not None
        assert callable(JsonSchemaValidator)

    def test_fwf_extension_validator_import(self):
        """Test that FwfExtensionValidator can be imported."""
        assert FwfExtensionValidator is not None
        assert callable(FwfExtensionValidator)

    def test_field_validator_import(self):
        """Test that FieldValidator can be imported."""
        assert FieldValidator is not None
        assert callable(FieldValidator)

    def test_parquet_type_validator_import(self):
        """Test that ParquetTypeValidator can be imported."""
        assert ParquetTypeValidator is not None
        assert callable(ParquetTypeValidator)

    def test_compatibility_validator_import(self):
        """Test that CompatibilityValidator can be imported."""
        assert CompatibilityValidator is not None
        assert callable(CompatibilityValidator)

    def test_package_exports(self):
        """Test that the package exports the expected items."""
        from forklift.schema.fwf import validation

        assert hasattr(validation, 'JsonSchemaValidator')
        assert hasattr(validation, 'FwfExtensionValidator')
        assert hasattr(validation, 'FieldValidator')
        assert hasattr(validation, 'ParquetTypeValidator')
        assert hasattr(validation, 'CompatibilityValidator')

    def test_all_exports(self):
        """Test __all__ contains expected exports."""
        from forklift.schema.fwf import validation

        expected_exports = [
            'JsonSchemaValidator',
            'FwfExtensionValidator',
            'FieldValidator',
            'ParquetTypeValidator',
            'CompatibilityValidator'
        ]

        assert validation.__all__ == expected_exports

    def test_direct_imports(self):
        """Test direct imports from the validation package."""
        from forklift.schema.fwf.validation import JsonSchemaValidator as DirectJson
        from forklift.schema.fwf.validation import FwfExtensionValidator as DirectFwf
        from forklift.schema.fwf.validation import FieldValidator as DirectField
        from forklift.schema.fwf.validation import ParquetTypeValidator as DirectParquet
        from forklift.schema.fwf.validation import CompatibilityValidator as DirectCompat

        assert DirectJson is JsonSchemaValidator
        assert DirectFwf is FwfExtensionValidator
        assert DirectField is FieldValidator
        assert DirectParquet is ParquetTypeValidator
        assert DirectCompat is CompatibilityValidator
