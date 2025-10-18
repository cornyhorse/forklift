"""Tests for FWF schema package initialization."""

from unittest.mock import Mock

import pytest

from forklift.schema.fwf import FwfSchemaImporter, SchemaValidationError


class TestFwfSchemaInit:
    """Test cases for the FWF schema package __init__ module."""

    def test_fwf_schema_importer_import(self):
        """Test that FwfSchemaImporter can be imported."""
        assert FwfSchemaImporter is not None
        assert callable(FwfSchemaImporter)

    def test_schema_validation_error_import(self):
        """Test that SchemaValidationError can be imported."""
        assert SchemaValidationError is not None
        assert issubclass(SchemaValidationError, Exception)

    def test_package_exports(self):
        """Test that the package exports the expected items."""
        from forklift.schema import fwf

        assert hasattr(fwf, "FwfSchemaImporter")
        assert hasattr(fwf, "SchemaValidationError")

    def test_direct_imports(self):
        """Test direct imports from the fwf package."""
        from forklift.schema.fwf import FwfSchemaImporter as DirectImporter
        from forklift.schema.fwf import SchemaValidationError as DirectError

        assert DirectImporter is FwfSchemaImporter
        assert DirectError is SchemaValidationError
