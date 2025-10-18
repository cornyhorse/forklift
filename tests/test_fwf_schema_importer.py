"""Tests for FWF schema importer backward compatibility."""

from unittest.mock import Mock

import pytest

from forklift.schema import fwf_schema_importer


class TestFwfSchemaImporterModule:
    """Test cases for the FWF schema importer module."""

    def test_module_imports(self):
        """Test that all expected components are importable."""
        # Test that all components are available
        assert hasattr(fwf_schema_importer, "FwfSchemaImporter")
        assert hasattr(fwf_schema_importer, "SchemaValidationError")

    def test_all_exports(self):
        """Test that __all__ contains expected exports."""
        expected_exports = ["FwfSchemaImporter", "SchemaValidationError"]

        assert fwf_schema_importer.__all__ == expected_exports

    def test_backward_compatibility_imports(self):
        """Test that imports work for backward compatibility."""
        # These should not raise ImportError
        from forklift.schema.fwf_schema_importer import (FwfSchemaImporter,
                                                         SchemaValidationError)

        # Verify they are the same as the module attributes
        assert FwfSchemaImporter is fwf_schema_importer.FwfSchemaImporter
        assert SchemaValidationError is fwf_schema_importer.SchemaValidationError

    def test_module_docstring(self):
        """Test that the module has proper documentation."""
        assert fwf_schema_importer.__doc__ is not None
        assert "Backward compatibility wrapper" in fwf_schema_importer.__doc__
        assert "FWF schema importer" in fwf_schema_importer.__doc__
