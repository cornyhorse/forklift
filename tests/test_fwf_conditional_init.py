"""Tests for FWF conditional schema package initialization."""

from unittest.mock import Mock

import pytest

from forklift.schema.fwf.conditional import ConditionalSchemaManager, VariantManager


class TestFwfConditionalInit:
    """Test cases for the FWF conditional schema package __init__ module."""

    def test_conditional_schema_manager_import(self):
        """Test that ConditionalSchemaManager can be imported."""
        assert ConditionalSchemaManager is not None
        assert callable(ConditionalSchemaManager)

    def test_variant_manager_import(self):
        """Test that VariantManager can be imported."""
        assert VariantManager is not None
        assert callable(VariantManager)

    def test_package_exports(self):
        """Test that the package exports the expected items."""
        from forklift.schema.fwf import conditional

        assert hasattr(conditional, "ConditionalSchemaManager")
        assert hasattr(conditional, "VariantManager")

    def test_all_exports(self):
        """Test __all__ contains expected exports."""
        from forklift.schema.fwf import conditional

        expected_exports = ["ConditionalSchemaManager", "VariantManager"]

        assert conditional.__all__ == expected_exports

    def test_direct_imports(self):
        """Test direct imports from the conditional package."""
        from forklift.schema.fwf.conditional import ConditionalSchemaManager as DirectManager
        from forklift.schema.fwf.conditional import VariantManager as DirectVariant

        assert DirectManager is ConditionalSchemaManager
        assert DirectVariant is VariantManager
