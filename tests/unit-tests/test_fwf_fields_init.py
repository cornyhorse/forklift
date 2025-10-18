"""Tests for FWF fields package initialization."""

from unittest.mock import Mock

import pytest

from forklift.schema.fwf.fields import FieldMapper, FieldParser, PositionCalculator


class TestFwfFieldsInit:
    """Test cases for the FWF fields package __init__ module."""

    def test_field_parser_import(self):
        """Test that FieldParser can be imported."""
        assert FieldParser is not None
        assert callable(FieldParser)

    def test_position_calculator_import(self):
        """Test that PositionCalculator can be imported."""
        assert PositionCalculator is not None
        assert callable(PositionCalculator)

    def test_field_mapper_import(self):
        """Test that FieldMapper can be imported."""
        assert FieldMapper is not None
        assert callable(FieldMapper)

    def test_package_exports(self):
        """Test that the package exports the expected items."""
        from forklift.schema.fwf import fields

        assert hasattr(fields, "FieldParser")
        assert hasattr(fields, "PositionCalculator")
        assert hasattr(fields, "FieldMapper")

    def test_all_exports(self):
        """Test __all__ contains expected exports."""
        from forklift.schema.fwf import fields

        expected_exports = ["FieldParser", "PositionCalculator", "FieldMapper"]

        assert fields.__all__ == expected_exports

    def test_direct_imports(self):
        """Test direct imports from the fields package."""
        from forklift.schema.fwf.fields import FieldMapper as DirectMapper
        from forklift.schema.fwf.fields import FieldParser as DirectParser
        from forklift.schema.fwf.fields import PositionCalculator as DirectCalculator

        assert DirectParser is FieldParser
        assert DirectCalculator is PositionCalculator
        assert DirectMapper is FieldMapper
