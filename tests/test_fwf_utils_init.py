"""Tests for FWF utils package initialization."""

from unittest.mock import Mock

import pytest

from forklift.schema.fwf.utils import ColumnNameProcessor, ParquetMappingUtils


class TestFwfUtilsInit:
    """Test cases for the FWF utils package __init__ module."""

    def test_column_name_processor_import(self):
        """Test that ColumnNameProcessor can be imported."""
        assert ColumnNameProcessor is not None
        assert callable(ColumnNameProcessor)

    def test_parquet_mapping_utils_import(self):
        """Test that ParquetMappingUtils can be imported."""
        assert ParquetMappingUtils is not None
        assert callable(ParquetMappingUtils)

    def test_package_exports(self):
        """Test that the package exports the expected items."""
        from forklift.schema.fwf import utils

        assert hasattr(utils, "ColumnNameProcessor")
        assert hasattr(utils, "ParquetMappingUtils")

    def test_all_exports(self):
        """Test __all__ contains expected exports."""
        from forklift.schema.fwf import utils

        expected_exports = ["ColumnNameProcessor", "ParquetMappingUtils"]

        assert utils.__all__ == expected_exports

    def test_direct_imports(self):
        """Test direct imports from the utils package."""
        from forklift.schema.fwf.utils import \
            ColumnNameProcessor as DirectProcessor
        from forklift.schema.fwf.utils import \
            ParquetMappingUtils as DirectUtils

        assert DirectProcessor is ColumnNameProcessor
        assert DirectUtils is ParquetMappingUtils
