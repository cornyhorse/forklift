"""Tests for SQL input module backward compatibility."""

import pytest
import logging
from unittest.mock import Mock

from forklift.inputs import sql


class TestSqlInputsModule:
    """Test cases for the SQL inputs module."""

    def test_module_imports(self):
        """Test that all expected components are importable."""
        # Test that all components are available
        assert hasattr(sql, 'SqlInputHandler')
        assert hasattr(sql, 'SqlConnectionManager')
        assert hasattr(sql, 'SqlSchemaManager')
        assert hasattr(sql, 'SqlDataReader')
        assert hasattr(sql, 'SqlTypeConverter')
        assert hasattr(sql, 'logger')

    def test_logger_configuration(self):
        """Test that the logger is properly configured."""
        assert isinstance(sql.logger, logging.Logger)
        assert sql.logger.name == 'forklift.inputs.sql'

    def test_all_exports(self):
        """Test that __all__ contains expected exports."""
        expected_exports = [
            'SqlInputHandler',
            'SqlConnectionManager',
            'SqlSchemaManager',
            'SqlDataReader',
            'SqlTypeConverter',
            'logger'
        ]

        assert sql.__all__ == expected_exports

    def test_backward_compatibility_imports(self):
        """Test that imports work for backward compatibility."""
        # These should not raise ImportError
        from forklift.inputs.sql import SqlInputHandler
        from forklift.inputs.sql import SqlConnectionManager
        from forklift.inputs.sql import SqlSchemaManager
        from forklift.inputs.sql import SqlDataReader
        from forklift.inputs.sql import SqlTypeConverter
        from forklift.inputs.sql import logger

        # Verify they are the same as the module attributes
        assert SqlInputHandler is sql.SqlInputHandler
        assert SqlConnectionManager is sql.SqlConnectionManager
        assert SqlSchemaManager is sql.SqlSchemaManager
        assert SqlDataReader is sql.SqlDataReader
        assert SqlTypeConverter is sql.SqlTypeConverter
        assert logger is sql.logger

    def test_module_docstring(self):
        """Test that the module has proper documentation."""
        assert sql.__doc__ is not None
        assert "SQL input package" in sql.__doc__
        assert "database connectivity" in sql.__doc__
