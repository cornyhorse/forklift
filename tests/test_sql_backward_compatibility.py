"""Tests for SQL input module backward compatibility."""

import pytest
import logging


class TestSqlBackwardCompatibility:
    """Test backward compatibility of SQL input module."""

    def test_sql_imports(self):
        """Test that all SQL classes can be imported from the main module."""
        from forklift.inputs.sql import (
            SqlInputHandler,
            SqlConnectionManager,
            SqlSchemaManager,
            SqlDataReader,
            SqlTypeConverter,
            logger
        )

        # Verify all classes are available
        assert SqlInputHandler is not None
        assert SqlConnectionManager is not None
        assert SqlSchemaManager is not None
        assert SqlDataReader is not None
        assert SqlTypeConverter is not None
        assert logger is not None

    def test_sql_all_exports(self):
        """Test that __all__ contains expected exports."""
        import forklift.inputs.sql as sql_module

        expected_exports = [
            'SqlInputHandler',
            'SqlConnectionManager',
            'SqlSchemaManager',
            'SqlDataReader',
            'SqlTypeConverter',
            'logger'
        ]

        assert hasattr(sql_module, '__all__')
        assert set(sql_module.__all__) == set(expected_exports)

    def test_sql_module_docstring(self):
        """Test that the module has proper documentation."""
        import forklift.inputs.sql as sql_module

        assert sql_module.__doc__ is not None
        assert "SQL database input handler" in sql_module.__doc__
        assert "backward compatibility" in sql_module.__doc__

    def test_sql_logger_setup(self):
        """Test that logger is properly configured."""
        from forklift.inputs.sql import logger

        assert isinstance(logger, logging.Logger)
        assert logger.name == 'forklift.inputs.sql'

    def test_sql_classes_are_callable(self):
        """Test that imported classes are actually callable."""
        from forklift.inputs.sql import (
            SqlInputHandler,
            SqlConnectionManager,
            SqlSchemaManager,
            SqlDataReader,
            SqlTypeConverter
        )

        # Verify classes are callable (can be instantiated)
        assert callable(SqlInputHandler)
        assert callable(SqlConnectionManager)
        assert callable(SqlSchemaManager)
        assert callable(SqlDataReader)
        assert callable(SqlTypeConverter)
