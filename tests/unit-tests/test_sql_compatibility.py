"""Tests for SQL input backward compatibility module.

This test file ensures 100% coverage of the backward-compatibility interface
in src/forklift/inputs/sql.py by testing the import statements and __all__ exports.
"""

import logging

import pytest


class TestSqlCompatibility:
    """Test cases for SQL input backward compatibility."""

    def test_import_all_classes(self):
        """Test importing all classes from the SQL compatibility module."""
        # Import from the backward-compatibility module
        from forklift.inputs.sql import (
            SqlConnectionManager,
            SqlDataReader,
            SqlInputHandler,
            SqlSchemaManager,
            SqlTypeConverter,
        )

        # Verify all classes are imported and are callable
        assert callable(SqlInputHandler)
        assert callable(SqlConnectionManager)
        assert callable(SqlSchemaManager)
        assert callable(SqlDataReader)
        assert callable(SqlTypeConverter)

    def test_module_all_attribute(self):
        """Test that the __all__ attribute contains all expected exports."""
        import forklift.inputs.sql as sql_module

        expected_exports = [
            "SqlInputHandler",
            "SqlConnectionManager",
            "SqlSchemaManager",
            "SqlDataReader",
            "SqlTypeConverter",
            "logger",
        ]

        # Verify __all__ attribute exists and contains expected exports
        assert hasattr(sql_module, "__all__")
        assert sql_module.__all__ == expected_exports

        # Verify all items in __all__ are actually available in the module
        for export_name in expected_exports:
            assert hasattr(sql_module, export_name)
            export_item = getattr(sql_module, export_name)
            assert export_item is not None

    def test_logger_availability(self):
        """Test that the logger is available in the module."""
        import forklift.inputs.sql as sql_module

        # Verify logger exists
        assert hasattr(sql_module, "logger")
        assert isinstance(sql_module.logger, logging.Logger)
        assert sql_module.logger.name == "forklift.inputs.sql"

    def test_individual_imports(self):
        """Test importing each class individually."""
        # Test SqlInputHandler
        from forklift.inputs.sql import SqlInputHandler

        assert callable(SqlInputHandler)

        # Test SqlConnectionManager
        from forklift.inputs.sql import SqlConnectionManager

        assert callable(SqlConnectionManager)

        # Test SqlSchemaManager
        from forklift.inputs.sql import SqlSchemaManager

        assert callable(SqlSchemaManager)

        # Test SqlDataReader
        from forklift.inputs.sql import SqlDataReader

        assert callable(SqlDataReader)

        # Test SqlTypeConverter
        from forklift.inputs.sql import SqlTypeConverter

        assert callable(SqlTypeConverter)

    def test_module_docstring(self):
        """Test that the module has the expected docstring."""
        import forklift.inputs.sql as sql_module

        expected_docstring_parts = ["SQL input package", "database connectivity", "data reading"]

        assert sql_module.__doc__ is not None
        for part in expected_docstring_parts:
            assert part in sql_module.__doc__

    def test_imports_are_same_as_source_modules(self):
        """Test that imports from compatibility module are the same as source modules."""
        # Import from compatibility module
        from forklift.inputs.sql import SqlInputHandler as CompatSqlInputHandler

        # Import from source module directly
        from forklift.inputs.sql.handler import SqlInputHandler as SourceSqlInputHandler

        # They should be the same class
        assert CompatSqlInputHandler is SourceSqlInputHandler

    def test_all_exports_available(self):
        """Test that __all__ functionality works by checking module namespace."""
        import forklift.inputs.sql as sql_module

        # Get all public names from the module
        public_names = [name for name in dir(sql_module) if not name.startswith("_")]

        # All items in __all__ should be in the public namespace
        for export_name in sql_module.__all__:
            assert export_name in public_names

        # Test that we can access each export from __all__
        for export_name in sql_module.__all__:
            export_item = getattr(sql_module, export_name)
            assert export_item is not None

    def test_classes_have_expected_attributes(self):
        """Test that imported classes have expected attributes without instantiating."""
        from forklift.inputs.sql import (
            SqlConnectionManager,
            SqlDataReader,
            SqlInputHandler,
            SqlSchemaManager,
            SqlTypeConverter,
        )

        # Test that classes have expected methods/attributes (without instantiating)
        # This ensures the imports are working correctly
        # SqlInputHandler should be a class with certain methods
        assert hasattr(SqlInputHandler, "__init__")

        # SqlConnectionManager should be a class
        assert hasattr(SqlConnectionManager, "__init__")

        # SqlSchemaManager should be a class
        assert hasattr(SqlSchemaManager, "__init__")

        # SqlDataReader should be a class
        assert hasattr(SqlDataReader, "__init__")

        # SqlTypeConverter should be a class
        assert hasattr(SqlTypeConverter, "__init__")

    def test_import_error_handling(self):
        """Test that the module handles import scenarios correctly."""
        # Test that the module can be imported without errors
        # Test that re-importing works
        import forklift.inputs.sql
        import forklift.inputs.sql as sql_alias

        # Both should reference the same module
        assert forklift.inputs.sql is sql_alias

    def test_backward_compatibility_module_structure(self):
        """Test that the backward compatibility module has the expected structure."""
        # Import the module
        import forklift.inputs.sql as sql_module

        # Verify the module has all expected attributes from the backward-compatibility interface
        expected_attributes = [
            "SqlInputHandler",
            "SqlConnectionManager",
            "SqlSchemaManager",
            "SqlDataReader",
            "SqlTypeConverter",
            "logger",
            "__all__",
        ]

        for attr in expected_attributes:
            assert hasattr(sql_module, attr), f"Missing attribute: {attr}"

        # Verify __all__ contains exactly what we expect
        assert len(sql_module.__all__) == 6
        assert all(
            name in sql_module.__all__ for name in expected_attributes[:-1]
        )  # exclude __all__ itself

    def test_comprehensive_compatibility_scenario(self):
        """Test a comprehensive scenario using the backward compatibility interface."""
        # Import all exports through the compatibility interface
        from forklift.inputs.sql import (
            SqlConnectionManager,
            SqlDataReader,
            SqlInputHandler,
            SqlSchemaManager,
            SqlTypeConverter,
        )

        # Verify all exports are accessible and have expected properties
        exports_to_test = [
            ("SqlInputHandler", SqlInputHandler),
            ("SqlConnectionManager", SqlConnectionManager),
            ("SqlSchemaManager", SqlSchemaManager),
            ("SqlDataReader", SqlDataReader),
            ("SqlTypeConverter", SqlTypeConverter),
        ]

        for export_name, export_item in exports_to_test:
            assert export_item is not None, f"{export_name} should not be None"
            assert callable(export_item), f"{export_name} should be callable (class)"
            assert hasattr(export_item, "__init__"), f"{export_name} should have __init__ method"

    def test_module_level_imports_coverage(self):
        """Test that ensures all module-level import statements are executed."""
        # Import the module which will execute all import statements
        import forklift.inputs.sql

        # Verify that the module was loaded successfully and has the expected structure
        module = forklift.inputs.sql

        # This test ensures that the import statements and logger setup are executed
        assert hasattr(module, "SqlInputHandler")
        assert hasattr(module, "SqlConnectionManager")
        assert hasattr(module, "SqlSchemaManager")
        assert hasattr(module, "SqlDataReader")
        assert hasattr(module, "SqlTypeConverter")
        assert hasattr(module, "logger")
        assert hasattr(module, "__all__")

        # Verify the __all__ list matches exactly what's expected
        expected_all = [
            "SqlInputHandler",
            "SqlConnectionManager",
            "SqlSchemaManager",
            "SqlDataReader",
            "SqlTypeConverter",
            "logger",
        ]
        assert module.__all__ == expected_all
