"""Tests for SQL input backward compatibility wrapper file.

This test file ensures coverage of the SQL input backward compatibility by testing
the import behavior and verifying the package takes precedence over the single file.

Note: Due to Python import resolution, the package takes precedence over the file,
so these tests verify the expected behavior and document the import conflict.
"""

import logging
import os
import sys

import pytest


class TestSqlBackwardCompatibilityFile:
    """Test cases for SQL input backward compatibility."""

    def test_sql_package_takes_precedence_over_file(self):
        """Test that the SQL package is imported instead of the single file."""
        # Import the sql module
        import forklift.inputs.sql as sql_module

        # Verify it's the package, not the single file
        assert sql_module.__file__.endswith("sql/__init__.py")
        assert "sql/__init__.py" in sql_module.__file__

        # Verify the package provides the expected functionality
        assert hasattr(sql_module, "SqlInputHandler")
        assert hasattr(sql_module, "SqlConnectionManager")
        assert hasattr(sql_module, "SqlSchemaManager")
        assert hasattr(sql_module, "SqlDataReader")
        assert hasattr(sql_module, "SqlTypeConverter")
        assert hasattr(sql_module, "logger")

    def test_sql_file_exists_but_not_imported(self):
        """Test that the sql.py file exists but is not imported due to package precedence."""
        # Check that the sql.py file exists
        sql_file_path = os.path.join(os.path.dirname(__file__), "../../src/forklift/inputs/sql.py")
        sql_file_path = os.path.abspath(sql_file_path)

        assert os.path.exists(sql_file_path), "The sql.py file should exist"

        # Check that the sql package directory also exists
        sql_package_path = os.path.join(
            os.path.dirname(__file__), "../../src/forklift/inputs/sql/"
        )
        sql_package_path = os.path.abspath(sql_package_path)

        assert os.path.exists(sql_package_path), "The sql/ package directory should exist"
        assert os.path.isdir(sql_package_path), "The sql/ should be a directory"

    def test_sql_file_content_verification(self):
        """Test that the sql.py file has the expected content structure."""
        sql_file_path = os.path.join(os.path.dirname(__file__), "../../src/forklift/inputs/sql.py")
        sql_file_path = os.path.abspath(sql_file_path)

        # Read the file content directly
        with open(sql_file_path, "r") as f:
            content = f.read()

        # Verify the file has the expected structure
        expected_content_parts = [
            "SQL database input handler",
            "backward compatibility",
            "from .sql.handler import SqlInputHandler",
            "from .sql.connection import SqlConnectionManager",
            "from .sql.schema import SqlSchemaManager",
            "from .sql.reader import SqlDataReader",
            "from .sql.types import SqlTypeConverter",
            "logger = logging.getLogger(__name__)",
            "__all__ = [",
        ]

        for part in expected_content_parts:
            assert part in content, f"Expected content part '{part}' not found in sql.py"

    def test_import_conflict_documentation(self):
        """Test that documents the import resolution conflict."""
        # This test serves to document the behavior where Python prioritizes
        # packages over single files with the same name

        import forklift.inputs.sql as sql_module

        # The imported module should be the package
        assert "sql/__init__.py" in sql_module.__file__

        # The package should have a different docstring than the single file
        # Package docstring
        package_docstring = sql_module.__doc__
        assert "SQL input package for database connectivity" in package_docstring

        # This confirms we're importing the package, not the single file
        assert (
            "backward compatibility by importing from the new modular structure"
            not in package_docstring
        )

    def test_backward_compatibility_functionality_works(self):
        """Test that the backward compatibility functionality works through the package."""
        # Even though the single file isn't imported, the package provides
        # the same backward compatibility functionality

        from forklift.inputs.sql import (
            SqlConnectionManager,
            SqlDataReader,
            SqlInputHandler,
            SqlSchemaManager,
            SqlTypeConverter,
        )

        # Verify all classes are available and callable
        classes = [
            SqlInputHandler,
            SqlConnectionManager,
            SqlSchemaManager,
            SqlDataReader,
            SqlTypeConverter,
        ]

        for cls in classes:
            assert callable(cls)
            assert hasattr(cls, "__init__")

    def test_sql_package_exports_match_file_intent(self):
        """Test that the package exports match what the single file intended to export."""
        import forklift.inputs.sql as sql_module

        # The package should export the same classes the single file intended to
        expected_exports = [
            "SqlInputHandler",
            "SqlConnectionManager",
            "SqlSchemaManager",
            "SqlDataReader",
            "SqlTypeConverter",
            "logger",
        ]

        # Verify all expected exports are available
        for export_name in expected_exports:
            assert hasattr(sql_module, export_name), f"Missing export: {export_name}"
            export_item = getattr(sql_module, export_name)
            assert export_item is not None

    def test_sql_file_would_provide_same_functionality(self):
        """Test that verifies the single file would provide the same functionality if imported."""
        # Read the sql.py file to verify it would provide the same exports
        sql_file_path = os.path.join(os.path.dirname(__file__), "../../src/forklift/inputs/sql.py")
        sql_file_path = os.path.abspath(sql_file_path)

        with open(sql_file_path, "r") as f:
            content = f.read()

        # Verify the file defines the same exports as the package
        expected_in_all = [
            "SqlInputHandler",
            "SqlConnectionManager",
            "SqlSchemaManager",
            "SqlDataReader",
            "SqlTypeConverter",
        ]

        for export in expected_in_all:
            assert (
                f'"{export}"' in content
            ), f"Expected export '{export}' not found in sql.py __all__"

    def test_coverage_explanation(self):
        """Test that explains why this file shows 0% coverage."""
        # This test documents the technical reason for 0% coverage

        # 1. Both sql.py and sql/ exist in the same directory
        sql_file_path = os.path.join(os.path.dirname(__file__), "../../src/forklift/inputs/sql.py")
        sql_dir_path = os.path.join(os.path.dirname(__file__), "../../src/forklift/inputs/sql/")

        assert os.path.exists(os.path.abspath(sql_file_path))
        assert os.path.exists(os.path.abspath(sql_dir_path))

        # 2. Python's import resolution prioritizes packages over modules
        import forklift.inputs.sql as imported_module

        # 3. Therefore, the single file is never executed, resulting in 0% coverage
        assert "sql/__init__.py" in imported_module.__file__

        # 4. However, the package provides equivalent functionality
        assert hasattr(imported_module, "SqlInputHandler")
        assert hasattr(imported_module, "logger")

    def test_sql_logger_functionality(self):
        """Test that the SQL module provides logger functionality."""
        import forklift.inputs.sql as sql_module

        # Verify logger exists and has expected properties
        assert hasattr(sql_module, "logger")
        assert isinstance(sql_module.logger, logging.Logger)
        assert sql_module.logger.name == "forklift.inputs.sql"

    def test_sql_imports_work_correctly(self):
        """Test that importing from the SQL module works correctly."""
        # Test individual imports
        from forklift.inputs.sql import (
            SqlConnectionManager,
            SqlDataReader,
            SqlInputHandler,
            SqlSchemaManager,
            SqlTypeConverter,
            logger,
        )

        # Verify all imports are valid
        assert callable(SqlInputHandler)
        assert callable(SqlConnectionManager)
        assert callable(SqlSchemaManager)
        assert callable(SqlDataReader)
        assert callable(SqlTypeConverter)
        assert isinstance(logger, logging.Logger)
