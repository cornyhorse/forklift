#!/usr/bin/env python3
"""
Script to update SQL test files to work with the new modular structure.
This script will fix imports and method calls to match the refactored SQL input handler.
"""

import os
import re
from pathlib import Path

def update_sql_test_file(file_path):
    """Update a single SQL test file to work with the new modular structure."""
    print(f"Updating {file_path}...")

    with open(file_path, 'r') as f:
        content = f.read()

    original_content = content

    # 1. Update imports to include the new modular components
    import_section = """from forklift.inputs.sql import SqlInputHandler
from forklift.inputs.config import SqlInputConfig"""

    new_import_section = """from forklift.inputs.sql import SqlInputHandler, SqlConnectionManager, SqlSchemaManager, SqlDataReader, SqlTypeConverter
from forklift.inputs.config import SqlInputConfig"""

    content = content.replace(import_section, new_import_section)

    # 2. Fix direct access to connection attribute
    # Replace handler.connection with handler.connection_manager.connection
    content = re.sub(r'(\w+)\.connection(?!\w)', r'\1.connection_manager.connection', content)

    # 3. Fix private method calls that are now in different modules

    # Methods now in SqlSchemaManager
    content = re.sub(r'(\w+)\._parse_table_specification\(', r'\1.schema_manager._parse_table_specification(', content)
    content = re.sub(r'(\w+)\._quote_identifier\(', r'\1.schema_manager._quote_identifier(', content)

    # Methods now in SqlTypeConverter
    content = re.sub(r'(\w+)\._odbc_type_to_string\(', r'\1.data_reader.type_converter.odbc_type_to_string(', content)
    content = re.sub(r'(\w+)\._sql_type_to_pyarrow\(', r'\1.data_reader.type_converter.sql_type_to_pyarrow(', content)
    content = re.sub(r'(\w+)\._convert_column_data\(', r'\1.data_reader.type_converter.convert_column_data(', content)

    # Methods now in SqlDataReader
    content = re.sub(r'(\w+)\._rows_to_recordbatch\(', r'\1.data_reader._rows_to_recordbatch(', content)

    # 4. Fix logger references - these should reference the module logger
    content = re.sub(r'forklift\.inputs\.sql\.logger', r'forklift.inputs.sql.handler.logger', content)

    # 5. Fix test assertions that check for connection.close() calls
    # We need to check connection_manager.connection.close() instead
    content = re.sub(r'handler\.connection\.close\.assert_called_once\(\)', r'handler.connection_manager.connection.close.assert_called_once()', content)

    # 6. Fix mock expectations for schema importer methods
    # The get_include_patterns method should return the actual result, not a mock
    content = re.sub(r'mock_schema_importer\.get_include_patterns\.return_value = (.+)',
                    r'mock_schema_importer.get_include_patterns.return_value = \1', content)

    # 7. Add setup for component mocking in tests that need it
    # For tests that directly test private methods, we need to setup the components properly

    # 8. Fix tests that expect certain attributes to exist
    # Some tests check for handler.connection directly - update these
    if 'assert handler.connection is None' in content:
        content = content.replace('assert handler.connection is None',
                                'assert handler.connection_manager.connection is None')

    # Write back if changes were made
    if content != original_content:
        with open(file_path, 'w') as f:
            f.write(content)
        print(f"  ✅ Updated {file_path}")
        return True
    else:
        print(f"  ➡️  No changes needed for {file_path}")
        return False

def main():
    """Main function to update all SQL test files."""
    test_dir = Path("/Users/matt/PycharmProjects/forklift/tests/unit-tests")
    sql_test_files = list(test_dir.glob("test_sql*.py"))

    print(f"Found {len(sql_test_files)} SQL test files to update:")
    for file in sql_test_files:
        print(f"  - {file.name}")

    print("\nStarting updates...")

    updated_count = 0
    for file_path in sql_test_files:
        if update_sql_test_file(file_path):
            updated_count += 1

    print(f"\n✅ Update complete! Updated {updated_count} out of {len(sql_test_files)} files.")

    # Also provide specific guidance for manual fixes that might be needed
    print("\n📋 Manual fixes that might still be needed:")
    print("1. Tests that directly instantiate SqlTypeConverter, etc. may need component injection")
    print("2. Some complex mocking scenarios might need adjustment")
    print("3. Tests expecting specific error messages might need updates")

if __name__ == "__main__":
    main()
