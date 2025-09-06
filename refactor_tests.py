#!/usr/bin/env python3
"""Automated script to refactor data transformation tests into the new package structure."""

import os
import re
import shutil
from pathlib import Path


def create_test_package_structure():
    """Create the test package directory structure."""
    base_path = Path("/Users/matt/PycharmProjects/forklift/tests/utils/transformations")
    base_path.mkdir(parents=True, exist_ok=True)

    # Create __init__.py for test package
    init_file = base_path / "__init__.py"
    init_file.write_text('"""Tests for data transformation utilities."""\n')

    print(f"Created test package structure at {base_path}")


def update_import_statements(file_path: Path, new_imports: dict):
    """Update import statements in a test file."""
    if not file_path.exists():
        return

    content = file_path.read_text()

    # Update imports
    for old_import, new_import in new_imports.items():
        content = content.replace(old_import, new_import)

    file_path.write_text(content)
    print(f"Updated imports in {file_path}")


def find_and_split_test_files():
    """Find existing test files and split them by transformation type."""
    test_dir = Path("/Users/matt/PycharmProjects/forklift/tests")
    transformation_test_dir = Path("/Users/matt/PycharmProjects/forklift/tests/utils/transformations")

    # Look for existing test files that might contain data transformation tests
    test_files = list(test_dir.glob("**/test_*data_transformation*.py")) + \
                list(test_dir.glob("**/test_*string*.py")) + \
                list(test_dir.glob("**/test_*datetime*.py"))

    # Update import mappings
    import_mappings = {
        "from forklift.utils.transformations": "from forklift.utils.transformations",
        "from forklift.utils.transformations import": "from forklift.utils.transformations import",
        "import forklift.utils.transformations": "import forklift.utils.transformations",

        # Specific class imports
        "DataTransformer": "DataTransformer",
        "StringCleaningConfig": "StringCleaningConfig",
        "RegexReplaceConfig": "RegexReplaceConfig",
        "StringReplaceConfig": "StringReplaceConfig",
        "MoneyTypeConfig": "MoneyTypeConfig",
        "NumericCleaningConfig": "NumericCleaningConfig",
        "DateTimeTransformConfig": "DateTimeTransformConfig",
        "SSNConfig": "SSNConfig",
        "ZipCodeConfig": "ZipCodeConfig",
        "PhoneNumberConfig": "PhoneNumberConfig",
        "EmailConfig": "EmailConfig",
        "IPAddressConfig": "IPAddressConfig",
        "MACAddressConfig": "MACAddressConfig",
        "HTMLXMLConfig": "HTMLXMLConfig",
        "StringPaddingConfig": "StringPaddingConfig",
    }

    for test_file in test_files:
        print(f"Processing test file: {test_file}")

        # Create specific test files based on content patterns
        content = test_file.read_text()

        # Check for different types of tests and create appropriate files
        if "string" in test_file.name.lower() or re.search(r'StringCleaning|string_cleaning', content):
            new_file = transformation_test_dir / "test_string_transformations.py"
            update_content_and_save(content, new_file, import_mappings, "string")

        if "datetime" in test_file.name.lower() or re.search(r'DateTime|datetime', content):
            new_file = transformation_test_dir / "test_datetime_transformations.py"
            update_content_and_save(content, new_file, import_mappings, "datetime")

        # Generic data transformation tests
        if "data_transformation" in test_file.name.lower():
            # Split this into multiple focused test files
            split_generic_test_file(content, transformation_test_dir, import_mappings)


def update_content_and_save(content: str, file_path: Path, import_mappings: dict, test_type: str):
    """Update content with new imports and save to new location."""
    # Update imports
    for old_import, new_import in import_mappings.items():
        content = content.replace(old_import, new_import)

    # Add specific imports for the test type
    if test_type == "string":
        additional_imports = "from forklift.utils.transformations.string_transformations import StringTransformer\n"
    elif test_type == "datetime":
        additional_imports = "from forklift.utils.transformations.datetime_transformations import DateTimeTransformer\n"
    else:
        additional_imports = ""

    # Insert additional imports after existing imports
    import_section_end = content.find('\n\n')
    if import_section_end > 0:
        content = content[:import_section_end] + '\n' + additional_imports + content[import_section_end:]

    file_path.write_text(content)
    print(f"Created/updated {file_path}")


def split_generic_test_file(content: str, base_dir: Path, import_mappings: dict):
    """Split a generic test file into focused test modules."""

    test_patterns = {
        "test_string_transformations.py": [
            r'test.*string.*clean', r'test.*regex', r'test.*replace',
            r'class.*String.*Test', r'def test.*string'
        ],
        "test_numeric_transformations.py": [
            r'test.*money', r'test.*numeric', r'test.*padding',
            r'class.*Money.*Test', r'class.*Numeric.*Test'
        ],
        "test_datetime_transformations.py": [
            r'test.*datetime', r'test.*date.*time', r'test.*timestamp',
            r'class.*DateTime.*Test'
        ],
        "test_format_transformations.py": [
            r'test.*ssn', r'test.*zip', r'test.*phone', r'test.*email',
            r'test.*ip.*address', r'test.*mac.*address',
            r'class.*SSN.*Test', r'class.*Zip.*Test', r'class.*Phone.*Test'
        ],
        "test_html_xml_transformations.py": [
            r'test.*html', r'test.*xml', r'test.*tag',
            r'class.*HTML.*Test', r'class.*XML.*Test'
        ],
        "test_base_transformations.py": [
            r'test.*DataTransformer', r'class.*DataTransformer.*Test'
        ]
    }

    # Create base test content for each module
    for test_file, patterns in test_patterns.items():
        if any(re.search(pattern, content, re.IGNORECASE) for pattern in patterns):
            create_focused_test_file(base_dir / test_file, import_mappings, test_file)


def create_focused_test_file(file_path: Path, import_mappings: dict, test_file_name: str):
    """Create a focused test file with appropriate imports."""

    module_name = test_file_name.replace("test_", "").replace(".py", "")
    class_name = ''.join(word.capitalize() for word in module_name.split('_'))

    content = f'''"""Tests for {module_name.replace('_', ' ')} utilities."""

import pytest
import pyarrow as pa
import pandas as pd
from unittest.mock import Mock

from forklift.utils.transformations.configs import *
from forklift.utils.transformations import DataTransformer


class Test{class_name}:
    """Test suite for {module_name.replace('_', ' ')} operations."""

    def setup_method(self):
        """Set up test fixtures."""
        self.transformer = DataTransformer()

    def test_placeholder(self):
        """Placeholder test - replace with actual tests."""
        # TODO: Add specific tests for {module_name.replace('_', ' ')}
        assert True
'''

    file_path.write_text(content)
    print(f"Created focused test file: {file_path}")


def create_update_script():
    """Create a shell script to update existing imports across the codebase."""
    script_content = '''#!/bin/bash
# Script to update import statements across the codebase

echo "Updating import statements for refactored data transformations..."

# Find all Python files that import from data_transformations
find . -name "*.py" -type f -exec grep -l "from forklift.utils.transformations" {} \\; | while read file; do
    echo "Updating $file"
    
    # Update import statements
    sed -i '' 's/from forklift\\.utils\\.data_transformations/from forklift.utils.transformations/g' "$file"
    sed -i '' 's/import forklift\\.utils\\.data_transformations/import forklift.utils.transformations/g' "$file"
done

echo "Import updates completed!"
'''

    script_path = Path("/Users/matt/PycharmProjects/forklift/update_imports.sh")
    script_path.write_text(script_content)
    script_path.chmod(0o755)
    print(f"Created update script: {script_path}")


def main():
    """Execute the test refactoring process."""
    print("Starting test refactoring for data transformations...")

    # Create directory structure
    create_test_package_structure()

    # Process existing test files
    find_and_split_test_files()

    # Create update script
    create_update_script()

    print("\nTest refactoring completed!")
    print("\nNext steps:")
    print("1. Run './update_imports.sh' to update imports across the codebase")
    print("2. Review and migrate specific test cases to appropriate modules")
    print("3. Update any remaining import statements manually")
    print("4. Run pytest to verify all tests pass")


if __name__ == "__main__":
    main()
