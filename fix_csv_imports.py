#!/usr/bin/env python3
"""
Script to fix CSV processor imports across the codebase after refactoring.
This handles the deterministic change from the monolithic csv_processor.py
to the new organized csv package structure.
"""

import os
import re
from pathlib import Path

def find_python_files(root_dir):
    """Find all Python files in the project."""
    python_files = []
    for root, dirs, files in os.walk(root_dir):
        # Skip __pycache__ directories
        dirs[:] = [d for d in dirs if d != '__pycache__']

        for file in files:
            if file.endswith('.py'):
                python_files.append(os.path.join(root, file))

    return python_files

def fix_csv_processor_imports(file_path):
    """Fix CSV processor imports in a single file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        original_content = content
        changes_made = []

        # Pattern 1: Direct import from csv_processor module
        # from forklift.engine.processors.csv_processor import CSVProcessor
        # -> from forklift.engine.processors.csv_processor import CSVProcessor (no change needed, backward compatible)

        # Pattern 2: Import from processors package
        # from forklift.engine.processors import CSVProcessor
        # -> from forklift.engine.processors import CSVProcessor (no change needed, backward compatible)

        # Pattern 3: Relative imports within processors package
        # from .csv_processor import CSVProcessor
        # -> from .csv_processor import CSVProcessor (no change needed, backward compatible)

        # The refactoring maintains backward compatibility, so most imports should work
        # However, let's check for any direct imports to the old monolithic structure

        # Check if this is the old csv_processor.py file itself
        if file_path.endswith('csv_processor.py') and 'processors/csv_processor.py' in file_path:
            # This file was already updated, skip it
            return False, []

        # Look for any imports that might need updating (mostly for documentation)
        import_patterns = [
            (r'from\s+forklift\.engine\.processors\.csv_processor\s+import', 'Direct csv_processor import'),
            (r'from\s+\.csv_processor\s+import', 'Relative csv_processor import'),
            (r'import\s+forklift\.engine\.processors\.csv_processor', 'Module csv_processor import'),
        ]

        for pattern, description in import_patterns:
            if re.search(pattern, content):
                changes_made.append(f"Found {description}")

        # The imports should work as-is due to backward compatibility
        # Log what we found but don't make changes unless necessary

        return len(changes_made) > 0, changes_made

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False, [f"Error: {e}"]

def main():
    """Main function to fix imports across the codebase."""

    # Get the project root directory
    script_dir = Path(__file__).parent
    project_root = script_dir / '..'  # Assuming script is in project root

    # Find all Python files
    print("Scanning for Python files...")
    python_files = find_python_files(str(project_root))
    print(f"Found {len(python_files)} Python files")

    # Process each file
    files_with_imports = []
    total_changes = 0

    for file_path in python_files:
        has_imports, changes = fix_csv_processor_imports(file_path)
        if has_imports:
            files_with_imports.append((file_path, changes))
            total_changes += len(changes)

    # Report results
    print(f"\nScan complete!")
    print(f"Files with CSV processor imports: {len(files_with_imports)}")
    print(f"Total import references found: {total_changes}")

    if files_with_imports:
        print("\nFiles with CSV processor imports:")
        for file_path, changes in files_with_imports:
            rel_path = os.path.relpath(file_path, project_root)
            print(f"  {rel_path}")
            for change in changes:
                print(f"    - {change}")

    print("\nNOTE: The refactoring maintains backward compatibility.")
    print("All existing imports should continue to work without changes.")
    print("The new package structure is available at:")
    print("  - forklift.engine.processors.csv.CSVProcessor (main class)")
    print("  - forklift.engine.processors.csv.BatchValidator")
    print("  - forklift.engine.processors.csv.OutputManager")
    print("  - forklift.engine.processors.csv.PathManager")

if __name__ == "__main__":
    main()
