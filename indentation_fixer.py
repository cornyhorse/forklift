#!/usr/bin/env python3
"""
Fixed comprehensive script to repair indentation errors and other test file problems.
"""

import re
import os
from pathlib import Path

def fix_indentation_errors(content):
    """Fix indentation errors in test files."""
    # Fix the specific indentation error pattern where extra spaces are before def
    content = re.sub(r'(\n[ ]*)([ ]+)(def test_[^(]+\([^)]*\):)', r'\1\3', content)

    # Ensure proper class method indentation (4 spaces)
    lines = content.split('\n')
    fixed_lines = []
    inside_class = False

    for line in lines:
        # Detect class definitions
        if line.strip().startswith('class ') and line.strip().endswith(':'):
            inside_class = True
            fixed_lines.append(line)
        # Detect method definitions that should be inside a class
        elif inside_class and re.match(r'^\s*def test_', line):
            # Ensure proper 4-space indentation for class methods
            method_def = line.strip()
            fixed_lines.append('    ' + method_def)
        # Reset class detection for top-level definitions
        elif line.strip() and not line.startswith(' ') and not line.startswith('\t'):
            if not line.strip().startswith('#') and not line.strip().startswith('"""') and not line.strip().startswith("'''"):
                inside_class = False
            fixed_lines.append(line)
        else:
            fixed_lines.append(line)

    return '\n'.join(fixed_lines)

def fix_test_method_bodies(content):
    """Fix test method bodies that have incorrect indentation."""
    # Pattern to match pytest.skip lines that need proper indentation
    content = re.sub(
        r'(    def test_[^(]+\([^)]*\):\n)(\s*"""[^"]*"""\n)?(\s*pytest\.skip\([^)]+\))',
        r'\1\2        \3',
        content,
        flags=re.MULTILINE
    )

    # Fix docstrings that are not properly indented
    content = re.sub(
        r'(    def test_[^(]+\([^)]*\):\n)(\s*)("""[^"]*""")',
        r'\1        \3',
        content,
        flags=re.MULTILINE
    )

    return content

def fix_test_file(file_path):
    """Fix a single test file comprehensively."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except UnicodeDecodeError:
        print(f"Skipping {file_path} - encoding issue")
        return False

    original_content = content

    # Apply indentation fixes
    content = fix_indentation_errors(content)
    content = fix_test_method_bodies(content)

    # Only write if content changed
    if content != original_content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    return False

def main():
    """Main function to fix indentation errors in test files."""
    test_dir = Path('tests')
    fixed_files = []

    # Get all test files that are mentioned in the error
    error_files = [
        'test_final_surgical_coverage.py',
        'test_forklift_core_100_percent.py',
        'test_forklift_core_100_percent_surgical.py',
        'test_forklift_core_complete_coverage.py',
        'test_forklift_core_coverage.py',
        'test_forklift_core_final_coverage.py',
        'test_forklift_core_final_missing_lines.py',
        'test_forklift_core_line_288_coverage.py',
        'test_forklift_core_line_561_coverage.py',
        'test_forklift_core_line_684_coverage.py',
        'test_forklift_core_line_706_coverage.py',
        'test_forklift_core_missing_lines_batch1.py',
        'test_forklift_core_missing_lines_batch2.py',
        'test_forklift_core_missing_lines_batch3.py',
        'test_forklift_core_ultra_precision.py',
        'test_readers_comprehensive.py',
        'test_schema_validator_comprehensive.py',
        'test_ultra_precision_coverage.py',
        'test_ultra_targeted_coverage.py'
    ]

    print(f"Fixing indentation errors in {len(error_files)} test files")

    for file_name in error_files:
        test_file = test_dir / file_name
        if test_file.exists():
            print(f"Processing {file_name}...")
            try:
                if fix_test_file(test_file):
                    fixed_files.append(test_file)
                    print(f"  ✓ Fixed {file_name}")
                else:
                    print(f"  - No changes needed for {file_name}")
            except Exception as e:
                print(f"  ✗ Error processing {file_name}: {e}")
        else:
            print(f"  - File {file_name} not found")

    print(f"\nSummary:")
    print(f"  Processed: {len(error_files)} files")
    print(f"  Fixed: {len(fixed_files)} files")
    print(f"  Unchanged: {len(error_files) - len(fixed_files)} files")

    if fixed_files:
        print(f"\nFixed files:")
        for file in fixed_files:
            print(f"  - {file.name}")

if __name__ == '__main__':
    main()
