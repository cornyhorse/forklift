#!/usr/bin/env python3
"""
Simple and effective script to fix pytest.skip indentation issues.
"""

import re
from pathlib import Path

def fix_pytest_indentation_in_file(file_path):
    """Fix pytest.skip indentation in a single file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        original_content = content

        # Fix pytest.skip lines that have wrong indentation
        # Pattern: any number of spaces followed by pytest.skip(
        # Replace with exactly 8 spaces (correct indentation for class method)
        content = re.sub(r'^(\s{12,})pytest\.skip\(', r'        pytest.skip(', content, flags=re.MULTILINE)

        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            return True
        return False

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def main():
    """Fix pytest indentation in all problematic test files."""
    test_dir = Path('tests')

    # Get all test files that had indentation errors
    test_files = [
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

    fixed_count = 0

    for file_name in test_files:
        file_path = test_dir / file_name
        if file_path.exists():
            print(f"Processing {file_name}...")
            if fix_pytest_indentation_in_file(file_path):
                print(f"  ✓ Fixed {file_name}")
                fixed_count += 1
            else:
                print(f"  - No changes needed for {file_name}")
        else:
            print(f"  - File not found: {file_name}")

    print(f"\nSummary: Fixed {fixed_count} files")

if __name__ == '__main__':
    main()
