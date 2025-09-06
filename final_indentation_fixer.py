#!/usr/bin/env python3
"""
Final comprehensive script to fix all remaining indentation issues in test files.
"""

import re
from pathlib import Path

def fix_pytest_skip_indentation(content):
    """Fix pytest.skip lines that have incorrect indentation."""
    # Fix pytest.skip lines that are incorrectly indented
    lines = content.split('\n')
    fixed_lines = []

    for i, line in enumerate(lines):
        # If this line contains pytest.skip
        if 'pytest.skip(' in line:
            # Check if it's inside a test method (previous non-empty line should be a method def or docstring)
            prev_line_idx = i - 1
            while prev_line_idx >= 0 and lines[prev_line_idx].strip() == '':
                prev_line_idx -= 1

            if prev_line_idx >= 0:
                prev_line = lines[prev_line_idx]
                # If previous line is a test method def or docstring
                if ('def test_' in prev_line or
                    (prev_line.strip().startswith('"""') and prev_line.strip().endswith('"""'))):
                    # Ensure pytest.skip is properly indented (8 spaces for class method)
                    skip_content = line.strip()
                    fixed_lines.append('        ' + skip_content)
                    continue
                # Check if previous line is a docstring and the line before that is a test method
                elif (prev_line.strip().startswith('"""') and prev_line_idx > 0 and
                      'def test_' in lines[prev_line_idx - 1]):
                    skip_content = line.strip()
                    fixed_lines.append('        ' + skip_content)
                    continue

        fixed_lines.append(line)

    return '\n'.join(fixed_lines)

def fix_all_test_files():
    """Fix all test files with indentation issues."""
    test_dir = Path('tests')

    # Get all test files that were mentioned in the errors
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

    fixed_count = 0

    for file_name in error_files:
        file_path = test_dir / file_name
        if file_path.exists():
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                original_content = content
                content = fix_pytest_skip_indentation(content)

                if content != original_content:
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(content)
                    print(f"Fixed {file_name}")
                    fixed_count += 1
                else:
                    print(f"No changes needed for {file_name}")

            except Exception as e:
                print(f"Error processing {file_name}: {e}")
        else:
            print(f"File not found: {file_name}")

    print(f"\nFixed {fixed_count} files")

if __name__ == '__main__':
    fix_all_test_files()
