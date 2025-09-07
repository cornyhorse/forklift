#!/usr/bin/env python3
"""
Script to analyze test failures and generate deterministic changes needed
to maintain compatibility after refactoring the data validation module.
"""

import ast
import os
from typing import List, Dict, Set


def analyze_test_file(test_file_path: str) -> Dict[str, Set[str]]:
    """Analyze the test file to find all accessed attributes and methods."""

    with open(test_file_path, 'r') as f:
        content = f.read()

    tree = ast.parse(content)

    # Track what attributes/methods are accessed on DataValidationProcessor instances
    accessed_attributes = set()
    accessed_methods = set()

    class AttributeVisitor(ast.NodeVisitor):
        def visit_Attribute(self, node):
            # Look for attribute access on self or processor instances
            if isinstance(node.ctx, ast.Load):
                if isinstance(node.value, ast.Name):
                    if node.value.id in ['self', 'processor']:
                        if node.attr.startswith('_'):
                            accessed_methods.add(node.attr)
                        else:
                            accessed_attributes.add(node.attr)
                elif isinstance(node.value, ast.Attribute):
                    # Handle chained attribute access like self.processor.bad_rows
                    if isinstance(node.value.value, ast.Name) and node.value.value.id == 'self':
                        if node.value.attr == 'processor':
                            if node.attr.startswith('_'):
                                accessed_methods.add(node.attr)
                            else:
                                accessed_attributes.add(node.attr)
            self.generic_visit(node)

    visitor = AttributeVisitor()
    visitor.visit(tree)

    return {
        'attributes': accessed_attributes,
        'methods': accessed_methods
    }


def generate_compatibility_changes(analysis: Dict[str, Set[str]]) -> List[str]:
    """Generate the changes needed for backward compatibility."""

    changes = []

    # Map old attributes/methods to new implementations
    attribute_mappings = {
        'bad_rows': 'self.bad_rows_handler.bad_rows',
        'unique_value_tracker': 'self.unique_value_tracker',  # This one stays
        'total_rows_processed': 'self.total_rows_processed',  # This one stays
    }

    method_mappings = {
        '_is_null_or_empty': 'self.validation_rules.is_null_or_empty',
        '_validate_range': 'self.validation_rules.validate_range',
        '_validate_string': 'self.validation_rules.validate_string',
        '_validate_enum': 'self.validation_rules.validate_enum',
        '_validate_date': 'self.validation_rules.validate_date',
        '_handle_bad_row': 'self.bad_rows_handler.add_bad_row',
        '_infer_field_type': 'self.bad_rows_handler._infer_field_type',
    }

    # Generate property wrappers for attributes
    for attr in analysis['attributes']:
        if attr in attribute_mappings:
            if attr == 'bad_rows':
                changes.append(f"""
    @property
    def {attr}(self):
        \"\"\"Backward compatibility property for {attr}.\"\"\"
        return {attribute_mappings[attr]}
""")

    # Generate method wrappers for methods
    for method in analysis['methods']:
        if method in method_mappings:
            if method == '_is_null_or_empty':
                changes.append(f"""
    def {method}(self, value):
        \"\"\"Backward compatibility wrapper for {method}.\"\"\"
        return {method_mappings[method]}(value)
""")
            elif method in ['_validate_range', '_validate_string', '_validate_enum', '_validate_date']:
                changes.append(f"""
    def {method}(self, field_name, value, validation_config):
        \"\"\"Backward compatibility wrapper for {method}.\"\"\"
        return {method_mappings[method]}(field_name, value, validation_config)
""")
            elif method == '_handle_bad_row':
                changes.append(f"""
    def {method}(self, batch, row_idx, errors):
        \"\"\"Backward compatibility wrapper for {method}.\"\"\"
        return {method_mappings[method]}(batch, row_idx, errors)
""")
            elif method == '_infer_field_type':
                changes.append(f"""
    def {method}(self, field_name, bad_rows):
        \"\"\"Backward compatibility wrapper for {method}.\"\"\"
        return {method_mappings[method]}(field_name, bad_rows)
""")

    return changes


def main():
    """Main function to analyze and generate changes."""

    test_file = "/Users/matt/PycharmProjects/forklift/tests/test_data_validation_comprehensive.py"

    if not os.path.exists(test_file):
        print(f"Test file not found: {test_file}")
        return

    print("Analyzing test file for accessed attributes and methods...")
    analysis = analyze_test_file(test_file)

    print("\nAccessed attributes:")
    for attr in sorted(analysis['attributes']):
        print(f"  - {attr}")

    print("\nAccessed methods:")
    for method in sorted(analysis['methods']):
        print(f"  - {method}")

    print("\nGenerating compatibility changes...")
    changes = generate_compatibility_changes(analysis)

    print("\nChanges needed for DataValidationProcessor:")
    for change in changes:
        print(change)

    # Write the changes to a file
    with open("compatibility_changes.py", "w") as f:
        f.write("# Compatibility changes for DataValidationProcessor\n")
        f.write("# Add these methods/properties to the DataValidationProcessor class\n\n")
        for change in changes:
            f.write(change)

    print(f"\nChanges written to compatibility_changes.py")


if __name__ == "__main__":
    main()
