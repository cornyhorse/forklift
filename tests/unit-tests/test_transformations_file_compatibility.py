"""Tests for transformations backward compatibility wrapper file.

This test file ensures 100% coverage of the single file
src/forklift/processors/transformations.py which provides backward compatibility.
"""

import pytest


class TestTransformationsBackwardCompatibilityFile:
    """Test cases for the transformations backward compatibility single file."""

    def test_import_from_file_works(self):
        """Test that importing from the file works correctly."""
        from forklift.processors.transformations import (
            ColumnTransformer,
            DataTransformer,
            MoneyTypeConfig,
            SchemaBasedTransformer,
            apply_html_xml_cleaning,
            apply_money_conversion,
            apply_numeric_cleaning,
            apply_regex_replace,
            apply_string_padding,
            apply_string_replace,
            apply_string_trimming,
            create_transformation_from_config,
            lowercase,
            trim_whitespace,
            uppercase,
        )

        # Verify all classes and functions are imported and are callable
        assert callable(ColumnTransformer)
        assert callable(SchemaBasedTransformer)
        assert callable(trim_whitespace)
        assert callable(uppercase)
        assert callable(lowercase)
        assert callable(apply_money_conversion)
        assert callable(apply_numeric_cleaning)
        assert callable(apply_regex_replace)
        assert callable(apply_string_replace)
        assert callable(apply_html_xml_cleaning)
        assert callable(apply_string_padding)
        assert callable(apply_string_trimming)
        assert callable(DataTransformer)
        assert callable(create_transformation_from_config)
        assert callable(MoneyTypeConfig)

    def test_file_docstring(self):
        """Test that the file has the expected docstring."""
        import forklift.processors.transformations as transformations_file

        expected_docstring_parts = [
            "Transformation processors package",
            "data transformation capabilities",
            "Basic column transformations",
            "Schema-driven transformations",
            "Common transformation functions",
        ]

        assert transformations_file.__doc__ is not None
        for part in expected_docstring_parts:
            assert part in transformations_file.__doc__

    def test_file_all_attribute(self):
        """Test that the __all__ attribute is correctly defined."""
        import forklift.processors.transformations as transformations_file

        expected_all = [
            "ColumnTransformer",
            "SchemaBasedTransformer",
            "trim_whitespace",
            "uppercase",
            "lowercase",
            "apply_money_conversion",
            "apply_numeric_cleaning",
            "apply_regex_replace",
            "apply_string_replace",
            "apply_html_xml_cleaning",
            "apply_string_padding",
            "apply_string_trimming",
            "DataTransformer",
            "create_transformation_from_config",
            "MoneyTypeConfig",
            "NumericCleaningConfig",
            "RegexReplaceConfig",
            "StringReplaceConfig",
            "HTMLXMLConfig",
            "StringPaddingConfig",
            "DateTimeTransformConfig",
            "StringCleaningConfig",
        ]

        # Verify __all__ attribute exists and contains expected exports
        assert hasattr(transformations_file, "__all__")
        assert transformations_file.__all__ == expected_all

    def test_transformer_classes_import_coverage(self):
        """Test that the transformer classes imports are executed."""
        import forklift.processors.transformations as transformations_file

        # This tests lines 9-10 (transformer classes imports)
        assert hasattr(transformations_file, "ColumnTransformer")
        assert hasattr(transformations_file, "SchemaBasedTransformer")
        assert callable(transformations_file.ColumnTransformer)
        assert callable(transformations_file.SchemaBasedTransformer)

    def test_common_functions_import_coverage(self):
        """Test that the common functions imports are executed."""
        import forklift.processors.transformations as transformations_file

        # This tests lines 11-15 (common functions imports)
        common_functions = ["trim_whitespace", "uppercase", "lowercase"]

        for func_name in common_functions:
            assert hasattr(transformations_file, func_name)
            func = getattr(transformations_file, func_name)
            assert func is not None
            assert callable(func)

    def test_factory_functions_import_coverage(self):
        """Test that the factory functions imports are executed."""
        import forklift.processors.transformations as transformations_file

        # This tests lines 16-24 (factory functions imports)
        factory_functions = [
            "apply_money_conversion",
            "apply_numeric_cleaning",
            "apply_regex_replace",
            "apply_string_replace",
            "apply_html_xml_cleaning",
            "apply_string_padding",
            "apply_string_trimming",
        ]

        for func_name in factory_functions:
            assert hasattr(transformations_file, func_name)
            func = getattr(transformations_file, func_name)
            assert func is not None
            assert callable(func)

    def test_utils_imports_coverage(self):
        """Test that the utils imports are executed."""
        import forklift.processors.transformations as transformations_file

        # This tests lines 26-35 (utils imports)
        utils_items = [
            "DataTransformer",
            "create_transformation_from_config",
            "MoneyTypeConfig",
            "NumericCleaningConfig",
            "RegexReplaceConfig",
            "StringReplaceConfig",
            "HTMLXMLConfig",
            "StringPaddingConfig",
            "DateTimeTransformConfig",
            "StringCleaningConfig",
        ]

        for item_name in utils_items:
            assert hasattr(transformations_file, item_name)
            item = getattr(transformations_file, item_name)
            assert item is not None
            assert callable(item)

    def test_backward_compatibility_maintained(self):
        """Test that backward compatibility is maintained."""
        # Import from the compatibility file
        from forklift.processors.transformations import ColumnTransformer as FileTransformer

        # Import from the package directly
        from forklift.processors.transformations.column_transformer import (
            ColumnTransformer as PackageTransformer,
        )

        # They should be the same class
        assert FileTransformer is PackageTransformer

    def test_complete_file_coverage(self):
        """Test to ensure complete coverage of all lines in the file."""
        # Import the module to execute all lines
        import forklift.processors.transformations

        # Access all attributes to ensure imports are executed
        module = forklift.processors.transformations

        # Verify docstring (lines 1-7)
        assert module.__doc__ is not None

        # Verify all imports by checking available attributes
        assert hasattr(module, "ColumnTransformer")
        assert hasattr(module, "SchemaBasedTransformer")
        assert hasattr(module, "trim_whitespace")
        assert hasattr(module, "uppercase")
        assert hasattr(module, "lowercase")
        assert hasattr(module, "apply_money_conversion")
        assert hasattr(module, "apply_numeric_cleaning")
        assert hasattr(module, "apply_regex_replace")
        assert hasattr(module, "apply_string_replace")
        assert hasattr(module, "apply_html_xml_cleaning")
        assert hasattr(module, "apply_string_padding")
        assert hasattr(module, "apply_string_trimming")
        assert hasattr(module, "DataTransformer")
        assert hasattr(module, "create_transformation_from_config")
        assert hasattr(module, "MoneyTypeConfig")
        assert hasattr(module, "NumericCleaningConfig")
        assert hasattr(module, "RegexReplaceConfig")
        assert hasattr(module, "StringReplaceConfig")
        assert hasattr(module, "HTMLXMLConfig")
        assert hasattr(module, "StringPaddingConfig")
        assert hasattr(module, "DateTimeTransformConfig")
        assert hasattr(module, "StringCleaningConfig")

        # Verify __all__ definition (lines 37-59)
        assert hasattr(module, "__all__")
        assert len(module.__all__) == 22

    def test_all_items_in_all_are_accessible(self):
        """Test that all items listed in __all__ are accessible."""
        import forklift.processors.transformations as transformations_file

        # Test that we can access each export from __all__
        for export_name in transformations_file.__all__:
            assert hasattr(transformations_file, export_name)
            export_item = getattr(transformations_file, export_name)
            assert export_item is not None
            assert callable(export_item)

    def test_module_structure_after_import(self):
        """Test that the module has the expected structure after import."""
        import forklift.processors.transformations as transformations_file

        # Check that the module has the expected attributes
        required_attrs = ["__doc__", "__all__"] + transformations_file.__all__

        for attr in required_attrs:
            assert hasattr(transformations_file, attr), f"Missing attribute: {attr}"

        # Verify the module structure matches expectations
        assert isinstance(transformations_file.__all__, list)
        assert len(transformations_file.__all__) == 22
