"""Tests for transformations processor backward compatibility module.

This test file ensures 100% coverage of the backward-compatibility interface
in src/forklift/processors/transformations.py by testing the import statements and __all__ exports.
"""

import pytest


class TestTransformationsCompatibility:
    """Test cases for transformations processor backward compatibility."""

    def test_import_all_classes_and_functions(self):
        """Test importing all classes and functions from the transformations compatibility module."""
        # Import from the backward-compatibility module
        from forklift.processors.transformations import (
            ColumnTransformer, DataTransformer, DateTimeTransformConfig,
            HTMLXMLConfig, MoneyTypeConfig, NumericCleaningConfig,
            RegexReplaceConfig, SchemaBasedTransformer, StringCleaningConfig,
            StringPaddingConfig, StringReplaceConfig, apply_html_xml_cleaning,
            apply_money_conversion, apply_numeric_cleaning,
            apply_regex_replace, apply_string_padding, apply_string_replace,
            apply_string_trimming, create_transformation_from_config,
            lowercase, trim_whitespace, uppercase)

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
        assert callable(NumericCleaningConfig)
        assert callable(RegexReplaceConfig)
        assert callable(StringReplaceConfig)
        assert callable(HTMLXMLConfig)
        assert callable(StringPaddingConfig)
        assert callable(DateTimeTransformConfig)
        assert callable(StringCleaningConfig)

    def test_module_all_attribute(self):
        """Test that the __all__ attribute contains all expected exports."""
        import forklift.processors.transformations as transformations_module

        expected_exports = [
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
        assert hasattr(transformations_module, "__all__")
        assert transformations_module.__all__ == expected_exports

        # Verify all items in __all__ are actually available in the module
        for export_name in expected_exports:
            assert hasattr(transformations_module, export_name)
            export_item = getattr(transformations_module, export_name)
            assert export_item is not None

    def test_individual_imports(self):
        """Test importing each class and function individually."""
        # Test transformer classes
        from forklift.processors.transformations import ColumnTransformer

        assert callable(ColumnTransformer)

        from forklift.processors.transformations import SchemaBasedTransformer

        assert callable(SchemaBasedTransformer)

        # Test common transformation functions
        from forklift.processors.transformations import trim_whitespace

        assert callable(trim_whitespace)

        from forklift.processors.transformations import uppercase

        assert callable(uppercase)

        from forklift.processors.transformations import lowercase

        assert callable(lowercase)

        # Test factory functions
        from forklift.processors.transformations import apply_money_conversion

        assert callable(apply_money_conversion)

        from forklift.processors.transformations import apply_numeric_cleaning

        assert callable(apply_numeric_cleaning)

        from forklift.processors.transformations import apply_regex_replace

        assert callable(apply_regex_replace)

        from forklift.processors.transformations import apply_string_replace

        assert callable(apply_string_replace)

        from forklift.processors.transformations import apply_html_xml_cleaning

        assert callable(apply_html_xml_cleaning)

        from forklift.processors.transformations import apply_string_padding

        assert callable(apply_string_padding)

        from forklift.processors.transformations import apply_string_trimming

        assert callable(apply_string_trimming)

        # Test utility classes
        from forklift.processors.transformations import DataTransformer

        assert callable(DataTransformer)

        from forklift.processors.transformations import \
            create_transformation_from_config

        assert callable(create_transformation_from_config)

        # Test config classes
        from forklift.processors.transformations import MoneyTypeConfig

        assert callable(MoneyTypeConfig)

    def test_module_docstring(self):
        """Test that the module has the expected docstring."""
        import forklift.processors.transformations as transformations_module

        expected_docstring_parts = [
            "Transformation processors package",
            "data transformation capabilities",
            "Basic column transformations",
            "Schema-driven transformations",
        ]

        assert transformations_module.__doc__ is not None
        for part in expected_docstring_parts:
            assert part in transformations_module.__doc__

    def test_imports_are_same_as_source_modules(self):
        """Test that imports from compatibility module are the same as source modules."""
        # Import from compatibility module
        from forklift.processors.transformations import \
            ColumnTransformer as CompatTransformer
        # Import from source module directly
        from forklift.processors.transformations.column_transformer import \
            ColumnTransformer as SourceTransformer

        # They should be the same class
        assert CompatTransformer is SourceTransformer

    def test_all_exports_available(self):
        """Test that __all__ functionality works by checking module namespace."""
        import forklift.processors.transformations as transformations_module

        # Get all public names from the module
        public_names = [name for name in dir(transformations_module) if not name.startswith("_")]

        # All items in __all__ should be in the public namespace
        for export_name in transformations_module.__all__:
            assert export_name in public_names

        # Test that we can access each export from __all__
        for export_name in transformations_module.__all__:
            export_item = getattr(transformations_module, export_name)
            assert export_item is not None

    def test_classes_and_functions_have_expected_attributes(self):
        """Test that imported classes and functions have expected attributes."""
        from forklift.processors.transformations import (
            ColumnTransformer, DataTransformer, MoneyTypeConfig,
            SchemaBasedTransformer, lowercase, trim_whitespace, uppercase)

        # Test classes have __init__ method
        classes = [ColumnTransformer, SchemaBasedTransformer, DataTransformer, MoneyTypeConfig]
        for cls in classes:
            assert hasattr(cls, "__init__"), f"{cls.__name__} should have __init__ method"

        # Test functions are callable
        functions = [trim_whitespace, uppercase, lowercase]
        for func in functions:
            assert callable(func), f"{func.__name__} should be callable"

    def test_import_error_handling(self):
        """Test that the module handles import scenarios correctly."""
        # Test that the module can be imported without errors
        # Test that re-importing works
        import forklift.processors.transformations
        import forklift.processors.transformations as transformations_alias

        # Both should reference the same module
        assert forklift.processors.transformations is transformations_alias

    def test_backward_compatibility_module_structure(self):
        """Test that the backward compatibility module has the expected structure."""
        # Import the module
        import forklift.processors.transformations as transformations_module

        # Verify the module has all expected attributes from the backward-compatibility interface
        expected_attributes = [
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
            "__all__",
        ]

        for attr in expected_attributes:
            assert hasattr(transformations_module, attr), f"Missing attribute: {attr}"

        # Verify __all__ contains exactly what we expect
        assert len(transformations_module.__all__) == 22
        assert all(
            name in transformations_module.__all__ for name in expected_attributes[:-1]
        )  # exclude __all__ itself

    def test_comprehensive_compatibility_scenario(self):
        """Test a comprehensive scenario using the backward compatibility interface."""
        # Import core exports through the compatibility interface
        from forklift.processors.transformations import (
            ColumnTransformer, DataTransformer, MoneyTypeConfig,
            SchemaBasedTransformer, trim_whitespace)

        # Verify exports are accessible and have expected properties
        class_exports = [
            ("ColumnTransformer", ColumnTransformer),
            ("SchemaBasedTransformer", SchemaBasedTransformer),
            ("DataTransformer", DataTransformer),
            ("MoneyTypeConfig", MoneyTypeConfig),
        ]

        function_exports = [("trim_whitespace", trim_whitespace)]

        # Test classes
        for export_name, export_item in class_exports:
            assert export_item is not None, f"{export_name} should not be None"
            assert callable(export_item), f"{export_name} should be callable (class)"
            assert hasattr(export_item, "__init__"), f"{export_name} should have __init__ method"

        # Test functions
        for export_name, export_item in function_exports:
            assert export_item is not None, f"{export_name} should not be None"
            assert callable(export_item), f"{export_name} should be callable (function)"

    def test_module_level_imports_coverage(self):
        """Test that ensures all module-level import statements are executed."""
        # Import the module which will execute all import statements
        import forklift.processors.transformations

        # Verify that the module was loaded successfully and has the expected structure
        module = forklift.processors.transformations

        # This test ensures that all import statements and __all__ definition are executed
        assert hasattr(module, "ColumnTransformer")
        assert hasattr(module, "SchemaBasedTransformer")
        assert hasattr(module, "trim_whitespace")
        assert hasattr(module, "uppercase")
        assert hasattr(module, "lowercase")
        assert hasattr(module, "apply_money_conversion")
        assert hasattr(module, "DataTransformer")
        assert hasattr(module, "MoneyTypeConfig")
        assert hasattr(module, "__all__")

        # Verify the __all__ list has the expected length
        assert len(module.__all__) == 22

    def test_transformer_classes_imports_coverage(self):
        """Test that the transformer classes imports are executed."""
        import forklift.processors.transformations as transformations_module

        # Verify that transformer classes are available
        transformer_classes = ["ColumnTransformer", "SchemaBasedTransformer"]

        for class_name in transformer_classes:
            assert hasattr(transformations_module, class_name)
            cls = getattr(transformations_module, class_name)
            assert cls is not None
            assert callable(cls)

    def test_common_functions_imports_coverage(self):
        """Test that the common functions imports are executed."""
        import forklift.processors.transformations as transformations_module

        # Verify that common functions are available
        common_functions = ["trim_whitespace", "uppercase", "lowercase"]

        for func_name in common_functions:
            assert hasattr(transformations_module, func_name)
            func = getattr(transformations_module, func_name)
            assert func is not None
            assert callable(func)

    def test_factory_functions_imports_coverage(self):
        """Test that the factory functions imports are executed."""
        import forklift.processors.transformations as transformations_module

        # Verify that factory functions are available
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
            assert hasattr(transformations_module, func_name)
            func = getattr(transformations_module, func_name)
            assert func is not None
            assert callable(func)

    def test_utility_classes_imports_coverage(self):
        """Test that the utility classes imports are executed."""
        import forklift.processors.transformations as transformations_module

        # Verify that utility classes are available
        utility_classes = ["DataTransformer", "create_transformation_from_config"]

        for class_name in utility_classes:
            assert hasattr(transformations_module, class_name)
            cls = getattr(transformations_module, class_name)
            assert cls is not None
            assert callable(cls)

    def test_config_classes_imports_coverage(self):
        """Test that the config classes imports are executed."""
        import forklift.processors.transformations as transformations_module

        # Verify that config classes are available
        config_classes = [
            "MoneyTypeConfig",
            "NumericCleaningConfig",
            "RegexReplaceConfig",
            "StringReplaceConfig",
            "HTMLXMLConfig",
            "StringPaddingConfig",
            "DateTimeTransformConfig",
            "StringCleaningConfig",
        ]

        for class_name in config_classes:
            assert hasattr(transformations_module, class_name)
            cls = getattr(transformations_module, class_name)
            assert cls is not None
            assert callable(cls)
