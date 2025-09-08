"""Tests for transformations processor backward compatibility."""

import pytest


class TestTransformationsBackwardCompatibility:
    """Test backward compatibility of transformations processor module."""

    def test_transformations_imports(self):
        """Test that all transformation classes can be imported from the main module."""
        from forklift.processors.transformations import (
            ColumnTransformer,
            SchemaBasedTransformer,
            trim_whitespace,
            uppercase,
            lowercase,
            apply_money_conversion,
            apply_numeric_cleaning,
            apply_regex_replace,
            apply_string_replace,
            apply_html_xml_cleaning,
            apply_string_padding,
            apply_string_trimming
        )

        # Verify all classes and functions are available
        assert ColumnTransformer is not None
        assert SchemaBasedTransformer is not None
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

    def test_transformations_module_docstring(self):
        """Test that the module has proper documentation."""
        import forklift.processors.transformations as trans_module

        assert trans_module.__doc__ is not None
        assert "Column transformation processor" in trans_module.__doc__
        assert "backward compatibility" in trans_module.__doc__

    def test_transformations_classes_are_callable(self):
        """Test that imported classes are actually callable."""
        from forklift.processors.transformations import (
            ColumnTransformer,
            SchemaBasedTransformer
        )

        # Verify classes are callable (can be instantiated)
        assert callable(ColumnTransformer)
        assert callable(SchemaBasedTransformer)

    def test_basic_transformation_functions(self):
        """Test that basic transformation functions work correctly."""
        from forklift.processors.transformations import (
            trim_whitespace,
            uppercase,
            lowercase
        )

        # Test basic functions with simple inputs
        test_string = "  Hello World  "

        trimmed = trim_whitespace(test_string)
        assert trimmed == "Hello World"

        upper = uppercase("hello")
        assert upper == "HELLO"

        lower = lowercase("WORLD")
        assert lower == "world"
