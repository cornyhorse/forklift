"""Tests for transformations processor backward compatibility."""

import pytest
from unittest.mock import Mock

from forklift.processors import transformations


class TestTransformationsModule:
    """Test cases for the transformations processor module."""

    def test_module_imports(self):
        """Test that all expected components are importable."""
        # Test that all components are available
        assert hasattr(transformations, 'ColumnTransformer')
        assert hasattr(transformations, 'SchemaBasedTransformer')
        assert hasattr(transformations, 'trim_whitespace')
        assert hasattr(transformations, 'uppercase')
        assert hasattr(transformations, 'lowercase')
        assert hasattr(transformations, 'apply_money_conversion')
        assert hasattr(transformations, 'apply_numeric_cleaning')
        assert hasattr(transformations, 'apply_regex_replace')
        assert hasattr(transformations, 'apply_string_replace')
        assert hasattr(transformations, 'apply_html_xml_cleaning')
        assert hasattr(transformations, 'apply_string_padding')
        assert hasattr(transformations, 'apply_string_trimming')

    def test_backward_compatibility_imports(self):
        """Test that imports work for backward compatibility."""
        # These should not raise ImportError
        from forklift.processors.transformations import ColumnTransformer
        from forklift.processors.transformations import SchemaBasedTransformer
        from forklift.processors.transformations import trim_whitespace
        from forklift.processors.transformations import uppercase
        from forklift.processors.transformations import lowercase
        from forklift.processors.transformations import apply_money_conversion
        from forklift.processors.transformations import apply_numeric_cleaning
        from forklift.processors.transformations import apply_regex_replace
        from forklift.processors.transformations import apply_string_replace
        from forklift.processors.transformations import apply_html_xml_cleaning
        from forklift.processors.transformations import apply_string_padding
        from forklift.processors.transformations import apply_string_trimming

        # Verify they are the same as the module attributes
        assert ColumnTransformer is transformations.ColumnTransformer
        assert SchemaBasedTransformer is transformations.SchemaBasedTransformer
        assert trim_whitespace is transformations.trim_whitespace
        assert uppercase is transformations.uppercase
        assert lowercase is transformations.lowercase
        assert apply_money_conversion is transformations.apply_money_conversion
        assert apply_numeric_cleaning is transformations.apply_numeric_cleaning
        assert apply_regex_replace is transformations.apply_regex_replace
        assert apply_string_replace is transformations.apply_string_replace
        assert apply_html_xml_cleaning is transformations.apply_html_xml_cleaning
        assert apply_string_padding is transformations.apply_string_padding
        assert apply_string_trimming is transformations.apply_string_trimming

    def test_module_docstring(self):
        """Test that the module has proper documentation."""
        assert transformations.__doc__ is not None
        assert "Transformation processors package" in transformations.__doc__
        assert "data transformation capabilities" in transformations.__doc__
