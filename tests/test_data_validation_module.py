"""Tests for data validation processor backward compatibility."""

from unittest.mock import Mock

import pytest

from forklift.processors import data_validation


class TestDataValidationModule:
    """Test cases for the data validation processor module."""

    def test_module_imports(self):
        """Test that all expected components are importable."""
        # Test that all components are available
        assert hasattr(data_validation, "RangeValidation")
        assert hasattr(data_validation, "StringValidation")
        assert hasattr(data_validation, "EnumValidation")
        assert hasattr(data_validation, "DateValidation")
        assert hasattr(data_validation, "FieldValidationRule")
        assert hasattr(data_validation, "BadRowsConfig")
        assert hasattr(data_validation, "ValidationConfig")
        assert hasattr(data_validation, "ValidationRules")
        assert hasattr(data_validation, "BadRowsHandler")
        assert hasattr(data_validation, "DataValidationProcessor")

    def test_backward_compatibility_imports(self):
        """Test that imports work for backward compatibility."""
        # These should not raise ImportError
        from forklift.processors.data_validation import (
            BadRowsConfig, BadRowsHandler, DataValidationProcessor,
            DateValidation, EnumValidation, FieldValidationRule,
            RangeValidation, StringValidation, ValidationConfig,
            ValidationRules)

        # Verify they are the same as the module attributes
        assert RangeValidation is data_validation.RangeValidation
        assert StringValidation is data_validation.StringValidation
        assert EnumValidation is data_validation.EnumValidation
        assert DateValidation is data_validation.DateValidation
        assert FieldValidationRule is data_validation.FieldValidationRule
        assert BadRowsConfig is data_validation.BadRowsConfig
        assert ValidationConfig is data_validation.ValidationConfig
        assert ValidationRules is data_validation.ValidationRules
        assert BadRowsHandler is data_validation.BadRowsHandler
        assert DataValidationProcessor is data_validation.DataValidationProcessor

    def test_module_docstring(self):
        """Test that the module has proper documentation."""
        assert data_validation.__doc__ is not None
        assert "Data validation package" in data_validation.__doc__
        assert "comprehensive data validation functionality" in data_validation.__doc__
