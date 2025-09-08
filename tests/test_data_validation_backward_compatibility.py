"""Tests for data validation processor backward compatibility."""

import pytest


class TestDataValidationBackwardCompatibility:
    """Test backward compatibility of data validation processor module."""

    def test_data_validation_imports(self):
        """Test that all data validation classes can be imported from the main module."""
        from forklift.processors.data_validation import (
            RangeValidation,
            StringValidation,
            EnumValidation,
            DateValidation,
            FieldValidationRule,
            BadRowsConfig,
            ValidationConfig,
            ValidationRules,
            BadRowsHandler,
            DataValidationProcessor
        )

        # Verify all classes are available
        assert RangeValidation is not None
        assert StringValidation is not None
        assert EnumValidation is not None
        assert DateValidation is not None
        assert FieldValidationRule is not None
        assert BadRowsConfig is not None
        assert ValidationConfig is not None
        assert ValidationRules is not None
        assert BadRowsHandler is not None
        assert DataValidationProcessor is not None

    def test_data_validation_module_docstring(self):
        """Test that the module has proper documentation."""
        import forklift.processors.data_validation as dv_module

        assert dv_module.__doc__ is not None
        assert "Data validation processor" in dv_module.__doc__
        assert "backward compatibility" in dv_module.__doc__

    def test_data_validation_classes_are_callable(self):
        """Test that imported classes are actually callable."""
        from forklift.processors.data_validation import (
            RangeValidation,
            StringValidation,
            EnumValidation,
            DateValidation,
            FieldValidationRule,
            BadRowsConfig,
            ValidationConfig,
            ValidationRules,
            BadRowsHandler,
            DataValidationProcessor
        )

        # Verify classes are callable (can be instantiated)
        assert callable(RangeValidation)
        assert callable(StringValidation)
        assert callable(EnumValidation)
        assert callable(DateValidation)
        assert callable(FieldValidationRule)
        assert callable(BadRowsConfig)
        assert callable(ValidationConfig)
        assert callable(ValidationRules)
        assert callable(BadRowsHandler)
        assert callable(DataValidationProcessor)
