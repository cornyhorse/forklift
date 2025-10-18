"""Tests for FWF input module backward compatibility."""

from unittest.mock import Mock

import pytest

from forklift.inputs import fwf


class TestFwfInputsModule:
    """Test cases for the FWF inputs module."""

    def test_module_imports(self):
        """Test that all expected components are importable."""
        # Test that all components are available
        assert hasattr(fwf, "FwfInputHandler")
        assert hasattr(fwf, "FwfConfigValidator")
        assert hasattr(fwf, "FwfTypeConverter")
        assert hasattr(fwf, "FwfValueProcessor")
        assert hasattr(fwf, "FwfEncodingDetector")
        assert hasattr(fwf, "FwfSchemaDetector")
        assert hasattr(fwf, "FwfFieldExtractor")
        assert hasattr(fwf, "FwfLineParser")

    def test_all_exports(self):
        """Test that __all__ contains expected exports."""
        expected_exports = [
            "FwfInputHandler",
            "FwfConfigValidator",
            "FwfTypeConverter",
            "FwfValueProcessor",
            "FwfEncodingDetector",
            "FwfSchemaDetector",
            "FwfFieldExtractor",
            "FwfLineParser",
        ]

        assert fwf.__all__ == expected_exports

    def test_backward_compatibility_imports(self):
        """Test that imports work for backward compatibility."""
        # These should not raise ImportError
        from forklift.inputs.fwf import (FwfConfigValidator,
                                         FwfEncodingDetector,
                                         FwfFieldExtractor, FwfInputHandler,
                                         FwfLineParser, FwfSchemaDetector,
                                         FwfTypeConverter, FwfValueProcessor)

        # Verify they are the same as the module attributes
        assert FwfInputHandler is fwf.FwfInputHandler
        assert FwfConfigValidator is fwf.FwfConfigValidator
        assert FwfTypeConverter is fwf.FwfTypeConverter
        assert FwfValueProcessor is fwf.FwfValueProcessor
        assert FwfEncodingDetector is fwf.FwfEncodingDetector
        assert FwfSchemaDetector is fwf.FwfSchemaDetector
        assert FwfFieldExtractor is fwf.FwfFieldExtractor
        assert FwfLineParser is fwf.FwfLineParser

    def test_module_docstring(self):
        """Test that the module has proper documentation."""
        assert fwf.__doc__ is not None
        assert "FWF (Fixed Width File) input processing package" in fwf.__doc__
        assert "fixed-width files" in fwf.__doc__
