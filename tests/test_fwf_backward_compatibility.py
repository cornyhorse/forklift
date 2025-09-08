"""Tests for fwf input module backward compatibility."""

import pytest


class TestFwfBackwardCompatibility:
    """Test backward compatibility of fwf input module."""

    def test_fwf_imports(self):
        """Test that all FWF classes can be imported from the main module."""
        from forklift.inputs.fwf import (
            FwfInputHandler,
            FwfConfigValidator,
            FwfTypeConverter,
            FwfValueProcessor,
            FwfEncodingDetector,
            FwfSchemaDetector,
            FwfFieldExtractor,
            FwfLineParser,
        )

        # Verify all classes are available
        assert FwfInputHandler is not None
        assert FwfConfigValidator is not None
        assert FwfTypeConverter is not None
        assert FwfValueProcessor is not None
        assert FwfEncodingDetector is not None
        assert FwfSchemaDetector is not None
        assert FwfFieldExtractor is not None
        assert FwfLineParser is not None

    def test_fwf_all_exports(self):
        """Test that __all__ contains expected exports."""
        import forklift.inputs.fwf as fwf_module

        expected_exports = [
            'FwfInputHandler',
            'FwfConfigValidator',
            'FwfTypeConverter',
            'FwfValueProcessor',
            'FwfEncodingDetector',
            'FwfSchemaDetector',
            'FwfFieldExtractor',
            'FwfLineParser',
        ]

        assert hasattr(fwf_module, '__all__')
        assert set(fwf_module.__all__) == set(expected_exports)

    def test_fwf_module_docstring(self):
        """Test that the module has proper documentation."""
        import forklift.inputs.fwf as fwf_module

        assert fwf_module.__doc__ is not None
        assert "Fixed-width file input handler" in fwf_module.__doc__
        assert "backward-compatible" in fwf_module.__doc__

    def test_fwf_classes_are_callable(self):
        """Test that imported classes are actually callable."""
        from forklift.inputs.fwf import (
            FwfInputHandler,
            FwfConfigValidator,
            FwfTypeConverter,
            FwfValueProcessor,
            FwfEncodingDetector,
            FwfSchemaDetector,
            FwfFieldExtractor,
            FwfLineParser,
        )

        # Verify classes are callable (can be instantiated)
        assert callable(FwfInputHandler)
        assert callable(FwfConfigValidator)
        assert callable(FwfTypeConverter)
        assert callable(FwfValueProcessor)
        assert callable(FwfEncodingDetector)
        assert callable(FwfSchemaDetector)
        assert callable(FwfFieldExtractor)
        assert callable(FwfLineParser)
