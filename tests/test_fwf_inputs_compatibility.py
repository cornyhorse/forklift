"""Tests for FWF inputs backward compatibility module.

This test file ensures 100% coverage of the backward-compatibility interface
in src/forklift/inputs/fwf.py by testing the import statements and __all__ exports.
"""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestFwfInputsCompatibility:
    """Test cases for FWF inputs backward compatibility."""

    def test_import_fwf_py_module_directly(self):
        """Test importing the fwf.py module directly to ensure coverage."""
        # We need to mock the relative import since we're loading the module directly
        mock_classes = {
            "FwfInputHandler": MagicMock,
            "FwfConfigValidator": MagicMock,
            "FwfTypeConverter": MagicMock,
            "FwfValueProcessor": MagicMock,
            "FwfEncodingDetector": MagicMock,
            "FwfSchemaDetector": MagicMock,
            "FwfFieldExtractor": MagicMock,
            "FwfLineParser": MagicMock,
        }

        # Create a mock module for the relative import
        mock_fwf_module = MagicMock()
        for name, cls in mock_classes.items():
            setattr(mock_fwf_module, name, cls)

        # Patch the import system to handle the relative import
        with patch.dict("sys.modules", {"forklift.inputs.fwf.fwf": mock_fwf_module}):
            # Load the fwf.py file
            spec = importlib.util.spec_from_file_location(
                "forklift.inputs.fwf_compat",
                Path(__file__).parent.parent / "src" / "forklift" / "inputs" / "fwf.py",
            )
            fwf_compat = importlib.util.module_from_spec(spec)

            # Set up the module's package context
            fwf_compat.__package__ = "forklift.inputs"

            # Execute the module
            spec.loader.exec_module(fwf_compat)

            # Verify all expected classes are available
            expected_classes = [
                "FwfInputHandler",
                "FwfConfigValidator",
                "FwfTypeConverter",
                "FwfValueProcessor",
                "FwfEncodingDetector",
                "FwfSchemaDetector",
                "FwfFieldExtractor",
                "FwfLineParser",
            ]

            # Test __all__ attribute
            assert hasattr(fwf_compat, "__all__")
            assert fwf_compat.__all__ == expected_classes

            # Test that all classes in __all__ are available
            for class_name in expected_classes:
                assert hasattr(fwf_compat, class_name)
                assert callable(getattr(fwf_compat, class_name))

    def test_fwf_py_module_docstring(self):
        """Test the docstring of the fwf.py module."""
        # Read the file directly to get the docstring without executing imports
        fwf_py_path = Path(__file__).parent.parent / "src" / "forklift" / "inputs" / "fwf.py"
        content = fwf_py_path.read_text()

        # Check docstring content in the file
        expected_docstring_parts = [
            "Fixed-width file input handler",
            "backward-compatible interface",
            "package structure",
        ]

        for part in expected_docstring_parts:
            assert part in content

    def test_import_all_classes(self):
        """Test importing all classes from the compatibility module."""
        # Import from the backward-compatibility module
        from forklift.inputs.fwf import (FwfConfigValidator,
                                         FwfEncodingDetector,
                                         FwfFieldExtractor, FwfInputHandler,
                                         FwfLineParser, FwfSchemaDetector,
                                         FwfTypeConverter, FwfValueProcessor)

        # Verify all classes are imported and are callable
        assert callable(FwfInputHandler)
        assert callable(FwfConfigValidator)
        assert callable(FwfTypeConverter)
        assert callable(FwfValueProcessor)
        assert callable(FwfEncodingDetector)
        assert callable(FwfSchemaDetector)
        assert callable(FwfFieldExtractor)
        assert callable(FwfLineParser)

    def test_module_all_attribute(self):
        """Test that the __all__ attribute contains all expected classes."""
        import forklift.inputs.fwf as fwf_module

        expected_classes = [
            "FwfInputHandler",
            "FwfConfigValidator",
            "FwfTypeConverter",
            "FwfValueProcessor",
            "FwfEncodingDetector",
            "FwfSchemaDetector",
            "FwfFieldExtractor",
            "FwfLineParser",
        ]

        # Verify __all__ attribute exists and contains expected classes
        assert hasattr(fwf_module, "__all__")
        assert fwf_module.__all__ == expected_classes

        # Verify all classes in __all__ are actually available in the module
        for class_name in expected_classes:
            assert hasattr(fwf_module, class_name)
            assert callable(getattr(fwf_module, class_name))

    def test_all_exports_available(self):
        """Test that __all__ functionality works by checking module namespace."""
        import forklift.inputs.fwf as fwf_module

        # Get all public names from the module
        public_names = [name for name in dir(fwf_module) if not name.startswith("_")]

        # All items in __all__ should be in the public namespace
        for class_name in fwf_module.__all__:
            assert class_name in public_names

        # Test that we can access each class from __all__
        for class_name in fwf_module.__all__:
            cls = getattr(fwf_module, class_name)
            assert callable(cls)

    def test_individual_class_imports(self):
        """Test importing each class individually."""
        # Test FwfInputHandler
        from forklift.inputs.fwf import FwfInputHandler

        assert callable(FwfInputHandler)

        # Test FwfConfigValidator
        from forklift.inputs.fwf import FwfConfigValidator

        assert callable(FwfConfigValidator)

        # Test FwfTypeConverter
        from forklift.inputs.fwf import FwfTypeConverter

        assert callable(FwfTypeConverter)

        # Test FwfValueProcessor
        from forklift.inputs.fwf import FwfValueProcessor

        assert callable(FwfValueProcessor)

        # Test FwfEncodingDetector
        from forklift.inputs.fwf import FwfEncodingDetector

        assert callable(FwfEncodingDetector)

        # Test FwfSchemaDetector
        from forklift.inputs.fwf import FwfSchemaDetector

        assert callable(FwfSchemaDetector)

        # Test FwfFieldExtractor
        from forklift.inputs.fwf import FwfFieldExtractor

        assert callable(FwfFieldExtractor)

        # Test FwfLineParser
        from forklift.inputs.fwf import FwfLineParser

        assert callable(FwfLineParser)

    def test_classes_are_same_as_package_imports(self):
        """Test that classes imported from compatibility module are the same as package imports."""
        # Import from compatibility module
        from forklift.inputs.fwf import FwfInputHandler as CompatHandler
        # Import from package directly
        from forklift.inputs.fwf.handlers import \
            FwfInputHandler as PackageHandler

        # They should be the same class
        assert CompatHandler is PackageHandler

    def test_all_classes_functionality_accessible(self):
        """Test that all classes can be instantiated or have expected attributes."""
        from forklift.inputs.fwf import (FwfConfigValidator,
                                         FwfEncodingDetector,
                                         FwfFieldExtractor, FwfInputHandler,
                                         FwfLineParser, FwfSchemaDetector,
                                         FwfTypeConverter, FwfValueProcessor)

        # Test that classes have expected methods/attributes (without instantiating)
        # This ensures the imports are working correctly
        # FwfInputHandler should be a class with certain methods
        assert hasattr(FwfInputHandler, "__init__")

        # FwfConfigValidator should be a class
        assert hasattr(FwfConfigValidator, "__init__")

        # Type converters should be classes
        assert hasattr(FwfTypeConverter, "__init__")
        assert hasattr(FwfValueProcessor, "__init__")

        # Detectors should be classes
        assert hasattr(FwfEncodingDetector, "__init__")
        assert hasattr(FwfSchemaDetector, "__init__")

        # Parsers should be classes
        assert hasattr(FwfFieldExtractor, "__init__")
        assert hasattr(FwfLineParser, "__init__")

    def test_import_error_handling(self):
        """Test that the module handles import scenarios correctly."""
        # Test that the module can be imported without errors
        # Test that re-importing works
        import forklift.inputs.fwf
        import forklift.inputs.fwf as fwf_alias

        # Both should reference the same module
        assert forklift.inputs.fwf is fwf_alias

    def test_backward_compatibility_module_structure(self):
        """Test that the backward compatibility module has the expected structure."""
        # This test exercises the actual fwf.py file by importing through the normal mechanism
        # which will execute all the import statements and __all__ definition

        # Import the module (this will execute the fwf.py file)
        import forklift.inputs.fwf as fwf_module

        # Verify the module has all expected attributes from the backward-compatibility interface
        expected_attributes = [
            "FwfInputHandler",
            "FwfConfigValidator",
            "FwfTypeConverter",
            "FwfValueProcessor",
            "FwfEncodingDetector",
            "FwfSchemaDetector",
            "FwfFieldExtractor",
            "FwfLineParser",
            "__all__",
        ]

        for attr in expected_attributes:
            assert hasattr(fwf_module, attr), f"Missing attribute: {attr}"

        # Verify __all__ contains exactly what we expect
        assert len(fwf_module.__all__) == 8
        assert all(
            name in fwf_module.__all__ for name in expected_attributes[:-1]
        )  # exclude __all__ itself
