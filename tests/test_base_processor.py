"""Comprehensive unit tests for BaseProcessor - targeting 100% coverage."""

import pytest
from abc import ABC
from unittest.mock import Mock, patch

from forklift.engine.processors.base import BaseProcessor
from forklift.engine.config import ImportConfig, ProcessingResults, HeaderMode


class TestBaseProcessor:
    """Test suite for BaseProcessor abstract base class."""

    def test_base_processor_is_abstract(self):
        """Test that BaseProcessor cannot be instantiated directly."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class BaseProcessor"):
            BaseProcessor()

    def test_base_processor_inheritance(self):
        """Test that BaseProcessor is properly configured as an ABC."""
        assert issubclass(BaseProcessor, ABC)
        assert hasattr(BaseProcessor, '__abstractmethods__')
        assert 'process' in BaseProcessor.__abstractmethods__

    def test_concrete_implementation_must_implement_process(self):
        """Test that concrete subclasses must implement the process method."""

        # Create a concrete class that doesn't implement process
        class IncompleteProcessor(BaseProcessor):
            pass

        with pytest.raises(TypeError, match="Can't instantiate abstract class IncompleteProcessor"):
            IncompleteProcessor()

    def test_concrete_implementation_with_process_method(self):
        """Test that concrete subclasses can be instantiated when process is implemented."""

        class ConcreteProcessor(BaseProcessor):
            def process(self, config: ImportConfig) -> ProcessingResults:
                return ProcessingResults()

        # Should be able to instantiate
        processor = ConcreteProcessor()
        assert isinstance(processor, BaseProcessor)
        assert isinstance(processor, ConcreteProcessor)

    def test_process_method_signature(self):
        """Test that the process method has the correct signature."""

        class TestProcessor(BaseProcessor):
            def process(self, config: ImportConfig) -> ProcessingResults:
                return ProcessingResults(total_rows=100, valid_rows=95, invalid_rows=5)

        processor = TestProcessor()

        # Create a mock config
        config = Mock(spec=ImportConfig)

        # Call the process method
        result = processor.process(config)

        # Verify return type and content
        assert isinstance(result, ProcessingResults)
        assert result.total_rows == 100
        assert result.valid_rows == 95
        assert result.invalid_rows == 5

    def test_multiple_concrete_implementations(self):
        """Test that multiple concrete implementations can coexist."""

        class ProcessorA(BaseProcessor):
            def process(self, config: ImportConfig) -> ProcessingResults:
                return ProcessingResults(total_rows=50)

        class ProcessorB(BaseProcessor):
            def process(self, config: ImportConfig) -> ProcessingResults:
                return ProcessingResults(total_rows=100)

        processor_a = ProcessorA()
        processor_b = ProcessorB()

        config = Mock(spec=ImportConfig)

        result_a = processor_a.process(config)
        result_b = processor_b.process(config)

        assert result_a.total_rows == 50
        assert result_b.total_rows == 100

    def test_process_method_with_real_config(self):
        """Test process method with a real ImportConfig object."""

        class RealConfigProcessor(BaseProcessor):
            def process(self, config: ImportConfig) -> ProcessingResults:
                # Use some config values in processing
                results = ProcessingResults()
                results.total_rows = config.batch_size
                results.execution_time = 1.5
                return results

        processor = RealConfigProcessor()

        # Create real config
        config = ImportConfig(
            input_path="/test/input.csv",
            output_path="/test/output",
            batch_size=5000,
            header_mode=HeaderMode.PRESENT
        )

        result = processor.process(config)

        assert result.total_rows == 5000
        assert result.execution_time == 1.5

    def test_inheritance_chain(self):
        """Test inheritance behavior with multiple levels."""

        class MiddleProcessor(BaseProcessor):
            def process(self, config: ImportConfig) -> ProcessingResults:
                return ProcessingResults()

            def common_method(self):
                return "middle"

        class FinalProcessor(MiddleProcessor):
            def process(self, config: ImportConfig) -> ProcessingResults:
                result = super().process(config)
                result.total_rows = 200
                return result

            def common_method(self):
                return "final"

        processor = FinalProcessor()
        assert isinstance(processor, BaseProcessor)
        assert isinstance(processor, MiddleProcessor)
        assert processor.common_method() == "final"

        config = Mock(spec=ImportConfig)
        result = processor.process(config)
        assert result.total_rows == 200

    def test_method_resolution_order(self):
        """Test method resolution order for BaseProcessor."""

        class TestProcessor(BaseProcessor):
            def process(self, config: ImportConfig) -> ProcessingResults:
                return ProcessingResults()

        # Check MRO
        mro = TestProcessor.__mro__
        assert BaseProcessor in mro
        assert ABC in mro
        assert object in mro

    def test_abstract_method_detection(self):
        """Test that the abstract method is properly detected."""

        # Verify the abstract method is correctly identified
        abstract_methods = BaseProcessor.__abstractmethods__
        assert len(abstract_methods) == 1
        assert 'process' in abstract_methods

    def test_processor_with_additional_methods(self):
        """Test processor with additional non-abstract methods."""

        class ExtendedProcessor(BaseProcessor):
            def process(self, config: ImportConfig) -> ProcessingResults:
                self.validate_config(config)
                return ProcessingResults(total_rows=1000)

            def validate_config(self, config: ImportConfig):
                """Additional validation method."""
                if not config.input_path:
                    raise ValueError("Input path is required")

            def get_processor_name(self):
                """Get processor name."""
                return "ExtendedProcessor"

        processor = ExtendedProcessor()
        assert processor.get_processor_name() == "ExtendedProcessor"

        # Test with valid config
        config = ImportConfig(input_path="/test/input.csv", output_path="/test/output")
        result = processor.process(config)
        assert result.total_rows == 1000

        # Test with invalid config
        invalid_config = ImportConfig(input_path="", output_path="/test/output")
        with pytest.raises(ValueError, match="Input path is required"):
            processor.process(invalid_config)

    def test_processor_error_handling(self):
        """Test error handling in processor implementations."""

        class ErrorProneProcessor(BaseProcessor):
            def process(self, config: ImportConfig) -> ProcessingResults:
                if config.batch_size <= 0:
                    raise ValueError("Batch size must be positive")
                return ProcessingResults(total_rows=config.batch_size)

        processor = ErrorProneProcessor()

        # Test valid case
        valid_config = ImportConfig(
            input_path="/test/input.csv",
            output_path="/test/output",
            batch_size=1000
        )
        result = processor.process(valid_config)
        assert result.total_rows == 1000

        # Test error case
        invalid_config = ImportConfig(
            input_path="/test/input.csv",
            output_path="/test/output",
            batch_size=-5
        )
        with pytest.raises(ValueError, match="Batch size must be positive"):
            processor.process(invalid_config)

    def test_abstract_method_enforcement(self):
        """Test that abstract method enforcement works at different inheritance levels."""

        # Direct inheritance without implementation
        class IncompleteDirectProcessor(BaseProcessor):
            def some_other_method(self):
                return "implemented"

        with pytest.raises(TypeError):
            IncompleteDirectProcessor()

        # Indirect inheritance without implementation
        class IntermediateClass(BaseProcessor):
            def helper_method(self):
                return "helper"

        class IncompleteIndirectProcessor(IntermediateClass):
            def another_method(self):
                return "another"

        with pytest.raises(TypeError):
            IncompleteIndirectProcessor()

    def test_isinstance_and_issubclass_behavior(self):
        """Test isinstance and issubclass behavior with BaseProcessor."""

        class ConcreteProcessor(BaseProcessor):
            def process(self, config: ImportConfig) -> ProcessingResults:
                return ProcessingResults()

        processor = ConcreteProcessor()

        # isinstance tests
        assert isinstance(processor, BaseProcessor)
        assert isinstance(processor, ConcreteProcessor)
        assert isinstance(processor, ABC)

        # issubclass tests
        assert issubclass(ConcreteProcessor, BaseProcessor)
        assert issubclass(ConcreteProcessor, ABC)
        assert issubclass(BaseProcessor, ABC)

    def test_processor_with_properties(self):
        """Test processor implementation with properties."""

        class PropertyProcessor(BaseProcessor):
            def __init__(self):
                self._name = "PropertyProcessor"
                self._version = "1.0"

            @property
            def name(self):
                return self._name

            @property
            def version(self):
                return self._version

            def process(self, config: ImportConfig) -> ProcessingResults:
                return ProcessingResults(
                    total_rows=100,
                    execution_time=2.0
                )

        processor = PropertyProcessor()
        assert processor.name == "PropertyProcessor"
        assert processor.version == "1.0"

        config = Mock(spec=ImportConfig)
        result = processor.process(config)
        assert result.total_rows == 100
        assert result.execution_time == 2.0

    def test_base_processor_module_attributes(self):
        """Test module-level attributes and imports."""
        from forklift.engine.processors.base import BaseProcessor as ImportedBaseProcessor

        assert ImportedBaseProcessor is BaseProcessor
        assert hasattr(BaseProcessor, '__module__')
        assert BaseProcessor.__module__ == 'forklift.engine.processors.base'
