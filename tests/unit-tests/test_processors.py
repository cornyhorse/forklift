"""Comprehensive tests for the processors module to improve code coverage."""

import pytest
import pyarrow as pa
import pyarrow.compute as pc
import re
from typing import List, Dict, Any

from forklift.processors import (
    ValidationResult,
    BaseProcessor,
    SchemaValidator,
    DataQualityProcessor,
    ColumnTransformer,
    ProcessorPipeline
)


class TestValidationResult:
    """Test the ValidationResult dataclass."""

    def test_validation_result_creation(self):
        """Test creating ValidationResult instances."""
        # Test minimal creation
        result = ValidationResult(is_valid=True)
        assert result.is_valid is True
        assert result.error_message is None
        assert result.error_code is None
        assert result.row_index is None
        assert result.column_name is None

    def test_validation_result_with_all_fields(self):
        """Test creating ValidationResult with all fields."""
        result = ValidationResult(
            is_valid=False,
            error_message="Test error",
            error_code="TEST_ERROR",
            row_index=5,
            column_name="test_column"
        )
        assert result.is_valid is False
        assert result.error_message == "Test error"
        assert result.error_code == "TEST_ERROR"
        assert result.row_index == 5
        assert result.column_name == "test_column"


class MockProcessor(BaseProcessor):
    """Mock processor for testing BaseProcessor functionality."""

    def __init__(self, should_error=False):
        self.should_error = should_error

    def process_batch(self, batch: pa.RecordBatch):
        if self.should_error:
            return batch, [ValidationResult(
                is_valid=False,
                error_message="Mock error",
                error_code="MOCK_ERROR"
            )]
        return batch, []


class TestBaseProcessor:
    """Test the BaseProcessor abstract base class."""

    def test_mock_processor_success(self):
        """Test mock processor with successful processing."""
        processor = MockProcessor(should_error=False)
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string())
        ])
        batch = pa.RecordBatch.from_arrays([
            pa.array([1, 2, 3]),
            pa.array(["Alice", "Bob", "Charlie"])
        ], schema=schema)

        result_batch, validation_results = processor.process_batch(batch)
        assert result_batch == batch
        assert len(validation_results) == 0

    def test_mock_processor_error(self):
        """Test mock processor with error generation."""
        processor = MockProcessor(should_error=True)
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string())
        ])
        batch = pa.RecordBatch.from_arrays([
            pa.array([1, 2, 3]),
            pa.array(["Alice", "Bob", "Charlie"])
        ], schema=schema)

        result_batch, validation_results = processor.process_batch(batch)
        assert result_batch == batch
        assert len(validation_results) == 1
        assert validation_results[0].is_valid is False
        assert validation_results[0].error_message == "Mock error"
        assert validation_results[0].error_code == "MOCK_ERROR"


class TestSchemaValidator:
    """Test the SchemaValidator processor."""

    def test_schema_validator_initialization(self):
        """Test SchemaValidator initialization."""
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string())
        ])
        validator = SchemaValidator(schema, strict_mode=True)
        assert validator.schema == schema
        assert validator.strict_mode is True

        validator_non_strict = SchemaValidator(schema, strict_mode=False)
        assert validator_non_strict.strict_mode is False

    def test_schema_validator_valid_data(self):
        """Test SchemaValidator with valid data."""
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string())
        ])
        validator = SchemaValidator(schema)

        batch = pa.RecordBatch.from_arrays([
            pa.array([1, 2, 3]),
            pa.array(["Alice", "Bob", "Charlie"])
        ], schema=schema)

        result_batch, validation_results = validator.process_batch(batch)
        assert len(validation_results) == 0

    def test_schema_validator_null_validation(self):
        """Test SchemaValidator with null validation."""
        schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=True)
        ])
        validator = SchemaValidator(schema)

        # Create batch with null in non-nullable field
        batch = pa.RecordBatch.from_arrays([
            pa.array([1, None, 3]),  # Null in non-nullable field
            pa.array(["Alice", "Bob", None])  # Null in nullable field (OK)
        ], schema=pa.schema([
            pa.field("id", pa.int64(), nullable=True),  # Different nullability
            pa.field("name", pa.string(), nullable=True)
        ]))

        result_batch, validation_results = validator.process_batch(batch)
        # Should have validation errors for null in non-nullable field
        null_errors = [r for r in validation_results if r.error_code == "NULL_IN_REQUIRED_FIELD"]
        assert len(null_errors) >= 1

    def test_schema_validator_type_casting(self):
        """Test SchemaValidator with type casting."""
        target_schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("score", pa.float64())
        ])
        validator = SchemaValidator(target_schema)

        # Create batch with compatible types that need casting
        source_schema = pa.schema([
            pa.field("id", pa.int32()),  # Will cast to int64
            pa.field("score", pa.float32())  # Will cast to float64
        ])
        batch = pa.RecordBatch.from_arrays([
            pa.array([1, 2, 3], type=pa.int32()),
            pa.array([1.5, 2.5, 3.5], type=pa.float32())
        ], schema=source_schema)

        result_batch, validation_results = validator.process_batch(batch)
        # Compatible types should cast successfully with no errors
        assert len([r for r in validation_results if r.error_code == "TYPE_CAST_ERROR"]) == 0


class TestDataQualityProcessor:
    """Test the DataQualityProcessor."""

    def test_data_quality_processor_initialization(self):
        """Test DataQualityProcessor initialization."""
        rules = {
            "column_rules": {
                "name": {"min_length": 2, "max_length": 50},
                "age": {"min_value": 0, "max_value": 150}
            }
        }
        processor = DataQualityProcessor(rules)
        assert processor.rules == rules

    def test_string_length_validation(self):
        """Test string length validation."""
        rules = {
            "column_rules": {
                "name": {"min_length": 3, "max_length": 10}
            }
        }
        processor = DataQualityProcessor(rules)

        schema = pa.schema([pa.field("name", pa.string())])
        batch = pa.RecordBatch.from_arrays([
            pa.array(["Al", "Alice", "VeryLongName123"])  # Too short, OK, too long
        ], schema=schema)

        result_batch, validation_results = processor.process_batch(batch)

        # Should have errors for too short and too long
        length_errors = [r for r in validation_results
                        if r.error_code in ["MIN_LENGTH_VIOLATION", "MAX_LENGTH_VIOLATION"]]
        assert len(length_errors) == 2

    def test_pattern_validation(self):
        """Test pattern validation."""
        rules = {
            "column_rules": {
                "email": {"pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"}
            }
        }
        processor = DataQualityProcessor(rules)

        schema = pa.schema([pa.field("email", pa.string())])
        batch = pa.RecordBatch.from_arrays([
            pa.array(["valid@example.com", "invalid-email", "another@test.org"])
        ], schema=schema)

        result_batch, validation_results = processor.process_batch(batch)

        # Should have one pattern violation
        pattern_errors = [r for r in validation_results if r.error_code == "PATTERN_VIOLATION"]
        assert len(pattern_errors) == 1
        assert "invalid-email" in pattern_errors[0].error_message

    def test_numeric_range_validation(self):
        """Test numeric range validation."""
        rules = {
            "column_rules": {
                "age": {"min_value": 0, "max_value": 120}
            }
        }
        processor = DataQualityProcessor(rules)

        schema = pa.schema([pa.field("age", pa.int64())])
        batch = pa.RecordBatch.from_arrays([
            pa.array([-5, 25, 150])  # Too low, OK, too high
        ], schema=schema)

        result_batch, validation_results = processor.process_batch(batch)

        # Should have errors for out of range values
        range_errors = [r for r in validation_results
                       if r.error_code in ["MIN_VALUE_VIOLATION", "MAX_VALUE_VIOLATION"]]
        assert len(range_errors) == 2

    def test_non_applicable_rules(self):
        """Test that rules are only applied to appropriate column types."""
        rules = {
            "column_rules": {
                "id": {"min_length": 5},  # String rule on integer column
                "name": {"min_value": 0}   # Numeric rule on string column
            }
        }
        processor = DataQualityProcessor(rules)

        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string())
        ])
        batch = pa.RecordBatch.from_arrays([
            pa.array([1, 2, 3]),
            pa.array(["Alice", "Bob", "Charlie"])
        ], schema=schema)

        result_batch, validation_results = processor.process_batch(batch)

        # Should have no validation errors since rules don't apply to column types
        assert len(validation_results) == 0

    def test_missing_column_rules(self):
        """Test processor with rules for non-existent columns."""
        rules = {
            "column_rules": {
                "non_existent_column": {"min_length": 5}
            }
        }
        processor = DataQualityProcessor(rules)

        schema = pa.schema([pa.field("name", pa.string())])
        batch = pa.RecordBatch.from_arrays([
            pa.array(["Alice", "Bob", "Charlie"])
        ], schema=schema)

        result_batch, validation_results = processor.process_batch(batch)

        # Should have no validation errors since column doesn't exist
        assert len(validation_results) == 0


class TestColumnTransformer:
    """Test the ColumnTransformer processor."""

    def test_column_transformer_initialization(self):
        """Test ColumnTransformer initialization."""
        def uppercase_transform(column):
            return pc.utf8_upper(column)

        transformations = {
            "name": [uppercase_transform]
        }
        transformer = ColumnTransformer(transformations)
        assert transformer.transformations == transformations

    def test_string_transformation(self):
        """Test string transformations."""
        def uppercase_transform(column):
            return pc.utf8_upper(column)

        def trim_transform(column):
            return pc.utf8_trim_whitespace(column)

        transformations = {
            "name": [trim_transform, uppercase_transform]
        }
        transformer = ColumnTransformer(transformations)

        schema = pa.schema([pa.field("name", pa.string())])
        batch = pa.RecordBatch.from_arrays([
            pa.array([" alice ", " bob ", " charlie "])
        ], schema=schema)

        result_batch, validation_results = transformer.process_batch(batch)

        # Should have no validation errors
        assert len(validation_results) == 0

        # Check that transformations were applied
        transformed_names = result_batch.column(0).to_pylist()
        expected = ["ALICE", "BOB", "CHARLIE"]
        assert transformed_names == expected

    def test_transformation_error_handling(self):
        """Test error handling in transformations."""
        def failing_transform(column):
            raise ValueError("Transformation failed")

        transformations = {
            "name": [failing_transform]
        }
        transformer = ColumnTransformer(transformations)

        schema = pa.schema([pa.field("name", pa.string())])
        batch = pa.RecordBatch.from_arrays([
            pa.array(["Alice", "Bob", "Charlie"])
        ], schema=schema)

        result_batch, validation_results = transformer.process_batch(batch)

        # Should have transformation error
        assert len(validation_results) == 1
        assert validation_results[0].error_code == "TRANSFORMATION_ERROR"
        assert "Transformation failed" in validation_results[0].error_message

    def test_missing_column_transformation(self):
        """Test transformations for non-existent columns."""
        def uppercase_transform(column):
            return pc.utf8_upper(column)

        transformations = {
            "non_existent_column": [uppercase_transform]
        }
        transformer = ColumnTransformer(transformations)

        schema = pa.schema([pa.field("name", pa.string())])
        batch = pa.RecordBatch.from_arrays([
            pa.array(["Alice", "Bob", "Charlie"])
        ], schema=schema)

        result_batch, validation_results = transformer.process_batch(batch)

        # Should have no errors and original batch should be unchanged
        assert len(validation_results) == 0
        assert result_batch == batch


class TestProcessorPipeline:
    """Test the ProcessorPipeline."""

    def test_processor_pipeline_initialization(self):
        """Test ProcessorPipeline initialization."""
        processor1 = MockProcessor()
        processor2 = MockProcessor()

        pipeline = ProcessorPipeline([processor1, processor2])
        assert pipeline.processors == [processor1, processor2]

    def test_pipeline_processing(self):
        """Test processing through pipeline."""
        # Create pipeline with multiple processors
        rules = {
            "column_rules": {
                "name": {"min_length": 2}
            }
        }
        quality_processor = DataQualityProcessor(rules)

        def uppercase_transform(column):
            return pc.utf8_upper(column)

        transformations = {"name": [uppercase_transform]}
        transformer = ColumnTransformer(transformations)

        pipeline = ProcessorPipeline([quality_processor, transformer])

        schema = pa.schema([pa.field("name", pa.string())])
        batch = pa.RecordBatch.from_arrays([
            pa.array(["alice", "x", "bob"])  # One too short
        ], schema=schema)

        result_batch, validation_results = pipeline.process_batch(batch)

        # Should have validation error from quality processor
        length_errors = [r for r in validation_results if r.error_code == "MIN_LENGTH_VIOLATION"]
        assert len(length_errors) == 1

        # Should have applied transformation
        transformed_names = result_batch.column(0).to_pylist()
        assert "ALICE" in transformed_names
        assert "BOB" in transformed_names

    def test_empty_pipeline(self):
        """Test empty pipeline."""
        pipeline = ProcessorPipeline([])

        schema = pa.schema([pa.field("name", pa.string())])
        batch = pa.RecordBatch.from_arrays([
            pa.array(["Alice", "Bob", "Charlie"])
        ], schema=schema)

        result_batch, validation_results = pipeline.process_batch(batch)

        # Should return original batch with no validation results
        assert result_batch == batch
        assert len(validation_results) == 0

    def test_pipeline_error_accumulation(self):
        """Test that pipeline accumulates errors from all processors."""
        error_processor1 = MockProcessor(should_error=True)
        error_processor2 = MockProcessor(should_error=True)

        pipeline = ProcessorPipeline([error_processor1, error_processor2])

        schema = pa.schema([pa.field("name", pa.string())])
        batch = pa.RecordBatch.from_arrays([
            pa.array(["Alice", "Bob", "Charlie"])
        ], schema=schema)

        result_batch, validation_results = pipeline.process_batch(batch)

        # Should accumulate errors from both processors
        assert len(validation_results) == 2
        assert all(r.error_code == "MOCK_ERROR" for r in validation_results)


class TestIntegrationScenarios:
    """Test integration scenarios combining multiple processors."""

    def test_comprehensive_data_processing_pipeline(self):
        """Test a comprehensive data processing pipeline."""
        # Define schema
        target_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=False),
            pa.field("email", pa.string(), nullable=True),
            pa.field("age", pa.int64(), nullable=True)
        ])

        # Create processors
        schema_validator = SchemaValidator(target_schema)

        quality_rules = {
            "column_rules": {
                "name": {"min_length": 2, "max_length": 50},
                "email": {"pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"},
                "age": {"min_value": 0, "max_value": 150}
            }
        }
        quality_processor = DataQualityProcessor(quality_rules)

        def normalize_name(column):
            return pc.utf8_title(pc.utf8_trim_whitespace(column))

        transformations = {"name": [normalize_name]}
        transformer = ColumnTransformer(transformations)

        # Create pipeline
        pipeline = ProcessorPipeline([
            schema_validator,
            quality_processor,
            transformer
        ])

        # Create test data with various issues
        source_schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("email", pa.string()),
            pa.field("age", pa.int64())
        ])

        batch = pa.RecordBatch.from_arrays([
            pa.array([1, 2, 3, 4]),
            pa.array([" alice smith ", "x", " bob jones ", " charlie brown "]),  # One too short
            pa.array(["alice@example.com", "invalid-email", "bob@test.org", "charlie@demo.com"]),
            pa.array([25, 200, 30, -5])  # One too high, one too low
        ], schema=source_schema)

        result_batch, validation_results = pipeline.process_batch(batch)

        # Should have validation errors
        error_types = {r.error_code for r in validation_results}
        expected_errors = {"MIN_LENGTH_VIOLATION", "PATTERN_VIOLATION", "MIN_VALUE_VIOLATION", "MAX_VALUE_VIOLATION"}
        assert error_types.intersection(expected_errors)

        # Should have applied name normalization
        transformed_names = result_batch.column(1).to_pylist()
        assert "Alice Smith" in transformed_names
        assert "Bob Jones" in transformed_names
        assert "Charlie Brown" in transformed_names

    def test_processor_with_null_handling(self):
        """Test processors with null value handling."""
        rules = {
            "column_rules": {
                "optional_field": {"min_length": 3}
            }
        }
        processor = DataQualityProcessor(rules)

        schema = pa.schema([pa.field("optional_field", pa.string())])
        batch = pa.RecordBatch.from_arrays([
            pa.array(["valid", None, "x"])  # Valid, null (should skip), too short
        ], schema=schema)

        result_batch, validation_results = processor.process_batch(batch)

        # Should only have one error (for "x"), null should be skipped
        length_errors = [r for r in validation_results if r.error_code == "MIN_LENGTH_VIOLATION"]
        assert len(length_errors) == 1


if __name__ == "__main__":
    pytest.main([__file__])
