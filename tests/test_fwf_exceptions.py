"""Tests for FWF schema exceptions."""

import pytest

from forklift.schema.fwf.exceptions import (
    ConditionalSchemaError,
    FieldValidationError,
    ParquetTypeError,
    SchemaValidationError,
)


class TestFwfExceptions:
    """Test cases for FWF schema exception classes."""

    def test_schema_validation_error(self):
        """Test SchemaValidationError exception."""
        # Test basic instantiation
        error = SchemaValidationError("Test message")
        assert str(error) == "Test message"
        assert isinstance(error, Exception)

        # Test without message
        error_no_msg = SchemaValidationError()
        assert isinstance(error_no_msg, Exception)

    def test_field_validation_error(self):
        """Test FieldValidationError exception."""
        # Test basic instantiation
        error = FieldValidationError("Field validation failed")
        assert str(error) == "Field validation failed"
        assert isinstance(error, SchemaValidationError)
        assert isinstance(error, Exception)

        # Test inheritance chain
        assert issubclass(FieldValidationError, SchemaValidationError)

    def test_conditional_schema_error(self):
        """Test ConditionalSchemaError exception."""
        # Test basic instantiation
        error = ConditionalSchemaError("Conditional schema processing failed")
        assert str(error) == "Conditional schema processing failed"
        assert isinstance(error, SchemaValidationError)
        assert isinstance(error, Exception)

        # Test inheritance chain
        assert issubclass(ConditionalSchemaError, SchemaValidationError)

    def test_parquet_type_error(self):
        """Test ParquetTypeError exception."""
        # Test basic instantiation
        error = ParquetTypeError("Parquet type validation failed")
        assert str(error) == "Parquet type validation failed"
        assert isinstance(error, SchemaValidationError)
        assert isinstance(error, Exception)

        # Test inheritance chain
        assert issubclass(ParquetTypeError, SchemaValidationError)

    def test_exception_hierarchy(self):
        """Test the exception hierarchy is correctly structured."""
        # All custom exceptions should inherit from SchemaValidationError
        assert issubclass(FieldValidationError, SchemaValidationError)
        assert issubclass(ConditionalSchemaError, SchemaValidationError)
        assert issubclass(ParquetTypeError, SchemaValidationError)

        # SchemaValidationError should inherit from Exception
        assert issubclass(SchemaValidationError, Exception)

    def test_exception_raising(self):
        """Test that exceptions can be raised and caught properly."""
        # Test raising SchemaValidationError
        with pytest.raises(SchemaValidationError) as exc_info:
            raise SchemaValidationError("Test schema error")
        assert str(exc_info.value) == "Test schema error"

        # Test raising FieldValidationError
        with pytest.raises(FieldValidationError) as exc_info:
            raise FieldValidationError("Test field error")
        assert str(exc_info.value) == "Test field error"

        # Test raising ConditionalSchemaError
        with pytest.raises(ConditionalSchemaError) as exc_info:
            raise ConditionalSchemaError("Test conditional error")
        assert str(exc_info.value) == "Test conditional error"

        # Test raising ParquetTypeError
        with pytest.raises(ParquetTypeError) as exc_info:
            raise ParquetTypeError("Test parquet error")
        assert str(exc_info.value) == "Test parquet error"

    def test_catching_base_exception(self):
        """Test that derived exceptions can be caught by base exception."""
        # FieldValidationError should be catchable as SchemaValidationError
        with pytest.raises(SchemaValidationError):
            raise FieldValidationError("Field error")

        # ConditionalSchemaError should be catchable as SchemaValidationError
        with pytest.raises(SchemaValidationError):
            raise ConditionalSchemaError("Conditional error")

        # ParquetTypeError should be catchable as SchemaValidationError
        with pytest.raises(SchemaValidationError):
            raise ParquetTypeError("Parquet error")
