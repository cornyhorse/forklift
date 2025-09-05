"""Comprehensive tests for calculated_columns.py to achieve 100% code coverage."""

import pytest
import sys
import math
from datetime import datetime, date
from unittest.mock import Mock, patch, MagicMock

import pyarrow as pa
import pyarrow.compute as pc

# Add src to Python path for imports
sys.path.insert(0, 'src')

# Import the base classes first
from forklift.processors.base import BaseProcessor, ValidationResult

# Import the calculated_columns module directly to ensure coverage tracking
import forklift.processors.calculated_columns

# Now import the specific classes
from forklift.processors.calculated_columns import (
    CalculatedColumn,
    CalculatedColumnsConfig,
    CalculatedColumnsProcessor,
    ConstantColumn,
    ExpressionColumn
)


class TestCalculatedColumn:
    """Test cases for CalculatedColumn dataclass."""

    def test_calculated_column_init_defaults(self):
        """Test CalculatedColumn initialization with defaults."""
        col = CalculatedColumn(name="test_col", expression="x + y")
        assert col.name == "test_col"
        assert col.expression == "x + y"
        assert col.data_type == pa.string()
        assert col.description is None
        assert col.dependencies == []

    def test_calculated_column_init_with_values(self):
        """Test CalculatedColumn initialization with explicit values."""
        col = CalculatedColumn(
            name="test_col",
            expression="x + y",
            data_type=pa.int64(),
            description="Test column",
            dependencies=["x", "y"]
        )
        assert col.name == "test_col"
        assert col.expression == "x + y"
        assert col.data_type == pa.int64()
        assert col.description == "Test column"
        assert col.dependencies == ["x", "y"]

    def test_calculated_column_post_init(self):
        """Test CalculatedColumn __post_init__ method."""
        # Test with None dependencies
        col = CalculatedColumn(name="test", expression="x", dependencies=None)
        assert col.dependencies == []

        # Test with None data_type
        col = CalculatedColumn(name="test", expression="x", data_type=None)
        assert col.data_type is None


class TestCalculatedColumnsConfig:
    """Test cases for CalculatedColumnsConfig dataclass."""

    def test_config_init_defaults(self):
        """Test CalculatedColumnsConfig initialization with defaults."""
        columns = [CalculatedColumn(name="test", expression="x")]
        config = CalculatedColumnsConfig(columns=columns)
        assert config.columns == columns
        assert config.fail_on_error is True
        assert config.add_metadata is False
        assert config.validate_dependencies is True

    def test_config_init_with_values(self):
        """Test CalculatedColumnsConfig initialization with explicit values."""
        columns = [CalculatedColumn(name="test", expression="x")]
        config = CalculatedColumnsConfig(
            columns=columns,
            fail_on_error=False,
            add_metadata=True,
            validate_dependencies=False
        )
        assert config.columns == columns
        assert config.fail_on_error is False
        assert config.add_metadata is True
        assert config.validate_dependencies is False


class TestCalculatedColumnsProcessor:
    """Test cases for CalculatedColumnsProcessor class."""

    def create_sample_batch(self):
        """Create a sample PyArrow RecordBatch for testing."""
        schema = pa.schema([
            pa.field("x", pa.int64()),
            pa.field("y", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("date_col", pa.date32())
        ])

        data = {
            "x": [1, 2, 3, None],
            "y": [10, 20, 30, 40],
            "name": ["Alice", "Bob", "Charlie", "David"],
            "date_col": [date(2023, 1, 1), date(2023, 2, 1), date(2023, 3, 1), date(2023, 4, 1)]
        }

        arrays = []
        for field in schema:
            arrays.append(pa.array(data[field.name], type=field.type))

        return pa.RecordBatch.from_arrays(arrays, schema=schema)

    def test_processor_init_basic(self):
        """Test basic processor initialization."""
        columns = [CalculatedColumn(name="sum_col", expression="x + y")]
        config = CalculatedColumnsConfig(columns=columns)
        processor = CalculatedColumnsProcessor(config)

        assert processor.config == config
        assert len(processor._available_functions) > 0
        assert 'add' in processor._available_functions

    def test_processor_init_with_validation_disabled(self):
        """Test processor initialization with dependency validation disabled."""
        columns = [CalculatedColumn(name="sum_col", expression="x + y")]
        config = CalculatedColumnsConfig(columns=columns, validate_dependencies=False)
        processor = CalculatedColumnsProcessor(config)

        assert processor.config == config

    def test_init_functions_arithmetic(self):
        """Test arithmetic functions initialization."""
        config = CalculatedColumnsConfig(columns=[])
        processor = CalculatedColumnsProcessor(config)

        funcs = processor._available_functions

        # Test arithmetic functions
        assert funcs['add'](5, 3) == 8
        assert funcs['subtract'](5, 3) == 2
        assert funcs['multiply'](5, 3) == 15
        assert funcs['divide'](6, 3) == 2
        assert funcs['power'](2, 3) == 8
        assert funcs['mod'](7, 3) == 1

        # Test null handling
        assert funcs['add'](None, 3) is None
        assert funcs['divide'](5, 0) is None

    def test_init_functions_mathematical(self):
        """Test mathematical functions initialization."""
        config = CalculatedColumnsConfig(columns=[])
        processor = CalculatedColumnsProcessor(config)

        funcs = processor._available_functions

        # Test mathematical functions
        assert funcs['abs'](-5) == 5
        assert funcs['round'](3.14159, 2) == 3.14
        assert funcs['floor'](3.7) == 3
        assert funcs['ceil'](3.2) == 4
        assert funcs['sqrt'](9) == 3
        assert abs(funcs['log'](math.e) - 1) < 0.001
        assert funcs['log10'](100) == 2

        # Test null and invalid input handling
        assert funcs['sqrt'](None) is None
        assert funcs['sqrt'](-1) is None
        assert funcs['log'](0) is None
        assert funcs['log'](-1) is None

    def test_init_functions_string(self):
        """Test string functions initialization."""
        config = CalculatedColumnsConfig(columns=[])
        processor = CalculatedColumnsProcessor(config)

        funcs = processor._available_functions

        # Test string functions
        assert funcs['concat']("Hello", " ", "World") == "Hello World"
        assert funcs['upper']("hello") == "HELLO"
        assert funcs['lower']("HELLO") == "hello"
        assert funcs['trim']("  hello  ") == "hello"
        assert funcs['length']("hello") == 5
        assert funcs['substring']("hello", 1, 3) == "ell"
        assert funcs['replace']("hello", "l", "x") == "hexxo"
        assert funcs['left']("hello", 3) == "hel"
        assert funcs['right']("hello", 3) == "llo"

        # Test null handling
        assert funcs['upper'](None) is None
        assert funcs['length'](None) is None

    def test_init_functions_conditional(self):
        """Test conditional functions initialization."""
        config = CalculatedColumnsConfig(columns=[])
        processor = CalculatedColumnsProcessor(config)

        funcs = processor._available_functions

        # Test conditional functions
        assert funcs['if_then_else'](True, "yes", "no") == "yes"
        assert funcs['if_then_else'](False, "yes", "no") == "no"
        assert funcs['coalesce'](None, None, "default") == "default"
        assert funcs['nullif'](5, 5) is None
        assert funcs['nullif'](5, 3) == 5
        assert funcs['isnull'](None) is True
        assert funcs['isnull'](5) is False
        assert funcs['isnotnull'](5) is True
        assert funcs['isnotnull'](None) is False

    def test_init_functions_type_conversion(self):
        """Test type conversion functions initialization."""
        config = CalculatedColumnsConfig(columns=[])
        processor = CalculatedColumnsProcessor(config)

        funcs = processor._available_functions

        # Test type conversion functions
        assert funcs['to_string'](123) == "123"
        assert funcs['to_int']("123") == 123
        assert funcs['to_float']("123.45") == 123.45
        assert funcs['to_bool'](1) is True
        assert funcs['to_bool'](0) is False

        # Test null handling
        assert funcs['to_string'](None) is None
        assert funcs['to_int'](None) is None

    def test_init_functions_datetime(self):
        """Test date/time functions initialization."""
        config = CalculatedColumnsConfig(columns=[])
        processor = CalculatedColumnsProcessor(config)

        funcs = processor._available_functions

        # Test date/time functions
        test_date = date(2023, 5, 15)
        test_datetime = datetime(2023, 5, 15, 10, 30, 45)

        assert funcs['year'](test_date) == 2023
        assert funcs['month'](test_date) == 5
        assert funcs['day'](test_date) == 15
        assert funcs['weekday'](test_date) == 0  # Monday

        assert funcs['year'](test_datetime) == 2023
        assert funcs['month'](test_datetime) == 5
        assert funcs['day'](test_datetime) == 15

        # Test null handling
        assert funcs['year'](None) is None
        assert funcs['year']("not_a_date") is None

    def test_init_functions_comparison(self):
        """Test comparison functions initialization."""
        config = CalculatedColumnsConfig(columns=[])
        processor = CalculatedColumnsProcessor(config)

        funcs = processor._available_functions

        # Test comparison functions
        assert funcs['equals'](5, 5) is True
        assert funcs['equals'](5, 3) is False
        assert funcs['not_equals'](5, 3) is True
        assert funcs['greater_than'](5, 3) is True
        assert funcs['less_than'](3, 5) is True
        assert funcs['greater_equal'](5, 5) is True
        assert funcs['less_equal'](3, 5) is True

        # Test null handling
        assert funcs['greater_than'](None, 5) is False
        assert funcs['less_than'](5, None) is False

    def test_init_functions_logical(self):
        """Test logical functions initialization."""
        config = CalculatedColumnsConfig(columns=[])
        processor = CalculatedColumnsProcessor(config)

        funcs = processor._available_functions

        # Test logical functions
        assert funcs['and'](True, True) is True
        assert funcs['and'](True, False) is False
        assert funcs['or'](True, False) is True
        assert funcs['or'](False, False) is False
        assert funcs['not'](True) is False
        assert funcs['not'](False) is True

    def test_init_functions_utility(self):
        """Test utility functions initialization."""
        config = CalculatedColumnsConfig(columns=[])
        processor = CalculatedColumnsProcessor(config)

        funcs = processor._available_functions

        # Test utility functions
        assert funcs['min'](1, 2, 3) == 1
        assert funcs['max'](1, 2, 3) == 3
        assert funcs['sum'](1, 2, 3) == 6
        assert funcs['avg'](2, 4, 6) == 4

        # Test with null values
        assert funcs['min'](None, 2, 3) == 2
        assert funcs['max'](1, None, 3) == 3
        assert funcs['sum'](1, None, 3) == 4
        assert funcs['avg'](2, None, 6) == 4

        # Test all null values
        assert funcs['min'](None, None) is None
        assert funcs['avg'](None, None) is None

    def test_validate_dependencies_no_circular(self):
        """Test dependency validation with no circular dependencies."""
        columns = [
            CalculatedColumn(name="a", expression="x + 1"),
            CalculatedColumn(name="b", expression="a + 2", dependencies=["a"]),
            CalculatedColumn(name="c", expression="b + 3", dependencies=["b"])
        ]
        config = CalculatedColumnsConfig(columns=columns)

        # Should not raise an exception
        processor = CalculatedColumnsProcessor(config)
        assert processor is not None

    def test_validate_dependencies_circular(self):
        """Test dependency validation with circular dependencies."""
        columns = [
            CalculatedColumn(name="a", expression="b + 1", dependencies=["b"]),
            CalculatedColumn(name="b", expression="a + 2", dependencies=["a"])
        ]
        config = CalculatedColumnsConfig(columns=columns)

        with pytest.raises(ValueError, match="Circular dependency detected"):
            CalculatedColumnsProcessor(config)

    def test_has_circular_dependency_simple(self):
        """Test circular dependency detection for simple case."""
        columns = [
            CalculatedColumn(name="a", expression="b + 1", dependencies=["b"]),
            CalculatedColumn(name="b", expression="a + 2", dependencies=["a"])
        ]
        config = CalculatedColumnsConfig(columns=columns, validate_dependencies=False)
        processor = CalculatedColumnsProcessor(config)

        all_columns = {"a", "b"}
        assert processor._has_circular_dependency(columns[0], all_columns, set()) is True

    def test_has_circular_dependency_self_reference(self):
        """Test circular dependency detection for self-reference."""
        columns = [
            CalculatedColumn(name="a", expression="a + 1", dependencies=["a"])
        ]
        config = CalculatedColumnsConfig(columns=columns, validate_dependencies=False)
        processor = CalculatedColumnsProcessor(config)

        all_columns = {"a"}
        assert processor._has_circular_dependency(columns[0], all_columns, set()) is True

    def test_has_circular_dependency_no_circular(self):
        """Test circular dependency detection with no circular dependencies."""
        columns = [
            CalculatedColumn(name="a", expression="x + 1"),
            CalculatedColumn(name="b", expression="a + 2", dependencies=["a"])
        ]
        config = CalculatedColumnsConfig(columns=columns, validate_dependencies=False)
        processor = CalculatedColumnsProcessor(config)

        all_columns = {"a", "b"}
        assert processor._has_circular_dependency(columns[1], all_columns, set()) is False

    def test_process_batch_simple_arithmetic(self):
        """Test processing batch with simple arithmetic calculation."""
        batch = self.create_sample_batch()

        columns = [
            CalculatedColumn(name="sum_col", expression="x + y", data_type=pa.int64())
        ]
        config = CalculatedColumnsConfig(columns=columns)
        processor = CalculatedColumnsProcessor(config)

        result_batch, validation_results = processor.process_batch(batch)

        assert result_batch.num_columns == batch.num_columns + 1
        assert "sum_col" in result_batch.schema.names

        sum_column = result_batch.column("sum_col")
        assert sum_column[0].as_py() == 11  # 1 + 10
        assert sum_column[1].as_py() == 22  # 2 + 20
        assert sum_column[2].as_py() == 33  # 3 + 30
        assert sum_column[3].as_py() is None  # None + 40

    def test_process_batch_string_operations(self):
        """Test processing batch with string operations."""
        batch = self.create_sample_batch()

        columns = [
            CalculatedColumn(name="upper_name", expression="upper(name)"),
            CalculatedColumn(name="name_length", expression="length(name)", data_type=pa.int64())
        ]
        config = CalculatedColumnsConfig(columns=columns)
        processor = CalculatedColumnsProcessor(config)

        result_batch, validation_results = processor.process_batch(batch)

        upper_column = result_batch.column("upper_name")
        length_column = result_batch.column("name_length")

        assert upper_column[0].as_py() == "ALICE"
        assert length_column[0].as_py() == 5

    def test_process_batch_with_metadata(self):
        """Test processing batch with metadata enabled."""
        batch = self.create_sample_batch()

        columns = [
            CalculatedColumn(name="sum_col", expression="x + y", data_type=pa.int64())
        ]
        config = CalculatedColumnsConfig(columns=columns, add_metadata=True)
        processor = CalculatedColumnsProcessor(config)

        result_batch, validation_results = processor.process_batch(batch)

        assert len(validation_results) == 1
        assert validation_results[0].is_valid is True
        assert validation_results[0].error_code == "CALCULATION_SUCCESS"

    def test_process_batch_with_error_fail_on_error_true(self):
        """Test processing batch with calculation error and fail_on_error=True."""
        batch = self.create_sample_batch()

        columns = [
            CalculatedColumn(name="error_col", expression="nonexistent_function(x)")
        ]
        config = CalculatedColumnsConfig(columns=columns, fail_on_error=True)
        processor = CalculatedColumnsProcessor(config)

        result_batch, validation_results = processor.process_batch(batch)

        assert result_batch == batch  # Original batch returned on error
        assert len(validation_results) == 1
        assert validation_results[0].is_valid is False
        assert validation_results[0].error_code == "CALCULATION_ERROR"

    def test_process_batch_with_error_fail_on_error_false(self):
        """Test processing batch with calculation error and fail_on_error=False."""
        batch = self.create_sample_batch()

        columns = [
            CalculatedColumn(name="error_col", expression="nonexistent_function(x)")
        ]
        config = CalculatedColumnsConfig(columns=columns, fail_on_error=False)
        processor = CalculatedColumnsProcessor(config)

        result_batch, validation_results = processor.process_batch(batch)

        assert result_batch.num_columns == batch.num_columns + 1
        assert "error_col" in result_batch.schema.names

        error_column = result_batch.column("error_col")
        assert all(error_column[i].as_py() is None for i in range(len(error_column)))

        # When fail_on_error=False, we don't add validation results for individual row errors
        # but we do add a null column, so no validation results are expected unless add_metadata=True
        assert len(validation_results) == 0

    def test_process_batch_processor_exception(self):
        """Test processing batch with processor-level exception."""
        batch = self.create_sample_batch()

        columns = [
            CalculatedColumn(name="test_col", expression="x + y")
        ]
        config = CalculatedColumnsConfig(columns=columns)
        processor = CalculatedColumnsProcessor(config)

        # Mock _sort_columns_by_dependencies to raise an exception
        with patch.object(processor, '_sort_columns_by_dependencies', side_effect=Exception("Test error")):
            result_batch, validation_results = processor.process_batch(batch)

        assert result_batch == batch
        assert len(validation_results) == 1
        assert validation_results[0].is_valid is False
        assert validation_results[0].error_code == "PROCESSOR_ERROR"

    def test_sort_columns_by_dependencies_simple(self):
        """Test sorting columns by dependencies."""
        columns = [
            CalculatedColumn(name="c", expression="b + 1", dependencies=["b"]),
            CalculatedColumn(name="a", expression="x + 1"),
            CalculatedColumn(name="b", expression="a + 1", dependencies=["a"])
        ]
        config = CalculatedColumnsConfig(columns=columns, validate_dependencies=False)
        processor = CalculatedColumnsProcessor(config)

        sorted_columns = processor._sort_columns_by_dependencies()
        sorted_names = [col.name for col in sorted_columns]

        # 'a' should come before 'b', and 'b' should come before 'c'
        assert sorted_names.index('a') < sorted_names.index('b')
        assert sorted_names.index('b') < sorted_names.index('c')

    def test_sort_columns_by_dependencies_no_dependencies(self):
        """Test sorting columns with no dependencies."""
        columns = [
            CalculatedColumn(name="a", expression="x + 1"),
            CalculatedColumn(name="b", expression="y + 1"),
            CalculatedColumn(name="c", expression="x + y")
        ]
        config = CalculatedColumnsConfig(columns=columns)
        processor = CalculatedColumnsProcessor(config)

        sorted_columns = processor._sort_columns_by_dependencies()
        assert len(sorted_columns) == 3

    def test_sort_columns_by_dependencies_circular_fallback(self):
        """Test sorting with circular dependencies fallback."""
        columns = [
            CalculatedColumn(name="a", expression="b + 1", dependencies=["b"]),
            CalculatedColumn(name="b", expression="a + 1", dependencies=["a"])
        ]
        config = CalculatedColumnsConfig(columns=columns, validate_dependencies=False)
        processor = CalculatedColumnsProcessor(config)

        sorted_columns = processor._sort_columns_by_dependencies()
        assert len(sorted_columns) == 2

    def test_calculate_column_basic(self):
        """Test calculating a single column."""
        batch = self.create_sample_batch()

        config = CalculatedColumnsConfig(columns=[])
        processor = CalculatedColumnsProcessor(config)

        column_config = CalculatedColumn(name="sum_col", expression="x + y", data_type=pa.int64())
        result_array = processor._calculate_column(batch, column_config)

        assert result_array[0].as_py() == 11
        assert result_array[1].as_py() == 22
        assert result_array[2].as_py() == 33
        assert result_array[3].as_py() is None

    def test_calculate_column_with_error_fail_on_error_false(self):
        """Test calculating column with error and fail_on_error=False."""
        batch = self.create_sample_batch()

        config = CalculatedColumnsConfig(columns=[], fail_on_error=False)
        processor = CalculatedColumnsProcessor(config)

        column_config = CalculatedColumn(name="error_col", expression="undefined_function(x)")
        result_array = processor._calculate_column(batch, column_config)

        assert all(result_array[i].as_py() is None for i in range(len(result_array)))

    def test_calculate_column_with_error_fail_on_error_true(self):
        """Test calculating column with error and fail_on_error=True."""
        batch = self.create_sample_batch()

        config = CalculatedColumnsConfig(columns=[], fail_on_error=True)
        processor = CalculatedColumnsProcessor(config)

        column_config = CalculatedColumn(name="error_col", expression="undefined_function(x)")

        with pytest.raises(Exception):
            processor._calculate_column(batch, column_config)

    def test_evaluate_expression_basic(self):
        """Test evaluating basic expressions."""
        batch = self.create_sample_batch()

        config = CalculatedColumnsConfig(columns=[])
        processor = CalculatedColumnsProcessor(config)

        # Test simple arithmetic
        result = processor._evaluate_expression(batch, 0, "x + y")
        assert result == 11

        # Test with constants
        result = processor._evaluate_expression(batch, 0, "x + 10")
        assert result == 11

        # Test string function
        result = processor._evaluate_expression(batch, 0, "upper(name)")
        assert result == "ALICE"

    def test_evaluate_expression_with_constants(self):
        """Test evaluating expressions with built-in constants."""
        batch = self.create_sample_batch()

        config = CalculatedColumnsConfig(columns=[])
        processor = CalculatedColumnsProcessor(config)

        # Test mathematical constants
        result = processor._evaluate_expression(batch, 0, "PI")
        assert abs(result - math.pi) < 0.001

        result = processor._evaluate_expression(batch, 0, "E")
        assert abs(result - math.e) < 0.001

        # Test boolean constants
        result = processor._evaluate_expression(batch, 0, "TRUE")
        assert result is True

        result = processor._evaluate_expression(batch, 0, "FALSE")
        assert result is False

        result = processor._evaluate_expression(batch, 0, "NULL")
        assert result is None

    def test_evaluate_expression_error(self):
        """Test evaluating expressions with errors."""
        batch = self.create_sample_batch()

        config = CalculatedColumnsConfig(columns=[])
        processor = CalculatedColumnsProcessor(config)

        with pytest.raises(ValueError, match="Expression evaluation failed"):
            processor._evaluate_expression(batch, 0, "undefined_variable")

    def test_add_column_to_batch(self):
        """Test adding a new column to a batch."""
        batch = self.create_sample_batch()

        config = CalculatedColumnsConfig(columns=[])
        processor = CalculatedColumnsProcessor(config)

        new_column = pa.array([100, 200, 300, 400], type=pa.int64())
        result_batch = processor._add_column_to_batch(batch, "new_col", new_column)

        assert result_batch.num_columns == batch.num_columns + 1
        assert "new_col" in result_batch.schema.names
        assert result_batch.column("new_col")[0].as_py() == 100

    def test_get_calculated_columns_info(self):
        """Test getting calculated columns information."""
        columns = [
            CalculatedColumn(name="a", expression="x + y"),
            CalculatedColumn(name="b", expression="a * 2", dependencies=["a"])
        ]
        config = CalculatedColumnsConfig(columns=columns, fail_on_error=False, add_metadata=True)
        processor = CalculatedColumnsProcessor(config)

        info = processor.get_calculated_columns_info()

        assert info['total_columns'] == 2
        assert info['column_names'] == ["a", "b"]
        assert info['has_dependencies'] is True
        assert info['fail_on_error'] is False
        assert info['add_metadata'] is True
        assert 'add' in info['available_functions']

    def test_get_calculated_columns_info_no_dependencies(self):
        """Test getting calculated columns info with no dependencies."""
        columns = [
            CalculatedColumn(name="a", expression="x + y"),
            CalculatedColumn(name="b", expression="x * 2")
        ]
        config = CalculatedColumnsConfig(columns=columns)
        processor = CalculatedColumnsProcessor(config)

        info = processor.get_calculated_columns_info()
        assert info['has_dependencies'] is False

    def test_validate_expressions_valid(self):
        """Test validating valid expressions."""
        columns = [
            CalculatedColumn(name="sum_col", expression="x + y"),
            CalculatedColumn(name="name_upper", expression="upper(name)")
        ]
        config = CalculatedColumnsConfig(columns=columns)
        processor = CalculatedColumnsProcessor(config)

        sample_data = {"x": 5, "y": 10, "name": "test"}
        results = processor.validate_expressions(sample_data)

        assert len(results) == 2
        assert all(result.is_valid for result in results)
        assert all(result.error_code == "EXPRESSION_VALID" for result in results)

    def test_validate_expressions_invalid(self):
        """Test validating invalid expressions."""
        columns = [
            CalculatedColumn(name="error_col", expression="undefined_function(x)")
        ]
        config = CalculatedColumnsConfig(columns=columns)
        processor = CalculatedColumnsProcessor(config)

        sample_data = {"x": 5}
        results = processor.validate_expressions(sample_data)

        assert len(results) == 1
        assert results[0].is_valid is False
        assert results[0].error_code == "EXPRESSION_INVALID"

    def test_complex_expressions(self):
        """Test complex expressions with multiple operations."""
        batch = self.create_sample_batch()

        columns = [
            CalculatedColumn(
                name="complex_calc",
                expression="if_then_else(greater_than(x, 2), multiply(x, y), add(x, y))",
                data_type=pa.int64()
            )
        ]
        config = CalculatedColumnsConfig(columns=columns)
        processor = CalculatedColumnsProcessor(config)

        result_batch, validation_results = processor.process_batch(batch)

        # The column should be successfully created
        assert result_batch.num_columns == batch.num_columns + 1
        assert "complex_calc" in result_batch.schema.names

        complex_column = result_batch.column("complex_calc")
        assert complex_column[0].as_py() == 11   # 1 + 10 (1 <= 2)
        assert complex_column[1].as_py() == 22   # 2 + 20 (2 <= 2)
        assert complex_column[2].as_py() == 90   # 3 * 30 (3 > 2)
        assert complex_column[3].as_py() is None # None case

    def test_date_operations(self):
        """Test date-related operations."""
        batch = self.create_sample_batch()

        columns = [
            CalculatedColumn(
                name="year_col",
                expression="year(date_col)",
                data_type=pa.int64()
            )
        ]
        config = CalculatedColumnsConfig(columns=columns)
        processor = CalculatedColumnsProcessor(config)

        result_batch, validation_results = processor.process_batch(batch)

        year_column = result_batch.column("year_col")
        assert year_column[0].as_py() == 2023

    def test_null_handling_functions(self):
        """Test null handling functions."""
        batch = self.create_sample_batch()

        columns = [
            CalculatedColumn(
                name="coalesce_col",
                expression="coalesce(x, 999)",
                data_type=pa.int64()
            )
        ]
        config = CalculatedColumnsConfig(columns=columns)
        processor = CalculatedColumnsProcessor(config)

        result_batch, validation_results = processor.process_batch(batch)

        coalesce_column = result_batch.column("coalesce_col")
        assert coalesce_column[0].as_py() == 1    # x is not null
        assert coalesce_column[3].as_py() == 999  # x is null, use default

    def test_mathematical_functions(self):
        """Test mathematical functions with real data."""
        schema = pa.schema([pa.field("value", pa.float64())])
        data = {"value": [4.0, 9.0, 16.0, -5.0]}
        arrays = [pa.array(data["value"], type=pa.float64())]
        batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

        columns = [
            CalculatedColumn(
                name="sqrt_col",
                expression="sqrt(value)",
                data_type=pa.float64()
            ),
            CalculatedColumn(
                name="abs_col",
                expression="abs(value)",
                data_type=pa.float64()
            )
        ]
        config = CalculatedColumnsConfig(columns=columns)
        processor = CalculatedColumnsProcessor(config)

        result_batch, validation_results = processor.process_batch(batch)

        sqrt_column = result_batch.column("sqrt_col")
        abs_column = result_batch.column("abs_col")

        assert sqrt_column[0].as_py() == 2.0
        assert sqrt_column[1].as_py() == 3.0
        assert sqrt_column[2].as_py() == 4.0
        assert sqrt_column[3].as_py() is None  # sqrt of negative number

        assert abs_column[3].as_py() == 5.0

    def test_multiple_dependencies(self):
        """Test columns with multiple dependencies in correct order."""
        batch = self.create_sample_batch()

        columns = [
            CalculatedColumn(
                name="step1",
                expression="x + 1",
                data_type=pa.int64()
            ),
            CalculatedColumn(
                name="step2",
                expression="step1 + y",
                dependencies=["step1"],
                data_type=pa.int64()
            ),
            CalculatedColumn(
                name="step3",
                expression="step2 * 2",
                dependencies=["step2"],
                data_type=pa.int64()
            )
        ]
        config = CalculatedColumnsConfig(columns=columns)
        processor = CalculatedColumnsProcessor(config)

        result_batch, validation_results = processor.process_batch(batch)

        step3_column = result_batch.column("step3")
        # For first row: step1 = 1+1=2, step2 = 2+10=12, step3 = 12*2=24
        assert step3_column[0].as_py() == 24


class TestConstantColumn:
    """Test cases for ConstantColumn dataclass."""

    def test_constant_column_init_defaults(self):
        """Test ConstantColumn initialization with defaults."""
        col = ConstantColumn(name="const_col", value="test_value")
        assert col.name == "const_col"
        assert col.value == "test_value"
        assert col.data_type is None
        assert col.description is None

    def test_constant_column_init_with_values(self):
        """Test ConstantColumn initialization with explicit values."""
        col = ConstantColumn(
            name="const_col",
            value=42,
            data_type=pa.int64(),
            description="Test constant"
        )
        assert col.name == "const_col"
        assert col.value == 42
        assert col.data_type == pa.int64()
        assert col.description == "Test constant"

    def test_constant_column_post_init(self):
        """Test ConstantColumn __post_init__ method."""
        # Test with None data_type
        col = ConstantColumn(name="test", value="test", data_type=None)
        assert col.data_type == pa.string()

    def test_constant_column_to_calculated_column_string(self):
        """Test converting string ConstantColumn to CalculatedColumn."""
        const_col = ConstantColumn(name="const_str", value="hello")
        calc_col = const_col.to_calculated_column()

        assert calc_col.name == "const_str"
        assert calc_col.expression == "'hello'"
        assert calc_col.data_type is None
        assert calc_col.dependencies == []

    def test_constant_column_to_calculated_column_numeric(self):
        """Test converting numeric ConstantColumn to CalculatedColumn."""
        const_col = ConstantColumn(name="const_num", value=42, data_type=pa.int64())
        calc_col = const_col.to_calculated_column()

        assert calc_col.name == "const_num"
        assert calc_col.expression == "42"
        assert calc_col.data_type == pa.int64()
        assert calc_col.dependencies == []

    def test_constant_column_to_calculated_column_boolean(self):
        """Test converting boolean ConstantColumn to CalculatedColumn."""
        const_col = ConstantColumn(name="const_bool", value=True, data_type=pa.bool_())
        calc_col = const_col.to_calculated_column()

        assert calc_col.name == "const_bool"
        assert calc_col.expression == "True"
        assert calc_col.data_type == pa.bool_()
        assert calc_col.dependencies == []


class TestExpressionColumn:
    """Test cases for ExpressionColumn dataclass."""

    def test_expression_column_init_defaults(self):
        """Test ExpressionColumn initialization with defaults."""
        col = ExpressionColumn(name="expr_col", expression="x + y")
        assert col.name == "expr_col"
        assert col.expression == "x + y"
        assert col.data_type == pa.string()
        assert col.description is None
        assert col.dependencies == []

    def test_expression_column_init_with_values(self):
        """Test ExpressionColumn initialization with explicit values."""
        col = ExpressionColumn(
            name="expr_col",
            expression="x + y",
            data_type=pa.int64(),
            description="Test expression",
            dependencies=["x", "y"]
        )
        assert col.name == "expr_col"
        assert col.expression == "x + y"
        assert col.data_type == pa.int64()
        assert col.description == "Test expression"
        assert col.dependencies == ["x", "y"]

    def test_expression_column_post_init(self):
        """Test ExpressionColumn __post_init__ method."""
        # Test with None dependencies
        col = ExpressionColumn(name="test", expression="x", dependencies=None)
        assert col.dependencies == []

        # Test with None data_type
        col = ExpressionColumn(name="test", expression="x", data_type=None)
        assert col.data_type == pa.string()

    def test_expression_column_to_calculated_column(self):
        """Test converting ExpressionColumn to CalculatedColumn."""
        expr_col = ExpressionColumn(
            name="expr_test",
            expression="add(x, y)",
            data_type=pa.int64(),
            description="Test expression",
            dependencies=["x", "y"]
        )
        calc_col = expr_col.to_calculated_column()

        assert calc_col.name == "expr_test"
        assert calc_col.expression == "add(x, y)"
        assert calc_col.data_type == pa.int64()
        assert calc_col.description == "Test expression"
        assert calc_col.dependencies == ["x", "y"]
