"""Test calculated columns functionality with constants, expressions, and computed fields."""

import pytest
import pyarrow as pa
from datetime import datetime, date

from src.forklift.processors.calculated_columns import (
    CalculatedColumnsProcessor,
    CalculatedColumnsConfig,
    ConstantColumn,
    ExpressionColumn,
    CalculatedColumn
)
from src.forklift.processors.calculated_columns_factory import create_calculated_columns_processor_from_schema
from src.forklift.schema.csv_schema_importer import CsvSchemaImporter


def test_constant_columns():
    """Test adding constant columns for partitioning."""
    print("\n🔢 CONSTANT COLUMNS TEST")
    print("-" * 40)

    # Create test data
    test_data = {
        "id": [1, 2, 3],
        "name": ["John", "Jane", "Bob"],
        "age": [25, 30, 35]
    }

    arrays = [pa.array(values) for values in test_data.values()]
    schema = pa.schema([pa.field(name, array.type) for name, array in zip(test_data.keys(), arrays)])
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

    print(f"Original columns: {batch.schema.names}")

    # Configure constant columns
    config = CalculatedColumnsConfig(
        constants=[
            ConstantColumn(name="data_source", value="test_data", data_type=pa.string()),
            ConstantColumn(name="load_date", value="2024-08-26", data_type=pa.string()),
            ConstantColumn(name="version", value=1, data_type=pa.int32())
        ],
        partition_columns=["data_source", "load_date"]
    )

    processor = CalculatedColumnsProcessor(config)
    result_batch, validation_results = processor.process_batch(batch)

    print(f"After adding constants: {result_batch.schema.names}")
    print(f"Partition columns: {processor.get_partition_columns()}")

    # Verify results
    assert len(result_batch.schema.names) == 6  # 3 original + 3 constants
    assert "data_source" in result_batch.schema.names
    assert "load_date" in result_batch.schema.names
    assert "version" in result_batch.schema.names

    # Check constant values
    assert result_batch.column("data_source").to_pylist() == ["test_data", "test_data", "test_data"]
    assert result_batch.column("load_date").to_pylist() == ["2024-08-26", "2024-08-26", "2024-08-26"]
    assert result_batch.column("version").to_pylist() == [1, 1, 1]

    assert len(validation_results) == 0  # No errors
    print("✅ Constant columns test passed!")


def test_expression_columns():
    """Test expression-based calculated columns."""
    print("\n🧮 EXPRESSION COLUMNS TEST")
    print("-" * 40)

    # Create test data
    test_data = {
        "first_name": ["John", "Jane", "Bob"],
        "last_name": ["Doe", "Smith", "Wilson"],
        "age": [17, 30, 70],
        "salary": [30000, 75000, 120000]
    }

    arrays = [pa.array(values) for values in test_data.values()]
    schema = pa.schema([pa.field(name, array.type) for name, array in zip(test_data.keys(), arrays)])
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

    print(f"Original columns: {batch.schema.names}")

    # Configure expression columns
    config = CalculatedColumnsConfig(
        expressions=[
            ExpressionColumn(
                name="full_name",
                expression="first_name + ' ' + last_name",
                dependencies=["first_name", "last_name"]
            ),
            ExpressionColumn(
                name="age_category",
                expression="CASE WHEN age < 18 THEN 'minor' WHEN age < 65 THEN 'adult' ELSE 'senior' END",
                dependencies=["age"]
            )
        ]
    )

    processor = CalculatedColumnsProcessor(config)
    result_batch, validation_results = processor.process_batch(batch)

    print(f"After adding expressions: {result_batch.schema.names}")

    # Verify results
    assert len(result_batch.schema.names) == 6  # 4 original + 2 expressions
    assert "full_name" in result_batch.schema.names
    assert "age_category" in result_batch.schema.names

    # Check expression values
    full_names = result_batch.column("full_name").to_pylist()
    age_categories = result_batch.column("age_category").to_pylist()

    print(f"Full names: {full_names}")
    print(f"Age categories: {age_categories}")

    assert full_names == ["John Doe", "Jane Smith", "Bob Wilson"]
    assert age_categories == ["minor", "adult", "senior"]

    assert len(validation_results) == 0  # No errors
    print("✅ Expression columns test passed!")


def test_calculated_columns():
    """Test calculated columns using functions."""
    print("\n⚙️ CALCULATED COLUMNS TEST")
    print("-" * 40)

    # Create test data
    test_data = {
        "first_name": ["John", "Jane", "Bob"],
        "last_name": ["Doe", "Smith", "Wilson"],
        "created_timestamp": [
            datetime(2020, 1, 1),
            datetime(2021, 6, 15),
            datetime(2022, 12, 31)
        ]
    }

    arrays = [pa.array(values) for values in test_data.values()]
    schema = pa.schema([pa.field(name, array.type) for name, array in zip(test_data.keys(), arrays)])
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

    print(f"Original columns: {batch.schema.names}")

    # Configure calculated columns
    config = CalculatedColumnsConfig(
        calculated=[
            CalculatedColumn(
                name="full_name",
                function="full_name",
                dependencies=["first_name", "last_name"]
            ),
            CalculatedColumn(
                name="name_length",
                function="string_length",
                dependencies=["first_name"]
            )
        ]
    )

    processor = CalculatedColumnsProcessor(config)
    result_batch, validation_results = processor.process_batch(batch)

    print(f"After adding calculated columns: {result_batch.schema.names}")

    # Verify results
    assert len(result_batch.schema.names) == 5  # 3 original + 2 calculated
    assert "full_name" in result_batch.schema.names
    assert "name_length" in result_batch.schema.names

    # Check calculated values
    full_names = result_batch.column("full_name").to_pylist()
    name_lengths = result_batch.column("name_length").to_pylist()

    print(f"Full names: {full_names}")
    print(f"Name lengths: {name_lengths}")

    assert full_names == ["John Doe", "Jane Smith", "Bob Wilson"]

    assert len(validation_results) == 0  # No errors
    print("✅ Calculated columns test passed!")


def test_schema_integration():
    """Test integration with schema standards."""
    print("\n📄 SCHEMA INTEGRATION TEST")
    print("-" * 40)

    # Load the updated schema with calculated columns
    schema_path = "/Users/matt/PycharmProjects/forklift/schema-standards/20250826-csv.json"
    importer = CsvSchemaImporter(schema_path)

    # Check if calculated columns are present
    assert importer.has_calculated_columns()

    calc_config = importer.get_calculated_columns_config()
    assert calc_config is not None

    print(f"Constants defined: {len(calc_config.get('constants', []))}")
    print(f"Expressions defined: {len(calc_config.get('expressions', []))}")
    print(f"Calculated columns defined: {len(calc_config.get('calculated', []))}")

    # Get partition columns
    partition_columns = importer.get_partition_columns()
    print(f"Partition columns: {partition_columns}")

    assert "data_source" in partition_columns
    assert "load_date" in partition_columns

    # Create processor from schema
    processor = create_calculated_columns_processor_from_schema(calc_config)
    assert processor is not None

    # Test with sample data
    test_data = {
        "id": [1, 2, 3],
        "name": ["John", "Jane", "Bob"],
        "age": [17, 30, 70],
        "salary": [30000, 75000, 120000],
        "created_timestamp": [
            datetime(2020, 1, 1),
            datetime(2021, 6, 15),
            datetime(2022, 12, 31)
        ]
    }

    arrays = [pa.array(values) for values in test_data.values()]
    schema = pa.schema([pa.field(name, array.type) for name, array in zip(test_data.keys(), arrays)])
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

    result_batch, validation_results = processor.process_batch(batch)

    print(f"Original columns: {batch.schema.names}")
    print(f"After processing: {result_batch.schema.names}")

    # Verify constants were added
    assert "data_source" in result_batch.schema.names
    assert "load_date" in result_batch.schema.names
    assert "processing_version" in result_batch.schema.names

    # Verify expressions were added
    assert "age_category" in result_batch.schema.names
    assert "salary_tier" in result_batch.schema.names

    # Check some values
    data_source_values = result_batch.column("data_source").to_pylist()
    age_categories = result_batch.column("age_category").to_pylist()

    print(f"Data source values: {data_source_values}")
    print(f"Age categories: {age_categories}")

    assert all(val == "census_2020" for val in data_source_values)
    assert age_categories == ["minor", "adult", "senior"]

    assert len(validation_results) == 0  # No errors
    print("✅ Schema integration test passed!")


def test_missing_dependencies():
    """Test error handling for missing dependencies."""
    print("\n❌ MISSING DEPENDENCIES TEST")
    print("-" * 40)

    # Create test data without required columns
    test_data = {
        "id": [1, 2, 3],
        "name": ["John", "Jane", "Bob"]
    }

    arrays = [pa.array(values) for values in test_data.values()]
    schema = pa.schema([pa.field(name, array.type) for name, array in zip(test_data.keys(), arrays)])
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

    # Configure expression that depends on missing column
    config = CalculatedColumnsConfig(
        expressions=[
            ExpressionColumn(
                name="age_category",
                expression="CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END",
                dependencies=["age"]  # 'age' column doesn't exist
            )
        ]
    )

    processor = CalculatedColumnsProcessor(config)
    result_batch, validation_results = processor.process_batch(batch)

    # Should have validation errors
    assert len(validation_results) > 0
    assert not validation_results[0].is_valid
    assert "missing columns" in validation_results[0].error_message.lower()

    print(f"Expected error caught: {validation_results[0].error_message}")
    print("✅ Missing dependencies test passed!")


if __name__ == "__main__":
    test_constant_columns()
    test_expression_columns()
    test_calculated_columns()
    test_schema_integration()
    test_missing_dependencies()
    print("\n🎉 All calculated columns tests passed!")
