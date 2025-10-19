"""Comprehensive demonstration of data validation with bad rows handling."""

import pyarrow as pa
from datetime import datetime, date
import json
import os

from src.forklift.processors.data_validation import (
    DataValidationProcessor,
    ValidationConfig,
    FieldValidationRule,
    BadRowsConfig,
    RangeValidation,
    StringValidation,
    EnumValidation,
    DateValidation,
)
from src.forklift.processors.validation_factory import (
    create_validation_processor_from_schema,
    get_validation_config_from_schema_file,
    create_default_validation_rules,
)
from src.forklift.schema.csv_schema_importer import CsvSchemaImporter


def demo_required_field_validation():
    """Demonstrate required field validation with bad rows handling."""
    print("\n✅ REQUIRED FIELD VALIDATION DEMO")
    print("=" * 50)

    # Create test data with some missing required fields
    test_data = {
        "id": [1, 2, None, 4, 5],  # Missing required ID for row 3
        "name": [
            "John Doe",
            "",
            "Bob Wilson",
            "Alice Johnson",
            "Charlie Brown",
        ],  # Empty name for row 2
        "email": [
            "john@company.com",
            "jane@company.com",
            "bob@company.com",
            None,
            "charlie@company.com",
        ],  # Missing email for row 4
        "age": [25, 30, 35, 28, 42],
    }

    arrays = [pa.array(values) for values in test_data.values()]
    schema = pa.schema(
        [pa.field(name, array.type) for name, array in zip(test_data.keys(), arrays)]
    )
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

    print(f"Original data: {len(batch)} rows")
    print("Sample data with missing required fields:")
    for i in range(len(batch)):
        row = {col: batch.column(col)[i].as_py() for col in batch.schema.names}
        print(f"  Row {i+1}: {row}")

    # Configure validation rules
    validation_rules = [
        FieldValidationRule(
            field_name="id",
            required=True,
            unique=True,
            range_validation=RangeValidation(min_value=1, max_value=999999),
        ),
        FieldValidationRule(
            field_name="name",
            required=True,
            string_validation=StringValidation(min_length=1, allow_empty=False),
        ),
        FieldValidationRule(
            field_name="email",
            required=True,
            unique=True,
            string_validation=StringValidation(
                pattern=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
            ),
        ),
    ]

    config = ValidationConfig(
        field_validations=validation_rules,
        bad_rows_config=BadRowsConfig(enabled=True, include_validation_errors=True),
        uniqueness_strategy="first_wins",
    )

    processor = DataValidationProcessor(config)
    clean_batch, validation_results = processor.process_batch(batch)

    print(f"\n✅ Validation Results:")
    print(f"Clean rows: {len(clean_batch)}")
    print(f"Bad rows: {len(processor.bad_rows)}")
    print(f"Validation errors: {len(validation_results)}")

    print("\nClean data:")
    for i in range(len(clean_batch)):
        row = {col: clean_batch.column(col)[i].as_py() for col in clean_batch.schema.names}
        print(f"  Row {i+1}: {row}")

    print("\nBad rows with errors:")
    for i, bad_row in enumerate(processor.bad_rows):
        print(f"  Bad Row {i+1}: {bad_row}")

    return processor


def demo_uniqueness_validation():
    """Demonstrate uniqueness validation with different strategies."""
    print("\n🔄 UNIQUENESS VALIDATION DEMO")
    print("=" * 50)

    # Create test data with duplicate values
    test_data = {
        "id": [1, 2, 1, 4, 2],  # Duplicates: ID 1 and 2 appear twice
        "email": [
            "john@company.com",
            "jane@company.com",
            "different@company.com",
            "alice@company.com",
            "john@company.com",
        ],  # Duplicate email
        "name": ["John Doe", "Jane Smith", "John Different", "Alice Johnson", "John Duplicate"],
        "department": ["IT", "HR", "IT", "Finance", "IT"],  # Non-unique field (allowed)
    }

    arrays = [pa.array(values) for values in test_data.values()]
    schema = pa.schema(
        [pa.field(name, array.type) for name, array in zip(test_data.keys(), arrays)]
    )
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

    print(f"Original data: {len(batch)} rows")
    print("Data with duplicate IDs and emails:")
    for i in range(len(batch)):
        row = {col: batch.column(col)[i].as_py() for col in batch.schema.names}
        print(f"  Row {i+1}: {row}")

    # Configure uniqueness validation
    validation_rules = [
        FieldValidationRule(field_name="id", required=True, unique=True),
        FieldValidationRule(field_name="email", required=True, unique=True),
        FieldValidationRule(
            field_name="department", required=False, unique=False  # Allowed to have duplicates
        ),
    ]

    config = ValidationConfig(
        field_validations=validation_rules,
        bad_rows_config=BadRowsConfig(enabled=True, include_validation_errors=True),
        uniqueness_strategy="first_wins",  # First occurrence wins, later duplicates go to bad rows
    )

    processor = DataValidationProcessor(config)
    clean_batch, validation_results = processor.process_batch(batch)

    print(f"\n🔄 Uniqueness Validation Results (first_wins strategy):")
    print(f"Clean rows: {len(clean_batch)}")
    print(f"Bad rows: {len(processor.bad_rows)}")

    print("\nClean data (unique values only):")
    for i in range(len(clean_batch)):
        row = {col: clean_batch.column(col)[i].as_py() for col in clean_batch.schema.names}
        print(f"  Row {i+1}: {row}")

    print("\nBad rows (duplicates):")
    for i, bad_row in enumerate(processor.bad_rows):
        print(f"  Bad Row {i+1}: {bad_row}")

    return processor


def demo_range_validation():
    """Demonstrate range validation for numeric and date fields."""
    print("\n📊 RANGE VALIDATION DEMO")
    print("=" * 50)

    # Create test data with values outside acceptable ranges
    test_data = {
        "id": [1, 2, 3, 4, 5],
        "age": [-5, 25, 200, 30, 35],  # Invalid: -5 (negative), 200 (too high)
        "salary": [
            50000,
            -10000,
            75000,
            15000000,
            60000,
        ],  # Invalid: -10000 (negative), 15000000 (too high)
        "score": [85.5, 105.2, 67.8, -15.0, 92.1],  # Invalid: 105.2 (>100), -15.0 (negative)
        "birth_date": [
            "1990-01-01",
            "1850-12-25",
            "2020-06-15",
            "2150-03-10",
            "1985-07-22",
        ],  # Invalid: 1850 (too old), 2150 (future)
    }

    arrays = [pa.array(values) for values in test_data.values()]
    schema = pa.schema(
        [pa.field(name, array.type) for name, array in zip(test_data.keys(), arrays)]
    )
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

    print(f"Original data: {len(batch)} rows")
    print("Data with values outside acceptable ranges:")
    for i in range(len(batch)):
        row = {col: batch.column(col)[i].as_py() for col in batch.schema.names}
        print(f"  Row {i+1}: {row}")

    # Configure range validation
    validation_rules = [
        FieldValidationRule(
            field_name="id",
            required=True,
            range_validation=RangeValidation(min_value=1, max_value=999999),
        ),
        FieldValidationRule(
            field_name="age",
            required=False,
            range_validation=RangeValidation(min_value=0, max_value=150, inclusive=True),
        ),
        FieldValidationRule(
            field_name="salary",
            required=False,
            range_validation=RangeValidation(min_value=0, max_value=10000000, inclusive=True),
        ),
        FieldValidationRule(
            field_name="score",
            required=False,
            range_validation=RangeValidation(min_value=0.0, max_value=100.0, inclusive=True),
        ),
        FieldValidationRule(
            field_name="birth_date",
            required=False,
            date_validation=DateValidation(min_date="1900-01-01", max_date="2100-12-31"),
        ),
    ]

    config = ValidationConfig(
        field_validations=validation_rules,
        bad_rows_config=BadRowsConfig(enabled=True, include_validation_errors=True),
    )

    processor = DataValidationProcessor(config)
    clean_batch, validation_results = processor.process_batch(batch)

    print(f"\n📊 Range Validation Results:")
    print(f"Clean rows: {len(clean_batch)}")
    print(f"Bad rows: {len(processor.bad_rows)}")

    print("\nClean data (within acceptable ranges):")
    for i in range(len(clean_batch)):
        row = {col: clean_batch.column(col)[i].as_py() for col in clean_batch.schema.names}
        print(f"  Row {i+1}: {row}")

    print("\nBad rows (range violations):")
    for i, bad_row in enumerate(processor.bad_rows):
        print(f"  Bad Row {i+1}: {bad_row}")

    return processor


def demo_comprehensive_validation():
    """Demonstrate comprehensive validation with multiple rule types."""
    print("\n🎯 COMPREHENSIVE VALIDATION DEMO")
    print("=" * 50)

    # Create test data with various validation issues
    test_data = {
        "employee_id": [1, 2, None, 4, 1],  # Missing required, duplicate
        "name": ["John Doe", "", "Bob Wilson", "Alice Johnson", "Charlie Brown"],  # Empty required
        "email": [
            "john@company.com",
            "invalid-email",
            "bob@company.com",
            "alice@company.com",
            "john@company.com",
        ],  # Invalid format, duplicate
        "age": [25, 200, 35, -5, 42],  # Out of range
        "salary": [50000, 75000, -10000, 60000, 15000000],  # Out of range
        "department": ["IT", "HR", "INVALID", "Finance", "IT"],  # Invalid enum value
        "status": ["active", "active", "inactive", "active", "pending"],  # Some valid enum
    }

    arrays = [pa.array(values) for values in test_data.values()]
    schema = pa.schema(
        [pa.field(name, array.type) for name, array in zip(test_data.keys(), arrays)]
    )
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

    print(f"Original data: {len(batch)} rows")
    print("Data with multiple validation issues:")
    for i in range(len(batch)):
        row = {col: batch.column(col)[i].as_py() for col in batch.schema.names}
        print(f"  Row {i+1}: {row}")

    # Configure comprehensive validation
    validation_rules = [
        FieldValidationRule(
            field_name="employee_id",
            required=True,
            unique=True,
            range_validation=RangeValidation(min_value=1, max_value=999999),
        ),
        FieldValidationRule(
            field_name="name",
            required=True,
            string_validation=StringValidation(min_length=1, allow_empty=False),
        ),
        FieldValidationRule(
            field_name="email",
            required=True,
            unique=True,
            string_validation=StringValidation(
                pattern=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", max_length=254
            ),
        ),
        FieldValidationRule(
            field_name="age",
            required=False,
            range_validation=RangeValidation(min_value=0, max_value=150),
        ),
        FieldValidationRule(
            field_name="salary",
            required=False,
            range_validation=RangeValidation(min_value=0, max_value=10000000),
        ),
        FieldValidationRule(
            field_name="department",
            required=True,
            enum_validation=EnumValidation(
                allowed_values=["IT", "HR", "Finance", "Marketing", "Operations"],
                case_sensitive=True,
            ),
        ),
        FieldValidationRule(
            field_name="status",
            required=True,
            enum_validation=EnumValidation(
                allowed_values=["active", "inactive", "pending"], case_sensitive=True
            ),
        ),
    ]

    config = ValidationConfig(
        field_validations=validation_rules,
        bad_rows_config=BadRowsConfig(
            enabled=True,
            include_validation_errors=True,
            max_bad_rows_percent=80.0,  # Allow high percentage for demo
        ),
        uniqueness_strategy="first_wins",
    )

    processor = DataValidationProcessor(config)
    clean_batch, validation_results = processor.process_batch(batch)

    print(f"\n🎯 Comprehensive Validation Results:")
    print(f"Clean rows: {len(clean_batch)}")
    print(f"Bad rows: {len(processor.bad_rows)}")
    print(f"Total validation errors: {len(validation_results)}")

    summary = processor.get_validation_summary()
    print(f"\nValidation Summary:")
    print(f"  Total rows processed: {summary['total_rows_processed']}")
    print(f"  Bad rows percentage: {summary['bad_rows_percent']:.1f}%")
    print(f"  Unique fields tracked: {summary['unique_fields_tracked']}")

    if len(clean_batch) > 0:
        print("\nClean data (passed all validations):")
        for i in range(len(clean_batch)):
            row = {col: clean_batch.column(col)[i].as_py() for col in clean_batch.schema.names}
            print(f"  Row {i+1}: {row}")
    else:
        print("\nNo clean rows (all rows had validation errors)")

    print("\nBad rows with detailed errors:")
    for i, bad_row in enumerate(processor.bad_rows):
        print(f"  Bad Row {i+1}:")
        print(
            f"    Data: {{{', '.join(f'{k}: {v}' for k, v in bad_row.items() if not k.startswith('_'))}}}"
        )
        print(f"    Errors: {bad_row.get('_validation_errors', 'No errors recorded')}")
        print(f"    Error Count: {bad_row.get('_error_count', 0)}")

    return processor


def demo_schema_driven_validation():
    """Demonstrate validation using schema configuration."""
    print("\n📋 SCHEMA-DRIVEN VALIDATION DEMO")
    print("=" * 50)

    # Load validation configuration from updated schema
    schema_path = "/Users/matt/PycharmProjects/forklift/schema-standards/20250826-csv.json"

    try:
        validation_config = get_validation_config_from_schema_file(schema_path)
        if validation_config:
            print("✅ Loaded validation configuration from schema:")
            print(
                f"  Bad rows handling enabled: {validation_config.get('badRowsHandling', {}).get('enabled', False)}"
            )
            print(
                f"  Field validations defined: {len(validation_config.get('fieldValidations', {}))}"
            )
            print(
                f"  Uniqueness strategy: {validation_config.get('uniquenessHandling', {}).get('strategy', 'not specified')}"
            )

            # Create processor from schema
            processor = create_validation_processor_from_schema(validation_config)

            if processor:
                print("✅ Successfully created validation processor from schema configuration")

                # Test with sample data
                test_data = {
                    "id": [1, 2, 1, 4],  # Duplicate ID
                    "name": ["John Doe", "", "Bob Wilson", "Alice Johnson"],  # Empty name
                    "age": [25, 200, 35, 28],  # Age out of range
                    "salary": [50000, 75000, -10000, 60000],  # Negative salary
                    "email": [
                        "john@company.com",
                        "invalid",
                        "bob@company.com",
                        "alice@company.com",
                    ],  # Invalid email format
                    "category": ["A", "B", "INVALID", "C"],  # Invalid enum
                }

                arrays = [pa.array(values) for values in test_data.values()]
                schema = pa.schema(
                    [pa.field(name, array.type) for name, array in zip(test_data.keys(), arrays)]
                )
                batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

                clean_batch, validation_results = processor.process_batch(batch)

                print(f"\nSchema-based validation results:")
                print(f"  Original rows: {len(batch)}")
                print(f"  Clean rows: {len(clean_batch)}")
                print(f"  Bad rows: {len(processor.bad_rows)}")
                print(f"  Validation errors: {len(validation_results)}")

                return processor
            else:
                print("❌ Failed to create processor from schema")
        else:
            print("❌ No validation configuration found in schema")

    except Exception as e:
        print(f"❌ Error loading schema validation: {e}")

    return None


def main():
    """Run all validation demonstrations."""
    print("🚀 COMPREHENSIVE DATA VALIDATION DEMONSTRATION")
    print("=" * 70)
    print("Demonstrating required, unique, and range validation with bad rows handling")

    try:
        # Run all demonstrations
        demo_required_field_validation()
        demo_uniqueness_validation()
        demo_range_validation()
        demo_comprehensive_validation()
        demo_schema_driven_validation()

        print("\n" + "=" * 70)
        print("🎉 ALL VALIDATION DEMOS COMPLETED SUCCESSFULLY!")

        print("\nKey Features Demonstrated:")
        print("✅ Required Field Validation - Null/empty values routed to bad rows")
        print("✅ Uniqueness Validation - Duplicate values handled with configurable strategies")
        print("✅ Range Validation - Min/max constraints for numeric and date fields")
        print("��� String Validation - Length and pattern matching constraints")
        print("✅ Enum Validation - Allowed values with case sensitivity options")
        print("✅ Bad Rows Handling - Violations captured with detailed error messages")
        print("✅ Schema-Driven Configuration - Validation rules defined in JSON schema")
        print("✅ Multiple Strategies - Configurable handling for uniqueness violations")
        print("✅ Comprehensive Reporting - Validation summaries and error tracking")

    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
