#!/usr/bin/env python3
"""
Comprehensive demonstration of constraint validation and bad rows handling functionality.

This script demonstrates how the new constraint validation and bad rows handling
features work together to process data files according to schema standards.
"""

import sys
import tempfile
import json
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, "src")

import pyarrow as pa
from forklift.processors.constraint_validator import (
    ConstraintValidator,
    ConstraintConfig,
    ConstraintErrorMode,
    create_constraint_config_from_schema,
)
from forklift.processors.bad_rows_handler import BadRowsHandler, BadRowsConfig
from forklift.processors.enhanced_processor import EnhancedDataProcessor


def test_primary_key_constraint_validation():
    """Test primary key constraint validation with duplicates."""
    print("=" * 60)
    print("Testing Primary Key Constraint Validation")
    print("=" * 60)

    # Configure constraint validation
    config = ConstraintConfig(
        primary_key_columns=["id"],
        enforce_uniqueness=True,
        allow_nulls_in_pk=False,
        error_mode=ConstraintErrorMode.BAD_ROWS,
    )

    validator = ConstraintValidator(config)

    # Create test data with primary key issues
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=True),
            pa.field("name", pa.string()),
            pa.field("email", pa.string()),
        ]
    )

    # Test data with duplicate ids and null id
    batch = pa.record_batch(
        [
            [1, 2, 1, None, 4],  # Duplicate id=1, null id
            ["Alice", "Bob", "Charlie", "David", "Eve"],
            [
                "alice@test.com",
                "bob@test.com",
                "charlie@test.com",
                "david@test.com",
                "eve@test.com",
            ],
        ],
        schema=schema,
    )

    print(f"Original batch: {batch.num_rows} rows")

    # Process the batch
    valid_batch, validation_results = validator.process_batch(batch)

    print(f"Valid rows after constraint validation: {valid_batch.num_rows}")
    print(f"Validation errors: {len([r for r in validation_results if not r.is_valid])}")

    # Show violations
    violations = validator.get_all_violations()
    print(f"Constraint violations: {len(violations)}")

    for i, violation in enumerate(violations):
        print(f"  Violation {i+1}: {violation.violation_type} - {violation.error_message}")

    return violations


def test_unique_constraints():
    """Test unique constraint validation."""
    print("\n" + "=" * 60)
    print("Testing Unique Constraint Validation")
    print("=" * 60)

    config = ConstraintConfig(
        unique_constraints=[["name", "email"]], error_mode=ConstraintErrorMode.BAD_ROWS
    )

    validator = ConstraintValidator(config)

    schema = pa.schema(
        [pa.field("id", pa.int64()), pa.field("name", pa.string()), pa.field("email", pa.string())]
    )

    # Test data with duplicate name+email combinations
    batch = pa.record_batch(
        [
            [1, 2, 3, 4],
            ["Alice", "Bob", "Alice", "Charlie"],  # Alice appears twice
            ["alice@test.com", "bob@test.com", "alice@test.com", "charlie@test.com"],  # Same email
        ],
        schema=schema,
    )

    print(f"Original batch: {batch.num_rows} rows")

    valid_batch, validation_results = validator.process_batch(batch)

    print(f"Valid rows after unique constraint validation: {valid_batch.num_rows}")
    print(f"Validation errors: {len([r for r in validation_results if not r.is_valid])}")

    violations = validator.get_all_violations()
    for violation in violations:
        print(f"  Unique violation: {violation.error_message}")


def test_bad_rows_handler():
    """Test bad rows collection and output."""
    print("\n" + "=" * 60)
    print("Testing Bad Rows Handler")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmp_dir:
        bad_rows_config = BadRowsConfig(
            output_path=Path(tmp_dir) / "bad_rows.parquet",
            output_format="parquet",
            include_original_data=True,
            include_error_details=True,
            create_summary=True,
        )

        handler = BadRowsHandler(bad_rows_config)

        # Add some bad rows
        from forklift.processors.base import ValidationResult

        handler.add_bad_row(
            row_data={"id": 1, "name": "Alice", "age": -5},
            row_index=0,
            validation_results=[
                ValidationResult(
                    is_valid=False,
                    error_message="Age cannot be negative",
                    error_code="INVALID_AGE",
                    row_index=0,
                    column_name="age",
                )
            ],
        )

        handler.add_bad_row(
            row_data={"id": None, "name": "Bob", "age": 25},
            row_index=1,
            validation_results=[
                ValidationResult(
                    is_valid=False,
                    error_message="ID cannot be null",
                    error_code="NULL_ID",
                    row_index=1,
                    column_name="id",
                )
            ],
        )

        print(f"Bad rows collected: {handler.get_bad_row_count()}")

        # Write bad rows to file
        output_path = handler.write_bad_rows()
        print(f"Bad rows written to: {output_path}")

        # Check if files exist
        if output_path and output_path.exists():
            print(f"Bad rows file size: {output_path.stat().st_size} bytes")

            summary_path = output_path.with_suffix(".summary.json")
            if summary_path.exists():
                with open(summary_path) as f:
                    summary = json.load(f)
                print(
                    f"Summary: {summary['bad_rows_count']} bad rows, {summary['bad_rows_percentage']:.1f}% of total"
                )


def test_schema_based_configuration():
    """Test creating constraint configuration from schema dictionary."""
    print("\n" + "=" * 60)
    print("Testing Schema-Based Configuration")
    print("=" * 60)

    # Use the schema structure from the CSV schema standard
    schema_dict = {
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "email": {"type": "string", "nullable": False},
        },
        "required": ["id", "name"],
        "x-primaryKey": {"columns": ["id"], "enforceUniqueness": True, "allowNulls": False},
        "x-uniqueConstraints": [{"columns": ["email"]}],
        "x-constraintHandling": {"errorMode": "bad_rows"},
    }

    config = create_constraint_config_from_schema(schema_dict)

    print(f"Primary key columns: {config.primary_key_columns}")
    print(f"Enforce uniqueness: {config.enforce_uniqueness}")
    print(f"Allow nulls in PK: {config.allow_nulls_in_pk}")
    print(f"Not null columns: {config.not_null_columns}")
    print(f"Error mode: {config.error_mode.value}")

    if config.unique_constraints:
        print(f"Unique constraints: {len(config.unique_constraints)} defined")


def test_enhanced_processor_integration():
    """Test the enhanced processor that combines all functionality."""
    print("\n" + "=" * 60)
    print("Testing Enhanced Data Processor Integration")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmp_dir:
        # Create Arrow schema
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("email", pa.string()),
            ]
        )

        # Schema dictionary with constraints
        schema_dict = {
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "email": {"type": "string"},
            },
            "required": ["id", "name"],
            "x-primaryKey": {"columns": ["id"], "enforceUniqueness": True, "allowNulls": False},
            "x-constraintHandling": {"errorMode": "bad_rows"},
        }

        # Configure bad rows
        bad_rows_config = BadRowsConfig(
            output_path=Path(tmp_dir) / "enhanced_bad_rows.parquet", create_summary=True
        )

        processor = EnhancedDataProcessor(
            schema=schema, schema_dict=schema_dict, bad_rows_config=bad_rows_config
        )

        # Test data with various issues
        batch = pa.record_batch(
            [
                [1, 2, 1, None, 4],  # Duplicate id=1, null id
                ["Alice", "Bob", "Charlie", "David", "Eve"],
                [
                    "alice@test.com",
                    "bob@test.com",
                    "charlie@test.com",
                    "david@test.com",
                    "eve@test.com",
                ],
            ],
            schema=schema,
        )

        print(f"Processing batch with {batch.num_rows} rows...")

        # Process batch
        valid_batch, validation_results = processor.process_batch(batch)

        print(f"Valid rows after processing: {valid_batch.num_rows}")
        print(f"Validation issues: {len([r for r in validation_results if not r.is_valid])}")

        # Finalize processing
        results = processor.finalize()

        print(f"Has bad rows: {results['has_bad_rows']}")
        if results.get("bad_rows_file"):
            print(f"Bad rows file: {results['bad_rows_file']}")

        # Show constraint violations summary
        summary = processor.get_constraint_violations_summary()
        print(f"Total constraint violations: {summary['total_violations']}")
        for vtype, count in summary["violation_types"].items():
            print(f"  {vtype}: {count}")


def demonstrate_error_modes():
    """Demonstrate different error handling modes."""
    print("\n" + "=" * 60)
    print("Testing Different Error Handling Modes")
    print("=" * 60)

    schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])

    batch = pa.record_batch(
        [[1, 1, 2], ["Alice", "Bob", "Charlie"]], schema=schema  # Duplicate id=1
    )

    # Test BAD_ROWS mode
    print("\n1. BAD_ROWS mode:")
    config = ConstraintConfig(primary_key_columns=["id"], error_mode=ConstraintErrorMode.BAD_ROWS)
    validator = ConstraintValidator(config)
    valid_batch, _ = validator.process_batch(batch)
    print(f"   Continues processing: {valid_batch.num_rows} valid rows returned")

    # Test FAIL_COMPLETE mode
    print("\n2. FAIL_COMPLETE mode:")
    config = ConstraintConfig(
        primary_key_columns=["id"], error_mode=ConstraintErrorMode.FAIL_COMPLETE
    )
    validator = ConstraintValidator(config)
    valid_batch, _ = validator.process_batch(batch)  # Should not fail yet
    print(f"   Processes all data: {valid_batch.num_rows} rows")

    try:
        validator.finalize()  # Should fail here
        print("   Unexpected: finalize() should have failed")
    except Exception as e:
        print(f"   Fails on finalize(): {type(e).__name__}")

    # Test FAIL_FAST mode
    print("\n3. FAIL_FAST mode:")
    config = ConstraintConfig(primary_key_columns=["id"], error_mode=ConstraintErrorMode.FAIL_FAST)
    validator = ConstraintValidator(config)

    try:
        validator.process_batch(batch)
        print("   Unexpected: process_batch() should have failed")
    except Exception as e:
        print(f"   Fails immediately: {type(e).__name__}")


if __name__ == "__main__":
    print("Constraint Validation and Bad Rows Handling Demonstration")
    print("=" * 60)

    try:
        test_primary_key_constraint_validation()
        test_unique_constraints()
        test_bad_rows_handler()
        test_schema_based_configuration()
        test_enhanced_processor_integration()
        demonstrate_error_modes()

        print("\n" + "=" * 60)
        print("All tests completed successfully!")
        print("=" * 60)

    except Exception as e:
        print(f"\nError during testing: {e}")
        import traceback

        traceback.print_exc()
