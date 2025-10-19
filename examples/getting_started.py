"""Getting Started with Forklift: Basic CSV Processing with Schema Validation

This example demonstrates the fundamental usage of Forklift for processing CSV files
with schema validation. It covers:

1. Basic CSV reading and processing
2. Schema validation with all columns defined
3. Passthrough mode for processing subsets of columns
4. Different validation modes (STRICT, PERMISSIVE)
5. Handling excess columns in the data

Run this example to see how Forklift handles CSV data validation and processing.
"""

import tempfile
import json
from pathlib import Path
import pyarrow as pa
import pandas as pd

import forklift as fl
from forklift.engine.config.enums import ExcessColumnMode
from forklift.processors.schema_validator import (
    SchemaValidator,
    SchemaValidatorConfig,
    SchemaValidationMode,
    ColumnSchema
)


def create_sample_csv_data():
    """Create sample CSV data for demonstration."""

    # Sample data with various data types
    sample_data = {
        "customer_id": [1, 2, 3, 4, 5],
        "customer_name": ["Alice Johnson", "Bob Smith", "Carol Davis", "David Wilson", "Eve Brown"],
        "email": ["alice@email.com", "bob@email.com", "carol@email.com", "david@email.com", "eve@email.com"],
        "age": [28, 35, 42, 29, 31],
        "registration_date": ["2023-01-15", "2023-02-20", "2023-03-10", "2023-04-05", "2023-05-12"],
        "is_premium": [True, False, True, False, True],
        "total_orders": [12, 8, 25, 3, 18],
        "account_balance": [1250.50, 890.25, 2100.75, 450.00, 1680.90],
        # Extra columns that might not be in schema
        "phone_number": ["555-0101", "555-0102", "555-0103", "555-0104", "555-0105"],
        "address": ["123 Main St", "456 Oak Ave", "789 Pine Rd", "321 Elm St", "654 Birch Ln"]
    }

    # Create temporary CSV file
    temp_dir = Path(tempfile.mkdtemp(prefix="forklift_demo_"))
    csv_path = temp_dir / "sample_customers.csv"

    # Write to CSV
    df = pd.DataFrame(sample_data)
    df.to_csv(csv_path, index=False)

    print(f"📄 Created sample CSV file: {csv_path}")
    print(f"📊 Data shape: {df.shape[0]} rows, {df.shape[1]} columns")
    print(f"📋 Columns: {list(df.columns)}")
    print("\n📝 Sample data preview:")
    print(df.head(3).to_string(index=False))

    return csv_path, temp_dir


def create_complete_schema(temp_dir: Path):
    """Create a complete schema that validates all columns."""

    schema = {
        "name": "Complete Customer Schema",
        "description": "Validates all columns in the customer data",
        "version": "1.0.0",
        "x-csv": {
            "header_row": 0,
            "delimiter": ",",
            "quote_char": "\"",
            "encoding": "utf-8"
        },
        "columns": [
            {
                "name": "customer_id",
                "type": "int32",
                "nullable": False,
                "description": "Unique customer identifier"
            },
            {
                "name": "customer_name",
                "type": "string",
                "nullable": False,
                "description": "Customer full name",
                "constraints": {
                    "minLength": 2,
                    "maxLength": 100
                }
            },
            {
                "name": "email",
                "type": "string",
                "nullable": False,
                "description": "Customer email address",
                "constraints": {
                    "pattern": "^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$"
                }
            },
            {
                "name": "age",
                "type": "int32",
                "nullable": False,
                "description": "Customer age",
                "constraints": {
                    "minimum": 18,
                    "maximum": 120
                }
            },
            {
                "name": "registration_date",
                "type": "date32",
                "nullable": False,
                "description": "Date customer registered"
            },
            {
                "name": "is_premium",
                "type": "bool",
                "nullable": False,
                "description": "Premium customer status"
            },
            {
                "name": "total_orders",
                "type": "int32",
                "nullable": False,
                "description": "Total number of orders",
                "constraints": {
                    "minimum": 0
                }
            },
            {
                "name": "account_balance",
                "type": "double",
                "nullable": False,
                "description": "Current account balance",
                "constraints": {
                    "minimum": 0.0
                }
            },
            {
                "name": "phone_number",
                "type": "string",
                "nullable": True,
                "description": "Customer phone number"
            },
            {
                "name": "address",
                "type": "string",
                "nullable": True,
                "description": "Customer mailing address"
            }
        ]
    }

    schema_path = temp_dir / "complete_schema.json"
    with open(schema_path, 'w') as f:
        json.dump(schema, f, indent=2)

    return schema_path


def create_subset_schema(temp_dir: Path):
    """Create a schema that only validates a subset of columns."""

    schema = {
        "name": "Subset Customer Schema",
        "description": "Validates only core customer columns, allows others to pass through",
        "version": "1.0.0",
        "x-csv": {
            "header_row": 0,
            "delimiter": ",",
            "quote_char": "\"",
            "encoding": "utf-8"
        },
        "columns": [
            {
                "name": "customer_id",
                "type": "int32",
                "nullable": False,
                "description": "Unique customer identifier"
            },
            {
                "name": "customer_name",
                "type": "string",
                "nullable": False,
                "description": "Customer full name"
            },
            {
                "name": "email",
                "type": "string",
                "nullable": False,
                "description": "Customer email address"
            },
            {
                "name": "age",
                "type": "int32",
                "nullable": False,
                "description": "Customer age"
            }
        ]
    }

    schema_path = temp_dir / "subset_schema.json"
    with open(schema_path, 'w') as f:
        json.dump(schema, f, indent=2)

    return schema_path


def demo_basic_csv_processing():
    """Demonstrate basic CSV processing without schema validation."""
    print("\n" + "="*70)
    print("🚀 DEMO 1: Basic CSV Processing (No Schema)")
    print("="*70)

    csv_path, temp_dir = create_sample_csv_data()

    try:
        # Read CSV without schema validation
        print("\n📖 Processing CSV without schema validation...")
        reader = fl.read_csv(csv_path)
        df = reader.as_pandas()

        print(f"✅ Successfully processed {len(df)} rows")
        print(f"📋 Output columns: {list(df.columns)}")
        print(f"📊 Data types inferred:")
        for col, dtype in df.dtypes.items():
            print(f"   {col}: {dtype}")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)


def demo_complete_schema_validation():
    """Demonstrate CSV processing with complete schema validation."""
    print("\n" + "="*70)
    print("🔍 DEMO 2: Complete Schema Validation (All Columns)")
    print("="*70)

    csv_path, temp_dir = create_sample_csv_data()
    complete_schema_path = create_complete_schema(temp_dir)

    try:
        print(f"\n📋 Using complete schema: {complete_schema_path}")
        print("🔒 Schema validates ALL columns in the CSV file")

        # Read CSV with complete schema validation
        reader = fl.read_csv(csv_path, schema_file=complete_schema_path)
        df = reader.as_pandas()

        print(f"✅ Schema validation passed!")
        print(f"📊 Processed {len(df)} rows with {len(df.columns)} validated columns")
        print(f"📋 All columns: {list(df.columns)}")

        # Show sample of processed data
        print("\n📝 Sample processed data:")
        print(df.head(2).to_string(index=False))

    except Exception as e:
        print(f"❌ Schema validation failed: {e}")
    finally:
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)


def demo_subset_schema_with_passthrough():
    """Demonstrate CSV processing with subset schema and passthrough mode."""
    print("\n" + "="*70)
    print("🔄 DEMO 3: Subset Schema with Passthrough Mode")
    print("="*70)

    csv_path, temp_dir = create_sample_csv_data()
    subset_schema_path = create_subset_schema(temp_dir)

    try:
        print(f"\n📋 Using subset schema: {subset_schema_path}")
        print("🔓 Schema validates ONLY core columns (customer_id, name, email, age)")
        print("➡️  Extra columns will be passed through with PASSTHROUGH mode")

        # Read CSV with subset schema and passthrough mode
        reader = fl.read_csv(
            csv_path,
            schema_file=subset_schema_path,
            excess_column_mode="passthrough"  # This enables passthrough mode
        )
        df = reader.as_pandas()

        print(f"✅ Subset schema validation passed!")
        print(f"📊 Processed {len(df)} rows with {len(df.columns)} total columns")

        # Show which columns were validated vs passed through
        with open(subset_schema_path, 'r') as f:
            schema_data = json.load(f)
        validated_columns = [col['name'] for col in schema_data['columns']]
        passthrough_columns = [col for col in df.columns if col not in validated_columns]

        print(f"🔍 Validated columns ({len(validated_columns)}): {validated_columns}")
        print(f"➡️  Passthrough columns ({len(passthrough_columns)}): {passthrough_columns}")

        # Show sample of processed data
        print("\n📝 Sample processed data:")
        print(df.head(2).to_string(index=False))

    except Exception as e:
        print(f"❌ Processing failed: {e}")
    finally:
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)


def demo_schema_validation_modes():
    """Demonstrate different schema validation modes."""
    print("\n" + "="*70)
    print("⚙️  DEMO 4: Schema Validation Modes")
    print("="*70)

    # Create data with some issues for demonstration
    problematic_data = {
        "customer_id": [1, 2, "invalid", 4, 5],  # Invalid type in row 3
        "customer_name": ["Alice", "Bob", "Carol", "", "Eve"],  # Empty name in row 4
        "email": ["alice@email.com", "invalid-email", "carol@email.com", "david@email.com", "eve@email.com"],  # Invalid email
        "age": [28, 35, 150, 29, 31],  # Age out of range (150)
        "extra_column": ["data1", "data2", "data3", "data4", "data5"]  # Column not in schema
    }

    temp_dir = Path(tempfile.mkdtemp(prefix="forklift_validation_demo_"))
    csv_path = temp_dir / "problematic_data.csv"
    df = pd.DataFrame(problematic_data)
    df.to_csv(csv_path, index=False)

    # Create a strict schema
    strict_schema = {
        "name": "Strict Validation Schema",
        "version": "1.0.0",
        "x-csv": {"header_row": 0, "delimiter": ",", "encoding": "utf-8"},
        "columns": [
            {"name": "customer_id", "type": "int32", "nullable": False},
            {"name": "customer_name", "type": "string", "nullable": False, "constraints": {"minLength": 1}},
            {"name": "email", "type": "string", "nullable": False, "constraints": {"pattern": "^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$"}},
            {"name": "age", "type": "int32", "nullable": False, "constraints": {"minimum": 18, "maximum": 120}}
        ]
    }

    schema_path = temp_dir / "strict_schema.json"
    with open(schema_path, 'w') as f:
        json.dump(strict_schema, f, indent=2)

    print(f"📄 Created problematic CSV: {csv_path}")
    print("🚨 Data contains validation issues:")
    print("   - Invalid customer_id type (string instead of int)")
    print("   - Empty customer name")
    print("   - Invalid email format")
    print("   - Age out of range (150)")
    print("   - Extra column not in schema")

    try:
        # Try STRICT mode (should fail)
        print(f"\n🔒 Trying STRICT validation mode...")
        try:
            reader = fl.read_csv(
                csv_path,
                schema_file=schema_path,
                excess_column_mode="reject"  # Strict about extra columns
            )
            df_result = reader.as_pandas()
            print("✅ STRICT mode passed (unexpected)")
        except Exception as e:
            print(f"❌ STRICT mode failed as expected: {str(e)[:100]}...")

        # Try PERMISSIVE mode with passthrough
        print(f"\n🔓 Trying PERMISSIVE mode with passthrough...")
        try:
            reader = fl.read_csv(
                csv_path,
                schema_file=schema_path,
                excess_column_mode="passthrough"  # Allow extra columns
            )
            df_result = reader.as_pandas()
            print(f"✅ PERMISSIVE mode with passthrough processed {len(df_result)} rows")
            print(f"📋 Resulting columns: {list(df_result.columns)}")
        except Exception as e:
            print(f"❌ PERMISSIVE mode failed: {str(e)[:100]}...")

    except Exception as e:
        print(f"❌ Demo setup failed: {e}")
    finally:
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)


def main():
    """Run all getting started demonstrations."""
    print("🎯 FORKLIFT GETTING STARTED GUIDE")
    print("🎯 CSV Processing with Schema Validation")
    print("=" * 70)
    print("This demo shows how to use Forklift for CSV processing with different")
    print("schema validation approaches:")
    print("• Basic processing without schema")
    print("• Complete schema validation (all columns)")
    print("• Subset schema with passthrough mode")
    print("• Different validation modes and error handling")

    # Run all demonstrations
    demo_basic_csv_processing()
    demo_complete_schema_validation()
    demo_subset_schema_with_passthrough()
    demo_schema_validation_modes()

    print("\n" + "="*70)
    print("🎉 GETTING STARTED DEMOS COMPLETE!")
    print("="*70)
    print("Key takeaways:")
    print("• Use fl.read_csv() for basic CSV processing")
    print("• Add schema_file parameter for validation")
    print("• Use excess_column_mode='passthrough' to allow extra columns")
    print("• Complete schemas validate all columns")
    print("• Subset schemas validate only specified columns")
    print("• Passthrough mode preserves non-validated columns")
    print("\nNext steps:")
    print("• Explore other examples for advanced features")
    print("• Check out schema-standards/ for schema format details")
    print("• See docs/ for comprehensive documentation")


if __name__ == "__main__":
    main()
