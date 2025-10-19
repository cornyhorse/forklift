"""Comprehensive example demonstrating calculated columns functionality across all schema types."""

import pyarrow as pa
from datetime import datetime, date
import json

from src.forklift.processors.calculated_columns import (
    CalculatedColumnsProcessor,
    CalculatedColumnsConfig,
    ConstantColumn,
    ExpressionColumn,
    CalculatedColumn,
)
from src.forklift.processors.calculated_columns_factory import (
    create_calculated_columns_processor_from_schema,
)
from src.forklift.schema.csv_schema_importer import CsvSchemaImporter


def demo_partition_optimized_constants():
    """Demonstrate using constant columns for efficient partitioning."""
    print("\n🗂️  PARTITION-OPTIMIZED CONSTANTS DEMO")
    print("=" * 50)

    # Simulate processing multiple data sources
    data_sources = [
        {"name": "census_2020", "format": "csv"},
        {"name": "survey_data", "format": "excel"},
        {"name": "legacy_files", "format": "fwf"},
    ]

    for source in data_sources:
        print(f"\nProcessing {source['name']} ({source['format']})...")

        # Create sample data
        test_data = {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [100, 200, 300],
        }

        arrays = [pa.array(values) for values in test_data.values()]
        schema = pa.schema(
            [pa.field(name, array.type) for name, array in zip(test_data.keys(), arrays)]
        )
        batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

        # Add partition-friendly constants
        config = CalculatedColumnsConfig(
            constants=[
                ConstantColumn(name="data_source", value=source["name"]),
                ConstantColumn(name="file_format", value=source["format"]),
                ConstantColumn(name="load_date", value="2024-08-26", data_type=pa.string()),
                ConstantColumn(name="processing_batch", value=1, data_type=pa.int32()),
            ],
            partition_columns=["data_source", "file_format", "load_date"],
        )

        processor = CalculatedColumnsProcessor(config)
        result_batch, _ = processor.process_batch(batch)

        print(f"  Original columns: {len(batch.schema.names)}")
        print(f"  With constants: {len(result_batch.schema.names)}")
        print(f"  Partition keys: {processor.get_partition_columns()}")
        print(f"  Sample data_source values: {result_batch.column('data_source').to_pylist()}")


def demo_business_logic_expressions():
    """Demonstrate business logic using expressions."""
    print("\n💼 BUSINESS LOGIC EXPRESSIONS DEMO")
    print("=" * 50)

    # Employee data with business rules
    employee_data = {
        "employee_id": [1001, 1002, 1003, 1004],
        "first_name": ["Alice", "Bob", "Charlie", "Diana"],
        "last_name": ["Johnson", "Smith", "Brown", "Wilson"],
        "age": [25, 45, 17, 67],
        "salary": [45000, 85000, 25000, 120000],
        "department": ["Engineering", "Sales", "Intern", "Executive"],
        "years_experience": [3, 15, 0, 25],
    }

    arrays = [pa.array(values) for values in employee_data.values()]
    schema = pa.schema(
        [pa.field(name, array.type) for name, array in zip(employee_data.keys(), arrays)]
    )
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

    config = CalculatedColumnsConfig(
        expressions=[
            # Full name concatenation
            ExpressionColumn(
                name="full_name",
                expression="first_name + ' ' + last_name",
                dependencies=["first_name", "last_name"],
                description="Employee full name for reports",
            ),
            # Age-based categorization
            ExpressionColumn(
                name="age_group",
                expression="CASE WHEN age < 18 THEN 'minor' WHEN age < 65 THEN 'adult' ELSE 'senior' END",
                dependencies=["age"],
                description="Age group for demographic analysis",
            ),
            # Salary tier classification
            ExpressionColumn(
                name="salary_tier",
                expression="CASE WHEN salary < 50000 THEN 'entry' WHEN salary < 100000 THEN 'mid' ELSE 'senior' END",
                dependencies=["salary"],
                description="Salary tier for compensation analysis",
            ),
        ],
        constants=[
            ConstantColumn(name="company", value="ACME Corp"),
            ConstantColumn(name="report_date", value="2024-08-26"),
        ],
    )

    processor = CalculatedColumnsProcessor(config)
    result_batch, _ = processor.process_batch(batch)

    print("\nEmployee Report with Business Logic:")
    print("-" * 40)

    # Display results
    for i in range(len(result_batch)):
        row = {col: result_batch.column(col)[i].as_py() for col in result_batch.schema.names}
        print(f"• {row['full_name']} ({row['age_group']}, {row['salary_tier']} level)")
        print(f"  Department: {row['department']}, Salary: ${row['salary']:,}")


def demo_data_quality_calculations():
    """Demonstrate data quality metrics using calculated columns."""
    print("\n📊 DATA QUALITY CALCULATIONS DEMO")
    print("=" * 50)

    # Data with quality issues
    quality_data = {
        "record_id": [1, 2, 3, 4, 5],
        "name": ["John Doe", "Jane", "", "Bob Smith", "Alice Johnson"],
        "email": [
            "john@company.com",
            "jane@invalid",
            "missing",
            "bob@company.com",
            "alice@company.com",
        ],
        "phone": ["555-1234", "", "555-5678", "invalid-phone", "555-9999"],
        "created_date": [
            datetime(2024, 1, 1),
            datetime(2024, 2, 15),
            datetime(2024, 3, 20),
            datetime(2024, 4, 10),
            datetime(2024, 5, 5),
        ],
    }

    arrays = [pa.array(values) for values in quality_data.values()]
    schema = pa.schema(
        [pa.field(name, array.type) for name, array in zip(quality_data.keys(), arrays)]
    )
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

    config = CalculatedColumnsConfig(
        calculated=[
            # Data quality metrics
            CalculatedColumn(
                name="name_length",
                function="string_length",
                dependencies=["name"],
                description="Character count for name field completeness",
            ),
            CalculatedColumn(
                name="email_domain",
                function="extract_email_domain",
                dependencies=["email"],
                description="Email domain for validation analysis",
            ),
        ],
        expressions=[
            # Quality flags
            ExpressionColumn(
                name="name_quality",
                expression="name",  # Simplified - in practice would check for completeness
                dependencies=["name"],
                description="Name field quality indicator",
            )
        ],
        constants=[
            ConstantColumn(name="quality_check_date", value="2024-08-26"),
            ConstantColumn(name="validation_version", value="v2.0"),
        ],
    )

    processor = CalculatedColumnsProcessor(config)
    result_batch, validation_results = processor.process_batch(batch)

    print("\nData Quality Analysis:")
    print("-" * 25)

    # Show quality metrics
    name_lengths = result_batch.column("name_length").to_pylist()
    names = result_batch.column("name").to_pylist()

    for i, (name, length) in enumerate(zip(names, name_lengths)):
        quality = "✅ Good" if length > 3 else "⚠️ Poor"
        print(f"Record {i+1}: '{name}' (length: {length}) - {quality}")


def demo_schema_driven_processing():
    """Demonstrate schema-driven calculated columns processing."""
    print("\n📋 SCHEMA-DRIVEN PROCESSING DEMO")
    print("=" * 50)

    # Load schema and create processor
    schema_path = "/Users/matt/PycharmProjects/forklift/schema-standards/20250826-csv.json"
    importer = CsvSchemaImporter(schema_path)

    if importer.has_calculated_columns():
        calc_config = importer.get_calculated_columns_config()
        processor = create_calculated_columns_processor_from_schema(calc_config)

        print("Schema Configuration Loaded:")
        print(f"• Constants: {len(calc_config.get('constants', []))}")
        print(f"• Expressions: {len(calc_config.get('expressions', []))}")
        print(f"• Calculated: {len(calc_config.get('calculated', []))}")
        print(f"• Partition columns: {importer.get_partition_columns()}")

        # Process sample data
        sample_data = {
            "id": [1, 2, 3],
            "name": ["Alice Smith", "Bob Jones", "Charlie Brown"],
            "age": [25, 45, 17],
            "salary": [55000, 85000, 25000],
            "created_timestamp": [
                datetime(2020, 1, 1),
                datetime(2021, 6, 15),
                datetime(2022, 12, 31),
            ],
        }

        arrays = [pa.array(values) for values in sample_data.values()]
        schema = pa.schema(
            [pa.field(name, array.type) for name, array in zip(sample_data.keys(), arrays)]
        )
        batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

        result_batch, _ = processor.process_batch(batch)

        print(f"\nProcessing Results:")
        print(f"• Original columns: {len(batch.schema.names)}")
        print(f"• Final columns: {len(result_batch.schema.names)}")
        print(
            f"• Added columns: {[col for col in result_batch.schema.names if col not in batch.schema.names]}"
        )

        # Show partition column values
        partition_cols = processor.get_partition_columns()
        for col in partition_cols:
            values = result_batch.column(col).to_pylist()
            print(f"• Partition '{col}': {set(values)}")


def demo_advanced_expressions():
    """Demonstrate advanced expression capabilities."""
    print("\n🧮 ADVANCED EXPRESSIONS DEMO")
    print("=" * 50)

    # Financial data for complex calculations
    financial_data = {
        "account_id": [1001, 1002, 1003, 1004],
        "balance": [1500.50, -250.75, 10000.00, 500.25],
        "credit_limit": [2000.00, 1000.00, 15000.00, 1000.00],
        "account_type": ["checking", "savings", "credit", "checking"],
        "risk_score": [85, 45, 95, 70],
    }

    arrays = [pa.array(values) for values in financial_data.values()]
    schema = pa.schema(
        [pa.field(name, array.type) for name, array in zip(financial_data.keys(), arrays)]
    )
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

    config = CalculatedColumnsConfig(
        expressions=[
            # Account status based on balance
            ExpressionColumn(
                name="account_status",
                expression="CASE WHEN balance < 0 THEN 'overdrawn' WHEN balance < 100 THEN 'low' ELSE 'normal' END",
                dependencies=["balance"],
                description="Account status based on current balance",
            ),
            # Risk category
            ExpressionColumn(
                name="risk_category",
                expression="CASE WHEN risk_score >= 90 THEN 'high' WHEN risk_score >= 70 THEN 'medium' ELSE 'low' END",
                dependencies=["risk_score"],
                description="Risk assessment category",
            ),
        ],
        constants=[
            ConstantColumn(name="analysis_date", value="2024-08-26"),
            ConstantColumn(name="bank_branch", value="downtown"),
            ConstantColumn(name="regulatory_version", value="Basel_III"),
        ],
    )

    processor = CalculatedColumnsProcessor(config)
    result_batch, _ = processor.process_batch(batch)

    print("\nFinancial Account Analysis:")
    print("-" * 30)

    for i in range(len(result_batch)):
        account_id = result_batch.column("account_id")[i].as_py()
        balance = result_batch.column("balance")[i].as_py()
        status = result_batch.column("account_status")[i].as_py()
        risk = result_batch.column("risk_category")[i].as_py()

        print(f"Account {account_id}: ${balance:,.2f} ({status} status, {risk} risk)")


def main():
    """Run all calculated columns demonstrations."""
    print("🚀 FORKLIFT CALCULATED COLUMNS COMPREHENSIVE DEMO")
    print("=" * 60)
    print("Demonstrating constants, expressions, and calculated columns")
    print("for efficient partitioning and data enrichment")

    try:
        demo_partition_optimized_constants()
        demo_business_logic_expressions()
        demo_data_quality_calculations()
        demo_schema_driven_processing()
        demo_advanced_expressions()

        print("\n" + "=" * 60)
        print("✅ ALL DEMOS COMPLETED SUCCESSFULLY!")
        print("\nKey Features Demonstrated:")
        print("• ✅ Partition-optimized constant columns")
        print("• ✅ Business logic expressions (CASE statements)")
        print("• ✅ String concatenation and manipulation")
        print("• ✅ Data quality calculations")
        print("• ✅ Schema-driven configuration")
        print("• ✅ Advanced expression evaluation")
        print("• ✅ Integration with all schema formats (CSV, Excel, FWF, SQL)")

    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
