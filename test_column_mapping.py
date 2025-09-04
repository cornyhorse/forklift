#!/usr/bin/env python3
"""
Test demonstrating the new column mapping capabilities for the Forklift schema standard.
"""

import pyarrow as pa
from src.forklift.processors.column_mapper import (
    ColumnMapper,
    ColumnMappingConfig,
    create_postgres_mapper,
    create_custom_mapper
)

def test_basic_column_mapping():
    """Test basic column name mapping functionality."""
    print("🔀 COLUMN MAPPING FUNCTIONALITY TEST")
    print("=" * 60)

    # Create test data with mixed column names
    test_data = {
        "A": [1, 2, 3],           # Simple letter column
        "B": [4, 5, 6],           # Another simple column
        "StateID": [7, 8, 9],     # CamelCase with ID
        "first_name": ["John", "Jane", "Bob"],  # Already snake_case
        "LastName": ["Doe", "Smith", "Wilson"], # PascalCase
        "XMLParser": [10, 11, 12] # Acronym case
    }

    # Create PyArrow batch
    arrays = [pa.array(values) for values in test_data.values()]
    schema = pa.schema([pa.field(name, array.type) for name, array in zip(test_data.keys(), arrays)])
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

    print(f"\n📋 Original Data Schema:")
    print(f"Columns: {batch.schema.names}")

    return batch

def test_explicit_mapping():
    """Test explicit column name mappings."""
    print("\n1️⃣ EXPLICIT COLUMN MAPPING")
    print("-" * 40)

    batch = test_basic_column_mapping()

    # Configure explicit mappings
    config = ColumnMappingConfig(
        explicit_mappings={
            "A": "StateCode",
            "B": "CountyCode",
            "StateID": "StateIdentifier"
        },
        allow_unmapped=True
    )

    mapper = ColumnMapper(config)
    result_batch, validation_results = mapper.process_batch(batch)

    print(f"Mappings applied:")
    for source, target in config.explicit_mappings.items():
        print(f"  '{source}' → '{target}'")

    print(f"\nResult columns: {result_batch.schema.names}")

    # Check for validation issues
    if validation_results:
        for result in validation_results:
            if not result.is_valid:
                print(f"⚠️  Warning: {result.error_message}")
    else:
        print("✅ Mapping completed successfully")

    return result_batch

def test_postgres_naming_convention():
    """Test PostgreSQL snake_case naming convention."""
    print("\n2️⃣ POSTGRESQL SNAKE_CASE CONVENTION")
    print("-" * 45)

    batch = test_basic_column_mapping()

    # Use the built-in PostgreSQL mapper
    postgres_mapper = create_postgres_mapper()
    result_batch, validation_results = postgres_mapper.process_batch(batch)

    print("PostgreSQL snake_case transformations:")
    for original, new in zip(batch.schema.names, result_batch.schema.names):
        if original != new:
            print(f"  '{original}' → '{new}'")
        else:
            print(f"  '{original}' (unchanged)")

    print(f"\nOriginal: {batch.schema.names}")
    print(f"Result:   {result_batch.schema.names}")

    if not validation_results:
        print("✅ PostgreSQL naming conversion completed successfully")

    return result_batch

def test_combined_mapping():
    """Test combining explicit mappings with naming conventions."""
    print("\n3️⃣ COMBINED MAPPING + NAMING CONVENTION")
    print("-" * 50)

    batch = test_basic_column_mapping()

    # First map specific columns, then apply PostgreSQL naming
    config = ColumnMappingConfig(
        explicit_mappings={
            "A": "StateCode",
            "B": "CountyCode"
        },
        naming_convention='snake_case',
        case_sensitive=False
    )

    mapper = ColumnMapper(config)
    result_batch, validation_results = mapper.process_batch(batch)

    print("Step 1 - Explicit mappings:")
    for source, target in config.explicit_mappings.items():
        print(f"  '{source}' → '{target}'")

    print("Step 2 - Apply snake_case convention:")
    print(f"  Original: {batch.schema.names}")
    print(f"  Result:   {result_batch.schema.names}")

    if not validation_results:
        print("✅ Combined mapping completed successfully")

    return result_batch

def test_custom_mapper_example():
    """Test the convenient custom mapper function."""
    print("\n4️⃣ CUSTOM MAPPER WITH POSTGRES STYLE")
    print("-" * 45)

    batch = test_basic_column_mapping()

    # Use the convenience function for common use case
    custom_mappings = {
        "A": "StateCode",
        "B": "CountyCode",
        "StateID": "StateIdentifier"
    }

    custom_mapper = create_custom_mapper(custom_mappings, postgres_style=True)
    result_batch, validation_results = custom_mapper.process_batch(batch)

    print("Custom mappings with PostgreSQL style:")
    print(f"  Explicit mappings: {custom_mappings}")
    print(f"  PostgreSQL style: Enabled (snake_case)")
    print(f"  Case sensitive: False")

    print(f"\nOriginal: {batch.schema.names}")
    print(f"Result:   {result_batch.schema.names}")

    if not validation_results:
        print("✅ Custom mapper completed successfully")

    return result_batch

def test_column_dropping():
    """Test dropping unmapped columns."""
    print("\n5️⃣ COLUMN DROPPING (UNMAPPED COLUMNS)")
    print("-" * 45)

    batch = test_basic_column_mapping()

    # Only map specific columns, drop the rest
    config = ColumnMappingConfig(
        explicit_mappings={
            "A": "StateCode",
            "StateID": "StateIdentifier"
        },
        allow_unmapped=False,
        drop_unmapped=True
    )

    mapper = ColumnMapper(config)
    result_batch, validation_results = mapper.process_batch(batch)

    print("Configuration:")
    print(f"  Explicit mappings: {config.explicit_mappings}")
    print(f"  Drop unmapped: {config.drop_unmapped}")

    print(f"\nOriginal columns: {batch.schema.names}")
    print(f"Kept columns:     {result_batch.schema.names}")
    print(f"Dropped columns:  {set(batch.schema.names) - set(result_batch.schema.names)}")

    if not validation_results:
        print("✅ Column dropping completed successfully")

    return result_batch

def test_schema_integration_example():
    """Show how this would integrate with schema configuration."""
    print("\n6️⃣ SCHEMA CONFIGURATION INTEGRATION")
    print("-" * 45)

    # Example of how this could be configured in a schema
    schema_config_example = {
        "columnMapping": {
            "enabled": True,
            "explicit_mappings": {
                "A": "StateCode",
                "B": "CountyCode",
                "StateID": "StateIdentifier"
            },
            "naming_convention": "snake_case",
            "case_sensitive": False,
            "allow_unmapped": True,
            "drop_unmapped": False
        },
        "properties": {
            "state_code": {"type": "string"},
            "county_code": {"type": "string"},
            "state_identifier": {"type": "integer"}
        }
    }

    print("Example schema configuration:")
    print("```json")
    import json
    print(json.dumps(schema_config_example, indent=2))
    print("```")

    print("\nThis configuration would:")
    print("  1. Map 'A' → 'StateCode' → 'state_code'")
    print("  2. Map 'B' → 'CountyCode' → 'county_code'")
    print("  3. Map 'StateID' → 'StateIdentifier' → 'state_identifier'")
    print("  4. Convert all other columns to snake_case")
    print("  5. Keep unmapped columns")

if __name__ == "__main__":
    test_explicit_mapping()
    test_postgres_naming_convention()
    test_combined_mapping()
    test_custom_mapper_example()
    test_column_dropping()
    test_schema_integration_example()

    print("\n" + "=" * 60)
    print("🎉 All column mapping tests completed!")

    print("\n📝 SUMMARY OF COLUMN MAPPING FEATURES:")
    print("   • explicit_mappings: Direct column name mappings")
    print("   • naming_convention: Apply standard conventions (snake_case, camelCase, etc.)")
    print("   • custom_transform: Custom transformation functions")
    print("   • case_sensitive: Control case sensitivity")
    print("   • allow_unmapped/drop_unmapped: Control unmapped columns")
    print("   • Built-in PostgreSQL mapper for snake_case conversion")
    print("   • Convenience functions for common use cases")
