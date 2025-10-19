#!/usr/bin/env python3
"""
Test demonstrating the new column mapping capabilities for the Forklift schema standard.
"""

import pyarrow as pa

from forklift.processors.column_mapper import (
    ColumnMapper,
    ColumnMappingConfig,
    create_custom_mapper,
    create_postgres_mapper,
)


def test_basic_column_mapping():
    """Test basic column name mapping functionality."""
    print("🔀 COLUMN MAPPING FUNCTIONALITY TEST")
    print("=" * 60)

    # Create test data with mixed column names
    test_data = {
        "A": [1, 2, 3],  # Simple letter column
        "B": [4, 5, 6],  # Another simple column
        "StateID": [7, 8, 9],  # CamelCase with ID
        "first_name": ["John", "Jane", "Bob"],  # Already snake_case
        "LastName": ["Doe", "Smith", "Wilson"],  # PascalCase
        "XMLParser": [10, 11, 12],  # Acronym case
    }

    # Create PyArrow batch
    arrays = [pa.array(values) for values in test_data.values()]
    schema = pa.schema(
        [pa.field(name, array.type) for name, array in zip(test_data.keys(), arrays)]
    )
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

    print(f"\n📋 Original Data Schema:")
    print(f"Columns: {batch.schema.names}")

    # Assert that the batch was created successfully
    assert batch is not None
    assert len(batch.schema.names) == 6
    assert "A" in batch.schema.names
    assert "StateID" in batch.schema.names


def test_explicit_mapping():
    """Test explicit column name mappings."""
    print("\n1️⃣ EXPLICIT COLUMN MAPPING")
    print("-" * 40)

    # Create test data
    test_data = {
        "A": [1, 2, 3],
        "B": [4, 5, 6],
        "StateID": [7, 8, 9],
        "first_name": ["John", "Jane", "Bob"],
        "LastName": ["Doe", "Smith", "Wilson"],
        "XMLParser": [10, 11, 12],
    }
    arrays = [pa.array(values) for values in test_data.values()]
    schema = pa.schema(
        [pa.field(name, array.type) for name, array in zip(test_data.keys(), arrays)]
    )
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

    # Configure explicit mappings
    config = ColumnMappingConfig(
        explicit_mappings={"A": "StateCode", "B": "CountyCode", "StateID": "StateIdentifier"},
        allow_unmapped=True,
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

    # Assert the mappings worked correctly
    assert "StateCode" in result_batch.schema.names
    assert "CountyCode" in result_batch.schema.names
    assert "StateIdentifier" in result_batch.schema.names
    assert "A" not in result_batch.schema.names  # Should be mapped to StateCode
    assert "B" not in result_batch.schema.names  # Should be mapped to CountyCode
    assert len(validation_results) == 0  # Should be no validation errors


def test_postgres_naming_convention():
    """Test PostgreSQL snake_case naming convention."""
    print("\n2️⃣ POSTGRESQL SNAKE_CASE CONVENTION")
    print("-" * 45)

    # Create test data
    test_data = {
        "A": [1, 2, 3],
        "B": [4, 5, 6],
        "StateID": [7, 8, 9],
        "first_name": ["John", "Jane", "Bob"],
        "LastName": ["Doe", "Smith", "Wilson"],
        "XMLParser": [10, 11, 12],
    }
    arrays = [pa.array(values) for values in test_data.values()]
    schema = pa.schema(
        [pa.field(name, array.type) for name, array in zip(test_data.keys(), arrays)]
    )
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

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

    # Assert the snake_case conversions worked correctly
    assert "state_id" in result_batch.schema.names  # StateID -> state_id
    assert "last_name" in result_batch.schema.names  # LastName -> last_name
    assert "xml_parser" in result_batch.schema.names  # XMLParser -> xml_parser
    assert "first_name" in result_batch.schema.names  # Already snake_case
    assert len(validation_results) == 0  # Should be no validation errors


def test_combined_mapping():
    """Test combining explicit mappings with naming conventions."""
    print("\n3️⃣ COMBINED MAPPING + NAMING CONVENTION")
    print("-" * 50)

    # Create test data
    test_data = {
        "A": [1, 2, 3],
        "B": [4, 5, 6],
        "StateID": [7, 8, 9],
        "first_name": ["John", "Jane", "Bob"],
        "LastName": ["Doe", "Smith", "Wilson"],
        "XMLParser": [10, 11, 12],
    }
    arrays = [pa.array(values) for values in test_data.values()]
    schema = pa.schema(
        [pa.field(name, array.type) for name, array in zip(test_data.keys(), arrays)]
    )
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

    # First map specific columns, then apply PostgreSQL naming
    config = ColumnMappingConfig(
        explicit_mappings={"A": "StateCode", "B": "CountyCode"},
        naming_convention="snake_case",
        case_sensitive=False,
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

    # Assert the combined mapping worked correctly
    assert "state_code" in result_batch.schema.names  # A -> StateCode -> state_code
    assert "county_code" in result_batch.schema.names  # B -> CountyCode -> county_code
    assert "state_id" in result_batch.schema.names  # StateID -> state_id
    assert "last_name" in result_batch.schema.names  # LastName -> last_name
    assert len(validation_results) == 0  # Should be no validation errors


def test_custom_mapper_example():
    """Test the convenient custom mapper function."""
    print("\n4️⃣ CUSTOM MAPPER WITH POSTGRES STYLE")
    print("-" * 45)

    # Create test data
    test_data = {
        "A": [1, 2, 3],
        "B": [4, 5, 6],
        "StateID": [7, 8, 9],
        "first_name": ["John", "Jane", "Bob"],
        "LastName": ["Doe", "Smith", "Wilson"],
        "XMLParser": [10, 11, 12],
    }
    arrays = [pa.array(values) for values in test_data.values()]
    schema = pa.schema(
        [pa.field(name, array.type) for name, array in zip(test_data.keys(), arrays)]
    )
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

    # Use the convenience function for common use case
    custom_mappings = {"A": "StateCode", "B": "CountyCode", "StateID": "StateIdentifier"}

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

    # Assert the custom mapping worked correctly
    assert "state_code" in result_batch.schema.names  # A -> StateCode -> state_code
    assert "county_code" in result_batch.schema.names  # B -> CountyCode -> county_code
    assert (
        "state_identifier" in result_batch.schema.names
    )  # StateID -> StateIdentifier -> state_identifier
    assert "last_name" in result_batch.schema.names  # LastName -> last_name
    assert len(validation_results) == 0  # Should be no validation errors


def test_column_dropping():
    """Test dropping unmapped columns."""
    print("\n5️⃣ COLUMN DROPPING (UNMAPPED COLUMNS)")
    print("-" * 45)

    # Create test data
    test_data = {
        "A": [1, 2, 3],
        "B": [4, 5, 6],
        "StateID": [7, 8, 9],
        "first_name": ["John", "Jane", "Bob"],
        "LastName": ["Doe", "Smith", "Wilson"],
        "XMLParser": [10, 11, 12],
    }
    arrays = [pa.array(values) for values in test_data.values()]
    schema = pa.schema(
        [pa.field(name, array.type) for name, array in zip(test_data.keys(), arrays)]
    )
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

    # Only map specific columns, drop the rest
    config = ColumnMappingConfig(
        explicit_mappings={"A": "StateCode", "StateID": "StateIdentifier"},
        allow_unmapped=False,
        drop_unmapped=True,
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

    # Assert the column dropping worked correctly
    assert "StateCode" in result_batch.schema.names
    assert "StateIdentifier" in result_batch.schema.names
    assert len(result_batch.schema.names) == 2  # Only 2 columns should remain
    assert "B" not in result_batch.schema.names  # Should be dropped
    assert "first_name" not in result_batch.schema.names  # Should be dropped
    assert "LastName" not in result_batch.schema.names  # Should be dropped
    assert "XMLParser" not in result_batch.schema.names  # Should be dropped
    assert len(validation_results) == 0  # Should be no validation errors


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
                "StateID": "StateIdentifier",
            },
            "naming_convention": "snake_case",
            "case_sensitive": False,
            "allow_unmapped": True,
            "drop_unmapped": False,
        },
        "properties": {
            "state_code": {"type": "string"},
            "county_code": {"type": "string"},
            "state_identifier": {"type": "integer"},
        },
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
