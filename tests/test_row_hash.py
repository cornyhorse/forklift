"""Test script to demonstrate row hash functionality."""

import json
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# Import the row hash processor
from forklift.processors.row_hash import RowHashConfig, RowHashProcessor
from forklift.processors.row_hash_factory import create_row_hash_processor_from_schema


def test_row_hash_basic():
    """Test basic row hash functionality."""
    print("🔧 Testing basic row hash functionality...")

    # Create test data
    data = {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}

    # Create PyArrow batch
    batch = pa.RecordBatch.from_pydict(data)
    print(f"✅ Created test batch with {batch.num_rows} rows")

    # Test with SHA256 (default)
    config = RowHashConfig(enabled=True, column_name="data_hash", algorithm="sha256")

    processor = RowHashProcessor(config)
    processed_batch, validation_results = processor.process_batch(batch)

    print(f"✅ Processed batch now has {processed_batch.num_columns} columns")
    print(f"✅ Added column: {processed_batch.schema.field(-1).name}")

    # Check that hash column was added
    assert processed_batch.num_columns == batch.num_columns + 1
    assert processed_batch.schema.field(-1).name == "data_hash"

    # Convert to table for easier viewing
    table = pa.Table.from_batches([processed_batch])
    print("\n📊 Sample data with hash:")
    print(table.to_pandas().head())

    return processed_batch


def test_different_algorithms():
    """Test different hash algorithms."""
    print("\n🔧 Testing different hash algorithms...")

    # Create test data
    data = {"id": [1], "name": ["Test"]}
    batch = pa.RecordBatch.from_pydict(data)

    algorithms = ["md5", "sha1", "sha256", "sha384", "sha512"]

    for algo in algorithms:
        config = RowHashConfig(enabled=True, column_name=f"{algo}_hash", algorithm=algo)

        processor = RowHashProcessor(config)
        processed_batch, _ = processor.process_batch(batch)

        # Get the hash value
        hash_column = processed_batch.column(-1)
        hash_value = hash_column[0].as_py()

        print(f"✅ {algo.upper()}: {hash_value[:20]}... (length: {len(hash_value)})")


def test_column_selection():
    """Test including/excluding specific columns."""
    print("\n🔧 Testing column selection...")

    # Create test data
    data = {
        "id": [1, 2],
        "name": ["Alice", "Bob"],
        "secret": ["password123", "secret456"],
        "timestamp": ["2024-01-01", "2024-01-02"],
    }
    batch = pa.RecordBatch.from_pydict(data)

    # Test excluding sensitive columns
    config = RowHashConfig(
        enabled=True,
        column_name="content_hash",
        algorithm="sha256",
        exclude_columns=["secret", "timestamp"],
    )

    processor = RowHashProcessor(config)
    processed_batch, _ = processor.process_batch(batch)

    print("✅ Hash calculated excluding 'secret' and 'timestamp' columns")

    # Test including only specific columns
    config2 = RowHashConfig(
        enabled=True, column_name="key_hash", algorithm="sha256", include_columns=["id", "name"]
    )

    processor2 = RowHashProcessor(config2)
    processed_batch2, _ = processor2.process_batch(batch)

    print("✅ Hash calculated including only 'id' and 'name' columns")

    # The hashes should be the same since we're excluding the same columns
    hash1 = processed_batch.column(-1)[0].as_py()
    hash2 = processed_batch2.column(-1)[0].as_py()

    if hash1 == hash2:
        print("✅ Include/exclude logic working correctly - hashes match!")
    else:
        print("❌ Include/exclude logic issue - hashes don't match")


def test_schema_integration():
    """Test integration with schema configuration."""
    print("\n🔧 Testing schema integration...")

    # Create a schema configuration
    schema_config = {
        "enabled": True,
        "columnName": "row_signature",
        "algorithm": "md5",
        "excludeColumns": ["internal_id"],
        "nullValue": "MISSING",
        "separator": "|",
    }

    # Create processor from schema
    processor = create_row_hash_processor_from_schema(schema_config)

    if processor:
        print("✅ Successfully created processor from schema configuration")

        # Test with data
        data = {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "internal_id": [101, 102, 103],  # This should be excluded
        }
        batch = pa.RecordBatch.from_pydict(data)

        processed_batch, validation_results = processor.process_batch(batch)
        print(
            f"✅ Processed batch with schema config - added '{processed_batch.schema.field(-1).name}' column"
        )

        # Show sample
        table = pa.Table.from_batches([processed_batch])
        print("\n📊 Sample with schema-configured hash:")
        print(table.to_pandas().head())
    else:
        print("❌ Failed to create processor from schema")


def test_disabled_by_default():
    """Test that row hash is disabled by default."""
    print("\n🔧 Testing disabled by default behavior...")

    # Schema with row hash disabled (default)
    schema_config = {
        "enabled": False,  # Explicitly disabled
        "columnName": "row_hash",
        "algorithm": "sha256",
    }

    processor = create_row_hash_processor_from_schema(schema_config)

    if processor is None:
        print("✅ Correctly returns None when disabled")
    else:
        print("❌ Should return None when disabled")

    # Test with missing enabled field (should default to disabled)
    schema_config_no_enabled = {"columnName": "row_hash", "algorithm": "sha256"}

    processor2 = create_row_hash_processor_from_schema(schema_config_no_enabled)

    if processor2 is None:
        print("✅ Correctly defaults to disabled when 'enabled' not specified")
    else:
        print("❌ Should default to disabled")


def create_test_schema_file():
    """Create a test schema file with row hash enabled."""
    print("\n🔧 Creating test schema file...")

    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://github.com/cornyhorse/forklift/schema-standards/test-row-hash.json",
        "title": "Test Row Hash Schema",
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "email": {"type": "string"},
        },
        "required": ["id", "name"],
        "x-rowHash": {
            "enabled": True,
            "columnName": "record_hash",
            "algorithm": "sha256",
            "excludeColumns": ["email"],  # Exclude PII from hash
            "nullValue": "NULL",
            "separator": "||",
        },
    }

    # Save to temporary file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(schema, f, indent=2)
        schema_file = f.name

    print(f"✅ Created test schema file: {schema_file}")
    return schema_file


def main():
    """Run all row hash tests."""
    print("🚀 Testing Forklift Row Hash Functionality\n")

    try:
        # Run tests
        test_row_hash_basic()
        test_different_algorithms()
        test_column_selection()
        test_schema_integration()
        test_disabled_by_default()

        # Create test schema
        schema_file = create_test_schema_file()

        print(f"\n🎉 All tests completed successfully!")
        print(f"\n📄 Test schema file created at: {schema_file}")
        print("\nTo use row hash in your schemas, add the 'x-rowHash' section with:")
        print("- enabled: true/false")
        print("- columnName: name for hash column")
        print("- algorithm: md5, sha1, sha256, sha384, sha512")
        print("- includeColumns: list of columns to include (optional)")
        print("- excludeColumns: list of columns to exclude (optional)")
        print("- nullValue: string to use for NULL values")
        print("- separator: string to separate column values")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        raise


if __name__ == "__main__":
    main()
