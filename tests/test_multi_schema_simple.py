#!/usr/bin/env python3
"""Simple test script for multi-schema FWF functionality."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from forklift.inputs.fwf import FwfInputHandler
from forklift.inputs.fwf_utils import create_fwf_config_from_schema


def test_multi_schema_fwf():
    """Test the multi-schema FWF functionality."""
    print("Testing multi-schema FWF functionality...")

    # Path to our test files
    test_dir = Path(__file__).parent / "test-files" / "goodfwf"
    schema_path = test_dir / "multi_schema_example.json"
    data_path = test_dir / "multi_schema_example.txt"

    print(f"Schema file: {schema_path}")
    print(f"Data file: {data_path}")
    print(f"Schema exists: {schema_path.exists()}")
    print(f"Data exists: {data_path.exists()}")

    # Create FWF configuration from schema
    print("\n1. Loading schema configuration...")
    config = create_fwf_config_from_schema(schema_path)
    print(f"   - Encoding: {config.encoding}")
    print(f"   - Has conditional schemas: {config.conditional_schemas is not None}")
    if config.conditional_schemas:
        print(f"   - Number of schemas: {len(config.conditional_schemas)}")
        for i, schema in enumerate(config.conditional_schemas):
            print(
                f"     Schema {i+1}: flag='{schema.flag_value}', description='{schema.description}'"
            )

    # Create handler
    print("\n2. Creating FWF handler...")
    handler = FwfInputHandler(config)

    # Process the file
    print("\n3. Processing FWF file...")
    records = list(handler.read_file(data_path))
    print(f"   - Total records processed: {len(records)}")

    # Analyze records by type
    print("\n4. Analyzing record types...")
    record_types = {}
    for record in records:
        record_type = record.get("record_type", "unknown")
        if record_type not in record_types:
            record_types[record_type] = []
        record_types[record_type].append(record)

    for record_type, type_records in record_types.items():
        print(f"   - Type '{record_type}': {len(type_records)} records")
        if type_records:
            sample = type_records[0]
            fields = [k for k in sample.keys() if not k.startswith("__")]
            print(f"     Fields: {', '.join(fields)}")

    # Show sample records
    print("\n5. Sample records:")
    for record_type, type_records in record_types.items():
        if type_records:
            print(f"\n   Sample {record_type} record:")
            sample = type_records[0]
            for key, value in sample.items():
                if not key.startswith("__"):
                    print(f"     {key}: {value}")

    print("\n✅ Multi-schema FWF test completed successfully!")


if __name__ == "__main__":
    test_multi_schema_fwf()
    sys.exit(0)
