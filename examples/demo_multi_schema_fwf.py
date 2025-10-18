#!/usr/bin/env python3
"""Demonstration of multi-schema Fixed Width File processing."""

import json
import sys
from pathlib import Path

# Add src to Python path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))


def create_demo_files():
    """Create demonstration files for multi-schema FWF processing."""

    # Create a sample multi-schema FWF data file
    demo_data = """H20241201BATCH001   Daily Sales Report              
D000001PRODUCT_A    000012500USD2024120110
D000002PRODUCT_B    000008750USD2024120115
D000003PRODUCT_C    000015200USD2024120105
T000003000036450USD20241201Summary record        
H20241202BATCH002   Weekly Inventory Report         
D000004WIDGET_X     000005000USD2024120208
D000005WIDGET_Y     000007500USD2024120212
T000002000012500USD20241202End of file           """

    with open("demo_multi_schema.txt", "w") as f:
        f.write(demo_data)

    # Create the corresponding schema configuration
    schema_config = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Multi-Schema FWF Demo",
        "description": "Demonstration of fixed width file with Header, Detail, and Trailer records",
        "type": "object",
        "x-fwf": {
            "encoding": "utf-8",
            "trim": {"rstrip": True},
            "conditionalSchemas": {
                "flagColumn": {
                    "name": "record_type",
                    "start": 1,
                    "length": 1,
                    "parquetType": "string",
                },
                "schemas": [
                    {
                        "flagValue": "H",
                        "description": "Header records - batch information",
                        "fields": [
                            {
                                "name": "record_type",
                                "start": 1,
                                "length": 1,
                                "parquetType": "string",
                            },
                            {
                                "name": "batch_date",
                                "start": 2,
                                "length": 8,
                                "parquetType": "string",
                            },
                            {
                                "name": "batch_id",
                                "start": 10,
                                "length": 10,
                                "parquetType": "string",
                                "trim": True,
                            },
                            {
                                "name": "description",
                                "start": 20,
                                "length": 33,
                                "parquetType": "string",
                                "trim": True,
                            },
                        ],
                    },
                    {
                        "flagValue": "D",
                        "description": "Detail records - transaction data",
                        "fields": [
                            {
                                "name": "record_type",
                                "start": 1,
                                "length": 1,
                                "parquetType": "string",
                            },
                            {
                                "name": "transaction_id",
                                "start": 2,
                                "length": 6,
                                "align": "right",
                                "pad": "0",
                                "parquetType": "int64",
                            },
                            {
                                "name": "product_code",
                                "start": 8,
                                "length": 12,
                                "parquetType": "string",
                                "trim": True,
                            },
                            {
                                "name": "amount_cents",
                                "start": 20,
                                "length": 9,
                                "align": "right",
                                "pad": "0",
                                "parquetType": "int64",
                            },
                            {
                                "name": "currency",
                                "start": 29,
                                "length": 3,
                                "parquetType": "string",
                            },
                            {
                                "name": "transaction_date",
                                "start": 32,
                                "length": 8,
                                "parquetType": "string",
                            },
                            {
                                "name": "quantity",
                                "start": 40,
                                "length": 2,
                                "align": "right",
                                "pad": "0",
                                "parquetType": "int32",
                            },
                        ],
                    },
                    {
                        "flagValue": "T",
                        "description": "Trailer records - summary information",
                        "fields": [
                            {
                                "name": "record_type",
                                "start": 1,
                                "length": 1,
                                "parquetType": "string",
                            },
                            {
                                "name": "record_count",
                                "start": 2,
                                "length": 6,
                                "align": "right",
                                "pad": "0",
                                "parquetType": "int64",
                            },
                            {
                                "name": "total_amount_cents",
                                "start": 8,
                                "length": 9,
                                "align": "right",
                                "pad": "0",
                                "parquetType": "int64",
                            },
                            {
                                "name": "currency",
                                "start": 17,
                                "length": 3,
                                "parquetType": "string",
                            },
                            {
                                "name": "summary_date",
                                "start": 20,
                                "length": 8,
                                "parquetType": "string",
                            },
                            {
                                "name": "notes",
                                "start": 28,
                                "length": 25,
                                "parquetType": "string",
                                "trim": True,
                            },
                        ],
                    },
                ],
            },
            "nulls": {"global": ["", "NULL", "N/A"]},
        },
    }

    with open("demo_multi_schema.json", "w") as f:
        json.dump(schema_config, f, indent=2)

    print("Demo files created:")
    print("  - demo_multi_schema.txt (data file)")
    print("  - demo_multi_schema.json (schema configuration)")


def demonstrate_multi_schema_fwf():
    """Demonstrate multi-schema FWF processing capabilities."""

    try:
        from forklift.inputs.fwf import FwfInputHandler
        from forklift.inputs.fwf_utils import create_fwf_config_from_schema

        print("=== Multi-Schema Fixed Width File Demonstration ===\n")

        # Create demo files
        create_demo_files()

        # Load configuration
        print("1. Loading multi-schema configuration...")
        config = create_fwf_config_from_schema(Path("demo_multi_schema.json"))

        print(f"   ✓ Encoding: {config.encoding}")
        print(
            f"   ✓ Flag column: {config.flag_column.name} (pos {config.flag_column.start}, length {config.flag_column.length})"
        )
        print(f"   ✓ Number of conditional schemas: {len(config.conditional_schemas)}")

        for i, schema in enumerate(config.conditional_schemas):
            print(f"     - Schema {i+1}: flag='{schema.flag_value}' ({schema.description})")
            print(f"       Fields: {', '.join(f.name for f in schema.fields)}")

        # Create handler and process file
        print("\n2. Processing fixed width file with multiple schemas...")
        handler = FwfInputHandler(config)
        records = list(handler.read_file(Path("demo_multi_schema.txt")))

        print(f"   ✓ Total records processed: {len(records)}")

        # Analyze by record type
        print("\n3. Record type analysis:")
        by_type = {}
        for record in records:
            rtype = record["record_type"]
            if rtype not in by_type:
                by_type[rtype] = []
            by_type[rtype].append(record)

        for rtype, type_records in by_type.items():
            print(f"   ✓ Type '{rtype}': {len(type_records)} records")

        # Show sample records
        print("\n4. Sample records from each schema:")

        if "H" in by_type:
            header = by_type["H"][0]
            print(f"\n   Header Record (Type H):")
            print(f"     - Batch Date: {header['batch_date']}")
            print(f"     - Batch ID: {header['batch_id']}")
            print(f"     - Description: {header['description']}")

        if "D" in by_type:
            detail = by_type["D"][0]
            print(f"\n   Detail Record (Type D):")
            print(f"     - Transaction ID: {detail['transaction_id']}")
            print(f"     - Product Code: {detail['product_code']}")
            print(f"     - Amount (cents): {detail['amount_cents']}")
            print(f"     - Currency: {detail['currency']}")
            print(f"     - Quantity: {detail['quantity']}")

        if "T" in by_type:
            trailer = by_type["T"][0]
            print(f"\n   Trailer Record (Type T):")
            print(f"     - Record Count: {trailer['record_count']}")
            print(f"     - Total Amount (cents): {trailer['total_amount_cents']}")
            print(f"     - Currency: {trailer['currency']}")
            print(f"     - Notes: {trailer['notes']}")

        # Generate Arrow schema
        print("\n5. Generating PyArrow schema for all record types...")
        arrow_schema = handler.get_arrow_schema()
        print(f"   ✓ Generated schema with {len(arrow_schema)} fields:")

        field_groups = {
            "Common": ["record_type"],
            "Header": ["batch_date", "batch_id", "description"],
            "Detail": [
                "transaction_id",
                "product_code",
                "amount_cents",
                "currency",
                "transaction_date",
                "quantity",
            ],
            "Trailer": ["record_count", "total_amount_cents", "summary_date", "notes"],
            "Metadata": ["__line_number__", "__source_file__"],
        }

        for group, expected_fields in field_groups.items():
            actual_fields = [f.name for f in arrow_schema if f.name in expected_fields]
            if actual_fields:
                print(f"     - {group}: {', '.join(actual_fields)}")

        print("\n✅ Multi-schema FWF demonstration completed successfully!")
        print("\nKey Features Demonstrated:")
        print("  • Flag column-based schema selection")
        print("  • Multiple record types in single file")
        print("  • Field positioning and alignment")
        print("  • Data type conversion (string, int64, int32)")
        print("  • Whitespace trimming and padding")
        print("  • Unified PyArrow schema generation")

        return True

    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Make sure you're running from the correct directory with src/ in the path")
        return False
    except Exception as e:
        print(f"❌ Error during demonstration: {e}")
        import traceback

        traceback.print_exc()
        return False
    finally:
        # Clean up demo files
        for filename in ["demo_multi_schema.txt", "demo_multi_schema.json"]:
            try:
                Path(filename).unlink()
            except FileNotFoundError:
                pass


if __name__ == "__main__":
    success = demonstrate_multi_schema_fwf()
    sys.exit(0 if success else 1)
