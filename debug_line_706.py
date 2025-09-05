#!/usr/bin/env python3
"""Debug script to test line 706 coverage in forklift_core.py"""

import tempfile
import json
import os
from forklift.engine.forklift_core import ForkliftCore, ImportConfig, HeaderMode

def test_line_706():
    # Create CSV with all rows having missing age (null values)
    csv_content = """name,age,city
John,,NYC
Jane,,Chicago
Bob,,LA
"""

    # Schema with age as required (non-nullable)
    schema_content = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
            "city": {"type": "string"}
        },
        "required": ["name", "age", "city"]
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as csv_f:
        csv_f.write(csv_content)
        csv_f.flush()

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as schema_f:
            json.dump(schema_content, schema_f)
            schema_f.flush()

            try:
                config = ImportConfig(
                    input_path=csv_f.name,
                    output_path=tempfile.mkdtemp(),
                    schema_file=schema_f.name,
                    header_mode=HeaderMode.PRESENT,
                    validate_schema=True,
                    max_validation_errors=100
                )

                print("Config created successfully")
                print(f"Schema file path: {schema_f.name}")
                print(f"Schema file exists: {os.path.exists(schema_f.name)}")

                # Read the schema file to verify content
                with open(schema_f.name, 'r') as f:
                    loaded_schema = json.load(f)
                    print(f"Schema content loaded: {loaded_schema}")

                core = ForkliftCore(config)
                print("ForkliftCore created successfully")

                # Debug: Check if schema was loaded
                print(f"Schema loaded: {core.schema is not None}")
                if core.schema:
                    print(f"Schema fields: {[f.name + ':' + str(f.type) + ':nullable=' + str(f.nullable) for f in core.schema]}")
                else:
                    print("Schema is None - checking why...")
                    # Try to manually load the schema
                    try:
                        test_schema = core._load_schema()
                        print(f"Manual schema load result: {test_schema}")
                    except Exception as e:
                        print(f"Manual schema load failed: {e}")

                result = core.process_csv()
                print("Processing completed")

                print(f"Total rows: {result.total_rows}")
                print(f"Valid rows: {result.valid_rows}")
                print(f"Invalid rows: {result.invalid_rows}")

                # This should trigger line 706 if all rows are invalid
                if result.invalid_rows == 3 and result.valid_rows == 0:
                    print("SUCCESS: All rows invalid - line 706 should be hit!")
                else:
                    print("ISSUE: Not all rows were invalid as expected")

            finally:
                os.unlink(csv_f.name)
                os.unlink(schema_f.name)

if __name__ == "__main__":
    test_line_706()
