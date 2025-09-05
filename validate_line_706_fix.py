#!/usr/bin/env python3
"""Validation script to verify that line 706 coverage fix is working"""

import tempfile
import json
import os
import shutil
from forklift.engine.forklift_core import ForkliftCore, ImportConfig, HeaderMode

def test_line_706_fix():
    print("Testing line 706 coverage fix...")

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

    # Use regular files instead of NamedTemporaryFile
    temp_dir = tempfile.mkdtemp()
    csv_path = os.path.join(temp_dir, "test_data.csv")
    schema_path = os.path.join(temp_dir, "test_schema.json")
    output_path = os.path.join(temp_dir, "output")

    try:
        # Write files
        with open(csv_path, 'w') as f:
            f.write(csv_content)

        with open(schema_path, 'w') as f:
            json.dump(schema_content, f)

        print(f"Created test files in: {temp_dir}")
        print(f"CSV path: {csv_path}")
        print(f"Schema path: {schema_path}")
        print(f"Files exist: CSV={os.path.exists(csv_path)}, Schema={os.path.exists(schema_path)}")

        config = ImportConfig(
            input_path=csv_path,
            output_path=output_path,
            schema_file=schema_path,
            header_mode=HeaderMode.PRESENT,
            validate_schema=True,
            max_validation_errors=100
        )

        print("Config created successfully")
        core = ForkliftCore(config)
        print("ForkliftCore created successfully")

        # Manually load schema and verify
        try:
            core.schema = core._load_schema()
            print(f"Schema loaded: {core.schema is not None}")
            if core.schema:
                print(f"Schema fields: {[f.name + ':' + str(f.type) + ':nullable=' + str(f.nullable) for f in core.schema]}")

                # Verify age field is non-nullable
                age_field = None
                for field in core.schema:
                    if field.name == "age":
                        age_field = field
                        break

                if age_field and not age_field.nullable:
                    print("✓ Age field is correctly non-nullable (required)")
                else:
                    print("✗ Age field nullable setting is incorrect")
            else:
                print("✗ Schema failed to load")
                return False
        except Exception as e:
            print(f"✗ Schema loading failed with error: {e}")
            return False

        # Process CSV
        print("Processing CSV...")
        result = core.process_csv()
        print("Processing completed")

        print(f"Total rows: {result.total_rows}")
        print(f"Valid rows: {result.valid_rows}")
        print(f"Invalid rows: {result.invalid_rows}")

        # Verify results
        if result.invalid_rows == 3 and result.valid_rows == 0:
            print("✓ SUCCESS: All rows invalid - line 706 should now be hit!")
            return True
        else:
            print(f"✗ ISSUE: Expected 3 invalid, 0 valid rows. Got {result.invalid_rows} invalid, {result.valid_rows} valid")
            return False

    except Exception as e:
        print(f"✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
        print(f"Cleaned up temp directory: {temp_dir}")

if __name__ == "__main__":
    success = test_line_706_fix()
    if success:
        print("\n🎉 Line 706 coverage fix appears to be working!")
    else:
        print("\n❌ Line 706 coverage fix needs more work.")
