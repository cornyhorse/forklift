"""Targeted test for forklift_core.py missing line 706 - empty valid batch creation."""
import pytest
import tempfile
import os
import json
from pathlib import Path
from unittest.mock import patch, MagicMock

from forklift.engine.forklift_core import ForkliftCore, ImportConfig, HeaderMode


class TestForkliftCoreLine706Coverage:
    """Test suite targeting the specific missing line 706 in batch validation."""

    def test_batch_validation_all_rows_invalid(self):
        """Test batch validation where all rows are invalid, triggering empty valid batch (line 706)."""
        # Create CSV with data where ALL rows have missing required integer values (which become null)
        csv_content = """name,age,city
John,,NYC
Jane,,Chicago
Bob,,LA
"""

        # Create a schema with required fields (non-nullable) - age is required and will be null
        schema_content = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"},
                "city": {"type": "string"}
            },
            "required": ["name", "age", "city"]  # All fields are required (non-nullable)
        }

        # Use regular files instead of NamedTemporaryFile to avoid I/O handler issues
        import tempfile
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

            config = ImportConfig(
                input_path=csv_path,
                output_path=output_path,
                schema_file=schema_path,
                header_mode=HeaderMode.PRESENT,
                validate_schema=True,
                max_validation_errors=100
            )

            core = ForkliftCore(config)

            # Verify schema loads correctly
            core.schema = core._load_schema()
            assert core.schema is not None, "Schema should be loaded"
            assert len(core.schema) == 3, "Schema should have 3 fields"

            # Verify that age field is non-nullable (required)
            age_field = None
            for field in core.schema:
                if field.name == "age":
                    age_field = field
                    break
            assert age_field is not None, "Age field should exist"
            assert not age_field.nullable, "Age field should be non-nullable (required)"

            # This should cause all rows to fail validation (missing age values),
            # triggering line 706 to create an empty valid batch
            result = core.process_csv()

            # Should complete processing even with all invalid rows
            assert result is not None
            # All rows should be invalid due to null values in required age field
            assert result.invalid_rows == 3, f"Expected 3 invalid rows, got {result.invalid_rows}"
            assert result.valid_rows == 0, f"Expected 0 valid rows, got {result.valid_rows}"

        finally:
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_batch_validation_mixed_valid_invalid(self):
        """Test batch validation with some valid and some invalid rows."""
        # Create CSV with mix of valid and invalid data (some rows have missing age values)
        csv_content = """name,age,city
John,25,NYC
Jane,,LA
Bob,35,Chicago
Alice,,ValidCity
"""

        schema_content = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"},
                "city": {"type": "string"}
            },
            "required": ["name", "age", "city"]  # All fields are required (non-nullable)
        }

        # Use regular files instead of NamedTemporaryFile
        import tempfile
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

            config = ImportConfig(
                input_path=csv_path,
                output_path=output_path,
                schema_file=schema_path,
                header_mode=HeaderMode.PRESENT,
                validate_schema=True,
                max_validation_errors=100
            )

            core = ForkliftCore(config)

            # This should have both valid and invalid rows
            result = core.process_csv()

            assert result is not None
            assert result.valid_rows == 2, f"Expected 2 valid rows, got {result.valid_rows}"    # John and Bob should be valid
            assert result.invalid_rows == 2, f"Expected 2 invalid rows, got {result.invalid_rows}"  # Jane and Alice should be invalid

        finally:
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_batch_validation_no_schema_all_valid(self):
        """Test processing without schema validation (all rows should be valid)."""
        csv_content = """name,age,city
John,25,NYC
Jane,30,LA
Bob,35,Chicago
"""

        # Use regular files instead of NamedTemporaryFile
        import tempfile
        temp_dir = tempfile.mkdtemp()
        csv_path = os.path.join(temp_dir, "test_data.csv")
        output_path = os.path.join(temp_dir, "output")

        try:
            # Write files
            with open(csv_path, 'w') as f:
                f.write(csv_content)

            config = ImportConfig(
                input_path=csv_path,
                output_path=output_path,
                header_mode=HeaderMode.PRESENT,
                validate_schema=False  # No validation
            )

            core = ForkliftCore(config)

            # Without validation, all rows should be valid
            result = core.process_csv()

            assert result is not None
            assert result.valid_rows >= 3
            assert result.invalid_rows == 0

        finally:
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)
