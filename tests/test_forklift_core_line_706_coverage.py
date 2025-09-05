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
        # Create a CSV with data that will fail validation
        csv_content = """name,age,city
invalid_name_123456789012345678901234567890,invalid_age,invalid_city_123456789012345678901234567890
another_invalid_name_123456789012345678901234567890,also_invalid_age,another_invalid_city_123456789012345678901234567890
"""

        # Create a strict schema that will cause all rows to fail validation
        schema_content = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "maxLength": 10,  # Very restrictive - will fail
                    "pattern": "^[A-Za-z]+$"  # Only letters
                },
                "age": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 120
                },
                "city": {
                    "type": "string",
                    "maxLength": 15,  # Will fail due to long city names
                    "pattern": "^[A-Za-z ]+$"  # Only letters and spaces
                }
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
                        max_validation_errors=100  # Allow processing to continue
                    )

                    core = ForkliftCore(config)

                    # This should cause all rows to fail validation,
                    # triggering line 706 to create an empty valid batch
                    result = core.process_csv()

                    # Should complete processing even with all invalid rows
                    assert result is not None
                    # All rows should be invalid
                    assert result.invalid_rows > 0

                finally:
                    os.unlink(csv_f.name)
                    os.unlink(schema_f.name)

    def test_batch_validation_mixed_valid_invalid(self):
        """Test batch validation with some valid and some invalid rows."""
        # Create CSV with mix of valid and invalid data
        csv_content = """name,age,city
John,25,NYC
InvalidNameTooLong123456789,30,LA
Jane,invalid_age,Chicago
Bob,35,ValidCity
"""

        schema_content = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "maxLength": 10,
                    "pattern": "^[A-Za-z]+$"
                },
                "age": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 120
                },
                "city": {
                    "type": "string",
                    "maxLength": 20,
                    "pattern": "^[A-Za-z]+$"
                }
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

                    core = ForkliftCore(config)

                    # This should have both valid and invalid rows
                    result = core.process_csv()

                    assert result is not None
                    assert result.valid_rows > 0
                    assert result.invalid_rows > 0

                finally:
                    os.unlink(csv_f.name)
                    os.unlink(schema_f.name)

    def test_batch_validation_no_schema_all_valid(self):
        """Test processing without schema validation (all rows should be valid)."""
        csv_content = """name,age,city
John,25,NYC
Jane,30,LA
Bob,35,Chicago
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as csv_f:
            csv_f.write(csv_content)
            csv_f.flush()

            try:
                config = ImportConfig(
                    input_path=csv_f.name,
                    output_path=tempfile.mkdtemp(),
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
                os.unlink(csv_f.name)
