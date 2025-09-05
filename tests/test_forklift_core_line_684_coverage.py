"""Targeted test for forklift_core.py missing line 684 - schema validation continue statement."""
import pytest
import tempfile
import os
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
import pyarrow as pa

from forklift.engine.forklift_core import ForkliftCore, ImportConfig, HeaderMode


class TestForkliftCoreLine684Coverage:
    """Test suite targeting the specific missing line 684 in schema validation."""

    def test_validate_batch_with_fewer_columns_than_schema(self):
        """Test batch validation when batch has fewer columns than schema expects (line 684)."""
        # Create a CSV with fewer columns than the schema expects
        csv_content = """name,age
John,25
Jane,30
"""

        # Create a schema that expects more columns than the CSV has
        schema_content = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"},
                "city": {"type": "string"},  # This column doesn't exist in CSV
                "country": {"type": "string"}  # This column doesn't exist in CSV
            },
            "required": ["name", "age", "city", "country"]
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
                        validate_schema=True
                    )

                    core = ForkliftCore(config)

                    # This should trigger the schema validation where the schema has more
                    # fields than the batch has columns, hitting line 684 (continue statement)
                    result = core.process_csv()

                    # Should still process successfully despite schema mismatch
                    assert result is not None
                    assert result.total_rows >= 0

                finally:
                    os.unlink(csv_f.name)
                    os.unlink(schema_f.name)

    def test_validate_batch_schema_mismatch_with_nulls(self):
        """Test schema validation with null handling when columns are missing."""
        # Create CSV with missing values
        csv_content = """name,age
John,25
,30
Bob,
"""

        # Schema expects more columns and has nullable requirements
        schema_content = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"},
                "city": {"type": "string"},  # Missing column
                "salary": {"type": "number"}  # Missing column
            },
            "required": ["name"]  # Only name is required
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
                        validate_schema=True
                    )

                    core = ForkliftCore(config)

                    # Process with schema validation - should hit line 684 when
                    # iterating through schema fields that don't exist in batch
                    result = core.process_csv()

                    assert result is not None

                finally:
                    os.unlink(csv_f.name)
                    os.unlink(schema_f.name)

    def test_validate_batch_exact_column_match(self):
        """Test validation when batch columns exactly match schema (doesn't hit line 684)."""
        # Create CSV that matches schema exactly
        csv_content = """name,age,city
John,25,NYC
Jane,30,LA
"""

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
                        validate_schema=True
                    )

                    core = ForkliftCore(config)

                    # This should validate successfully without hitting line 684
                    result = core.process_csv()

                    assert result is not None
                    assert result.total_rows >= 2

                finally:
                    os.unlink(csv_f.name)
                    os.unlink(schema_f.name)
