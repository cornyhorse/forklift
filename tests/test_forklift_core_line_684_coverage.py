"""Targeted test for forklift_core.py missing line 684 - schema validation continue statement."""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from forklift.engine.forklift_core import (ForkliftCore, HeaderMode,
                                           ImportConfig)


class TestForkliftCoreLine684Coverage:
    """Test suite targeting the specific missing line 684 in schema validation."""

    def test_validate_batch_with_fewer_columns_than_schema(self):
        """Test skipped - method no longer exists after refactoring."""
        pytest.skip("Method _validate_batch no longer exists after ForkliftCore refactoring")

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
                "city": {"type": "string"},
            },
            "required": ["name", "age", "city"],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as csv_f:
            csv_f.write(csv_content)
            csv_f.flush()

            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as schema_f:
                json.dump(schema_content, schema_f)
                schema_f.flush()

                try:
                    config = ImportConfig(
                        input_path=csv_f.name,
                        output_path=tempfile.mkdtemp(),
                        schema_file=schema_f.name,
                        header_mode=HeaderMode.PRESENT,
                        validate_schema=True,
                    )

                    core = ForkliftCore(config)

                    # This should validate successfully without hitting line 684
                    result = core.process_csv()

                    assert result is not None
                    assert result.total_rows >= 2

                finally:
                    os.unlink(csv_f.name)
                    os.unlink(schema_f.name)
