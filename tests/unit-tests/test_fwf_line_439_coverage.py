"""Ultimate final test to achieve 100% coverage for fwf.py - targeting line 439."""

from unittest.mock import MagicMock, patch

import pytest

from forklift.inputs.config import (FwfConditionalSchema, FwfFieldSpec,
                                    FwfInputConfig)
from forklift.inputs.fwf import FwfInputHandler


class TestFwfLine439Coverage:
    """Ultimate test to cover line 439 for 100% coverage."""

    def test_parse_line_fields_to_use_none_scenario(self):
        """Test the exact scenario where fields_to_use becomes None (line 439)."""
        # Create a configuration that will cause fields_to_use to be None
        # This happens when we have conditional schemas but no matching schema is found

        flag_column = FwfFieldSpec("record_type", 1, 1, parquet_type="string")
        conditional_schemas = [
            FwfConditionalSchema(
                "A",
                "Type A Records",
                [
                    FwfFieldSpec("record_type", 1, 1, parquet_type="string"),
                    FwfFieldSpec("field1", 2, 10, parquet_type="string"),
                ],
            )
        ]

        # Create config with ONLY conditional schemas (no simple fields)
        config = FwfInputConfig(
            fields=None,  # Explicitly no simple fields
            flag_column=flag_column,
            conditional_schemas=conditional_schemas,
        )
        handler = FwfInputHandler(config)

        # Parse a line with a flag that doesn't match any conditional schema
        # This will cause:
        # 1. fields_to_use starts as self.config.fields (which is None)
        # 2. detect_conditional_schema returns None (no match)
        # 3. fields_to_use remains None
        # 4. The "if not fields_to_use:" check on line 439 triggers

        result = handler.parse_line("X1234567890")  # Flag 'X' doesn't match schema 'A'
        assert result is None  # Should hit line 439: return None

    def test_parse_line_conditional_schema_no_match_alternative(self):
        """Alternative test to ensure we hit line 439."""
        # Another approach: use conditional schemas where the flag extraction fails

        flag_column = FwfFieldSpec("flag", 1, 1, parquet_type="string")
        conditional_schemas = [
            FwfConditionalSchema(
                "Z",
                "Z Records",
                [
                    FwfFieldSpec("flag", 1, 1, parquet_type="string"),
                    FwfFieldSpec("data", 2, 5, parquet_type="string"),
                ],
            )
        ]

        config = FwfInputConfig(flag_column=flag_column, conditional_schemas=conditional_schemas)
        handler = FwfInputHandler(config)

        # Test with a line that has a different flag
        result = handler.parse_line("Y1234")  # Flag 'Y' doesn't match 'Z'
        assert result is None  # Should trigger line 439
