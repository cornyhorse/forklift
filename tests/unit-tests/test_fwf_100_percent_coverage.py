"""Absolute final test to achieve 100% coverage for fwf.py."""

import pytest
from unittest.mock import patch

from forklift.inputs.fwf import FwfInputHandler
from forklift.inputs.config import FwfInputConfig, FwfFieldSpec, FwfConditionalSchema


class TestFwf100PercentCoverage:
    """Final test to cover the last 2 remaining lines for 100% coverage."""

    def test_convert_value_exception_handling_line_295(self):
        """Test the except block in convert_value method (line 295)."""
        config = FwfInputConfig(fields=[
            FwfFieldSpec("test", 1, 5, parquet_type="int64")
        ])
        handler = FwfInputHandler(config)

        # Force both ValueError and TypeError to ensure we hit the exception handling
        with patch('builtins.int', side_effect=ValueError("Conversion failed")):
            result = handler.convert_value("invalid", "int64")
            assert result == "invalid"  # Should return original value on exception

    def test_parse_line_no_fields_scenario_line_439(self):
        """Test parse_line when fields_to_use is None (line 439)."""
        # Create config with conditional schemas only (no simple fields)
        flag_column = FwfFieldSpec("type", 1, 1, parquet_type="string")
        conditional_schemas = [
            FwfConditionalSchema("A", "Schema A", [
                FwfFieldSpec("type", 1, 1, parquet_type="string"),
                FwfFieldSpec("data", 2, 5, parquet_type="string")
            ])
        ]

        config = FwfInputConfig(
            flag_column=flag_column,
            conditional_schemas=conditional_schemas
        )
        handler = FwfInputHandler(config)

        # Test with a line that doesn't match any conditional schema
        # This should cause fields_to_use to remain None, hitting line 439
        result = handler.parse_line("X12345")  # Flag 'X' doesn't match schema 'A'
        assert result is None
