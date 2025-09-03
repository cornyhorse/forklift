"""Final precision test to achieve 100% coverage for fwf.py."""

import pytest
from unittest.mock import patch, MagicMock

from forklift.inputs.fwf import FwfInputHandler
from forklift.inputs.config import FwfInputConfig, FwfFieldSpec, FwfConditionalSchema


class TestFwfAbsoluteFinalCoverage:
    """Precision tests to cover the exact remaining lines for 100% coverage."""

    def test_convert_value_exception_return_original_value(self):
        """Test line 295: exception handling returns original value."""
        config = FwfInputConfig(fields=[
            FwfFieldSpec("test", 1, 5, parquet_type="uint8")
        ])
        handler = FwfInputHandler(config)

        # Force a ValueError in the uint type conversion to hit exception handling
        with patch('builtins.int') as mock_int:
            mock_int.side_effect = ValueError("Cannot convert")
            result = handler.convert_value("invalid", "uint8")
            # This should hit line 295: return value in except block
            assert result == "invalid"

    def test_parse_line_empty_fields_return_none(self):
        """Test line 439: return None when fields_to_use is empty."""
        # Create a scenario where fields_to_use will be None
        flag_column = FwfFieldSpec("flag", 1, 1, parquet_type="string")
        conditional_schemas = [
            FwfConditionalSchema("A", "Schema A", [
                FwfFieldSpec("flag", 1, 1, parquet_type="string"),
                FwfFieldSpec("data", 2, 5, parquet_type="string")
            ])
        ]

        config = FwfInputConfig(
            flag_column=flag_column,
            conditional_schemas=conditional_schemas
        )
        handler = FwfInputHandler(config)

        # Mock detect_conditional_schema to return None, ensuring fields_to_use is None
        with patch.object(handler, 'detect_conditional_schema') as mock_detect:
            mock_detect.return_value = None

            # This should cause fields_to_use to be None and hit line 439
            result = handler.parse_line("Z")
            assert result is None
