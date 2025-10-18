"""Final targeted test to achieve 100% coverage for fwf.py."""

import re
from unittest.mock import MagicMock, patch

import pytest

from forklift.inputs.config import FwfConditionalSchema, FwfFieldSpec, FwfInputConfig
from forklift.inputs.fwf import FwfInputHandler


class TestFwfPerfectCoverage:
    """Tests to cover the final 3 remaining lines for 100% coverage."""

    def test_convert_value_with_value_error(self):
        """Test convert_value with ValueError exception (line 295)."""
        config = FwfInputConfig(fields=[FwfFieldSpec("test", 1, 5, parquet_type="int64")])
        handler = FwfInputHandler(config)

        # Force a ValueError by mocking int() to raise a ValueError
        with patch("builtins.int", side_effect=ValueError("Invalid literal")):
            result = handler.convert_value("abc", "int64")
            # Should return original value when conversion fails with ValueError
            assert result == "abc"

    def test_comment_pattern_no_match(self):
        """Test comment pattern that doesn't match (line 335)."""
        config = FwfInputConfig(
            fields=[FwfFieldSpec("test", 1, 5, parquet_type="string")], comment_patterns=[r"^#.*"]
        )
        handler = FwfInputHandler(config)

        # Test line that doesn't match the comment pattern
        assert handler.is_comment_line("This is not a comment") is False
        # This should execute the "return False" line (335)

    def test_parse_line_with_simple_fields_none(self):
        """Test parse_line when fields_to_use becomes None (line 439)."""
        # Create a handler with conditional schemas only
        flag_column = FwfFieldSpec("type", 1, 1, parquet_type="string")
        conditional_schemas = [
            FwfConditionalSchema(
                "A",
                "Schema A",
                [
                    FwfFieldSpec("type", 1, 1, parquet_type="string"),
                    FwfFieldSpec("field1", 2, 5, parquet_type="string"),
                ],
            )
        ]
        config = FwfInputConfig(flag_column=flag_column, conditional_schemas=conditional_schemas)
        handler = FwfInputHandler(config)

        # Mock detect_conditional_schema to return None
        with patch.object(handler, "detect_conditional_schema", return_value=None):
            # This will make fields_to_use None since we have conditional schemas but no match
            # and no simple fields (config.fields is None)
            result = handler.parse_line("X12345")
            assert result is None  # Should hit the "if not fields_to_use:" check (line 439)
