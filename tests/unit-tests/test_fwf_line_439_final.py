"""Final test to hit line 439 specifically for 100% coverage."""

import pytest
from unittest.mock import patch

from forklift.inputs.fwf import FwfInputHandler
from forklift.inputs.config import FwfInputConfig, FwfFieldSpec


class TestFwfLine439Final:
    """Target line 439 specifically for 100% coverage."""

    def test_parse_line_simple_fields_becomes_none(self):
        """Test scenario where simple fields exists but becomes None, hitting line 439."""
        # Create config with simple fields
        config = FwfInputConfig(fields=[
            FwfFieldSpec("test", 1, 5, parquet_type="string")
        ])
        handler = FwfInputHandler(config)

        # Mock the config.fields to be None after initialization
        # This simulates a scenario where fields_to_use starts as self.config.fields (None)
        # but we don't have conditional schemas to override it
        original_fields = handler.config.fields
        handler.config.fields = None
        handler.config.conditional_schemas = None

        # This should cause fields_to_use to be None and hit line 439
        result = handler.parse_line("test ")
        assert result is None

        # Restore original fields
        handler.config.fields = original_fields

    def test_parse_line_empty_fields_list(self):
        """Test with empty fields list to hit line 439."""
        # Create config with valid fields first to pass validation
        config = FwfInputConfig(fields=[
            FwfFieldSpec("test", 1, 5, parquet_type="string")
        ])
        handler = FwfInputHandler(config)

        # After initialization, modify the config to have empty fields
        # This simulates a scenario where fields_to_use becomes empty
        handler.config.fields = []
        handler.config.conditional_schemas = None

        # This should make fields_to_use an empty list, hitting line 439
        result = handler.parse_line("test")
        assert result is None
