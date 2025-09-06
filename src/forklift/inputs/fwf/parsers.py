"""Line parsing and field extraction logic for FWF processing."""

from __future__ import annotations
from typing import Dict, Any, Optional, List

from ..config import FwfInputConfig, FwfFieldSpec
from .converters import FwfTypeConverter, FwfValueProcessor
from .detectors import FwfSchemaDetector


class FwfFieldExtractor:
    """Handles field extraction from FWF lines."""

    @staticmethod
    def extract_field_value(line: str, field: FwfFieldSpec) -> str:
        """Extract and process a field value from a line.

        Args:
            line: The input line to extract from
            field: Field specification

        Returns:
            Processed field value as string
        """
        # Convert 1-based to 0-based indexing
        start_idx = field.start - 1
        end_idx = start_idx + field.length

        # Extract the raw field value, handling short lines
        if start_idx >= len(line):
            raw_value = ""
        else:
            raw_value = line[start_idx:end_idx]

        # Pad if necessary
        if len(raw_value) < field.length:
            if field.align == "right":
                raw_value = field.pad * (field.length - len(raw_value)) + raw_value
            elif field.align == "center":
                padding_needed = field.length - len(raw_value)
                left_pad = padding_needed // 2
                right_pad = padding_needed - left_pad
                raw_value = field.pad * left_pad + raw_value + field.pad * right_pad
            else:  # left alignment
                raw_value = raw_value + field.pad * (field.length - len(raw_value))

        # Trim whitespace if configured
        if field.trim:
            raw_value = raw_value.strip()

        # Remove padding characters based on alignment - handle edge cases
        if field.align == "right" and field.pad != " ":
            # Strip leading pad characters, but preserve at least one character if all are pad chars
            stripped = raw_value.lstrip(field.pad)
            if not stripped and raw_value:
                raw_value = field.pad  # Keep one pad character if that's all we have
            else:
                raw_value = stripped
        elif field.align == "left" and field.pad != " ":
            raw_value = raw_value.rstrip(field.pad)

        return raw_value


class FwfLineParser:
    """Handles parsing of individual FWF lines."""

    def __init__(self, config: FwfInputConfig):
        """Initialize the line parser.

        Args:
            config: FWF configuration
        """
        self.config = config
        self.schema_detector = FwfSchemaDetector(config)
        self.field_extractor = FwfFieldExtractor()

    def parse_line(self, line: str) -> Optional[Dict[str, Any]]:
        """Parse a single line according to the FWF configuration.

        Args:
            line: Line to parse

        Returns:
            Dictionary of field values or None if line should be skipped
        """
        # Skip blank lines if configured
        if self.config.skip_blank_lines and self.schema_detector.is_blank_line(line):
            return None

        # Skip comment lines
        if self.schema_detector.is_comment_line(line):
            return None

        # Skip footer lines
        if self.schema_detector.is_footer_row(line):
            return None

        # Determine which fields to use
        fields_to_use = self.config.fields

        # Handle conditional schemas
        if self.config.conditional_schemas:
            conditional_schema = self.schema_detector.detect_conditional_schema(line)
            if conditional_schema:
                fields_to_use = conditional_schema.fields
            else:
                # No matching conditional schema found
                return None

        if not fields_to_use:
            return None

        # Extract field values
        result = {}
        for field in fields_to_use:
            raw_value = self.field_extractor.extract_field_value(line, field)

            # Process null values
            processed_value = FwfValueProcessor.process_null_values(
                raw_value, field.name, self.config.null_values
            )

            # Convert to appropriate type
            if processed_value is not None:
                converted_value = FwfTypeConverter.convert_value(processed_value, field.parquet_type)
            else:
                converted_value = None

            result[field.name] = converted_value

        return result
