"""Line parsing and field extraction logic for FWF processing."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from ..config import FwfFieldSpec, FwfInputConfig
from .converters import FwfTypeConverter, FwfValueProcessor
from .detectors import FwfSchemaDetector


class FwfFieldExtractor:
    """Handles field extraction from FWF lines."""

    @staticmethod
    def extract_field_value(line: str, field: FwfFieldSpec, trim_whitespace: bool = True) -> str:
        """Extract and process a field value from a line.

        Args:
            line: The input line to extract from
            field: Field specification
            trim_whitespace: Global trimming switch (``FwfInputConfig.trim_whitespace``); a
                field is only trimmed when both this and ``field.trim`` are true

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
        if field.trim and trim_whitespace:
            raw_value = raw_value.strip()

        # Remove padding characters based on alignment - handle edge cases
        if field.align == "right" and field.pad != " ":
            # Strip leading pad characters, but preserve at least one character
            # if all are pad chars
            stripped = raw_value.lstrip(field.pad)
            if not stripped and raw_value:
                # Keep one pad character if that's all we have
                raw_value = field.pad
            else:
                raw_value = stripped
        elif field.align == "left" and field.pad != " ":
            raw_value = raw_value.rstrip(field.pad)

        return raw_value


class FwfLineParser:
    """Handles parsing of individual FWF lines.

    Problems are collected rather than raised or silently swallowed:

    * ``errors``: one entry per field value that could not be converted to its declared type
      (``{"line_number", "field", "error": "invalid_value", "type"}``). The field becomes null.
    * ``rejected``: one entry per line that was dropped because no conditional schema matches
      its flag (``"error": "no_matching_schema"``) or a required field is blank
      (``"error": "required_missing"``, with the field name).

    Entries carry line numbers and field names only, never the offending data.
    """

    def __init__(self, config: FwfInputConfig):
        """Initialize the line parser.

        Args:
            config: FWF configuration
        """
        self.config = config
        self.schema_detector = FwfSchemaDetector(config)
        self.field_extractor = FwfFieldExtractor()
        self.errors: List[Dict[str, Any]] = []
        self.rejected: List[Dict[str, Any]] = []

    def reset(self) -> None:
        """Forget the problems collected so far (called at the start of each file)."""
        self.errors.clear()
        self.rejected.clear()

    def parse_line(self, line: str, line_number: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Parse a single line according to the FWF configuration.

        Args:
            line: Line to parse
            line_number: 1-based line number, used when recording errors

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
                # No matching conditional schema found: record the rejection, don't lose it
                self.rejected.append({"line_number": line_number, "error": "no_matching_schema"})
                return None

        if not fields_to_use:
            return None

        # Extract field values
        result = {}
        for field in fields_to_use:
            raw_value = self.field_extractor.extract_field_value(
                line, field, self.config.trim_whitespace
            )

            # Process null values
            processed_value = FwfValueProcessor.process_null_values(
                raw_value, field.name, self.config.null_values
            )

            if field.required and (processed_value is None or processed_value == ""):
                self.rejected.append(
                    {"line_number": line_number, "field": field.name, "error": "required_missing"}
                )
                return None

            # Convert to appropriate type
            if processed_value is not None:
                converted_value, ok = FwfTypeConverter.convert_value_checked(
                    processed_value, field.parquet_type
                )
                if not ok:
                    self.errors.append(
                        {
                            "line_number": line_number,
                            "field": field.name,
                            "error": "invalid_value",
                            "type": field.parquet_type,
                        }
                    )
            else:
                converted_value = None

            result[field.name] = converted_value

        # The flag column identifies the record type; populate it even when the matched
        # schema does not list it among its own fields (the Arrow schema always has it).
        flag_column = self.config.flag_column
        if self.config.conditional_schemas and flag_column and flag_column.name not in result:
            flag_raw = self.field_extractor.extract_field_value(
                line, flag_column, self.config.trim_whitespace
            )
            result[flag_column.name] = FwfTypeConverter.convert_value(
                flag_raw, flag_column.parquet_type
            )

        return result
