"""Main FWF input handler with simplified responsibilities."""

from __future__ import annotations
from typing import Dict, Any, List, Optional
from pathlib import Path
import pyarrow as pa

from ..config import FwfInputConfig, FwfFieldSpec, FwfConditionalSchema
from .validators import FwfConfigValidator
from .converters import FwfTypeConverter, FwfValueProcessor
from .detectors import FwfEncodingDetector, FwfSchemaDetector
from .parsers import FwfLineParser, FwfFieldExtractor


class FwfInputHandler:
    """Handles fixed-width file input with field extraction and validation.

    This class provides functionality for reading fixed-width files with various
    configurations including conditional schemas, field validation, and data type
    conversion.

    Args:
        config: FwfInputConfig instance with processing configuration

    Attributes:
        config: The configuration object for this input handler
    """

    def __init__(self, config: FwfInputConfig):
        """Initialize the FWF input handler.

        Args:
            config: Configuration object containing FWF processing parameters

        Raises:
            ValueError: If configuration is invalid
        """
        self.config = config
        FwfConfigValidator.validate_config(config)
        self.line_parser = FwfLineParser(config)
        self.schema_detector = FwfSchemaDetector(config)
        self.field_extractor = FwfFieldExtractor()

    # Backward compatibility methods - delegate to appropriate components

    def extract_field_value(self, line: str, field: FwfFieldSpec) -> str:
        """Extract and process a field value from a line.

        Args:
            line: The input line to extract from
            field: Field specification

        Returns:
            Processed field value as string
        """
        return self.field_extractor.extract_field_value(line, field)

    def parse_line(self, line: str) -> Optional[Dict[str, Any]]:
        """Parse a single line according to the FWF configuration.

        Args:
            line: Line to parse

        Returns:
            Dictionary of field values or None if line should be skipped
        """
        return self.line_parser.parse_line(line)

    def convert_value(self, value: str, parquet_type: str) -> Any:
        """Convert string value to appropriate Python type.

        Args:
            value: String value to convert
            parquet_type: Target Parquet data type

        Returns:
            Converted value
        """
        return FwfTypeConverter.convert_value(value, parquet_type)

    def convert_field_value(self, value: str, field: FwfFieldSpec) -> Any:
        """Convert field value to appropriate type based on field specification.

        Args:
            value: String value to convert
            field: Field specification

        Returns:
            Converted value
        """
        return FwfTypeConverter.convert_field_value(value, field)

    def process_null_values(self, value: str, field_name: str) -> Optional[str]:
        """Process null values according to configuration.

        Args:
            value: The field value to check
            field_name: Name of the field

        Returns:
            None if value should be treated as null, otherwise the original value
        """
        return FwfValueProcessor.process_null_values(value, field_name, self.config.null_values)

    def detect_encoding(self, file_path: Path) -> str:
        """Detect file encoding using chardet library.

        Args:
            file_path: Path to file to analyze

        Returns:
            Detected encoding string (defaults to utf-8 if detection fails)
        """
        return FwfEncodingDetector.detect_encoding(file_path)

    def detect_conditional_schema(self, line: str) -> Optional[FwfConditionalSchema]:
        """Detect which conditional schema applies to a line.

        Args:
            line: Line to analyze

        Returns:
            Matching conditional schema or None
        """
        return self.schema_detector.detect_conditional_schema(line)

    def determine_schema(self, line: str) -> Optional[FwfConditionalSchema]:
        """Determine which schema to use for a line (alias for detect_conditional_schema).

        Args:
            line: Line to analyze

        Returns:
            Matching conditional schema or None
        """
        return self.schema_detector.determine_schema(line)

    def is_comment_line(self, line: str) -> bool:
        """Check if a line is a comment based on configured patterns.

        Args:
            line: Line to check

        Returns:
            True if line matches a comment pattern
        """
        return self.schema_detector.is_comment_line(line)

    def is_comment_row(self, line: str) -> bool:
        """Alias for is_comment_line for backward compatibility."""
        return self.schema_detector.is_comment_row(line)

    def is_blank_line(self, line: str) -> bool:
        """Check if a line is blank or whitespace-only.

        Args:
            line: Line to check

        Returns:
            True if line is blank
        """
        return self.schema_detector.is_blank_line(line)

    def is_footer_row(self, line: str) -> bool:
        """Check if a line is a footer based on configured patterns.

        Args:
            line: Line to check

        Returns:
            True if line matches footer detection pattern
        """
        return self.schema_detector.is_footer_row(line)

    def _get_arrow_type(self, parquet_type: str) -> pa.DataType:
        """Convert parquet type string to PyArrow type.

        Args:
            parquet_type: String representation of the type

        Returns:
            PyArrow DataType object
        """
        return FwfTypeConverter.get_arrow_type(parquet_type)

    def get_arrow_schema(self) -> pa.Schema:
        """Generate PyArrow schema from field specifications.

        Returns:
            PyArrow Schema object
        """
        fields = []
        unique_fields = {}  # Use dict to avoid duplicates while preserving order

        # Handle simple fields if available
        if self.config.fields:
            for field_spec in self.config.fields:
                arrow_type = FwfTypeConverter.get_arrow_type(field_spec.parquet_type)
                unique_fields[field_spec.name] = pa.field(field_spec.name, arrow_type)

        # Handle conditional schemas - collect all unique fields from all schemas
        if self.config.conditional_schemas:
            # Add flag column if present
            if self.config.flag_column:
                arrow_type = FwfTypeConverter.get_arrow_type(self.config.flag_column.parquet_type)
                unique_fields[self.config.flag_column.name] = pa.field(self.config.flag_column.name, arrow_type)

            # Add all fields from all conditional schemas
            for schema in self.config.conditional_schemas:
                for field_spec in schema.fields:
                    if field_spec.name not in unique_fields:
                        arrow_type = FwfTypeConverter.get_arrow_type(field_spec.parquet_type)
                        unique_fields[field_spec.name] = pa.field(field_spec.name, arrow_type)

        # Convert to list maintaining order
        fields = list(unique_fields.values())

        # Add metadata fields
        fields.extend([
            pa.field('__line_number__', pa.int64()),
            pa.field('__source_file__', pa.string())
        ])

        return pa.schema(fields)

    def read_file(self, file_path: Path) -> List[Dict[str, Any]]:
        """Read and parse an entire FWF file.

        Args:
            file_path: Path to the FWF file

        Returns:
            List of parsed records

        Raises:
            FileNotFoundError: If file doesn't exist
            UnicodeDecodeError: If file can't be decoded with specified encoding
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Handle auto encoding detection
        encoding = self.config.encoding
        if encoding == "auto":
            encoding = FwfEncodingDetector.detect_encoding(file_path)

        records = []

        with open(file_path, 'r', encoding=encoding) as f:
            for line_num, line in enumerate(f, 1):
                # Remove newline characters
                line = line.rstrip('\r\n')

                try:
                    parsed_record = self.line_parser.parse_line(line)
                    if parsed_record is not None:
                        # Add metadata fields
                        parsed_record['__line_number__'] = line_num
                        parsed_record['__source_file__'] = str(file_path)
                        records.append(parsed_record)
                except Exception:
                    # Handle parsing exceptions gracefully - continue processing
                    continue

        return records

    def create_arrow_table(self, file_path: Path) -> pa.Table:
        """Create a PyArrow table from an FWF file.

        Args:
            file_path: Path to the FWF file

        Returns:
            PyArrow Table containing the parsed data

        Raises:
            FileNotFoundError: If file doesn't exist
        """
        # Read the file and get records
        records = self.read_file(file_path)

        if not records:
            # Return empty table with proper schema
            schema = self.get_arrow_schema()
            empty_arrays = []
            for field in schema:
                empty_arrays.append(pa.array([], type=field.type))
            return pa.table(empty_arrays, schema=schema)

        # Get schema
        schema = self.get_arrow_schema()

        # Prepare column data
        columns = {}
        for field in schema:
            columns[field.name] = []

        # Fill columns with data
        for record in records:
            for field in schema:
                value = record.get(field.name)

                # Handle type conversion for PyArrow
                if value is not None and field.type != pa.string():
                    try:
                        if field.type == pa.int64():
                            value = int(value) if value != "" else None
                        elif field.type == pa.float64():
                            value = float(value) if value != "" else None
                        elif field.type == pa.bool_():
                            value = bool(value) if value != "" else None
                        elif str(field.type).startswith('decimal128'):
                            try:
                                value = float(value) if value != "" else None
                            except (ValueError, TypeError):
                                value = None
                    except (ValueError, TypeError):
                        value = None

                columns[field.name].append(value)

        # Create arrays
        arrays = []
        for field in schema:
            try:
                arrays.append(pa.array(columns[field.name], type=field.type))
            except Exception:
                # Fallback to string array if type conversion fails
                string_values = [str(v) if v is not None else None for v in columns[field.name]]
                arrays.append(pa.array(string_values, type=pa.string()))

        return pa.table(arrays, schema=schema)
