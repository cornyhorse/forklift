"""Fixed-width file input handler for reading and processing FWF files."""

from __future__ import annotations
import re
from typing import Dict, Any, Optional, List
from pathlib import Path
import pyarrow as pa

from .config import FwfInputConfig, FwfFieldSpec, FwfConditionalSchema


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
        self._validate_config()

    def _validate_config(self) -> None:
        """Validate the FWF configuration.

        Raises:
            ValueError: If configuration is invalid
        """
        # Must have either fields or conditional schemas
        if not self.config.fields and not self.config.conditional_schemas:
            raise ValueError("Either fields or conditional_schemas must be specified")

        # If using conditional schemas, must have flag column
        if self.config.conditional_schemas and not self.config.flag_column:
            raise ValueError("Flag column must be specified when using conditional schemas")

        # Validate field overlaps for simple fields
        if self.config.fields:
            self._validate_field_overlaps(self.config.fields)

        # Validate conditional schema fields
        if self.config.conditional_schemas:
            for schema in self.config.conditional_schemas:
                self._validate_field_overlaps(schema.fields)

    def _validate_field_overlaps(self, fields: List[FwfFieldSpec]) -> None:
        """Validate that fields don't overlap.

        Args:
            fields: List of field specifications to validate

        Raises:
            ValueError: If fields overlap
        """
        for i, field1 in enumerate(fields):
            field1_end = field1.start + field1.length - 1
            for j, field2 in enumerate(fields[i + 1:], i + 1):
                field2_end = field2.start + field2.length - 1

                # Check for overlap
                if (field1.start <= field2.start <= field1_end or
                    field2.start <= field1.start <= field2_end):
                    raise ValueError(
                        f"Field '{field1.name}' (positions {field1.start}-{field1_end}) "
                        f"overlaps with field '{field2.name}' (positions {field2.start}-{field2_end})"
                    )

    def detect_encoding(self, file_path: Path) -> str:
        """Detect file encoding using chardet library.

        Args:
            file_path: Path to file to analyze

        Returns:
            Detected encoding string (defaults to utf-8 if detection fails)
        """
        try:
            import chardet
            with open(file_path, 'rb') as f:
                raw_data = f.read(10240)  # Read first 10KB
            result = chardet.detect(raw_data)
            return result.get('encoding', 'utf-8')
        except ImportError:
            return 'utf-8'

    def _get_arrow_type(self, parquet_type: str) -> pa.DataType:
        """Convert parquet type string to PyArrow type.

        Args:
            parquet_type: String representation of the type

        Returns:
            PyArrow DataType object
        """
        type_mapping = {
            'int8': pa.int8(),
            'int16': pa.int16(),
            'int32': pa.int32(),
            'int64': pa.int64(),
            'uint8': pa.uint8(),
            'uint16': pa.uint16(),
            'uint32': pa.uint32(),
            'uint64': pa.uint64(),
            'float32': pa.float32(),
            'float64': pa.float64(),
            'double': pa.float64(),  # Add double as alias for float64
            'bool': pa.bool_(),
            'string': pa.string(),
            'utf8': pa.string(),
            'binary': pa.binary(),
            'date32': pa.date32(),
            'date64': pa.date64(),
            'timestamp': pa.timestamp('ns'),
        }

        # Handle timestamp types with specific units
        if parquet_type.startswith('timestamp[') and parquet_type.endswith(']'):
            unit = parquet_type[10:-1]  # Extract unit between brackets
            return pa.timestamp(unit)

        # Handle duration types with specific units
        if parquet_type.startswith('duration[') and parquet_type.endswith(']'):
            unit = parquet_type[9:-1]  # Extract unit between brackets
            return pa.duration(unit)

        # Handle list types
        if parquet_type.startswith('list<') and parquet_type.endswith('>'):
            inner_type = parquet_type[5:-1]  # Extract inner type
            inner_arrow_type = self._get_arrow_type(inner_type)
            return pa.list_(inner_arrow_type)

        # Handle decimal types
        if parquet_type.startswith('decimal'):
            # Extract precision and scale if specified
            if '(' in parquet_type:
                params = parquet_type[parquet_type.find('(')+1:parquet_type.find(')')]
                if ',' in params:
                    precision, scale = map(int, params.split(','))
                    return pa.decimal128(precision, scale)
                else:
                    precision = int(params)
                    return pa.decimal128(precision, 2)
            return pa.decimal128(10, 2)

        return type_mapping.get(parquet_type, pa.string())

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
                arrow_type = self._get_arrow_type(field_spec.parquet_type)
                unique_fields[field_spec.name] = pa.field(field_spec.name, arrow_type)

        # Handle conditional schemas - collect all unique fields from all schemas
        if self.config.conditional_schemas:
            # Add flag column if present
            if self.config.flag_column:
                arrow_type = self._get_arrow_type(self.config.flag_column.parquet_type)
                unique_fields[self.config.flag_column.name] = pa.field(self.config.flag_column.name, arrow_type)

            # Add all fields from all conditional schemas
            for schema in self.config.conditional_schemas:
                for field_spec in schema.fields:
                    if field_spec.name not in unique_fields:
                        arrow_type = self._get_arrow_type(field_spec.parquet_type)
                        unique_fields[field_spec.name] = pa.field(field_spec.name, arrow_type)

        # Convert to list maintaining order
        fields = list(unique_fields.values())

        # Add metadata fields
        fields.extend([
            pa.field('__line_number__', pa.int64()),
            pa.field('__source_file__', pa.string())
        ])

        return pa.schema(fields)

    def extract_field_value(self, line: str, field: FwfFieldSpec) -> str:
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

    def process_null_values(self, value: str, field_name: str) -> Optional[str]:
        """Process null values according to configuration.

        Args:
            value: The field value to check
            field_name: Name of the field

        Returns:
            None if value should be treated as null, otherwise the original value
        """
        if not self.config.null_values:
            # Default behavior: empty strings are treated as None
            return None if value == "" else value

        # Check global null values
        global_nulls = self.config.null_values.get("global", [])
        if value in global_nulls:
            return None

        # Check per-column null values
        per_column = self.config.null_values.get("perColumn", {})
        field_nulls = per_column.get(field_name, [])
        if value in field_nulls:
            return None

        return value

    def convert_value(self, value: str, parquet_type: str) -> Any:
        """Convert string value to appropriate Python type.

        Args:
            value: String value to convert
            parquet_type: Target Parquet data type

        Returns:
            Converted value
        """
        if not value:
            return None

        try:
            if parquet_type in ["int8", "int16", "int32", "int64"]:
                return int(value)
            elif parquet_type in ["uint8", "uint16", "uint32", "uint64"]:
                return int(value)
            elif parquet_type in ["float32", "float64", "double"]:  # Add double support
                return float(value)
            elif parquet_type.startswith("decimal"):
                return float(value)  # Convert to float for testing purposes
            elif parquet_type == "bool":
                return value.lower() in ("true", "1", "yes", "y", "t")  # Add "t" for true
            else:  # string or other types
                return value
        except (ValueError, TypeError):
            return value  # Return original value if conversion fails

    def convert_field_value(self, value: str, field: FwfFieldSpec) -> Any:
        """Convert field value to appropriate type based on field specification.

        Args:
            value: String value to convert
            field: Field specification

        Returns:
            Converted value
        """
        return self.convert_value(value, field.parquet_type)

    def is_comment_line(self, line: str) -> bool:
        """Check if a line is a comment based on configured patterns.

        Args:
            line: Line to check

        Returns:
            True if line matches a comment pattern
        """
        if not self.config.comment_patterns:
            return False

        for pattern in self.config.comment_patterns:
            if re.match(pattern, line):
                return True

        return False

    def is_comment_row(self, line: str) -> bool:
        """Alias for is_comment_line for backward compatibility."""
        return self.is_comment_line(line)

    def is_blank_line(self, line: str) -> bool:
        """Check if a line is blank or whitespace-only.

        Args:
            line: Line to check

        Returns:
            True if line is blank
        """
        return not line.strip()

    def is_footer_row(self, line: str) -> bool:
        """Check if a line is a footer based on configured patterns.

        Args:
            line: Line to check

        Returns:
            True if line matches footer detection pattern
        """
        if not self.config.footer_detection:
            return False

        mode = self.config.footer_detection.get("mode")
        pattern = self.config.footer_detection.get("pattern")

        if mode == "regex" and pattern:
            return bool(re.match(pattern, line))

        return False

    def detect_conditional_schema(self, line: str) -> Optional[FwfConditionalSchema]:
        """Detect which conditional schema applies to a line.

        Args:
            line: Line to analyze

        Returns:
            Matching conditional schema or None
        """
        if not self.config.conditional_schemas or not self.config.flag_column:
            return None

        # Extract flag value
        flag_value = self.extract_field_value(line, self.config.flag_column)

        # Find matching schema
        for schema in self.config.conditional_schemas:
            if schema.flag_value == flag_value:
                return schema

        return None

    def determine_schema(self, line: str) -> Optional[FwfConditionalSchema]:
        """Determine which schema to use for a line (alias for detect_conditional_schema).

        Args:
            line: Line to analyze

        Returns:
            Matching conditional schema or None
        """
        return self.detect_conditional_schema(line)

    def parse_line(self, line: str) -> Optional[Dict[str, Any]]:
        """Parse a single line according to the FWF configuration.

        Args:
            line: Line to parse

        Returns:
            Dictionary of field values or None if line should be skipped
        """
        # Skip blank lines if configured
        if self.config.skip_blank_lines and self.is_blank_line(line):
            return None

        # Skip comment lines
        if self.is_comment_line(line):
            return None

        # Skip footer lines
        if self.is_footer_row(line):
            return None

        # Determine which fields to use
        fields_to_use = self.config.fields

        # Handle conditional schemas
        if self.config.conditional_schemas:
            conditional_schema = self.detect_conditional_schema(line)
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
            raw_value = self.extract_field_value(line, field)

            # Process null values
            processed_value = self.process_null_values(raw_value, field.name)

            # Convert to appropriate type
            if processed_value is not None:
                converted_value = self.convert_value(processed_value, field.parquet_type)
            else:
                converted_value = None

            result[field.name] = converted_value

        return result

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
            encoding = self.detect_encoding(file_path)

        records = []

        with open(file_path, 'r', encoding=encoding) as f:
            for line_num, line in enumerate(f, 1):
                # Remove newline characters
                line = line.rstrip('\r\n')

                try:
                    parsed_record = self.parse_line(line)
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
