"""Fixed Width File input handler for reading and preprocessing FWF files."""

from __future__ import annotations
import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Iterator
import pyarrow as pa

from .config import FwfInputConfig, FwfFieldSpec, FwfConditionalSchema


class FwfInputHandler:
    """Handles Fixed Width File input with field positioning and conditional schemas.

    This class provides functionality for reading fixed-width files with various
    configurations including conditional schemas based on flag columns, field
    alignment, padding, and data type conversion.

    Args:
        config: FwfInputConfig instance with processing configuration

    Attributes:
        config: The configuration object for this input handler
    """

    def __init__(self, config: FwfInputConfig):
        """Initialize the FWF input handler.

        Args:
            config: Configuration object containing FWF processing parameters
        """
        self.config = config
        self._validate_config()

    def _validate_config(self) -> None:
        """Validate the FWF configuration.

        Raises:
            ValueError: If configuration is invalid
        """
        if self.config.conditional_schemas and not self.config.flag_column:
            raise ValueError("Flag column must be specified when using conditional schemas")

        if not self.config.conditional_schemas and not self.config.fields:
            raise ValueError("Either fields or conditional_schemas must be specified")

        # Validate field positions don't overlap
        if self.config.fields:
            self._validate_field_positions(self.config.fields)

        if self.config.conditional_schemas:
            for schema in self.config.conditional_schemas:
                self._validate_field_positions(schema.fields)

    def _validate_field_positions(self, fields: List[FwfFieldSpec]) -> None:
        """Validate that field positions don't overlap.

        Args:
            fields: List of field specifications to validate

        Raises:
            ValueError: If fields overlap
        """
        sorted_fields = sorted(fields, key=lambda f: f.start)

        for i in range(len(sorted_fields) - 1):
            current = sorted_fields[i]
            next_field = sorted_fields[i + 1]

            current_end = current.start + current.length - 1
            if current_end >= next_field.start:
                raise ValueError(
                    f"Field '{current.name}' (pos {current.start}-{current_end}) "
                    f"overlaps with '{next_field.name}' (pos {next_field.start})"
                )

    def detect_encoding(self, file_path: Path) -> str:
        """Detect file encoding using chardet library.

        Reads the first 10KB of the file to detect the most likely encoding.

        Args:
            file_path: Path to the FWF file to analyze

        Returns:
            Detected encoding string (defaults to utf-8 if detection fails)
        """
        try:
            import chardet
            with open(file_path, 'rb') as f:
                raw_data = f.read(10000)  # Read first 10KB
                result = chardet.detect(raw_data)
                return result.get('encoding', 'utf-8')
        except ImportError:
            return 'utf-8'

    def extract_field_value(self, line: str, field: FwfFieldSpec) -> str:
        """Extract field value from a line based on field specification.

        Args:
            line: The input line
            field: Field specification

        Returns:
            Extracted and processed field value
        """
        # Convert 1-based position to 0-based
        start_pos = field.start - 1
        end_pos = start_pos + field.length

        # Extract the raw field value
        if start_pos >= len(line):
            raw_value = ""
        elif end_pos > len(line):
            raw_value = line[start_pos:]
        else:
            raw_value = line[start_pos:end_pos]

        # Pad if necessary
        if len(raw_value) < field.length:
            if field.align == "right":
                raw_value = raw_value.rjust(field.length, field.pad)
            elif field.align == "center":
                raw_value = raw_value.center(field.length, field.pad)
            else:  # left
                raw_value = raw_value.ljust(field.length, field.pad)

        # Trim whitespace if configured
        if field.trim or self.config.trim_whitespace:
            raw_value = raw_value.strip()

        # For right-aligned fields with zero padding, also strip leading zeros unless it's all zeros
        if field.align == "right" and field.pad == "0" and raw_value and raw_value != "0" * len(raw_value):
            raw_value = raw_value.lstrip("0") or "0"

        return raw_value

    def determine_schema(self, line: str) -> Optional[FwfConditionalSchema]:
        """Determine which conditional schema to use for a line.

        Args:
            line: The input line

        Returns:
            Matching conditional schema or None if no match
        """
        if not self.config.conditional_schemas or not self.config.flag_column:
            return None

        flag_value = self.extract_field_value(line, self.config.flag_column)

        for schema in self.config.conditional_schemas:
            if schema.flag_value == flag_value:
                return schema

        return None

    def is_comment_row(self, line: str) -> bool:
        """Check if line should be treated as a comment.

        Args:
            line: Input line to check

        Returns:
            True if line matches a comment pattern, False otherwise
        """
        if not self.config.comment_patterns:
            return False

        line_stripped = line.strip()
        for pattern in self.config.comment_patterns:
            if re.match(pattern, line_stripped):
                return True

        return False

    def is_footer_row(self, line: str) -> bool:
        """Check if line indicates start of footer section.

        Args:
            line: Input line to check

        Returns:
            True if line matches footer pattern, False otherwise
        """
        if not self.config.footer_detection:
            return False

        mode = self.config.footer_detection.get('mode')
        if mode == 'regex':
            pattern = self.config.footer_detection.get('pattern')
            if pattern:
                return bool(re.match(pattern, line.strip()))

        return False

    def process_null_values(self, value: str, field_name: str) -> Optional[str]:
        """Process null values according to configuration.

        Args:
            value: Raw field value
            field_name: Name of the field

        Returns:
            Processed value or None if it represents null
        """
        if not self.config.null_values:
            return value if value else None

        # Check global null values
        global_nulls = self.config.null_values.get('global', [])
        if value in global_nulls:
            return None

        # Check per-column null values
        per_column = self.config.null_values.get('perColumn', {})
        field_nulls = per_column.get(field_name, [])
        if value in field_nulls:
            return None

        return value if value else None

    def convert_field_value(self, raw_value: str, field: FwfFieldSpec) -> Any:
        """Convert field value to the appropriate data type.

        Args:
            raw_value: Raw string value extracted from the field
            field: Field specification with data type information

        Returns:
            Converted value in the appropriate data type
        """
        if raw_value is None or raw_value == '':
            return None

        try:
            if field.parquet_type == 'int64':
                return int(raw_value)
            elif field.parquet_type == 'int32':
                return int(raw_value)
            elif field.parquet_type == 'int16':
                return int(raw_value)
            elif field.parquet_type == 'int8':
                return int(raw_value)
            elif field.parquet_type == 'float32':
                return float(raw_value)
            elif field.parquet_type == 'double' or field.parquet_type == 'float64':
                return float(raw_value)
            elif field.parquet_type == 'bool':
                return raw_value.upper() in ('Y', 'YES', 'TRUE', '1', 'T')
            elif field.parquet_type.startswith('decimal128'):
                return float(raw_value)  # Convert to float for now, could be enhanced for exact decimal handling
            else:
                return raw_value  # Keep as string for string types
        except ValueError:
            # If conversion fails, return as string or None based on null handling
            return raw_value if raw_value else None

    def parse_line(self, line: str) -> Optional[Dict[str, Any]]:
        """Parse a single line into a dictionary of field values.

        Args:
            line: Input line to parse

        Returns:
            Dictionary of field values or None if line should be skipped
        """
        # Skip blank lines if configured
        if self.config.skip_blank_lines and not line.strip():
            return None

        # Skip comment rows
        if self.is_comment_row(line):
            return None

        # Check for footer
        if self.is_footer_row(line):
            return None

        # Determine which fields to use
        fields = self.config.fields
        if self.config.conditional_schemas:
            schema = self.determine_schema(line)
            if schema:
                fields = schema.fields
            else:
                # No matching conditional schema, skip line
                return None

        if not fields:
            return None

        # Extract field values
        result = {}
        for field in fields:
            raw_value = self.extract_field_value(line, field)
            processed_value = self.process_null_values(raw_value, field.name)
            # Convert to appropriate data type
            converted_value = self.convert_field_value(processed_value, field)
            result[field.name] = converted_value

        return result

    def read_file(self, file_path: Path) -> Iterator[Dict[str, Any]]:
        """Read and parse fixed-width file.

        Args:
            file_path: Path to the FWF file to read

        Yields:
            Dictionary of field values for each valid row

        Raises:
            FileNotFoundError: If file doesn't exist
            UnicodeDecodeError: If file encoding is incorrect
        """
        if not file_path.exists():
            raise FileNotFoundError(f"FWF file not found: {file_path}")

        encoding = self.config.encoding
        if encoding == "auto":
            encoding = self.detect_encoding(file_path)

        with open(file_path, 'r', encoding=encoding) as f:
            for line_num, line in enumerate(f, 1):
                try:
                    parsed_row = self.parse_line(line.rstrip('\n\r'))
                    if parsed_row is not None:
                        # Add metadata
                        parsed_row['__line_number__'] = line_num
                        parsed_row['__source_file__'] = str(file_path)
                        yield parsed_row
                except Exception as e:
                    # Log error but continue processing
                    print(f"Warning: Error parsing line {line_num}: {e}")
                    continue

    def get_arrow_schema(self) -> pa.Schema:
        """Generate PyArrow schema from field specifications.

        Returns:
            PyArrow schema for the FWF data

        Raises:
            ValueError: If no valid field configuration found
        """
        # Determine which fields to use for schema generation
        if self.config.fields:
            fields = self.config.fields
        elif self.config.conditional_schemas:
            # For conditional schemas, create a union of all possible fields
            all_fields = {}
            for schema in self.config.conditional_schemas:
                for field in schema.fields:
                    if field.name not in all_fields:
                        all_fields[field.name] = field
                    # If same field appears in multiple schemas, use the most permissive type
            fields = list(all_fields.values())
        else:
            raise ValueError("No field configuration found")

        # Convert to PyArrow fields
        arrow_fields = []
        for field in fields:
            arrow_type = self._get_arrow_type(field.parquet_type)
            arrow_fields.append(pa.field(field.name, arrow_type, nullable=not field.required))

        # Add metadata fields
        arrow_fields.extend([
            pa.field('__line_number__', pa.int64()),
            pa.field('__source_file__', pa.string())
        ])

        return pa.schema(arrow_fields)

    def _get_arrow_type(self, parquet_type: str) -> pa.DataType:
        """Convert parquet type string to PyArrow data type.

        Args:
            parquet_type: Parquet type specification

        Returns:
            Corresponding PyArrow data type
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
            'double': pa.float64(),
            'bool': pa.bool_(),
            'string': pa.string(),
            'binary': pa.binary(),
            'date32': pa.date32(),
            'date64': pa.date64(),
            'timestamp[s]': pa.timestamp('s'),
            'timestamp[ms]': pa.timestamp('ms'),
            'timestamp[us]': pa.timestamp('us'),
            'timestamp[ns]': pa.timestamp('ns'),
            'duration[s]': pa.duration('s'),
            'duration[ms]': pa.duration('ms'),
            'duration[us]': pa.duration('us'),
            'duration[ns]': pa.duration('ns'),
        }

        # Handle decimal types
        if parquet_type.startswith('decimal128'):
            # Extract precision and scale from decimal128(precision,scale)
            import re
            match = re.match(r'decimal128\((\d+),(\d+)\)', parquet_type)
            if match:
                precision, scale = int(match.group(1)), int(match.group(2))
                return pa.decimal128(precision, scale)
            else:
                return pa.decimal128(10, 2)  # Default precision and scale

        # Handle list types
        if parquet_type.startswith('list<'):
            # Extract inner type from list<type>
            inner_type = parquet_type[5:-1]  # Remove 'list<' and '>'
            inner_arrow_type = self._get_arrow_type(inner_type)
            return pa.list_(inner_arrow_type)

        # Handle dictionary types
        if parquet_type.startswith('dictionary<'):
            # For now, treat as string (can be enhanced later)
            return pa.string()

        return type_mapping.get(parquet_type, pa.string())

    def create_arrow_table(self, file_path: Path) -> pa.Table:
        """Create PyArrow table from FWF file.

        Args:
            file_path: Path to the FWF file to read

        Returns:
            PyArrow table containing the parsed data
        """
        # Collect all rows
        rows = list(self.read_file(file_path))

        # Get the schema
        schema = self.get_arrow_schema()

        if not rows:
            # Return empty table with correct schema
            # Create empty arrays for each field in schema
            empty_arrays = []
            for field in schema:
                empty_arrays.append(pa.array([], type=field.type))
            return pa.table(empty_arrays, schema=schema)

        # Convert rows to columnar format for PyArrow with proper type conversion
        columns = {}
        for field in schema:
            columns[field.name] = []

        # Fill columns with data and convert types
        for row in rows:
            for field in schema:
                value = row.get(field.name)

                # Convert string values to appropriate types for PyArrow
                if value is not None and field.type != pa.string():
                    try:
                        if field.type == pa.int64():
                            value = int(value) if value else None
                        elif field.type == pa.int32():
                            value = int(value) if value else None
                        elif field.type == pa.float32() or field.type == pa.float64():
                            value = float(value) if value else None
                        elif field.type == pa.bool_():
                            value = bool(value) if value else None
                        elif isinstance(field.type, pa.Decimal128Type):
                            # Convert to Python Decimal for PyArrow decimal types
                            from decimal import Decimal
                            if value is not None:
                                # Handle both string and numeric values
                                if isinstance(value, str):
                                    if value.strip():
                                        value = Decimal(value.strip())
                                    else:
                                        value = None
                                else:
                                    # Already a number, convert to Decimal
                                    value = Decimal(str(value))
                            else:
                                value = None
                    except (ValueError, TypeError):
                        # If conversion fails, keep as None or original value
                        value = None

                columns[field.name].append(value)

        # Create PyArrow arrays with proper type handling
        arrays = []
        for field in schema:
            try:
                arrays.append(pa.array(columns[field.name], type=field.type))
            except pa.ArrowInvalid:
                # If type conversion fails, convert to string first
                if field.type != pa.string():
                    arrays.append(pa.array(columns[field.name], type=pa.string()))
                else:
                    arrays.append(pa.array(columns[field.name]))

        return pa.table(arrays, schema=schema)
