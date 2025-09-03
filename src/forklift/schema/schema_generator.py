"""Schema generation functionality for Forklift.

This module provides capabilities to analyze data files and generate
schema objects that conform to the Forklift schema standards.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass
from enum import Enum

import pyarrow as pa
import pyarrow.csv as pv_csv
import pyarrow.parquet as pq
import pandas as pd

from ..io import UnifiedIOHandler, is_s3_path

# Import pyperclip with fallback for environments where it's not available
try:
    import pyperclip
    CLIPBOARD_AVAILABLE = True
except ImportError:
    CLIPBOARD_AVAILABLE = False


class OutputTarget(Enum):
    """Target for schema output."""
    STDOUT = "stdout"
    FILE = "file"
    CLIPBOARD = "clipboard"


class FileType(Enum):
    """Supported file types for schema generation."""
    CSV = "csv"
    EXCEL = "excel"
    PARQUET = "parquet"


@dataclass
class SchemaGenerationConfig:
    """Configuration for schema generation."""
    input_path: Union[str, Path]
    file_type: FileType
    nrows: Optional[int] = 1000  # Default to 1000 rows for analysis
    output_target: OutputTarget = OutputTarget.STDOUT
    output_path: Optional[Union[str, Path]] = None
    delimiter: str = ","
    encoding: str = "utf-8"
    sheet_name: Optional[str] = None  # For Excel files
    include_sample_data: bool = False  # Default to False to avoid sensitive data
    infer_primary_key: bool = True


class SchemaGenerator:
    """Generate Forklift schema objects from data files."""

    def __init__(self, config: SchemaGenerationConfig):
        self.config = config
        self.io_handler = UnifiedIOHandler()

    def generate_schema(self) -> Dict[str, Any]:
        """Generate a schema object from the configured file.

        Returns:
            Dict containing the generated schema object
        """
        # Read sample data based on file type
        if self.config.file_type == FileType.CSV:
            table = self._read_csv_sample()
        elif self.config.file_type == FileType.EXCEL:
            table = self._read_excel_sample()
        elif self.config.file_type == FileType.PARQUET:
            table = self._read_parquet_sample()
        else:
            raise ValueError(f"Unsupported file type: {self.config.file_type}")

        # Generate schema from Arrow table
        schema = self._generate_schema_from_table(table)

        return schema

    def _read_csv_sample(self) -> pa.Table:
        """Read CSV sample data."""
        read_options = pv_csv.ReadOptions(
            encoding=self.config.encoding,
            skip_rows=0,
            column_names=None,
            autogenerate_column_names=False
        )

        parse_options = pv_csv.ParseOptions(
            delimiter=self.config.delimiter,
            quote_char='"',
            double_quote=True,
            escape_char=None
        )

        convert_options = pv_csv.ConvertOptions(
            check_utf8=True,
            auto_dict_encode=True,
            auto_dict_max_cardinality=1000
        )

        if is_s3_path(str(self.config.input_path)):
            # Handle S3 path
            with self.io_handler.open_for_read(str(self.config.input_path), encoding='utf-8') as f:
                if self.config.nrows:
                    # Read limited rows for S3
                    content = f.read()
                    lines = content.split('\n')
                    if len(lines) > self.config.nrows + 1:  # +1 for header
                        lines = lines[:self.config.nrows + 1]
                    limited_content = '\n'.join(lines)

                    from io import StringIO
                    limited_stream = StringIO(limited_content)
                    table = pv_csv.read_csv(
                        limited_stream,
                        read_options=read_options,
                        parse_options=parse_options,
                        convert_options=convert_options
                    )
                else:
                    table = pv_csv.read_csv(
                        f,
                        read_options=read_options,
                        parse_options=parse_options,
                        convert_options=convert_options
                    )
        else:
            # Handle local file
            if self.config.nrows:
                # Read only specified number of rows
                df = pd.read_csv(
                    self.config.input_path,
                    nrows=self.config.nrows,
                    delimiter=self.config.delimiter,
                    encoding=self.config.encoding
                )
                table = pa.Table.from_pandas(df)
            else:
                table = pv_csv.read_csv(
                    str(self.config.input_path),
                    read_options=read_options,
                    parse_options=parse_options,
                    convert_options=convert_options
                )

        return table

    def _read_excel_sample(self) -> pa.Table:
        """Read Excel sample data."""
        if is_s3_path(str(self.config.input_path)):
            with self.io_handler.open_for_read(str(self.config.input_path), encoding='binary') as f:
                df = pd.read_excel(
                    f,
                    sheet_name=self.config.sheet_name,
                    nrows=self.config.nrows
                )
        else:
            df = pd.read_excel(
                self.config.input_path,
                sheet_name=self.config.sheet_name,
                nrows=self.config.nrows
            )

        return pa.Table.from_pandas(df)

    def _read_parquet_sample(self) -> pa.Table:
        """Read Parquet sample data."""
        if is_s3_path(str(self.config.input_path)):
            with self.io_handler.open_for_read(str(self.config.input_path), encoding='binary') as f:
                parquet_file = pq.ParquetFile(f)
                if self.config.nrows:
                    table = parquet_file.read()
                    table = table.slice(0, self.config.nrows)
                else:
                    table = parquet_file.read()
        else:
            parquet_file = pq.ParquetFile(self.config.input_path)
            if self.config.nrows:
                table = parquet_file.read()
                table = table.slice(0, self.config.nrows)
            else:
                table = parquet_file.read()

        return table

    def _generate_schema_from_table(self, table: pa.Table) -> Dict[str, Any]:
        """Generate schema object from PyArrow table."""
        # Get current timestamp for schema metadata
        timestamp = datetime.now().isoformat()

        # Base schema structure
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": f"https://github.com/cornyhorse/forklift/schema-standards/{datetime.now().strftime('%Y%m%d')}-{self.config.file_type.value}.json",
            "title": f"Forklift {self.config.file_type.value.upper()} Schema - Generated",
            "description": f"Auto-generated schema for {self.config.file_type.value.upper()} file processing with Forklift",
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": False
        }

        # Generate properties from table schema
        properties = {}
        required_fields = []

        for i, field in enumerate(table.schema):
            column_name = field.name
            arrow_type = field.type

            # Convert Arrow type to JSON Schema type
            json_type = self._arrow_to_json_schema_type(arrow_type)
            properties[column_name] = json_type

            # Check for required fields (non-nullable and has data)
            column_data = table.column(i)
            if not field.nullable and column_data.null_count == 0:
                required_fields.append(column_name)

        schema["properties"] = properties
        schema["required"] = required_fields

        # Add primary key inference if enabled
        if self.config.infer_primary_key:
            pk_candidates = self._infer_primary_key(table)
            if pk_candidates:
                schema["x-primaryKey"] = {
                    "description": "Inferred primary key configuration",
                    "columns": pk_candidates,
                    "type": "single" if len(pk_candidates) == 1 else "composite",
                    "enforceUniqueness": True,
                    "allowNulls": False,
                    "description_detail": f"Inferred primary key on {', '.join(pk_candidates)} field(s)"
                }

        # Add file-type specific extensions
        if self.config.file_type == FileType.CSV:
            schema["x-csv"] = self._generate_csv_extension(table)
        elif self.config.file_type == FileType.EXCEL:
            schema["x-excel"] = self._generate_excel_extension()

        # Add sample data if requested
        if self.config.include_sample_data:
            schema["x-sample"] = self._generate_sample_data(table)

        # Add generation metadata
        schema["x-generation"] = {
            "generated_at": timestamp,
            "source_file": str(self.config.input_path),
            "rows_analyzed": table.num_rows,
            "generator_version": "1.0.0"
        }

        return schema

    def _arrow_to_json_schema_type(self, arrow_type: pa.DataType) -> Dict[str, Any]:
        """Convert PyArrow type to JSON Schema type definition."""
        if pa.types.is_integer(arrow_type):
            return {"type": "integer"}
        elif pa.types.is_floating(arrow_type):
            return {"type": "number"}
        elif pa.types.is_boolean(arrow_type):
            return {"type": "boolean"}
        elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return {"type": "string"}
        elif pa.types.is_date(arrow_type):
            return {"type": "string", "format": "date"}
        elif pa.types.is_timestamp(arrow_type):
            return {"type": "string", "format": "date-time"}
        elif pa.types.is_time(arrow_type):
            return {"type": "string", "format": "time"}
        elif pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
            return {"type": "string", "contentEncoding": "base64"}
        elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
            # For list types, we need to handle the value_type properly
            if hasattr(arrow_type, 'value_type'):
                value_type = self._arrow_to_json_schema_type(arrow_type.value_type)
            else:
                value_type = {"type": "string"}  # fallback
            return {"type": "array", "items": value_type}
        elif pa.types.is_struct(arrow_type):
            return {"type": "object", "additionalProperties": True}
        elif pa.types.is_dictionary(arrow_type):
            # Handle dictionary as enum if reasonable cardinality
            return {"type": "string"}  # Could be enhanced to extract enum values
        else:
            # Default to string for unknown types
            return {"type": "string"}

    def _infer_primary_key(self, table: pa.Table) -> List[str]:
        """Infer potential primary key columns."""
        candidates = []

        for i, field in enumerate(table.schema):
            column_name = field.name
            column_data = table.column(i)

            # Check if column could be a primary key
            # 1. No nulls
            # 2. All unique values
            # 3. Reasonable name pattern

            if column_data.null_count == 0:  # No nulls
                # Convert to pandas for easier uniqueness check
                pandas_series = column_data.to_pandas()
                if pandas_series.nunique() == len(pandas_series):  # All unique
                    # Check for typical primary key naming patterns
                    if any(pattern in column_name.lower() for pattern in ['id', 'key', 'pk']):
                        candidates.append(column_name)

        # Return the first candidate or empty list
        return candidates[:1] if candidates else []

    def _generate_csv_extension(self, table: pa.Table) -> Dict[str, Any]:
        """Generate CSV-specific extension configuration."""
        return {
            "encodingPriority": [self.config.encoding, "utf-8-sig", "utf-8", "latin-1"],
            "delimiter": self.config.delimiter,
            "quotechar": "\"",
            "escapechar": "\\",
            "multiline": True,
            "header": {
                "mode": "present",
                "keywords": list(table.schema.names)[:4]  # First 4 columns as keywords
            },
            "footer": {
                "mode": "regex",
                "pattern": "^(total|summary|count)\\b"
            },
            "nulls": {
                "global": ["", "NA", "N/A", "-", "NULL", "null"],
                "perColumn": {}
            },
            "dataTypes": {
                col_name: self._get_parquet_type_string(table.schema.field(col_name).type)
                for col_name in table.schema.names
            },
            "validation": {
                "enabled": True,
                "onError": "log",
                "maxErrors": 1000
            }
        }

    def _generate_excel_extension(self) -> Dict[str, Any]:
        """Generate Excel-specific extension configuration."""
        return {
            "sheet": self.config.sheet_name or 0,
            "header": {"mode": "present"},
            "skipRows": 0,
            "skipFooter": 0,
            "nulls": {
                "global": ["", "NA", "N/A", "-", "NULL"]
            },
            "validation": {
                "enabled": True,
                "onError": "log",
                "maxErrors": 1000
            }
        }

    def _get_parquet_type_string(self, arrow_type: pa.DataType) -> str:
        """Convert Arrow type to Parquet type string."""
        if pa.types.is_int8(arrow_type):
            return "int8"
        elif pa.types.is_int16(arrow_type):
            return "int16"
        elif pa.types.is_int32(arrow_type):
            return "int32"
        elif pa.types.is_int64(arrow_type):
            return "int64"
        elif pa.types.is_uint8(arrow_type):
            return "uint8"
        elif pa.types.is_uint16(arrow_type):
            return "uint16"
        elif pa.types.is_uint32(arrow_type):
            return "uint32"
        elif pa.types.is_uint64(arrow_type):
            return "uint64"
        elif pa.types.is_float32(arrow_type):
            return "float32"
        elif pa.types.is_float64(arrow_type):
            return "double"
        elif pa.types.is_boolean(arrow_type):
            return "bool"
        elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return "string"
        elif pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
            return "binary"
        elif pa.types.is_date32(arrow_type):
            return "date32"
        elif pa.types.is_date64(arrow_type):
            return "date64"
        elif pa.types.is_timestamp(arrow_type):
            return "timestamp[ms]"  # Default to milliseconds
        elif pa.types.is_duration(arrow_type):
            return "duration[ms]"   # Default to milliseconds
        elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
            if hasattr(arrow_type, 'value_type'):
                return f"list<{self._get_parquet_type_string(arrow_type.value_type)}>"
            else:
                return "list<string>"  # fallback
        elif pa.types.is_struct(arrow_type):
            return "struct"
        elif pa.types.is_dictionary(arrow_type):
            return "dictionary<values=string, indices=int32>"
        else:
            return "string"  # Default fallback

    def _generate_sample_data(self, table: pa.Table) -> Dict[str, Any]:
        """Generate sample data from the table."""
        # Take first 3 rows as sample
        sample_size = min(3, table.num_rows)
        sample_table = table.slice(0, sample_size)

        # Convert to pandas for easier JSON serialization
        df = sample_table.to_pandas()

        # Convert to records format
        records = df.to_dict('records')

        return {
            "description": f"Sample data from first {sample_size} rows",
            "rows": records
        }

    def output_schema(self, schema: Dict[str, Any]) -> None:
        """Output the schema to the configured target."""
        schema_json = json.dumps(schema, indent=2, default=str)

        if self.config.output_target == OutputTarget.STDOUT:
            print(schema_json)
        elif self.config.output_target == OutputTarget.FILE:
            if not self.config.output_path:
                raise ValueError("output_path must be specified when output_target is FILE")

            if is_s3_path(str(self.config.output_path)):
                with self.io_handler.open_for_write(str(self.config.output_path), encoding='utf-8') as f:
                    f.write(schema_json)
            else:
                with open(self.config.output_path, 'w', encoding='utf-8') as f:
                    f.write(schema_json)

            print(f"Schema written to: {self.config.output_path}")
        elif self.config.output_target == OutputTarget.CLIPBOARD:
            if not CLIPBOARD_AVAILABLE:
                print("Pyperclip not available. Falling back to stdout:")
                print(schema_json)
                return

            try:
                pyperclip.copy(schema_json)
                print("Schema copied to clipboard")
            except Exception as e:
                print(f"Failed to copy to clipboard: {e}")
                print("Falling back to stdout:")
                print(schema_json)
