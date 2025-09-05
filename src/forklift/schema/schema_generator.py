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
import re

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
    pyperclip = None
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
    user_specified_primary_key: Optional[List[str]] = None  # Allow user to specify primary key columns
    # New metadata generation options
    generate_metadata: bool = True  # Default to True for metadata generation
    metadata_output_path: Optional[Union[str, Path]] = None  # Separate metadata file path
    enum_threshold: float = 0.1  # Threshold for suggesting enum types (10% distinct values)
    uniqueness_threshold: float = 0.95  # Threshold for considering a column too unique for enum
    top_n_values: int = 10  # Number of top values to include in metadata
    quantiles: List[float] = None  # Quantiles to calculate for numeric columns
    # CLI/API only - primary key inference from metadata
    infer_primary_key_from_metadata: bool = False  # Only available in CLI/API, not schema files

    def __post_init__(self):
        if self.quantiles is None:
            self.quantiles = [0.25, 0.5, 0.75, 0.9, 0.95, 0.99]


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
            from io import StringIO
            with self.io_handler.open_for_read(str(self.config.input_path), encoding='utf-8') as f:
                if self.config.nrows:
                    # Read limited rows for S3
                    content = f.read()
                    lines = content.split('\n')
                    if len(lines) > self.config.nrows + 1:  # +1 for header
                        lines = lines[:self.config.nrows + 1]
                    limited_content = '\n'.join(lines)

                    # Use pandas for S3 CSV reading with nrows to avoid pyarrow StringIO issues
                    df = pd.read_csv(
                        StringIO(limited_content),
                        delimiter=self.config.delimiter,
                        encoding=self.config.encoding
                    )
                    table = pa.Table.from_pandas(df)
                else:
                    # Use pandas for S3 CSV reading without nrows
                    content = f.read()
                    df = pd.read_csv(
                        StringIO(content),
                        delimiter=self.config.delimiter,
                        encoding=self.config.encoding
                    )
                    table = pa.Table.from_pandas(df)
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
                    sheet_name=self.config.sheet_name or 0,
                    nrows=self.config.nrows
                )
        else:
            df = pd.read_excel(
                self.config.input_path,
                sheet_name=self.config.sheet_name or 0,
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

        # Add primary key configuration
        primary_key_config = self._generate_primary_key_config(table)
        if primary_key_config:
            schema["x-primaryKey"] = primary_key_config

        # Add file-type specific extensions
        if self.config.file_type == FileType.CSV:
            schema["x-csv"] = self._generate_csv_extension(table)
        elif self.config.file_type == FileType.EXCEL:
            schema["x-excel"] = self._generate_excel_extension()

        # Add data transformation extensions
        schema["x-transformations"] = self._generate_transformation_extension(table)

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

        # Add metadata if requested
        if self.config.generate_metadata:
            metadata = self._generate_metadata(table)
            if metadata:
                schema["x-metadata"] = metadata

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

    def _generate_primary_key_config(self, table: pa.Table) -> Optional[Dict[str, Any]]:
        """Generate primary key configuration based on user input or inference from metadata."""
        if self.config.user_specified_primary_key:
            # Use user-specified primary key columns
            pk_columns = self.config.user_specified_primary_key
            return {
                "description": "User-specified primary key",
                "columns": pk_columns,
                "type": "composite" if len(pk_columns) > 1 else "single",
                "enforceUniqueness": True,
                "allowNulls": False,
                "description_detail": f"User-defined primary key on {', '.join(pk_columns)} field(s)"
            }
        elif self.config.infer_primary_key_from_metadata:
            # Infer primary key from the metadata that's already being generated
            return self._infer_primary_key_from_metadata(table)

        return None

    def _infer_primary_key_from_metadata(self, table: pa.Table) -> Optional[Dict[str, Any]]:
        """Infer primary key from metadata analysis without additional data loading."""
        # Generate metadata if not already available (this reuses the same data reading)
        metadata = self._generate_metadata(table)

        if not metadata or "column_metadata" not in metadata:
            return None

        candidates = []

        # Analyze each column's metadata for primary key characteristics
        for column_name, col_meta in metadata["column_metadata"].items():
            # Primary key criteria based on metadata:
            # 1. No nulls (null_percentage = 0)
            # 2. High uniqueness (uniqueness_ratio = 1.0 or very close)
            # 3. Reasonable column name pattern
            # 4. Not too many values (reasonable for indexing)

            is_not_null = col_meta.get("null_percentage", 100) == 0.0
            uniqueness_ratio = col_meta.get("uniqueness_ratio", 0.0)
            is_highly_unique = uniqueness_ratio >= 0.95  # At least 95% unique
            distinct_count = col_meta.get("distinct_count", 0)

            # Check for typical primary key naming patterns
            has_pk_name_pattern = any(pattern in column_name.lower()
                                    for pattern in ['id', 'key', 'pk', 'uuid', 'guid'])

            # For a column to be a primary key candidate:
            # - Must have no nulls
            # - Must be highly unique (preferably 100% unique)
            # - Should have reasonable naming
            # - Should not have too many distinct values for performance
            if (is_not_null and
                is_highly_unique and
                has_pk_name_pattern and
                distinct_count <= 1000000):  # Reasonable upper limit

                # Calculate a score for ranking candidates
                score = 0
                if uniqueness_ratio == 1.0:  # Perfect uniqueness
                    score += 10
                elif uniqueness_ratio >= 0.99:
                    score += 8
                elif uniqueness_ratio >= 0.95:
                    score += 5

                # Bonus for good naming patterns
                if 'id' in column_name.lower():
                    score += 5
                elif any(pattern in column_name.lower() for pattern in ['key', 'pk']):
                    score += 3
                elif any(pattern in column_name.lower() for pattern in ['uuid', 'guid']):
                    score += 4

                # Penalty for very large distinct counts (performance concern)
                if distinct_count > 100000:
                    score -= 2
                elif distinct_count > 10000:
                    score -= 1

                candidates.append({
                    'column': column_name,
                    'score': score,
                    'uniqueness_ratio': uniqueness_ratio,
                    'distinct_count': distinct_count
                })

        if not candidates:
            return None

        # Sort by score (highest first) and select the best candidate
        candidates.sort(key=lambda x: x['score'], reverse=True)
        best_candidate = candidates[0]

        # Only return if the score is reasonable (at least 8)
        if best_candidate['score'] >= 8:
            return {
                "description": "Inferred primary key from metadata analysis",
                "columns": [best_candidate['column']],
                "type": "single",
                "enforceUniqueness": True,
                "allowNulls": False,
                "description_detail": f"Inferred primary key on {best_candidate['column']} field "
                                    f"(uniqueness: {best_candidate['uniqueness_ratio']:.1%}, "
                                    f"distinct values: {best_candidate['distinct_count']}, "
                                    f"score: {best_candidate['score']})",
                "inference_metadata": {
                    "method": "metadata_analysis",
                    "score": best_candidate['score'],
                    "uniqueness_ratio": best_candidate['uniqueness_ratio'],
                    "distinct_count": best_candidate['distinct_count'],
                    "alternative_candidates": [c['column'] for c in candidates[1:3]]  # Show top 3 alternatives
                }
            }

        return None

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

    def _generate_transformation_extension(self, table: pa.Table) -> Dict[str, Any]:
        """Generate comprehensive data transformation extension configuration.

        This generates the x-transformations schema extension with all the
        data cleaning and transformation capabilities you requested.
        """
        # Analyze columns to suggest appropriate transformations
        column_transformations = {}

        for i, field in enumerate(table.schema):
            column_name = field.name
            arrow_type = field.type
            column_data = table.column(i)

            # Analyze column data to suggest transformations
            suggestions = self._analyze_column_for_transformations(column_name, column_data, arrow_type)
            if suggestions:
                column_transformations[column_name] = suggestions

        return {
            "description": "Data transformation configurations for cleaning and standardizing data",
            "version": "1.0.0",
            "global_settings": {
                "nan_handling": {
                    "allow_nan": True,
                    "nan_values": ["", "N/A", "NA", "NULL", "null", "NaN", "nan", "#N/A", "#NULL!", "None"],
                    "convert_to_null": True,
                    "error_on_nan": False
                },
                "error_handling": {
                    "on_transformation_error": "log",  # "log", "skip", "fail"
                    "max_errors": 1000,
                    "continue_on_error": True
                }
            },
            "column_transformations": column_transformations,
            "transformation_types": {
                "regex_replace": {
                    "description": "Apply regex pattern replacements",
                    "parameters": {
                        "pattern": "string",
                        "replacement": "string",
                        "flags": "int (re module flags)"
                    }
                },
                "string_replace": {
                    "description": "Simple string replacement (like Python str.replace)",
                    "parameters": {
                        "old": "string",
                        "new": "string",
                        "count": "int (-1 for all)"
                    }
                },
                "money_conversion": {
                    "description": "Convert money strings to decimal values",
                    "parameters": {
                        "currency_symbols": "array",
                        "thousands_separator": "string",
                        "decimal_separator": "string",
                        "parentheses_negative": "boolean",
                        "strip_whitespace": "boolean"
                    }
                },
                "numeric_cleaning": {
                    "description": "Clean numeric fields with separator handling",
                    "parameters": {
                        "thousands_separator": "string",
                        "decimal_separator": "string",
                        "allow_nan": "boolean",
                        "target_type": "string (int64, double, etc.)"
                    }
                },
                "string_padding": {
                    "description": "Pad strings (lpad/rpad)",
                    "parameters": {
                        "width": "int",
                        "fillchar": "string",
                        "side": "string (left, right, both)"
                    }
                },
                "string_trimming": {
                    "description": "Trim strings (lstrip/rstrip/strip)",
                    "parameters": {
                        "side": "string (left, right, both)",
                        "chars": "string (null for whitespace)"
                    }
                },
                "html_xml_cleaning": {
                    "description": "Remove HTML/XML tags and decode entities",
                    "parameters": {
                        "strip_tags": "boolean",
                        "decode_entities": "boolean",
                        "preserve_whitespace": "boolean"
                    }
                }
            }
        }

    def _analyze_column_for_transformations(self, column_name: str, column_data: pa.Array, arrow_type: pa.DataType) -> Optional[Dict[str, Any]]:
        """Analyze a column to suggest appropriate transformations based on data patterns."""
        suggestions = {}

        # Convert to pandas for analysis
        pandas_series = column_data.to_pandas()
        sample_values = pandas_series.dropna().head(10).astype(str).tolist()

        if not sample_values:
            return None

        # Check for money patterns
        money_patterns = [r'\$', r'€', r'£', r'¥', r'₹', r'₽', r'\(.*\)', r'\d+,\d+', r'\d+\.\d{2}$']
        if any(re.search(pattern, str(val)) for pattern in money_patterns for val in sample_values[:5]):
            suggestions["money_conversion"] = {
                "enabled": False,  # User can enable as needed
                "currency_symbols": ["$", "€", "£", "¥", "₹", "₽", "¢"],
                "thousands_separator": ",",
                "decimal_separator": ".",
                "parentheses_negative": True,
                "strip_whitespace": True
            }

        # Check for numeric fields with separators
        if pa.types.is_string(arrow_type):
            numeric_with_separators = any(re.search(r'\d+[,\.]\d+', str(val)) for val in sample_values[:5])
            if numeric_with_separators:
                suggestions["numeric_cleaning"] = {
                    "enabled": False,  # User can enable as needed
                    "thousands_separator": ",",
                    "decimal_separator": ".",
                    "allow_nan": True,
                    "target_type": "double"
                }

        # Check for HTML/XML content
        html_patterns = [r'<[^>]+>', r'&\w+;']
        if any(re.search(pattern, str(val)) for pattern in html_patterns for val in sample_values[:5]):
            suggestions["html_xml_cleaning"] = {
                "enabled": False,  # User can enable as needed
                "strip_tags": True,
                "decode_entities": True,
                "preserve_whitespace": False
            }

        # Check for excessive whitespace
        if any(re.search(r'^\s+|\s+$|\s{2,}', str(val)) for val in sample_values[:5]):
            suggestions["string_trimming"] = {
                "enabled": False,  # User can enable as needed
                "side": "both",
                "chars": None
            }
            suggestions["regex_replace"] = {
                "enabled": False,  # User can enable as needed
                "pattern": r'\s+',
                "replacement": " ",
                "flags": 0
            }

        # Add standard string operations for string columns
        if pa.types.is_string(arrow_type) and len(sample_values) > 0:
            # Add common string cleaning suggestions
            if "string_trimming" not in suggestions:
                suggestions["string_trimming"] = {
                    "enabled": False,
                    "side": "both",
                    "chars": None
                }

        return suggestions if suggestions else None

    def _generate_metadata(self, table: pa.Table) -> Dict[str, Any]:
        """Generate comprehensive metadata object from PyArrow table."""
        metadata = {
            "description": "Column-level metadata analysis for data profiling and enum type suggestions",
            "version": "1.0.0",
            "generated_at": datetime.now().isoformat(),
            "analysis_config": {
                "rows_analyzed": table.num_rows,
                "enum_threshold": self.config.enum_threshold,
                "uniqueness_threshold": self.config.uniqueness_threshold,
                "top_n_values": self.config.top_n_values,
                "quantiles": self.config.quantiles
            },
            "table_metadata": {
                "row_count": table.num_rows,
                "column_count": len(table.schema),
                "file_type": self.config.file_type.value,
                "source_file": str(self.config.input_path)
            },
            "column_metadata": {},
            "enum_suggestions": {}
        }

        # Generate column-level metadata
        for i, field in enumerate(table.schema):
            column_name = field.name
            column_data = table.column(i)
            arrow_type = field.type

            # Convert to pandas for easier analysis
            pandas_series = column_data.to_pandas()

            # Basic metadata
            column_metadata = {
                "name": column_name,
                "type": str(field.type),
                "parquet_type": self._get_parquet_type_string(arrow_type),
                "nullable": field.nullable,
                "null_count": int(column_data.null_count),
                "non_null_count": int(len(pandas_series) - column_data.null_count),
                "null_percentage": float(column_data.null_count / len(pandas_series) * 100) if len(pandas_series) > 0 else 0.0
            }

            # Add NaN count for numeric types
            if pa.types.is_floating(arrow_type):
                nan_count = int(pandas_series.isna().sum() - column_data.null_count)
                column_metadata["nan_count"] = nan_count
                column_metadata["nan_percentage"] = float(nan_count / len(pandas_series) * 100) if len(pandas_series) > 0 else 0.0

            # Calculate distinct values and uniqueness
            non_null_series = pandas_series.dropna()
            if len(non_null_series) > 0:
                distinct_count = non_null_series.nunique()
                column_metadata["distinct_count"] = int(distinct_count)
                column_metadata["uniqueness_ratio"] = float(distinct_count / len(non_null_series))

                # Generate value frequency analysis
                value_counts = non_null_series.value_counts()

                # Top N values
                top_values = []
                for value, count in value_counts.head(self.config.top_n_values).items():
                    top_values.append({
                        "value": str(value),
                        "count": int(count),
                        "percentage": float(count / len(non_null_series) * 100)
                    })
                column_metadata["top_values"] = top_values

                # Bottom N values (if there are enough unique values)
                if distinct_count > self.config.top_n_values:
                    bottom_values = []
                    for value, count in value_counts.tail(self.config.top_n_values).items():
                        bottom_values.append({
                            "value": str(value),
                            "count": int(count),
                            "percentage": float(count / len(non_null_series) * 100)
                        })
                    column_metadata["bottom_values"] = bottom_values

                # Enum type suggestions
                enum_suggestion = self._analyze_enum_potential(column_name, non_null_series, value_counts)
                if enum_suggestion:
                    metadata["enum_suggestions"][column_name] = enum_suggestion

            # Numeric statistics
            if pa.types.is_floating(arrow_type) or pa.types.is_integer(arrow_type):
                numeric_stats = self._calculate_numeric_statistics(non_null_series)
                column_metadata.update(numeric_stats)

            # String statistics
            elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
                string_stats = self._calculate_string_statistics(pandas_series)  # Pass original series
                column_metadata.update(string_stats)

            # Boolean statistics
            elif pa.types.is_boolean(arrow_type):
                boolean_stats = self._calculate_boolean_statistics(non_null_series)
                column_metadata.update(boolean_stats)

            metadata["column_metadata"][column_name] = column_metadata

        return metadata

    def _analyze_enum_potential(self, column_name: str, series: pd.Series, value_counts: pd.Series) -> Optional[Dict[str, Any]]:
        """Analyze if a column is a good candidate for enum type."""
        if len(series) == 0:
            return None

        distinct_count = series.nunique()
        total_count = len(series)
        uniqueness_ratio = distinct_count / total_count

        # Check if it meets enum criteria
        is_enum_candidate = (
            uniqueness_ratio <= self.config.enum_threshold and  # Low uniqueness
            distinct_count <= 50 and  # Reasonable number of distinct values
            uniqueness_ratio < self.config.uniqueness_threshold  # Not too unique
        )

        if is_enum_candidate:
            # Calculate distribution balance
            top_value_percentage = value_counts.iloc[0] / total_count * 100
            distribution_balance = "balanced" if top_value_percentage < 50 else "skewed"

            return {
                "is_enum_candidate": True,
                "confidence": "high" if uniqueness_ratio <= 0.05 else "medium",
                "distinct_count": int(distinct_count),
                "uniqueness_ratio": float(uniqueness_ratio),
                "distribution_balance": distribution_balance,
                "top_value_dominance_percentage": float(top_value_percentage),
                "suggested_enum_values": value_counts.index.tolist(),
                "recommendation": f"Column '{column_name}' appears to be categorical with {distinct_count} distinct values. "
                               f"Consider using enum type with values: {', '.join(map(str, value_counts.head(10).index.tolist()))}"
            }

        return {
            "is_enum_candidate": False,
            "reason": f"Too unique ({uniqueness_ratio:.2%}) or too many distinct values ({distinct_count})",
            "distinct_count": int(distinct_count),
            "uniqueness_ratio": float(uniqueness_ratio)
        }

    def _calculate_numeric_statistics(self, series: pd.Series) -> Dict[str, Any]:
        """Calculate comprehensive numeric statistics."""
        if len(series) == 0:
            return {}

        try:
            stats = {
                "min_value": float(series.min()),
                "max_value": float(series.max()),
                "mean": float(series.mean()),
                "median": float(series.median()),
                "std_dev": float(series.std()),
                "variance": float(series.var())
            }

            # Calculate quantiles
            quantile_dict = {}
            for q in self.config.quantiles:
                quantile_dict[f"quantile_{int(q*100)}"] = float(series.quantile(q))
            stats["quantiles"] = quantile_dict

            # Additional statistics
            stats["range"] = float(stats["max_value"] - stats["min_value"])
            stats["coefficient_of_variation"] = float(stats["std_dev"] / stats["mean"]) if stats["mean"] != 0 else None

            # Detect potential outliers using IQR method
            q1 = series.quantile(0.25)
            q3 = series.quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            outliers = series[(series < lower_bound) | (series > upper_bound)]

            stats["outlier_count"] = len(outliers)
            stats["outlier_percentage"] = float(len(outliers) / len(series) * 100)

            return stats
        except Exception as e:
            return {"error": f"Failed to calculate numeric statistics: {str(e)}"}

    def _calculate_string_statistics(self, series: pd.Series) -> Dict[str, Any]:
        """Calculate string-specific statistics."""
        if len(series) == 0:
            return {}

        try:
            # Convert to string to handle mixed types, but first check for empty strings in original data
            # We need to check for empty strings before dropping NaN values
            original_series = series.copy()

            # Convert to string, but preserve info about original empty strings and NaNs
            str_series = series.astype(str)

            # Count empty strings from the original data (before astype conversion)
            # Empty strings should be detected as either "" or actual empty strings, not NaN converted to "nan"
            empty_string_count = 0
            for val in original_series:
                if pd.isna(val):
                    # Check if this NaN was originally an empty string in the raw data
                    continue
                elif str(val) == "":
                    empty_string_count += 1

            # Additionally, check for NaN values that should be counted as empty strings
            # This handles the case where pandas converts "" to NaN during CSV parsing
            nan_count = original_series.isna().sum()

            # For string statistics, we'll work with the string-converted series
            # but include both actual empty strings and NaN-converted-to-"nan" in our count
            str_series_for_analysis = str_series.copy()

            lengths = str_series_for_analysis.str.len()

            stats = {
                "min_length": int(lengths.min()),
                "max_length": int(lengths.max()),
                "avg_length": float(lengths.mean()),
                "median_length": float(lengths.median())
            }

            # Pattern analysis - include both empty strings and NaN values that were originally empty
            # Count the actual empty strings plus NaN values (which were likely empty strings in source)
            actual_empty_strings = int((str_series_for_analysis == "").sum())
            stats["empty_strings"] = actual_empty_strings + int(nan_count)

            stats["contains_whitespace"] = int(str_series_for_analysis.str.contains(r'\s', na=False).sum())
            stats["contains_numbers"] = int(str_series_for_analysis.str.contains(r'\d', na=False).sum())
            stats["contains_special_chars"] = int(str_series_for_analysis.str.contains(r'[^a-zA-Z0-9\s]', na=False).sum())
            stats["all_uppercase"] = int(str_series_for_analysis.str.isupper().sum())
            stats["all_lowercase"] = int(str_series_for_analysis.str.islower().sum())

            # Character encoding analysis
            try:
                ascii_count = sum(1 for s in str_series_for_analysis if isinstance(s, str) and s.isascii())
                stats["ascii_only"] = ascii_count
                stats["non_ascii_count"] = len(str_series_for_analysis) - ascii_count
            except:
                stats["ascii_only"] = None
                stats["non_ascii_count"] = None

            return stats
        except Exception as e:
            return {"error": f"Failed to calculate string statistics: {str(e)}"}

    def _calculate_boolean_statistics(self, series: pd.Series) -> Dict[str, Any]:
        """Calculate boolean-specific statistics."""
        if len(series) == 0:
            return {}

        try:
            value_counts = series.value_counts()
            true_count = value_counts.get(True, 0)
            false_count = value_counts.get(False, 0)
            total = len(series)

            return {
                "true_count": int(true_count),
                "false_count": int(false_count),
                "true_percentage": float(true_count / total * 100) if total > 0 else 0.0,
                "false_percentage": float(false_count / total * 100) if total > 0 else 0.0
            }
        except Exception as e:
            return {"error": f"Failed to calculate boolean statistics: {str(e)}"}

    def generate_and_save_metadata(self, table: pa.Table) -> Optional[str]:
        """Generate metadata and save to separate file if configured."""
        if not self.config.generate_metadata:
            return None

        metadata = self._generate_metadata(table)

        if self.config.metadata_output_path:
            metadata_json = json.dumps(metadata, indent=2, default=str)

            if is_s3_path(str(self.config.metadata_output_path)):
                with self.io_handler.open_for_write(str(self.config.metadata_output_path), encoding='utf-8') as f:
                    f.write(metadata_json)
            else:
                with open(self.config.metadata_output_path, 'w', encoding='utf-8') as f:
                    f.write(metadata_json)

            return str(self.config.metadata_output_path)

        return None

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
