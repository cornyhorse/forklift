"""Core schema generator that orchestrates all components."""

import json
from typing import Any, Dict, Optional, Union
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

import pyarrow as pa

from .inference import DataTypeInferrer
from .validation import SchemaValidator
from ..processors.json_schema import JSONSchemaProcessor
from ..processors.config_parser import ConfigurationParser
from ..processors.metadata import MetadataGenerator
from ..types.special_types import SpecialTypeDetector
from ..utils.formatters import SchemaFormatter
from ...io import UnifiedIOHandler, is_s3_path

# Import pyperclip with fallback
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
    nrows: Optional[int] = 1000
    output_target: OutputTarget = OutputTarget.STDOUT
    output_path: Optional[Union[str, Path]] = None
    delimiter: str = ","
    encoding: str = "utf-8"
    sheet_name: Optional[str] = None
    include_sample_data: bool = False
    user_specified_primary_key: Optional[list] = None
    generate_metadata: bool = True
    metadata_output_path: Optional[Union[str, Path]] = None
    enum_threshold: float = 0.1
    uniqueness_threshold: float = 0.95
    top_n_values: int = 10
    quantiles: Optional[list] = None
    infer_primary_key_from_metadata: bool = False

    def __post_init__(self):
        if self.quantiles is None:
            self.quantiles = [0.25, 0.5, 0.75, 0.9, 0.95, 0.99]


class SchemaGenerator:
    """Main schema generation orchestrator."""

    def __init__(self, config: SchemaGenerationConfig):
        self.config = config
        self.io_handler = UnifiedIOHandler()

        # Initialize components
        self.inferrer = DataTypeInferrer()
        self.validator = SchemaValidator()
        self.json_processor = JSONSchemaProcessor()
        self.config_parser = ConfigurationParser()
        self.metadata_generator = MetadataGenerator()
        self.special_type_detector = SpecialTypeDetector()
        self.formatter = SchemaFormatter()

    def generate_schema(self) -> Dict[str, Any]:
        """Generate a schema object from the configured file.

        Returns:
            Dict containing the generated schema object
        """
        # Read sample data based on file type
        table = self._read_sample_data()

        # Generate schema from Arrow table
        schema = self._generate_schema_from_table(table)

        return schema

    def _read_sample_data(self) -> pa.Table:
        """Read sample data based on file type."""
        if self.config.file_type == FileType.CSV:
            return self.inferrer.read_csv_sample(
                self.config.input_path,
                self.config.nrows,
                self.config.delimiter,
                self.config.encoding
            )
        elif self.config.file_type == FileType.EXCEL:
            return self.inferrer.read_excel_sample(
                self.config.input_path,
                self.config.nrows,
                self.config.sheet_name
            )
        elif self.config.file_type == FileType.PARQUET:
            return self.inferrer.read_parquet_sample(
                self.config.input_path,
                self.config.nrows
            )
        else:
            raise ValueError(f"Unsupported file type: {self.config.file_type}")

    def _generate_schema_from_table(self, table: pa.Table) -> Dict[str, Any]:
        """Generate schema object from PyArrow table."""
        # Create base schema structure
        schema = self.formatter.create_base_schema(self.config.file_type.value)

        # Generate properties from table schema
        properties = self.json_processor.generate_properties_from_table(table)
        required_fields = self.json_processor.determine_required_fields(table)

        schema["properties"] = properties
        schema["required"] = required_fields

        # Add primary key configuration
        primary_key_config = self.config_parser.generate_primary_key_config(table, self.config)
        if primary_key_config:
            schema["x-primaryKey"] = primary_key_config

        # Add file-type specific extensions
        if self.config.file_type == FileType.CSV:
            schema["x-csv"] = self.json_processor.generate_csv_extension(table, self.config)
        elif self.config.file_type == FileType.EXCEL:
            schema["x-excel"] = self.json_processor.generate_excel_extension(self.config)

        # Add data transformation extensions
        schema["x-transformations"] = self.config_parser.generate_transformation_extension(table)

        # Add sample data if requested
        if self.config.include_sample_data:
            schema["x-sample"] = self.json_processor.generate_sample_data(table)

        # Add generation metadata
        schema = self.formatter.add_generation_metadata(
            schema,
            str(self.config.input_path),
            table.num_rows
        )

        # Add metadata if requested
        if self.config.generate_metadata:
            metadata_config = {
                'enum_threshold': self.config.enum_threshold,
                'uniqueness_threshold': self.config.uniqueness_threshold,
                'top_n_values': self.config.top_n_values,
                'quantiles': self.config.quantiles,
                'source_file': str(self.config.input_path)
            }
            metadata = self.metadata_generator.generate_metadata(table, metadata_config)
            if metadata:
                schema["x-metadata"] = metadata

        return schema

    def generate_and_save_metadata(self, table: pa.Table) -> Optional[str]:
        """Generate metadata and save to separate file if configured."""
        if not self.config.generate_metadata:
            return None

        metadata_config = {
            'enum_threshold': self.config.enum_threshold,
            'uniqueness_threshold': self.config.uniqueness_threshold,
            'top_n_values': self.config.top_n_values,
            'quantiles': self.config.quantiles,
            'source_file': str(self.config.input_path)
        }
        metadata = self.metadata_generator.generate_metadata(table, metadata_config)

        if self.config.metadata_output_path:
            metadata_json = self.formatter.format_schema_json(metadata)

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
        schema_json = self.formatter.format_schema_json(schema)

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

    def validate_generated_schema(self, schema: Dict[str, Any], table: pa.Table) -> tuple:
        """Validate the generated schema against the data.

        Args:
            schema: Generated schema
            table: Source data table

        Returns:
            tuple: (is_valid, errors/issues)
        """
        # Validate schema structure
        structure_valid, structure_errors = self.validator.validate_schema_structure(schema)

        # Validate data compatibility
        data_valid, data_issues = self.validator.validate_data_compatibility(schema, table)

        # Validate transformation config if present
        transform_valid = True
        transform_errors = []
        if "x-transformations" in schema:
            transform_valid, transform_errors = self.validator.validate_transformation_config(
                schema["x-transformations"]
            )

        all_valid = structure_valid and data_valid and transform_valid
        all_issues = structure_errors + data_issues + transform_errors

        return all_valid, all_issues
