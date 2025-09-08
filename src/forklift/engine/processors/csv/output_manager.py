"""Output management utilities for CSV processing."""

import json
from datetime import datetime
from pathlib import Path
from typing import Union, List, Optional

import pyarrow as pa

from ...config import ImportConfig, ProcessingResults
from ....io import UnifiedIOHandler, is_s3_path, S3Path, create_parquet_writer
from ....metadata import OutputMetadataCollector


class OutputManager:
    """Handles output file operations including writing, manifest, and metadata creation."""

    def __init__(self, io_handler: UnifiedIOHandler):
        """Initialize output manager with I/O handler."""
        self.io_handler = io_handler

    def initialize_metadata_collector(self, config: ImportConfig, schema_processor) -> Optional[OutputMetadataCollector]:
        """Initialize output metadata collector if enabled.

        Args:
            config: Import configuration
            schema_processor: Schema processor instance

        Returns:
            OutputMetadataCollector instance or None if disabled
        """
        if not config.create_metadata:
            return None

        # Read metadata configuration from schema if available
        metadata_config = {}
        if schema_processor.schema:
            metadata_config = schema_processor.get_metadata_config()

        return OutputMetadataCollector(
            enabled=metadata_config.get('enabled', True),
            enum_threshold=metadata_config.get('enum_detection', {}).get('uniqueness_threshold', 0.1),
            uniqueness_threshold=0.95,  # Default threshold for too unique columns
            top_n_values=metadata_config.get('statistics', {}).get('categorical', {}).get('top_n_values', 10),
            quantiles=metadata_config.get('statistics', {}).get('numeric', {}).get('quantiles', [0.25, 0.5, 0.75, 0.9, 0.95, 0.99])
        )

    def write_batch_to_parquet(self, batch: pa.RecordBatch, writer) -> None:
        """Write a batch to parquet file.

        Args:
            batch: PyArrow record batch to write
            writer: Parquet writer instance
        """
        if len(batch) > 0:
            table = pa.Table.from_batches([batch])
            writer.write_table(table)

    def close_writers(self, good_writer, bad_writer, good_file: str, bad_file: str, results: ProcessingResults) -> None:
        """Close parquet writers and update results.

        Args:
            good_writer: Writer for valid data
            bad_writer: Writer for invalid data
            good_file: Path to good data file
            bad_file: Path to bad data file
            results: Processing results to update
        """
        if good_writer:
            good_writer.close()
            results.output_files.append(good_file)

        if bad_writer:
            bad_writer.close()
            results.output_files.append(bad_file)

    def create_output_files(self, config: ImportConfig, results: ProcessingResults,
                          output_metadata_collector: Optional[OutputMetadataCollector], good_writer) -> None:
        """Create manifest and metadata files.

        Args:
            config: Import configuration
            results: Processing results
            output_metadata_collector: Metadata collector instance
            good_writer: Writer for valid data (used to get schema)
        """
        # Create manifest and metadata (support S3 outputs)
        if config.create_manifest:
            results.manifest_file = self.create_manifest(config.output_path, results.output_files)

        if config.create_metadata:
            # Generate and save output metadata if we collected it
            if output_metadata_collector and output_metadata_collector.total_rows > 0:
                # Get the schema from the good writer if available
                output_schema = good_writer.schema if good_writer else None

                # Generate source info for metadata
                source_info = {
                    "input_path": str(config.input_path),
                    "processing_type": "csv_processing",
                    "schema_file": str(config.schema_file) if config.schema_file else None,
                    "total_batches_processed": "streaming",
                    "final_output_files": results.output_files
                }

                # Generate comprehensive metadata about the final output data
                output_metadata = output_metadata_collector.generate_metadata(output_schema, source_info)

                # Save output metadata to separate file
                output_metadata_path = output_metadata_collector.save_metadata(
                    config.output_path,
                    "output_data_metadata.json"
                )

                if output_metadata_path:
                    print(f"Output data metadata saved to: {output_metadata_path}")

            # Still create the traditional processing metadata
            results.metadata_file = self.create_metadata(config.output_path, results, config)

    def create_manifest(self, output_path: Union[str, Path], files: List[str]) -> str:
        """Create manifest file supporting S3 output locations.

        Args:
            output_path: Output directory path
            files: List of output file paths

        Returns:
            Path to created manifest file
        """
        manifest = {
            "format_version": "1.0",
            "files": [
                {
                    "file_path": str(Path(f).name) if not is_s3_path(f) else S3Path(f).name,
                    "file_size": self.io_handler.get_size(f) if self.io_handler.exists(f) else 0,
                }
                for f in files
            ],
            "created_at": datetime.now().isoformat(),
        }

        if is_s3_path(output_path):
            # S3 output
            manifest_path = S3Path(str(output_path)).join("manifest.json")
            with self.io_handler.open_for_write(str(manifest_path), encoding='utf-8') as f:
                json.dump(manifest, f, indent=2)
            return str(manifest_path)
        else:
            # Local output
            output_dir = Path(output_path)
            manifest_path = output_dir / "manifest.json"
            with open(manifest_path, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, indent=2)
            return str(manifest_path)

    def create_metadata(self, output_path: Union[str, Path], results: ProcessingResults, config: ImportConfig) -> str:
        """Create metadata file supporting S3 output locations.

        Args:
            output_path: Output directory path
            results: Processing results
            config: Import configuration

        Returns:
            Path to created metadata file
        """
        metadata = {
            "processing_summary": {
                "total_rows": results.total_rows,
                "valid_rows": results.valid_rows,
                "invalid_rows": results.invalid_rows,
                "execution_time_seconds": results.execution_time,
            },
            "input_config": {
                "input_path": str(config.input_path),
                "schema_file": str(config.schema_file) if config.schema_file else None,
                "header_mode": config.header_mode.value if hasattr(config.header_mode, 'value') else str(config.header_mode),
                "batch_size": config.batch_size,
            },
            "output_files": results.output_files,
            "created_at": datetime.now().isoformat(),
        }

        if is_s3_path(output_path):
            # S3 output
            metadata_path = S3Path(str(output_path)).join("metadata.json")
            with self.io_handler.open_for_write(str(metadata_path), encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)
            return str(metadata_path)
        else:
            # Local output
            output_dir = Path(output_path)
            metadata_path = output_dir / "metadata.json"
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)
            return str(metadata_path)
