"""CSV-specific data processor implementation."""

from __future__ import annotations
import time
from pathlib import Path
from typing import Union

import pyarrow as pa

from .base_processor import BaseProcessor
from .header_detector import HeaderDetector
from .schema_processor import SchemaProcessor
from .batch_processor import BatchProcessor
from ..config import ImportConfig, ProcessingResults
from ...io import UnifiedIOHandler, is_s3_path, S3Path, create_parquet_writer
from ...metadata import OutputMetadataCollector
from ...processors.row_hash_factory import create_row_hash_processor_from_schema


class CSVProcessor(BaseProcessor):
    """Handles CSV-specific data processing operations."""

    def __init__(self):
        """Initialize the CSV processor."""
        self.io_handler = None
        self.schema_processor = None
        self.header_detector = None
        self.batch_processor = None

    def process(self, config: ImportConfig) -> ProcessingResults:
        """Process CSV file with streaming and validation.

        Main processing method that orchestrates the entire CSV import workflow
        including header detection, streaming processing, validation, and output generation.
        Now supports S3 streaming for both input and output.

        Args:
            config: ImportConfig instance with processing configuration

        Returns:
            ProcessingResults object containing processing statistics and output paths

        Raises:
            Exception: Various exceptions may be raised during processing,
                      all are captured in the results.errors list
        """
        start_time = time.time()
        results = ProcessingResults()

        try:
            # Initialize components
            self.io_handler = UnifiedIOHandler()
            self.schema_processor = SchemaProcessor(config, self.io_handler)
            self.header_detector = HeaderDetector(config, self.io_handler)
            self.batch_processor = BatchProcessor(config, self.io_handler)

            # Load schema if provided
            schema = self.schema_processor.load_schema()

            # Detect header - now works with S3 inputs
            header_row_index, column_names = self._detect_header_row(config)

            # Prepare output paths - support both local and S3 outputs
            good_file, bad_file, use_s3_output = self._prepare_output_paths(config)

            # Initialize parquet writers using unified I/O
            good_writer = None
            bad_writer = None

            # Initialize output metadata collector if enabled
            output_metadata_collector = self._initialize_metadata_collector(config)

            # Process batches using extracted batch processor
            for batch in self.batch_processor.create_s3_batch_reader(
                config.input_path,
                column_names,
                header_row_index,
                self.header_detector.should_stop_for_footer
            ):
                # Validate and split batch
                valid_batch, invalid_batch = self._validate_batch(batch, schema, config)

                # Initialize writers on first batch (to get schema)
                if good_writer is None:
                    good_writer = create_parquet_writer(
                        good_file,
                        valid_batch.schema,
                        s3_client=self.io_handler.s3_client if use_s3_output else None,
                        compression=config.compression
                    )
                if bad_writer is None and len(invalid_batch) > 0:
                    bad_writer = create_parquet_writer(
                        bad_file,
                        invalid_batch.schema,
                        s3_client=self.io_handler.s3_client if use_s3_output else None,
                        compression=config.compression
                    )

                # Write batches and collect metadata from FINAL OUTPUT DATA
                if len(valid_batch) > 0:
                    self._write_batch_to_parquet(valid_batch, good_writer)
                    # Collect metadata from the final transformed valid data
                    if output_metadata_collector:
                        output_metadata_collector.add_batch(valid_batch)
                    results.valid_rows += len(valid_batch)

                if len(invalid_batch) > 0:
                    if bad_writer is None:
                        bad_writer = create_parquet_writer(
                            bad_file,
                            invalid_batch.schema,
                            s3_client=self.io_handler.s3_client if use_s3_output else None,
                            compression=config.compression
                        )
                    self._write_batch_to_parquet(invalid_batch, bad_writer)
                    results.invalid_rows += len(invalid_batch)

                results.total_rows += len(batch)

            # Close writers
            self._close_writers(good_writer, bad_writer, good_file, bad_file, results)

            # Create manifest and metadata (support S3 outputs)
            self._create_output_files(config, results, output_metadata_collector, good_writer)

            results.execution_time = time.time() - start_time

        except Exception as e:
            results.errors.append(str(e))
            results.execution_time = time.time() - start_time
            raise

        return results

    def _detect_header_row(self, config: ImportConfig):
        """Detect header row location and extract column names."""
        header_idx, columns = self.header_detector.detect_header_row(config.input_path)

        # Handle ABSENT mode fallback to schema
        if config.header_mode.name == 'ABSENT' and not columns:
            schema_columns = self.schema_processor.get_column_names_from_schema()
            if schema_columns:
                return -1, schema_columns

        return header_idx, columns

    def _prepare_output_paths(self, config: ImportConfig):
        """Prepare output file paths for both local and S3 outputs."""
        if is_s3_path(config.output_path):
            # S3 output path
            output_s3_path = S3Path(str(config.output_path))
            good_file = str(output_s3_path.join("data.parquet"))
            bad_file = str(output_s3_path.join("bad_rows.parquet"))
            use_s3_output = True
        else:
            # Local output path
            output_dir = Path(config.output_path)
            output_dir.mkdir(parents=True, exist_ok=True)
            good_file = str(output_dir / "data.parquet")
            bad_file = str(output_dir / "bad_rows.parquet")
            use_s3_output = False

        return good_file, bad_file, use_s3_output

    def _initialize_metadata_collector(self, config: ImportConfig):
        """Initialize output metadata collector if enabled."""
        if not config.create_metadata:
            return None

        # Read metadata configuration from schema if available
        metadata_config = {}
        if self.schema_processor.schema:
            metadata_config = self.schema_processor.get_metadata_config()

        return OutputMetadataCollector(
            enabled=metadata_config.get('enabled', True),
            enum_threshold=metadata_config.get('enum_detection', {}).get('uniqueness_threshold', 0.1),
            uniqueness_threshold=0.95,  # Default threshold for too unique columns
            top_n_values=metadata_config.get('statistics', {}).get('categorical', {}).get('top_n_values', 10),
            quantiles=metadata_config.get('statistics', {}).get('numeric', {}).get('quantiles', [0.25, 0.5, 0.75, 0.9, 0.95, 0.99])
        )

    def _validate_batch(self, batch: pa.RecordBatch, schema: pa.Schema, config: ImportConfig):
        """Validate batch and separate good/bad rows."""
        if not config.validate_schema or not schema:
            # No validation, return all as good
            empty_batch = batch.slice(0, 0)  # Empty batch with same schema
            return batch, empty_batch

        # For now, let's simplify validation - just check for required fields
        num_rows = len(batch)
        valid_mask = pa.array([True] * num_rows)

        for i, field in enumerate(schema):
            if i >= batch.num_columns:
                continue

            column = batch.column(i)

            # Null validation for required fields
            if not field.nullable:
                import pyarrow.compute as pc
                null_mask = pc.is_valid(column)
                valid_mask = pc.and_(valid_mask, null_mask)

        # Split into valid and invalid batches
        import pyarrow.compute as pc
        valid_indices = pc.filter(
            pa.array(range(num_rows)),
            valid_mask
        )
        invalid_indices = pc.filter(
            pa.array(range(num_rows)),
            pc.invert(valid_mask)
        )

        if len(valid_indices) > 0:
            valid_batch = pc.take(batch, valid_indices)
        else:
            valid_batch = batch.slice(0, 0)  # Empty batch

        if len(invalid_indices) > 0:
            invalid_batch = pc.take(batch, invalid_indices)
        else:
            invalid_batch = batch.slice(0, 0)  # Empty batch

        return valid_batch, invalid_batch

    def _write_batch_to_parquet(self, batch: pa.RecordBatch, writer):
        """Write a batch to parquet file."""
        if len(batch) > 0:
            table = pa.Table.from_batches([batch])
            writer.write_table(table)

    def _close_writers(self, good_writer, bad_writer, good_file, bad_file, results):
        """Close parquet writers and update results."""
        if good_writer:
            good_writer.close()
            results.output_files.append(good_file)

        if bad_writer:
            bad_writer.close()
            results.output_files.append(bad_file)

    def _create_output_files(self, config: ImportConfig, results: ProcessingResults,
                           output_metadata_collector, good_writer):
        """Create manifest and metadata files."""
        # Create manifest and metadata (support S3 outputs)
        if config.create_manifest:
            results.manifest_file = self._create_s3_manifest(config.output_path, results.output_files)

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
            results.metadata_file = self._create_s3_metadata(config.output_path, results)

    def _create_s3_manifest(self, output_path: Union[str, Path], files: list) -> str:
        """Create manifest file supporting S3 output locations."""
        from datetime import datetime

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
                import json
                json.dump(manifest, f, indent=2)
            return str(manifest_path)
        else:
            # Local output
            output_dir = Path(output_path)
            manifest_path = output_dir / "manifest.json"
            with open(manifest_path, 'w', encoding='utf-8') as f:
                import json
                json.dump(manifest, f, indent=2)
            return str(manifest_path)

    def _create_s3_metadata(self, output_path: Union[str, Path], results: ProcessingResults) -> str:
        """Create metadata file supporting S3 output locations."""
        from datetime import datetime

        metadata = {
            "processing_summary": {
                "total_rows": results.total_rows,
                "valid_rows": results.valid_rows,
                "invalid_rows": results.invalid_rows,
                "execution_time_seconds": results.execution_time,
            },
            "input_config": {
                "input_path": str(self.schema_processor.config.input_path),
                "schema_file": str(self.schema_processor.config.schema_file) if self.schema_processor.config.schema_file else None,
                "header_mode": self.schema_processor.config.header_mode.value if hasattr(self.schema_processor.config.header_mode, 'value') else str(self.schema_processor.config.header_mode),
                "batch_size": self.schema_processor.config.batch_size,
            },
            "output_files": results.output_files,
            "created_at": datetime.now().isoformat(),
        }

        if is_s3_path(output_path):
            # S3 output
            metadata_path = S3Path(str(output_path)).join("metadata.json")
            with self.io_handler.open_for_write(str(metadata_path), encoding='utf-8') as f:
                import json
                json.dump(metadata, f, indent=2)
            return str(metadata_path)
        else:
            # Local output
            output_dir = Path(output_path)
            metadata_path = output_dir / "metadata.json"
            with open(metadata_path, 'w', encoding='utf-8') as f:
                import json
                json.dump(metadata, f, indent=2)
            return str(metadata_path)
