"""Core CSV processor that orchestrates the processing workflow."""

import time
from typing import Tuple

import pyarrow as pa

from ..base_processor import BaseProcessor
from ..header_detector import HeaderDetector
from ..schema_processor import SchemaProcessor
from ..batch_processor import BatchProcessor
from ...config import ImportConfig, ProcessingResults
from ....io import UnifiedIOHandler, create_parquet_writer

from .validator import BatchValidator
from .output_manager import OutputManager
from .path_manager import PathManager


class CSVProcessor(BaseProcessor):
    """Handles CSV-specific data processing operations using organized components."""

    def __init__(self):
        """Initialize the CSV processor."""
        self.io_handler = None
        self.schema_processor = None
        self.header_detector = None
        self.batch_processor = None
        self.validator = BatchValidator()
        self.output_manager = None
        self.path_manager = PathManager()

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
            self._initialize_components(config)

            # Load schema if provided
            schema = self.schema_processor.load_schema()

            # Detect header - now works with S3 inputs
            header_row_index, column_names = self._detect_header_row(config)

            # Prepare output paths - support both local and S3 outputs
            good_file, bad_file, use_s3_output = self.path_manager.prepare_output_paths(config.output_path)

            # Initialize parquet writers using unified I/O
            good_writer = None
            bad_writer = None

            # Initialize output metadata collector if enabled
            output_metadata_collector = self.output_manager.initialize_metadata_collector(
                config, self.schema_processor
            )

            # Process batches using extracted batch processor
            for batch in self.batch_processor.create_s3_batch_reader(
                config.input_path,
                column_names,
                header_row_index,
                self.header_detector.should_stop_for_footer
            ):
                # Validate and split batch
                valid_batch, invalid_batch = self.validator.validate_batch(batch, schema, config)

                # Initialize writers on first batch (to get schema)
                good_writer, bad_writer = self._initialize_writers_if_needed(
                    good_writer, bad_writer, valid_batch, invalid_batch,
                    good_file, bad_file, use_s3_output, config
                )

                # Write batches and collect metadata from FINAL OUTPUT DATA
                self._process_and_write_batches(
                    valid_batch, invalid_batch, good_writer, bad_writer,
                    output_metadata_collector, results,
                    bad_file, use_s3_output, config
                )

                results.total_rows += len(batch)

            # Close writers
            self.output_manager.close_writers(good_writer, bad_writer, good_file, bad_file, results)

            # Create manifest and metadata (support S3 outputs)
            self.output_manager.create_output_files(config, results, output_metadata_collector, good_writer)

            results.execution_time = time.time() - start_time

        except Exception as e:
            results.errors.append(str(e))
            results.execution_time = time.time() - start_time
            raise

        return results

    def _initialize_components(self, config: ImportConfig) -> None:
        """Initialize all processing components."""
        self.io_handler = UnifiedIOHandler()
        self.schema_processor = SchemaProcessor(config, self.io_handler)
        self.header_detector = HeaderDetector(config, self.io_handler)
        self.batch_processor = BatchProcessor(config, self.io_handler)
        self.output_manager = OutputManager(self.io_handler)

    def _detect_header_row(self, config: ImportConfig) -> Tuple[int, list]:
        """Detect header row location and extract column names."""
        header_idx, columns = self.header_detector.detect_header_row(config.input_path)

        # Handle ABSENT mode fallback to schema
        if config.header_mode.name == 'ABSENT' and not columns:
            schema_columns = self.schema_processor.get_column_names_from_schema()
            if schema_columns:
                return -1, schema_columns

        return header_idx, columns

    def _initialize_writers_if_needed(self, good_writer, bad_writer, valid_batch, invalid_batch,
                                    good_file: str, bad_file: str, use_s3_output: bool, config: ImportConfig):
        """Initialize parquet writers on first batch if needed."""
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

        return good_writer, bad_writer

    def _process_and_write_batches(self, valid_batch: pa.RecordBatch, invalid_batch: pa.RecordBatch,
                                 good_writer, bad_writer, output_metadata_collector, results: ProcessingResults,
                                 bad_file: str, use_s3_output: bool, config: ImportConfig) -> None:
        """Process and write valid and invalid batches."""
        if len(valid_batch) > 0:
            self.output_manager.write_batch_to_parquet(valid_batch, good_writer)
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
            self.output_manager.write_batch_to_parquet(invalid_batch, bad_writer)
            results.invalid_rows += len(invalid_batch)
