"""Excel importer implementation."""

from __future__ import annotations

import json
import logging
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Optional, Set, Union

import pyarrow.parquet as pq

from ...io import S3Path, UnifiedIOHandler, create_parquet_writer, is_s3_path
from ..config import ProcessingResults
from ..exceptions import SCHEMA_INVALID, ImportInterrupted, ProcessingError, with_error_code
from ..progress import ImportHooks
from .output_location import (
    OutputLocation,
    discard_finished_outputs,
    discard_partial_output,
    unique_stem,
)


class ExcelImporter:
    """Handles Excel file import operations."""

    @staticmethod
    def import_excel(
        input_path: Union[str, Path],
        output_path: Union[str, Path],
        schema_file: Union[str, Path] = None,
        **kwargs,
    ) -> ProcessingResults:
        """Import Excel file with multi-sheet support.

        Every sheet becomes ``<input stem>_<sheet name>.parquet`` in ``output_path``, which is
        either a local directory or an ``s3://bucket/prefix`` URI (written through the S3
        parquet writer). Sheet names are sanitised to plain file names; if sanitising makes two
        sheets collide (``Q1/Q2`` and ``Q1:Q2``) the later ones get a ``_2``, ``_3``... suffix in
        workbook order, so no sheet silently overwrites another.

        ``input_path`` and ``schema_file`` may be ``s3://`` URIs, read with ``s3_client`` (or
        boto3's default credentials). Excel readers need a seekable file, so an S3 workbook is
        copied to a temporary directory first and removed afterwards.

        ``progress`` and ``cancel`` (keyword arguments, see ``forklift.engine.progress``) are
        called after every sheet. A cancelled import (or one stopped by its progress callback)
        removes the sheets it had already written.

        Raises:
            ValueError: If an output file name is invalid or would leave the output directory
            ImportInterrupted: The import was cancelled or stopped by its progress callback
        """
        from ...inputs.excel import ExcelInputHandler
        from ...schema.excel_schema_importer import ExcelSchemaImporter

        logger = logging.getLogger(__name__)
        start_time = time.time()
        hooks = ImportHooks(kwargs.get("progress"), kwargs.get("cancel"))

        scratch: Optional[tempfile.TemporaryDirectory] = None
        written: list = []
        try:
            location = OutputLocation(output_path)
            if is_s3_path(input_path):
                scratch = tempfile.TemporaryDirectory(prefix="forklift-excel-")
                input_path = ExcelImporter._download(
                    input_path, Path(scratch.name), kwargs.get("s3_client")
                )
            input_path = Path(input_path) if isinstance(input_path, str) else input_path

            if not input_path.exists():
                raise FileNotFoundError(f"Input file not found: {input_path}")

            # Create output directory
            location.prepare()

            # Load and validate schema if provided
            excel_config = None
            if schema_file:
                # Parse schema
                try:
                    schema_importer = ExcelSchemaImporter(
                        ExcelImporter._schema_source(schema_file, kwargs.get("s3_client")),
                        validate=True,
                    )
                    excel_config = ExcelImporter._create_excel_config_from_schema(schema_importer)
                    logger.info(f"Loaded Excel schema from {schema_file}")
                except Exception as e:
                    logger.error(f"Failed to load Excel schema: {e}")
                    raise with_error_code(
                        ProcessingError(f"Schema validation failed: {e}"), SCHEMA_INVALID
                    ) from e

            # Create default config if no schema provided
            if excel_config is None:
                excel_config = ExcelImporter._create_default_excel_config(input_path, **kwargs)

            # Override config with kwargs
            if "values_only" in kwargs:
                excel_config.values_only = kwargs["values_only"]
            if "engine" in kwargs:
                excel_config.engine = kwargs["engine"]
            if "date_system" in kwargs:
                excel_config.date_system = kwargs["date_system"]

            # Initialize Excel input handler
            excel_handler = ExcelInputHandler(excel_config)

            # Get file information for logging
            file_info = excel_handler.get_sheet_info(input_path)
            logger.info(
                f"Processing Excel file with {file_info['sheet_count']} sheets "
                f"using {file_info['engine']} engine"
            )

            # Process sheets and collect results
            results = ProcessingResults()
            processed_sheets = 0
            total_rows = 0
            used_names: Set[str] = set()
            workbook_size = input_path.stat().st_size

            for sheet_name, arrow_table in excel_handler.process_sheets(input_path):
                logger.info(f"Processing sheet '{sheet_name}' with {arrow_table.num_rows} rows")

                # Generate a unique, validated output filename for this sheet
                safe_sheet_name = ExcelImporter._sanitize_filename(sheet_name)
                output_stem = unique_stem(f"{input_path.stem}_{safe_sheet_name}", used_names)
                sheet_output_path = location.target(output_stem)

                # Write sheet data to Parquet directly using PyArrow
                ExcelImporter._write_sheet(
                    arrow_table, sheet_output_path, location.is_s3, kwargs.get("s3_client")
                )
                logger.info(f"Wrote sheet '{sheet_name}' to {sheet_output_path}")

                # Update results
                processed_sheets += 1
                total_rows += arrow_table.num_rows
                results.output_files.append(str(sheet_output_path))
                written.append(sheet_output_path)

                # Sheet boundary: report progress, stop here if the caller cancelled
                hooks.report(total_rows, 0, workbook_size)

            # Finalize results
            processing_time = time.time() - start_time
            results.total_rows = total_rows
            results.valid_rows = total_rows  # All rows are considered valid for Excel
            results.invalid_rows = 0
            results.execution_time = processing_time

            logger.info(
                f"Excel import completed successfully: {processed_sheets} sheets, "
                f"{total_rows} total rows in {processing_time:.2f}s"
            )

            return results

        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"Excel import failed after {processing_time:.2f}s: {e}")
            if isinstance(e, ImportInterrupted):
                # A stopped import keeps nothing: the sheets written so far go too
                discard_finished_outputs(written, kwargs.get("s3_client"))

            # Return error results
            results = ProcessingResults()
            results.execution_time = processing_time
            results.errors.append(str(e))
            raise
        finally:
            if scratch is not None:
                scratch.cleanup()

    @staticmethod
    def _download(uri: Union[str, Path], directory: Path, s3_client: Any = None) -> Path:
        """Copy an S3 workbook into ``directory`` under its own file name."""
        target = directory / S3Path(str(uri)).name
        with UnifiedIOHandler(s3_client).open_for_read(str(uri), mode="rb") as source:
            with open(target, "wb") as sink:
                shutil.copyfileobj(source, sink)
        return target

    @staticmethod
    def _schema_source(
        schema_file: Union[str, Path], s3_client: Any = None
    ) -> Union[Path, Dict[str, Any]]:
        """A local schema path, or the parsed JSON of an S3 schema file."""
        if is_s3_path(schema_file):
            with UnifiedIOHandler(s3_client).open_for_read(str(schema_file)) as f:
                return json.load(f)
        return Path(schema_file)

    @staticmethod
    def _create_excel_config_from_schema(schema_importer):
        """Create ExcelInputConfig from schema importer."""
        from ...inputs.config import ExcelInputConfig, ExcelSheetConfig

        # Convert schema sheets to config objects
        sheet_configs = []
        for sheet_def in schema_importer.sheets:
            sheet_config = ExcelSheetConfig(
                select=sheet_def.get("select", {}),
                columns=sheet_def.get("columns"),
                header=sheet_def.get("header"),
                data_start_row=sheet_def.get("dataStartRow"),
                data_end_row=sheet_def.get("dataEndRow"),
                skip_blank_rows=sheet_def.get("skipBlankRows", True),
                name_override=sheet_def.get("nameOverride"),
            )
            sheet_configs.append(sheet_config)

        return ExcelInputConfig(
            sheets=sheet_configs,
            values_only=schema_importer.values_only,
            date_system=schema_importer.date_system,
            nulls=schema_importer.nulls,
        )

    @staticmethod
    def _create_default_excel_config(file_path: Path, **kwargs):
        """Create default ExcelInputConfig when no schema is provided."""
        from ...inputs.config import ExcelInputConfig, ExcelSheetConfig
        from ...inputs.excel import ExcelInputHandler

        # Create a temporary handler to get sheet info
        temp_config = ExcelInputConfig(sheets=[])
        temp_handler = ExcelInputHandler(temp_config)

        try:
            file_info = temp_handler.get_sheet_info(file_path)
            sheet_names = file_info["sheet_names"]

            # Create configs for all sheets or specific sheet
            sheet_configs = []
            sheet_spec = kwargs.get("sheet")
            if sheet_spec is not None:
                # Process specific sheet
                if (
                    isinstance(sheet_spec, str)
                    and sheet_spec not in sheet_names
                    and sheet_spec.isascii()
                    and sheet_spec.isdigit()
                ):
                    # e.g. the CLI passes "0": a sheet name wins, otherwise it is an index
                    sheet_spec = int(sheet_spec)
                if isinstance(sheet_spec, str):
                    # Sheet name
                    if sheet_spec in sheet_names:
                        sheet_config = ExcelSheetConfig(select={"name": sheet_spec})
                        sheet_configs.append(sheet_config)
                    else:
                        raise ValueError(f"Sheet '{sheet_spec}' not found in workbook")
                elif isinstance(sheet_spec, int) and not isinstance(sheet_spec, bool):
                    # Sheet index
                    if 0 <= sheet_spec < len(sheet_names):
                        sheet_config = ExcelSheetConfig(select={"index": sheet_spec})
                        sheet_configs.append(sheet_config)
                    else:
                        raise ValueError(f"Sheet index {sheet_spec} out of range")
                else:
                    raise ValueError(
                        f"Sheet must be a sheet name or a 0-based index, got {sheet_spec!r}"
                    )
            else:
                # Process all sheets (also for sheet=None)
                for i, sheet_name in enumerate(sheet_names):
                    sheet_config = ExcelSheetConfig(select={"name": sheet_name})
                    sheet_configs.append(sheet_config)

            return ExcelInputConfig(
                sheets=sheet_configs,
                values_only=kwargs.get("values_only", True),
                date_system=kwargs.get("date_system", "1900"),
                engine=kwargs.get("engine"),
            )

        finally:
            temp_handler.close_workbook()

    @staticmethod
    def _write_sheet(arrow_table, target, to_s3: bool, s3_client=None) -> None:
        """Write one sheet to ``target`` (local path or S3 URI); never leave a partial file."""
        if not to_s3:
            try:
                pq.write_table(arrow_table, target)
            except BaseException:
                Path(target).unlink(missing_ok=True)
                raise
            return

        writer = create_parquet_writer(target, arrow_table.schema, s3_client=s3_client)
        try:
            writer.write_table(arrow_table)
            writer.close()
        except BaseException:
            discard_partial_output(writer, target)
            raise

    @staticmethod
    def _sanitize_filename(filename: str) -> str:
        """Sanitize sheet name for use as filename."""
        import re

        # Replace invalid filename characters (and control characters) with underscores
        sanitized = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", filename)
        # Remove leading/trailing whitespace and dots
        sanitized = sanitized.strip(" .")
        # Ensure not empty
        return sanitized or "sheet"
