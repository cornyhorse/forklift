"""Excel importer implementation."""

from __future__ import annotations
import time
import logging
from pathlib import Path
from typing import Union

import pyarrow.parquet as pq

from ..config import ProcessingResults
from ..exceptions import ProcessingError


class ExcelImporter:
    """Handles Excel file import operations."""

    @staticmethod
    def import_excel(
        input_path: Union[str, Path],
        output_path: Union[str, Path],
        schema_file: Union[str, Path] = None,
        **kwargs
    ) -> ProcessingResults:
        """Import Excel file with multi-sheet support."""
        from ...inputs.excel import ExcelInputHandler
        from ...inputs.config import ExcelInputConfig, ExcelSheetConfig
        from ...schema.excel_schema_importer import ExcelSchemaImporter

        logger = logging.getLogger(__name__)
        start_time = time.time()

        try:
            # Convert paths to Path objects
            input_path = Path(input_path) if isinstance(input_path, str) else input_path
            output_path = Path(output_path) if isinstance(output_path, str) else output_path

            # For now, support local files only - S3 support can be added later
            if not input_path.exists():
                raise FileNotFoundError(f"Input file not found: {input_path}")

            # Create output directory
            output_path.mkdir(parents=True, exist_ok=True)

            # Load and validate schema if provided
            excel_config = None
            if schema_file:
                schema_path = Path(schema_file) if isinstance(schema_file, str) else schema_file

                # Parse schema
                try:
                    schema_importer = ExcelSchemaImporter(schema_path, validate=True)
                    excel_config = ExcelImporter._create_excel_config_from_schema(schema_importer)
                    logger.info(f"Loaded Excel schema from {schema_file}")
                except Exception as e:
                    logger.error(f"Failed to load Excel schema: {e}")
                    raise ProcessingError(f"Schema validation failed: {e}") from e

            # Create default config if no schema provided
            if excel_config is None:
                excel_config = ExcelImporter._create_default_excel_config(input_path, **kwargs)

            # Override config with kwargs
            if 'values_only' in kwargs:
                excel_config.values_only = kwargs['values_only']
            if 'engine' in kwargs:
                excel_config.engine = kwargs['engine']
            if 'date_system' in kwargs:
                excel_config.date_system = kwargs['date_system']

            # Initialize Excel input handler
            excel_handler = ExcelInputHandler(excel_config)

            # Get file information for logging
            file_info = excel_handler.get_sheet_info(input_path)
            logger.info(f"Processing Excel file with {file_info['sheet_count']} sheets using {file_info['engine']} engine")

            # Process sheets and collect results
            results = ProcessingResults()
            processed_sheets = 0
            total_rows = 0

            for sheet_name, arrow_table in excel_handler.process_sheets(input_path):
                logger.info(f"Processing sheet '{sheet_name}' with {arrow_table.num_rows} rows")

                # Generate output filename for this sheet
                safe_sheet_name = ExcelImporter._sanitize_filename(sheet_name)
                output_filename = f"{input_path.stem}_{safe_sheet_name}.parquet"
                sheet_output_path = output_path / output_filename

                # Write sheet data to Parquet directly using PyArrow
                pq.write_table(arrow_table, sheet_output_path)
                logger.info(f"Wrote sheet '{sheet_name}' to {sheet_output_path}")

                # Update results
                processed_sheets += 1
                total_rows += arrow_table.num_rows
                results.output_files.append(str(sheet_output_path))

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

            # Return error results
            results = ProcessingResults()
            results.execution_time = processing_time
            results.errors.append(str(e))
            raise

    @staticmethod
    def _create_excel_config_from_schema(schema_importer):
        """Create ExcelInputConfig from schema importer."""
        from ...inputs.config import ExcelInputConfig, ExcelSheetConfig

        # Convert schema sheets to config objects
        sheet_configs = []
        for sheet_def in schema_importer.sheets:
            sheet_config = ExcelSheetConfig(
                select=sheet_def.get('select', {}),
                columns=sheet_def.get('columns'),
                header=sheet_def.get('header'),
                data_start_row=sheet_def.get('dataStartRow'),
                data_end_row=sheet_def.get('dataEndRow'),
                skip_blank_rows=sheet_def.get('skipBlankRows', True),
                name_override=sheet_def.get('nameOverride')
            )
            sheet_configs.append(sheet_config)

        return ExcelInputConfig(
            sheets=sheet_configs,
            values_only=schema_importer.values_only,
            date_system=schema_importer.date_system,
            nulls=schema_importer.nulls
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
            sheet_names = file_info['sheet_names']

            # Create configs for all sheets or specific sheet
            sheet_configs = []
            if 'sheet' in kwargs:
                # Process specific sheet
                sheet_spec = kwargs['sheet']
                if isinstance(sheet_spec, str):
                    # Sheet name
                    if sheet_spec in sheet_names:
                        sheet_config = ExcelSheetConfig(select={'name': sheet_spec})
                        sheet_configs.append(sheet_config)
                    else:
                        raise ValueError(f"Sheet '{sheet_spec}' not found in workbook")
                elif isinstance(sheet_spec, int):
                    # Sheet index
                    if 0 <= sheet_spec < len(sheet_names):
                        sheet_config = ExcelSheetConfig(select={'index': sheet_spec})
                        sheet_configs.append(sheet_config)
                    else:
                        raise ValueError(f"Sheet index {sheet_spec} out of range")
            else:
                # Process all sheets
                for i, sheet_name in enumerate(sheet_names):
                    sheet_config = ExcelSheetConfig(select={'name': sheet_name})
                    sheet_configs.append(sheet_config)

            return ExcelInputConfig(
                sheets=sheet_configs,
                values_only=kwargs.get('values_only', True),
                date_system=kwargs.get('date_system', '1900'),
                engine=kwargs.get('engine')
            )

        finally:
            temp_handler.close_workbook()

    @staticmethod
    def _sanitize_filename(filename: str) -> str:
        """Sanitize sheet name for use as filename."""
        import re
        # Replace invalid filename characters with underscores
        sanitized = re.sub(r'[<>:"/\\|?*]', '_', filename)
        # Remove leading/trailing whitespace and dots
        sanitized = sanitized.strip(' .')
        # Ensure not empty
        return sanitized or 'sheet'
