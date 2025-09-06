"""SQL importer implementation."""

from __future__ import annotations
import time
import json
import logging
from pathlib import Path
from typing import Union
from datetime import datetime

from ..config import ProcessingResults
from ..exceptions import ProcessingError
from ...io import create_parquet_writer


class SqlImporter:
    """Handles SQL database import operations."""

    @staticmethod
    def import_sql(
        connection_string: str,
        output_path: Union[str, Path],
        schema_file: Union[str, Path] = None,
        **kwargs
    ) -> ProcessingResults:
        """Import data from SQL database with ODBC connectivity."""
        from ...inputs.sql import SqlInputHandler
        from ...inputs.config import SqlInputConfig
        from ...schema.sql_schema_importer import SqlSchemaImporter

        logger = logging.getLogger(__name__)
        start_time = time.time()

        try:
            # Convert paths to Path objects
            output_path = Path(output_path) if isinstance(output_path, str) else output_path

            # Create output directory
            output_path.mkdir(parents=True, exist_ok=True)

            # Schema file is now required for explicit table specification
            if not schema_file:
                raise ProcessingError("Schema file is required for SQL import to specify which tables to process")

            # Load and validate schema
            schema_path = Path(schema_file) if isinstance(schema_file, str) else schema_file

            try:
                schema_importer = SqlSchemaImporter(schema_path, validate=True)
                logger.info(f"Loaded SQL schema from {schema_file}")
            except Exception as e:
                logger.error(f"Failed to load SQL schema: {e}")
                raise ProcessingError(f"Schema validation failed: {e}") from e

            # Get explicit table list from schema
            tables_to_process = schema_importer.get_table_list()
            if not tables_to_process:
                raise ValueError("Schema file must specify at least one table to process")

            # Create SQL config
            config_kwargs = {
                'connection_string': connection_string,
                'batch_size': kwargs.get('batch_size', 10000),
                'query_timeout': kwargs.get('query_timeout', 300),
                'connection_timeout': kwargs.get('connection_timeout', 30),
                'use_quoted_identifiers': kwargs.get('use_quoted_identifiers', False),
                'schema_name': kwargs.get('schema_name'),
                'enable_streaming': kwargs.get('enable_streaming', True),
                'null_values': kwargs.get('null_values'),
            }

            # Remove None values
            config_kwargs = {k: v for k, v in config_kwargs.items() if v is not None}
            sql_config = SqlInputConfig(**config_kwargs)

            # Initialize SQL input handler
            sql_handler = SqlInputHandler(sql_config)
            sql_handler.set_schema_importer(schema_importer)

            # Connect to database and process tables
            with sql_handler:
                logger.info(f"Found {len(tables_to_process)} tables to process from schema")

                # Process each table
                total_rows = 0
                valid_rows = 0
                invalid_rows = 0
                processed_tables = 0
                output_files = []

                for schema_name, table_name, output_name in tables_to_process:
                    try:
                        logger.info(f"Processing table: {schema_name}.{table_name}")

                        # Generate output filename
                        if output_name:
                            table_output_name = output_name
                        elif schema_name and schema_name != 'default':
                            table_output_name = f"{schema_name}_{table_name}"
                        else:
                            table_output_name = table_name

                        output_file = output_path / f"{table_output_name}.parquet"

                        # Get table schema
                        table_schema = sql_handler.get_table_schema(schema_name, table_name)

                        # Create Parquet writer
                        writer = create_parquet_writer(output_file, table_schema)

                        # Process data in batches
                        table_rows = 0
                        for batch in sql_handler.read_table_data(schema_name, table_name):
                            writer.write_batch(batch)
                            table_rows += batch.num_rows
                            total_rows += batch.num_rows
                            valid_rows += batch.num_rows

                        # Close writer
                        writer.close()

                        if table_rows > 0:
                            output_files.append(str(output_file))
                            logger.info(f"Completed {schema_name}.{table_name}: {table_rows} rows -> {output_file}")
                        else:
                            logger.warning(f"Table {schema_name}.{table_name} contained no data")

                        processed_tables += 1

                    except Exception as e:
                        logger.error(f"Failed to process table {schema_name}.{table_name}: {e}")
                        invalid_rows += 1

            # Create results
            processing_time = time.time() - start_time
            results = ProcessingResults(
                total_rows=total_rows,
                valid_rows=valid_rows,
                invalid_rows=invalid_rows,
                execution_time=processing_time,
                output_files=output_files
            )

            # Create metadata file
            metadata = {
                "processing_summary": {
                    "total_tables_processed": processed_tables,
                    "total_rows": total_rows,
                    "valid_rows": valid_rows,
                    "invalid_rows": invalid_rows,
                    "execution_time_seconds": processing_time,
                    "processed_at": datetime.now().isoformat()
                },
                "input_config": {
                    "connection_string": connection_string,
                    "tables_processed": [(schema, table, output) for schema, table, output in tables_to_process],
                    "batch_size": sql_config.batch_size,
                    "query_timeout": sql_config.query_timeout,
                },
                "output_files": output_files
            }

            metadata_file = output_path / "metadata.json"
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)

            logger.info(
                f"SQL import completed successfully: {processed_tables} tables, "
                f"{total_rows} total rows in {processing_time:.2f}s"
            )

            return results

        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"SQL import failed after {processing_time:.2f}s: {e}")

            # Return error results
            results = ProcessingResults()
            results.execution_time = processing_time
            results.errors.append(str(e))
            raise
