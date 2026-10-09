"""SQL importer implementation."""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Set, Union

from ...io import create_parquet_writer
from ..config import ProcessingResults
from ..exceptions import ProcessingError
from .output_location import OutputLocation, discard_partial_output, validate_output_stem
from .redaction import redact_connection_string, scrub_secrets


class SqlImporter:
    """Handles SQL database import operations."""

    @staticmethod
    def import_sql(
        connection_string: str,
        output_path: Union[str, Path],
        schema_file: Union[str, Path] = None,
        **kwargs,
    ) -> ProcessingResults:
        """Import data from SQL database with ODBC connectivity.

        Each table listed in the schema file becomes ``<outputName>.parquet`` in ``output_path``
        (a local directory, or an ``s3://bucket/prefix`` URI which is written through the S3
        parquet writer). ``metadata.json`` describes the run; the connection string in it is
        redacted (passwords are never written).

        A table that fails is aborted: its writer is discarded, no partial ``.parquet`` is left
        behind, and it is recorded in ``results.errors`` and under ``failed_tables`` in
        ``metadata.json`` (never as ``invalid_rows``). The remaining tables are still processed.
        Afterwards, if any table failed, a :class:`ProcessingError` is raised (the partial
        ``ProcessingResults`` is available as ``error.results``) unless ``continue_on_error=True``
        was passed, in which case the results are returned with ``errors`` populated.

        Error text for failed tables contains only the exception class, never its message
        (database/Arrow messages can quote cell values).

        Args:
            connection_string: ODBC connection string
            output_path: Output directory (local) or S3 URI
            schema_file: Schema file listing the tables to import (required)
            **kwargs: ``batch_size``, ``query_timeout``, ``connection_timeout``,
                ``use_quoted_identifiers``, ``schema_name``, ``enable_streaming``,
                ``null_values`` (passed to the SQL input config), ``continue_on_error``
                (default False) and ``s3_client`` (optional client for S3 outputs)

        Raises:
            ProcessingError: No schema file, invalid schema, or one or more tables failed
            ValueError: No tables in the schema, or an invalid/duplicate output file name
        """
        from ...inputs.config import SqlInputConfig
        from ...inputs.sql import SqlInputHandler
        from ...schema.sql_schema_importer import SqlSchemaImporter

        logger = logging.getLogger(__name__)
        start_time = time.time()
        continue_on_error = kwargs.get("continue_on_error", False)
        s3_client = kwargs.get("s3_client")

        try:
            location = OutputLocation(output_path)

            # Schema file is now required for explicit table specification
            if not schema_file:
                raise ProcessingError(
                    "Schema file is required for SQL import to specify which tables to process"
                )

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

            # Validate every output file name before touching the database or the disk
            planned = SqlImporter._plan_outputs(location, tables_to_process)
            location.prepare()

            # Create SQL config
            config_kwargs = {
                "connection_string": connection_string,
                "batch_size": kwargs.get("batch_size", 10000),
                "query_timeout": kwargs.get("query_timeout", 300),
                "connection_timeout": kwargs.get("connection_timeout", 30),
                "use_quoted_identifiers": kwargs.get("use_quoted_identifiers", False),
                "schema_name": kwargs.get("schema_name"),
                "enable_streaming": kwargs.get("enable_streaming", True),
                "null_values": kwargs.get("null_values"),
            }

            # Remove None values
            config_kwargs = {k: v for k, v in config_kwargs.items() if v is not None}
            sql_config = SqlInputConfig(**config_kwargs)

            # Initialize SQL input handler
            sql_handler = SqlInputHandler(sql_config)
            sql_handler.set_schema_importer(schema_importer)

            # Connect to database and process tables
            total_rows = 0
            valid_rows = 0
            processed_tables = 0
            output_files: List[str] = []
            tables_done: List[Any] = []
            failed_tables: List[Dict[str, str]] = []
            results = ProcessingResults()

            with sql_handler:
                logger.info(f"Found {len(tables_to_process)} tables to process from schema")
                writer_kwargs = {"s3_client": s3_client} if location.is_s3 else {}

                for schema_name, table_name, output_name, target in planned:
                    writer = None
                    table_rows = 0
                    try:
                        logger.info(f"Processing table: {schema_name}.{table_name}")

                        # Get table schema
                        table_schema = sql_handler.get_table_schema(schema_name, table_name)

                        # Create Parquet writer
                        writer = create_parquet_writer(target, table_schema, **writer_kwargs)

                        # Process data in batches
                        for batch in sql_handler.read_table_data(schema_name, table_name):
                            writer.write_batch(batch)
                            table_rows += batch.num_rows

                        # Close writer (for S3 this completes the upload)
                        writer.close()
                        writer = None

                    except BaseException as exc:
                        # Never leave a truncated file (or a pending upload) behind
                        discard_partial_output(writer, target)
                        if not isinstance(exc, Exception):
                            raise
                        error_type = type(exc).__name__
                        logger.error(
                            "Failed to process table %s.%s: %s",
                            schema_name,
                            table_name,
                            error_type,
                        )
                        failed_tables.append(
                            {"schema": schema_name, "table": table_name, "error_type": error_type}
                        )
                        results.errors.append(f"{schema_name}.{table_name}: {error_type}")
                        continue

                    total_rows += table_rows
                    valid_rows += table_rows
                    if table_rows > 0:
                        output_files.append(str(target))
                        logger.info(
                            f"Completed {schema_name}.{table_name}: {table_rows} rows "
                            f"-> {target}"
                        )
                    else:
                        logger.warning(f"Table {schema_name}.{table_name} contained no data")

                    tables_done.append((schema_name, table_name, output_name))
                    processed_tables += 1

            # Create results
            processing_time = time.time() - start_time
            results.total_rows = total_rows
            results.valid_rows = valid_rows
            results.invalid_rows = 0  # SQL import has no row validation; failures are in errors
            results.execution_time = processing_time
            results.output_files = output_files

            # Create metadata file
            metadata = {
                "processing_summary": {
                    "total_tables_processed": processed_tables,
                    "total_tables_failed": len(failed_tables),
                    "total_rows": total_rows,
                    "valid_rows": valid_rows,
                    "invalid_rows": 0,
                    "execution_time_seconds": processing_time,
                    "processed_at": datetime.now().isoformat(),
                },
                "input_config": {
                    # Secrets are redacted: this file is stored next to the data
                    "connection_string": redact_connection_string(connection_string),
                    "tables_processed": tables_done,
                    "batch_size": sql_config.batch_size,
                    "query_timeout": sql_config.query_timeout,
                },
                "output_files": output_files,
                "failed_tables": failed_tables,
            }

            location.write_text(
                "metadata",
                ".json",
                json.dumps(metadata, indent=2, ensure_ascii=False),
                s3_client=s3_client,
            )

            if failed_tables and not continue_on_error:
                error = ProcessingError(
                    f"{len(failed_tables)} of {len(planned)} tables failed: "
                    + ", ".join(
                        f"{f['schema']}.{f['table']} ({f['error_type']})" for f in failed_tables
                    )
                )
                error.results = results  # partial results: tables that did succeed
                raise error

            logger.info(
                f"SQL import completed: {processed_tables} tables, "
                f"{total_rows} total rows in {processing_time:.2f}s"
                + (f", {len(failed_tables)} failed" if failed_tables else "")
            )

            return results

        except Exception as e:
            processing_time = time.time() - start_time
            # The message may echo the connection string; never log credentials
            logger.error(
                f"SQL import failed after {processing_time:.2f}s: "
                f"{scrub_secrets(str(e), connection_string)}"
            )
            raise

    @staticmethod
    def _plan_outputs(location: OutputLocation, tables: List[Any]) -> List[Any]:
        """Resolve and validate the output file of every table before any work is done.

        Returns:
            ``(schema_name, table_name, output_name, target)`` tuples in schema order

        Raises:
            ValueError: If a name is not a plain file name, would escape the output directory,
                or two tables would write the same file.
        """
        planned = []
        seen: Set[str] = set()
        for schema_name, table_name, output_name in tables:
            if output_name:
                stem = output_name
            elif schema_name and schema_name != "default":
                stem = f"{schema_name}_{table_name}"
            else:
                stem = table_name

            validate_output_stem(stem)
            if stem.casefold() in seen:
                raise ValueError(
                    f"Two tables would be written to the same output file name {stem!r}; "
                    "give them distinct outputName values"
                )
            seen.add(stem.casefold())
            planned.append((schema_name, table_name, output_name, location.target(stem)))
        return planned
