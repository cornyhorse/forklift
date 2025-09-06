"""Data type inference from file contents."""

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pv_csv
import pyarrow.parquet as pq
from pathlib import Path
from typing import Union
from io import StringIO

from ...io import UnifiedIOHandler, is_s3_path


class DataTypeInferrer:
    """Handles data type inference from various file formats."""

    def __init__(self):
        self.io_handler = UnifiedIOHandler()

    def read_csv_sample(self, input_path: Union[str, Path], nrows: int = 1000,
                       delimiter: str = ",", encoding: str = "utf-8") -> pa.Table:
        """Read CSV sample data for inference.

        Args:
            input_path: Path to CSV file
            nrows: Number of rows to read for analysis
            delimiter: CSV delimiter
            encoding: File encoding

        Returns:
            pa.Table: Sample data as PyArrow table
        """
        read_options = pv_csv.ReadOptions(
            encoding=encoding,
            skip_rows=0,
            column_names=None,
            autogenerate_column_names=False
        )

        parse_options = pv_csv.ParseOptions(
            delimiter=delimiter,
            quote_char='"',
            double_quote=True,
            escape_char=None
        )

        convert_options = pv_csv.ConvertOptions(
            check_utf8=True,
            auto_dict_encode=True,
            auto_dict_max_cardinality=1000
        )

        if is_s3_path(str(input_path)):
            # Handle S3 path
            with self.io_handler.open_for_read(str(input_path), encoding='utf-8') as f:
                if nrows:
                    # Read limited rows for S3
                    content = f.read()
                    lines = content.split('\n')
                    if len(lines) > nrows + 1:  # +1 for header
                        lines = lines[:nrows + 1]
                    limited_content = '\n'.join(lines)

                    # Use pandas for S3 CSV reading with nrows
                    df = pd.read_csv(
                        StringIO(limited_content),
                        delimiter=delimiter,
                        encoding=encoding
                    )
                    table = pa.Table.from_pandas(df)
                else:
                    # Use pandas for S3 CSV reading without nrows
                    content = f.read()
                    df = pd.read_csv(
                        StringIO(content),
                        delimiter=delimiter,
                        encoding=encoding
                    )
                    table = pa.Table.from_pandas(df)
        else:
            # Handle local file
            if nrows:
                # Read only specified number of rows
                df = pd.read_csv(
                    input_path,
                    nrows=nrows,
                    delimiter=delimiter,
                    encoding=encoding
                )
                table = pa.Table.from_pandas(df)
            else:
                table = pv_csv.read_csv(
                    str(input_path),
                    read_options=read_options,
                    parse_options=parse_options,
                    convert_options=convert_options
                )

        return table

    def read_excel_sample(self, input_path: Union[str, Path], nrows: int = 1000,
                         sheet_name: Union[str, int, None] = None) -> pa.Table:
        """Read Excel sample data for inference.

        Args:
            input_path: Path to Excel file
            nrows: Number of rows to read for analysis
            sheet_name: Sheet name or index

        Returns:
            pa.Table: Sample data as PyArrow table
        """
        if is_s3_path(str(input_path)):
            with self.io_handler.open_for_read(str(input_path), encoding='binary') as f:
                df = pd.read_excel(
                    f,
                    sheet_name=sheet_name or 0,
                    nrows=nrows
                )
        else:
            df = pd.read_excel(
                input_path,
                sheet_name=sheet_name or 0,
                nrows=nrows
            )

        return pa.Table.from_pandas(df)

    def read_parquet_sample(self, input_path: Union[str, Path], nrows: int = 1000) -> pa.Table:
        """Read Parquet sample data for inference.

        Args:
            input_path: Path to Parquet file
            nrows: Number of rows to read for analysis

        Returns:
            pa.Table: Sample data as PyArrow table
        """
        if is_s3_path(str(input_path)):
            with self.io_handler.open_for_read(str(input_path), encoding='binary') as f:
                parquet_file = pq.ParquetFile(f)
                if nrows:
                    table = parquet_file.read()
                    table = table.slice(0, nrows)
                else:
                    table = parquet_file.read()
        else:
            parquet_file = pq.ParquetFile(input_path)
            if nrows:
                table = parquet_file.read()
                table = table.slice(0, nrows)
            else:
                table = parquet_file.read()

        return table

    def infer_schema_from_data(self, table: pa.Table) -> dict:
        """Generate basic schema structure from table data.

        Args:
            table: PyArrow table to analyze

        Returns:
            dict: Basic schema structure with inferred types
        """
        from ..types.data_types import DataTypeConverter

        converter = DataTypeConverter()
        properties = {}

        for field in table.schema:
            column_name = field.name
            arrow_type = field.type

            # Convert Arrow type to JSON Schema type
            json_type = converter.arrow_to_json_schema_type(arrow_type)
            properties[column_name] = json_type

        return {
            "type": "object",
            "properties": properties,
            "additionalProperties": False
        }
