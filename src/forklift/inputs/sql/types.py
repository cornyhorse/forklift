"""SQL to PyArrow type conversion utilities."""

from __future__ import annotations
from typing import Optional
import pyarrow as pa
import logging

logger = logging.getLogger(__name__)


class SqlTypeConverter:
    """Handles conversion between SQL data types and PyArrow data types."""

    def __init__(self, schema_importer=None):
        """Initialize the type converter.

        Args:
            schema_importer: Optional SQL schema importer for custom type mappings
        """
        self.schema_importer = schema_importer

    def odbc_type_to_string(self, odbc_type: int) -> str:
        """Convert ODBC type constant to string representation.

        Args:
            odbc_type: ODBC type constant

        Returns:
            String representation of the type
        """
        try:
            import pyodbc

            type_map = {
                pyodbc.SQL_CHAR: 'CHAR',
                pyodbc.SQL_VARCHAR: 'VARCHAR',
                pyodbc.SQL_LONGVARCHAR: 'TEXT',
                pyodbc.SQL_WCHAR: 'NCHAR',
                pyodbc.SQL_WVARCHAR: 'NVARCHAR',
                pyodbc.SQL_WLONGVARCHAR: 'NTEXT',
                pyodbc.SQL_DECIMAL: 'DECIMAL',
                pyodbc.SQL_NUMERIC: 'NUMERIC',
                pyodbc.SQL_SMALLINT: 'SMALLINT',
                pyodbc.SQL_INTEGER: 'INTEGER',
                pyodbc.SQL_REAL: 'REAL',
                pyodbc.SQL_FLOAT: 'FLOAT',
                pyodbc.SQL_DOUBLE: 'DOUBLE',
                pyodbc.SQL_BIT: 'BIT',
                pyodbc.SQL_TINYINT: 'TINYINT',
                pyodbc.SQL_BIGINT: 'BIGINT',
                pyodbc.SQL_BINARY: 'BINARY',
                pyodbc.SQL_VARBINARY: 'VARBINARY',
                pyodbc.SQL_LONGVARBINARY: 'BLOB',
                pyodbc.SQL_TYPE_DATE: 'DATE',
                pyodbc.SQL_TYPE_TIME: 'TIME',
                pyodbc.SQL_TYPE_TIMESTAMP: 'TIMESTAMP',
            }

            return type_map.get(odbc_type, 'VARCHAR')

        except ImportError:
            return 'VARCHAR'

    def sql_type_to_pyarrow(self, sql_type: str, size: Optional[int] = None,
                           decimal_digits: Optional[int] = None) -> pa.DataType:
        """Convert SQL data type to PyArrow data type.

        Args:
            sql_type: SQL type name
            size: Column size
            decimal_digits: Number of decimal digits

        Returns:
            PyArrow data type
        """
        sql_type = sql_type.upper()

        # Use schema importer mapping if available
        if self.schema_importer and hasattr(self.schema_importer, 'parquet_type_mapping'):
            # This would need to be enhanced to map SQL types to Parquet types
            pass

        # Map common SQL types to PyArrow types
        if sql_type in ('INT', 'INTEGER', 'INT4'):
            return pa.int32()
        elif sql_type in ('BIGINT', 'INT8'):
            return pa.int64()
        elif sql_type in ('SMALLINT', 'INT2'):
            return pa.int16()
        elif sql_type in ('TINYINT', 'INT1'):
            return pa.int8()
        elif sql_type in ('FLOAT', 'REAL'):
            return pa.float32()
        elif sql_type in ('DOUBLE', 'DOUBLE PRECISION', 'FLOAT8'):
            return pa.float64()
        elif sql_type in ('DECIMAL', 'NUMERIC'):
            if size and decimal_digits is not None:
                return pa.decimal128(size, decimal_digits)
            return pa.float64()
        elif sql_type in ('BOOLEAN', 'BOOL', 'BIT'):
            return pa.bool_()
        elif sql_type in ('DATE',):
            return pa.date32()
        elif sql_type in ('TIME',):
            return pa.time64('us')
        elif sql_type in ('TIMESTAMP', 'DATETIME', 'TIMESTAMP WITHOUT TIME ZONE'):
            return pa.timestamp('us')
        elif sql_type in ('TIMESTAMPTZ', 'TIMESTAMP WITH TIME ZONE'):
            return pa.timestamp('us', tz='UTC')
        elif sql_type in ('BINARY', 'VARBINARY', 'BLOB', 'BYTEA'):
            return pa.binary()
        else:
            # Default to string for text types and unknown types
            return pa.string()

    def convert_column_data(self, column_data: tuple, pa_type: pa.DataType,
                           null_values: Optional[list] = None) -> pa.Array:
        """Convert column data to PyArrow array with proper type.

        Args:
            column_data: Tuple of column values
            pa_type: Target PyArrow data type
            null_values: List of values to treat as null

        Returns:
            PyArrow array
        """
        # Handle null values
        processed_data = []
        for value in column_data:
            if value is None:
                processed_data.append(None)
            elif null_values and str(value) in null_values:
                processed_data.append(None)
            else:
                processed_data.append(value)

        try:
            return pa.array(processed_data, type=pa_type)
        except Exception as e:
            logger.warning(f"Could not convert column to {pa_type}, using string: {e}")
            # Fallback to string type
            return pa.array([str(v) if v is not None else None for v in processed_data])
