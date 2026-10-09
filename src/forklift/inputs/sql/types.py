"""SQL to PyArrow type conversion utilities."""

from __future__ import annotations

import datetime
import decimal
import logging
import re
from typing import Any, Optional

import pyarrow as pa

logger = logging.getLogger(__name__)

# Arrow's decimal128 holds at most 38 digits.
_MAX_DECIMAL_PRECISION = 38


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
                pyodbc.SQL_CHAR: "CHAR",
                pyodbc.SQL_VARCHAR: "VARCHAR",
                pyodbc.SQL_LONGVARCHAR: "TEXT",
                pyodbc.SQL_WCHAR: "NCHAR",
                pyodbc.SQL_WVARCHAR: "NVARCHAR",
                pyodbc.SQL_WLONGVARCHAR: "NTEXT",
                pyodbc.SQL_DECIMAL: "DECIMAL",
                pyodbc.SQL_NUMERIC: "NUMERIC",
                pyodbc.SQL_SMALLINT: "SMALLINT",
                pyodbc.SQL_INTEGER: "INTEGER",
                pyodbc.SQL_REAL: "REAL",
                pyodbc.SQL_FLOAT: "FLOAT",
                pyodbc.SQL_DOUBLE: "DOUBLE",
                pyodbc.SQL_BIT: "BIT",
                pyodbc.SQL_TINYINT: "TINYINT",
                pyodbc.SQL_BIGINT: "BIGINT",
                pyodbc.SQL_BINARY: "BINARY",
                pyodbc.SQL_VARBINARY: "VARBINARY",
                pyodbc.SQL_LONGVARBINARY: "BLOB",
                pyodbc.SQL_TYPE_DATE: "DATE",
                pyodbc.SQL_TYPE_TIME: "TIME",
                pyodbc.SQL_TYPE_TIMESTAMP: "TIMESTAMP",
            }

            return type_map.get(odbc_type, "VARCHAR")

        except ImportError:
            return "VARCHAR"

    @staticmethod
    def python_type_to_string(python_type: Any) -> str:
        """Convert a DB-API ``cursor.description`` type code (a Python type) to a SQL type name.

        pyodbc reports the Python type of the values it will return rather than an ODBC
        type constant. Integers map to BIGINT so no driver-side width can overflow.

        Args:
            python_type: Python type class from ``cursor.description``

        Returns:
            SQL type name understood by :meth:`sql_type_to_pyarrow`
        """
        if isinstance(python_type, type):
            if issubclass(python_type, bool):
                return "BOOLEAN"
            if issubclass(python_type, int):
                return "BIGINT"
            if issubclass(python_type, float):
                return "DOUBLE"
            if issubclass(python_type, decimal.Decimal):
                return "DECIMAL"
            if issubclass(python_type, datetime.datetime):
                return "TIMESTAMP"
            if issubclass(python_type, datetime.date):
                return "DATE"
            if issubclass(python_type, datetime.time):
                return "TIME"
            if issubclass(python_type, (bytes, bytearray, memoryview)):
                return "VARBINARY"
        return "VARCHAR"

    def sql_type_to_pyarrow(
        self, sql_type: str, size: Optional[int] = None, decimal_digits: Optional[int] = None
    ) -> pa.DataType:
        """Convert SQL data type to PyArrow data type.

        Args:
            sql_type: SQL type name
            size: Column size
            decimal_digits: Number of decimal digits

        Returns:
            PyArrow data type
        """
        # SQL Server reports identity columns as e.g. "int identity"
        sql_type = re.sub(r"\s+IDENTITY$", "", sql_type.strip().upper())

        # Use schema importer mapping if available
        if self.schema_importer and hasattr(self.schema_importer, "parquet_type_mapping"):
            # This would need to be enhanced to map SQL types to Parquet types
            pass

        # Map common SQL types to PyArrow types
        if sql_type in ("INT", "INTEGER", "INT4"):
            return pa.int32()
        elif sql_type in ("BIGINT", "INT8"):
            return pa.int64()
        elif sql_type in ("SMALLINT", "INT2"):
            return pa.int16()
        elif sql_type in ("TINYINT", "INT1"):
            return pa.int8()
        elif sql_type in ("REAL", "FLOAT4"):
            return pa.float32()
        elif sql_type in ("FLOAT", "DOUBLE", "DOUBLE PRECISION", "FLOAT8"):
            # FLOAT is a double in SQL Server, SQLite and Oracle; float32 would silently
            # round values (123456789.123 -> 123456792.0). REAL is the single-precision type.
            return pa.float64()
        elif sql_type in ("DECIMAL", "NUMERIC", "DEC"):
            if size and decimal_digits is not None:
                if 0 <= decimal_digits <= size <= _MAX_DECIMAL_PRECISION:
                    return pa.decimal128(size, decimal_digits)
                # Wider than decimal128 (or nonsensical metadata): keep exact text instead
                # of failing the whole table.
                logger.warning(
                    f"{sql_type}({size},{decimal_digits}) does not fit decimal128; "
                    "mapping the column to string"
                )
                return pa.string()
            return pa.float64()
        elif sql_type == "MONEY":
            return pa.decimal128(19, 4)
        elif sql_type == "SMALLMONEY":
            return pa.decimal128(10, 4)
        elif sql_type in ("BOOLEAN", "BOOL", "BIT"):
            return pa.bool_()
        elif sql_type in ("DATE",):
            return pa.date32()
        elif sql_type in ("TIME",):
            return pa.time64("us")
        elif sql_type in (
            "TIMESTAMP",
            "DATETIME",
            "DATETIME2",
            "SMALLDATETIME",
            "TIMESTAMP WITHOUT TIME ZONE",
        ):
            return pa.timestamp("us")
        elif sql_type in ("TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE"):
            return pa.timestamp("us", tz="UTC")
        elif sql_type in ("BINARY", "VARBINARY", "BLOB", "BYTEA"):
            return pa.binary()
        elif sql_type == "UNIQUEIDENTIFIER":
            return pa.string()
        else:
            # Default to string for text types and unknown types
            return pa.string()

    def convert_column_data(
        self, column_data: tuple, pa_type: pa.DataType, null_values: Optional[list] = None
    ) -> pa.Array:
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
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError, OverflowError):
            pass

        # Second chance: let Arrow infer the values, then cast to the declared type
        # (e.g. a driver returning ISO date strings for a TIMESTAMP column).
        try:
            return pa.array(processed_data).cast(pa_type)
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError, OverflowError):
            pass

        if pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type):
            return pa.array(
                [str(v) if v is not None else None for v in processed_data], type=pa_type
            )

        # Returning a string array here would contradict the declared schema type and only
        # fail later when the batch is assembled; fail now with a clear message (no values).
        raise ValueError(f"Column data could not be converted to the declared type {pa_type}")
