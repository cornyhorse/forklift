"""Forklift - A data import tool with PyArrow streaming and validation."""

from .engine.forklift_core import import_csv, import_fwf, import_excel, import_sql
from .readers import read_csv, read_excel, read_fwf, read_sql, DataFrameReader
from .api import (
    generate_schema_from_csv,
    generate_schema_from_excel,
    generate_schema_from_parquet,
    generate_and_save_schema,
    generate_and_copy_schema
)

__version__ = "0.1.3"

__all__ = [
    # Primary ETL pipeline functions (write to Parquet files)
    "import_csv",
    "import_fwf",
    "import_excel",
    "import_sql",
    # Ad-hoc DataFrame reader functions (return DataFrames)
    "read_csv",
    "read_excel",
    "read_fwf",
    "read_sql",
    "DataFrameReader",
    # Schema generation API functions
    "generate_schema_from_csv",
    "generate_schema_from_excel",
    "generate_schema_from_parquet",
    "generate_and_save_schema",
    "generate_and_copy_schema",
]
