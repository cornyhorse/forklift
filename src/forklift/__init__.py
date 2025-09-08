"""Forklift - A data import tool with PyArrow streaming and validation."""

# Import from refactored locations instead of forklift_core
from .readers import read_csv, read_excel, read_fwf, read_sql, DataFrameReader
from .api import (
    generate_schema_from_csv,
    generate_schema_from_excel,
    generate_schema_from_parquet,
    generate_and_save_schema,
    generate_and_copy_schema
)

# Backward compatibility: Create wrapper functions for the old functional interface
def import_csv(*args, **kwargs):
    """Backward compatibility wrapper for CSV import functionality."""
    from .inputs.csv import CsvInputHandler
    from .inputs.config import CsvInputConfig
    from .engine.forklift_core import ForkliftCore, ImportConfig

    # This is a placeholder implementation for backward compatibility
    # In practice, you'd want to map the old function signature to the new classes
    raise NotImplementedError("import_csv has been refactored. Use CsvInputHandler from forklift.inputs.csv instead.")

def import_fwf(*args, **kwargs):
    """Backward compatibility wrapper for FWF import functionality."""
    from .inputs.fwf import FwfInputHandler
    from .inputs.config import FwfInputConfig

    raise NotImplementedError("import_fwf has been refactored. Use FwfInputHandler from forklift.inputs.fwf instead.")

def import_excel(*args, **kwargs):
    """Backward compatibility wrapper for Excel import functionality."""
    from .inputs.excel import ExcelInputHandler
    from .inputs.config import ExcelInputConfig

    raise NotImplementedError("import_excel has been refactored. Use ExcelInputHandler from forklift.inputs.excel instead.")

def import_sql(*args, **kwargs):
    """Backward compatibility wrapper for SQL import functionality."""
    from .inputs.sql import SqlInputHandler
    from .inputs.config import SqlInputConfig

    raise NotImplementedError("import_sql has been refactored. Use SqlInputHandler from forklift.inputs.sql instead.")

__version__ = "0.1.0"

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
