"""Enumeration types for Forklift engine configuration."""

from enum import Enum


class HeaderMode(Enum):
    """Header detection modes for CSV processing.

    Attributes:
        PRESENT: File has header row that should be used
        ABSENT: No header row, use schema or generate default names (col_1, col_2, ...)
        AUTO: Auto-detect header location by analyzing content
    """

    PRESENT = "present"  # File has header row
    ABSENT = "absent"  # No header, use schema or default names
    AUTO = "auto"  # Auto-detect header location


class ExcessColumnMode(Enum):
    """Modes for handling rows that have more fields than the header.

    Attributes:
        TRUNCATE: Remove excess fields and keep the row (default); the number of
            truncated rows is reported in ``ProcessingResults.truncated_rows``
        REJECT: Reject the entire row (written to bad_rows.parquet) if it has excess fields
        PASSTHROUGH: Keep all fields and name the extras col_N. The output schema is fixed
            by the first batch written, so a wider row that shows up later raises an error
    """

    TRUNCATE = "truncate"  # Remove excess data, keep row
    REJECT = "reject"  # Reject entire row with excess data
    PASSTHROUGH = "passthrough"  # Keep all columns, add defaults for extras
