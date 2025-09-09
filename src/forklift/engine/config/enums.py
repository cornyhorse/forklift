"""Enumeration types for Forklift engine configuration."""

from enum import Enum


class HeaderMode(Enum):
    """Header detection modes for CSV processing.

    Attributes:
        PRESENT: File has header row that should be used
        ABSENT: No header row, use schema or generate default names
        AUTO: Auto-detect header location by analyzing content
    """
    PRESENT = "present"  # File has header row
    ABSENT = "absent"   # No header, use schema or default names
    AUTO = "auto"       # Auto-detect header location


class ExcessColumnMode(Enum):
    """Modes for handling excess columns beyond expected schema.

    Attributes:
        TRUNCATE: Remove excess columns and keep the row (default)
        REJECT: Reject the entire row if it has excess columns
    """
    TRUNCATE = "truncate"  # Remove excess data, keep row
    REJECT = "reject"      # Reject entire row with excess data
