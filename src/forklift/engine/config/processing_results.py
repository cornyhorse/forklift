"""Processing results class for Forklift engine."""

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class ProcessingResults:
    """Results from data processing operation.

    Attributes:
        total_rows: Total number of rows processed (valid + invalid)
        valid_rows: Number of rows that passed validation
        invalid_rows: Number of rows that failed validation or conversion (see bad_rows_file)
        output_files: List of paths to generated output files. For CSV imports this lists the
            data file and, when rows were rejected, the bad rows file as well (kept for
            backward compatibility; use ``bad_rows_file`` to tell them apart)
        manifest_file: Path to generated manifest file (if created)
        metadata_file: Path to generated metadata file (if created)
        execution_time: Total processing time in seconds
        errors: List of error messages encountered during processing
        bad_rows_file: Path to the parquet file holding rejected rows (None if no rows were
            rejected). Rejected rows are stored as strings in the shape of the input columns.
        truncated_rows: Number of rows that had more fields than the header and were cut
            to the header width (ExcessColumnMode.TRUNCATE)
    """

    total_rows: int = 0
    valid_rows: int = 0
    invalid_rows: int = 0
    output_files: List[str] = field(default_factory=list)
    manifest_file: Optional[str] = None
    metadata_file: Optional[str] = None
    execution_time: float = 0.0
    errors: List[str] = field(default_factory=list)
    bad_rows_file: Optional[str] = None
    truncated_rows: int = 0
