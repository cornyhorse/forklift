"""Processing results class for Forklift engine."""

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class ProcessingResults:
    """Results from data processing operation.

    Attributes:
        total_rows: Total number of rows processed
        valid_rows: Number of rows that passed validation
        invalid_rows: Number of rows that failed validation
        output_files: List of paths to generated output files
        manifest_file: Path to generated manifest file (if created)
        metadata_file: Path to generated metadata file (if created)
        execution_time: Total processing time in seconds
        errors: List of error messages encountered during processing
    """
    total_rows: int = 0
    valid_rows: int = 0
    invalid_rows: int = 0
    output_files: List[str] = field(default_factory=list)
    manifest_file: Optional[str] = None
    metadata_file: Optional[str] = None
    execution_time: float = 0.0
    errors: List[str] = field(default_factory=list)
