"""Constraint validation classes for data quality checks."""

from __future__ import annotations
from typing import List, Any, Optional
from dataclasses import dataclass


@dataclass
class ConstraintViolation:
    """Represents a constraint violation found during data validation."""

    violation_type: str
    error_message: str
    columns: List[str]
    values: List[Any]
    constraint_name: str
    row_index: Optional[int] = None
