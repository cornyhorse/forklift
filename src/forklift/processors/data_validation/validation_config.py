"""Configuration classes for data validation."""

from __future__ import annotations
from typing import List, Dict, Optional, Any, Union
from dataclasses import dataclass


@dataclass
class RangeValidation:
    """Range validation configuration for numeric and date fields."""
    min_value: Optional[Union[int, float, str]] = None
    max_value: Optional[Union[int, float, str]] = None
    inclusive: bool = True


@dataclass
class StringValidation:
    """String validation configuration."""
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None
    allow_empty: bool = True


@dataclass
class EnumValidation:
    """Enumeration validation configuration."""
    allowed_values: List[Any]
    case_sensitive: bool = True


@dataclass
class DateValidation:
    """Date validation configuration."""
    min_date: Optional[str] = None
    max_date: Optional[str] = None
    formats: Optional[List[str]] = None


@dataclass
class FieldValidationRule:
    """Validation rule for a single field."""
    field_name: str
    required: bool = False
    unique: bool = False
    range_validation: Optional[RangeValidation] = None
    string_validation: Optional[StringValidation] = None
    enum_validation: Optional[EnumValidation] = None
    date_validation: Optional[DateValidation] = None
    on_violation: Dict[str, str] = None

    def __post_init__(self):
        if self.on_violation is None:
            self.on_violation = {}


@dataclass
class BadRowsConfig:
    """Configuration for bad rows handling."""
    enabled: bool = True
    output_path: str = "bad_rows"
    file_format: str = "parquet"
    include_original_row: bool = True
    include_validation_errors: bool = True
    max_bad_rows_percent: float = 10.0
    fail_on_exceed_threshold: bool = True


@dataclass
class ValidationConfig:
    """Configuration for data validation processor."""
    field_validations: List[FieldValidationRule]
    bad_rows_config: BadRowsConfig
    uniqueness_strategy: str = "first_wins"  # first_wins, last_wins, fail_on_duplicate, mark_all_duplicates

    def __post_init__(self):
        valid_strategies = ["first_wins", "last_wins", "fail_on_duplicate", "mark_all_duplicates"]
        if self.uniqueness_strategy not in valid_strategies:
            raise ValueError(f"Invalid uniqueness strategy: {self.uniqueness_strategy}")
