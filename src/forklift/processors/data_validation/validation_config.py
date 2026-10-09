"""Configuration classes for data validation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

from .._regex import compile_pattern


@dataclass
class RangeValidation:
    """Range validation configuration for numeric and date fields.

    Bounds may be numbers (``int``/``float``/``Decimal``/numeric strings) or dates
    (``date``/``datetime``/ISO strings such as ``"2020-01-01"``). Numeric bounds are compared
    exactly as decimals (``min_value=0.01`` accepts ``Decimal("0.01")``).
    """

    min_value: Optional[Union[int, float, str]] = None
    max_value: Optional[Union[int, float, str]] = None
    inclusive: bool = True


@dataclass
class StringValidation:
    """String validation configuration.

    ``pattern`` is an unanchored search (JSON Schema semantics, see ``processors/_regex.py``);
    anchor it with ``^``/``$`` to require the whole value to match. It is compiled when the
    configuration is created (invalid or catastrophic-backtracking-prone patterns raise
    ``ValueError`` unless ``allow_unsafe_regex=True``).

    ``allow_empty=False`` rejects empty and whitespace-only values. Length and pattern rules
    always apply to empty strings too (only ``None`` skips them).
    """

    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None
    allow_empty: bool = True
    allow_unsafe_regex: bool = False

    def __post_init__(self):
        if self.pattern is not None:
            compile_pattern(self.pattern, self.allow_unsafe_regex)


@dataclass
class EnumValidation:
    """Enumeration validation configuration."""

    allowed_values: List[Any]
    case_sensitive: bool = True


@dataclass
class DateValidation:
    """Date validation configuration.

    ``formats`` are ``strptime`` formats tried in order when the value is a string
    (default: ``["%Y-%m-%d"]``). ``min_date``/``max_date`` are ISO ``YYYY-MM-DD`` strings (or any
    of ``formats``).
    """

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
    """Configuration for bad rows handling.

    ``include_original_row=False`` keeps the original (possibly personal) data out of the bad
    rows output: only the error columns and the row number are kept.
    """

    enabled: bool = True
    output_path: str = "bad_rows"
    file_format: str = "parquet"
    include_original_row: bool = True
    include_validation_errors: bool = True
    max_bad_rows_percent: float = 10.0
    fail_on_exceed_threshold: bool = True


@dataclass
class ValidationConfig:
    """Configuration for data validation processor.

    ``include_values_in_errors`` (default False) puts the offending cell value into validation
    error messages. Leave it off unless the logs are allowed to hold the data.
    """

    field_validations: List[FieldValidationRule]
    bad_rows_config: BadRowsConfig
    uniqueness_strategy: str = (
        "first_wins"  # first_wins, last_wins, fail_on_duplicate, mark_all_duplicates
    )
    include_values_in_errors: bool = False

    def __post_init__(self):
        valid_strategies = ["first_wins", "last_wins", "fail_on_duplicate", "mark_all_duplicates"]
        if self.uniqueness_strategy not in valid_strategies:
            raise ValueError(f"Invalid uniqueness strategy: {self.uniqueness_strategy}")
