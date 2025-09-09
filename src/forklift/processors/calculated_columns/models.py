"""Data models and configuration classes for calculated columns."""

from __future__ import annotations
from typing import List, Any, Optional
from dataclasses import dataclass
import pyarrow as pa

# Sentinel value to distinguish between not provided and explicitly None
_UNSET = object()


@dataclass
class CalculatedColumn:
    """Configuration for a calculated column."""
    name: str
    expression: str
    data_type: Optional[pa.DataType] = _UNSET
    description: Optional[str] = None
    dependencies: Optional[List[str]] = None

    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []
        if self.data_type is _UNSET:
            self.data_type = pa.string()


@dataclass
class ConstantColumn:
    """Configuration for a constant value column."""
    name: str
    value: Any
    data_type: Optional[pa.DataType] = _UNSET
    description: Optional[str] = None

    def __post_init__(self):
        # Default to None when not provided, but convert explicit None to string
        if self.data_type is _UNSET:
            self.data_type = None  # Default to None when not provided
        elif self.data_type is None:
            self.data_type = pa.string()  # Convert explicit None to string

    def to_calculated_column(self) -> CalculatedColumn:
        """Convert to CalculatedColumn for processing."""
        # Create expression that returns the constant value
        if isinstance(self.value, str):
            expression = f"'{self.value}'"
        else:
            expression = str(self.value)

        return CalculatedColumn(
            name=self.name,
            expression=expression,
            data_type=self.data_type,
            description=self.description,
            dependencies=[]
        )


@dataclass
class ExpressionColumn:
    """Configuration for an expression-based calculated column."""
    name: str
    expression: str
    data_type: Optional[pa.DataType] = None
    description: Optional[str] = None
    dependencies: Optional[List[str]] = None

    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []
        # ExpressionColumn always defaults to string type when None
        if self.data_type is None:
            self.data_type = pa.string()

    def to_calculated_column(self) -> CalculatedColumn:
        """Convert to CalculatedColumn for processing."""
        return CalculatedColumn(
            name=self.name,
            expression=self.expression,
            data_type=self.data_type,
            description=self.description,
            dependencies=self.dependencies
        )


@dataclass
class CalculatedColumnsConfig:
    """Configuration for calculated columns processor."""
    columns: List[CalculatedColumn]
    fail_on_error: bool = True
    add_metadata: bool = False
    validate_dependencies: bool = True

    # Additional attributes for backward compatibility with factory tests
    constants: List[ConstantColumn] = None
    expressions: List[ExpressionColumn] = None
    calculated: List[CalculatedColumn] = None
    partition_columns: List[str] = None

    def __post_init__(self):
        # Initialize optional lists if None
        if self.constants is None:
            self.constants = []
        if self.expressions is None:
            self.expressions = []
        if self.calculated is None:
            self.calculated = []
        if self.partition_columns is None:
            self.partition_columns = []
