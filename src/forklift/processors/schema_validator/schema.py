"""Schema definition classes for column specifications."""

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class ColumnSchema:
    """Schema definition for a single column."""
    name: str
    data_type: str
    nullable: bool = True
    constraints: Optional[Dict[str, Any]] = None
    description: Optional[str] = None

    def __post_init__(self):
        if self.constraints is None:
            self.constraints = {}
