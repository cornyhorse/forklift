from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict, Any
from ..types import RowResult, Row

class BaseOutput(ABC):
    """Abstract base for output writers.

    Concrete implementations must provide lifecycle and row handling methods.

    :param dest: Destination path / identifier.
    :param schema: Optional schema dict for validation/metadata.
    :param opts: Additional implementation-specific options.
    """
    def __init__(self, dest: str, schema: Dict[str, Any] | None, **opts: Any):
        self.dest = dest
        self.schema = schema or {}
        self.opts = opts

    @abstractmethod
    def open(self) -> None:
        """Initialize resources (directories, files, connections)."""
        ...

    @abstractmethod
    def write(self, row: Row) -> None:
        """Persist a single accepted row.

        :param row: Row dictionary to write.
        """
        ...

    @abstractmethod
    def quarantine(self, rr: RowResult) -> None:
        """Record a rejected row and its associated error.

        :param rr: RowResult containing original row and error.
        """
        ...

    @abstractmethod
    def close(self) -> None:
        """Finalize and release resources, flushing buffers as needed."""
        ...
