"""Base classes for schema validation processors.

Re-exported from :mod:`forklift.processors.base` so that ``BaseProcessor`` and
``ValidationResult`` are the same classes everywhere (``isinstance`` checks and results from
different processors interoperate).
"""

from ..base import BaseProcessor, ValidationResult

__all__ = ["BaseProcessor", "ValidationResult"]
