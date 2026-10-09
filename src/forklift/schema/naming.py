"""Column-name standardization styles shared by the CSV and FWF schema importers.

``case.standardizeNames`` may be ``postgres``, ``snake_case`` or ``camelCase``; this module holds
the small, dependency-free implementations (``postgres`` lives in ``forklift.utils``).
"""

from __future__ import annotations

import re
from typing import List, Optional

from ..utils.column_name_utilities import standardize_postgres_column_name


def snake_case_name(name: str) -> str:
    """``"User ID"`` -> ``"user_id"``, ``"customerName"`` -> ``"customer_name"``,
    ``"HTTPServer"`` -> ``"http_server"``. Unicode letters and digits are kept."""
    text = str(name).strip()
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)
    text = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", text)
    return re.sub(r"[\W_]+", "_", text).strip("_").lower()


def camel_case_name(name: str) -> str:
    """``"user_id"`` -> ``"userId"``, ``"Order Details"`` -> ``"orderDetails"``."""
    words = [word for word in snake_case_name(name).split("_") if word]
    if not words:
        return ""
    return words[0] + "".join(word.capitalize() for word in words[1:])


def apply_name_style(names: List[str], method: Optional[str]) -> List[str]:
    """Apply a ``standardizeNames`` style to ``names``; unknown/empty methods change nothing."""
    if method == "postgres":
        return [standardize_postgres_column_name(name) for name in names]
    if method == "snake_case":
        return [snake_case_name(name) for name in names]
    if method == "camelCase":
        return [camel_case_name(name) for name in names]
    return names
