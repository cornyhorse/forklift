"""Small shared helpers for looking up and typing columns safely."""

from __future__ import annotations

from typing import Optional

import pyarrow as pa


def column_index(schema: pa.Schema, name: str) -> Optional[int]:
    """Index of the column called ``name``, or ``None`` when there is none.

    ``Schema.get_field_index`` returns ``-1`` for a name that occurs more than once, and
    ``batch.column(-1)`` then silently selects the *last* column. This lookup raises instead.

    Raises:
        ValueError: If the name is ambiguous (the schema has several columns with that name).
    """
    matches = [i for i, field_name in enumerate(schema.names) if field_name == name]
    if not matches:
        return None
    if len(matches) > 1:
        raise ValueError(f"Column '{name}' is ambiguous: it appears {len(matches)} times")
    return matches[0]


def is_text_type(data_type: pa.DataType) -> bool:
    """string, large_string (and string_view where available)."""
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return True
    is_view = getattr(pa.types, "is_string_view", None)
    return bool(is_view and is_view(data_type))
