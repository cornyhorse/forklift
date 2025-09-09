"""Utilities for deriving SQL table lists from a schema.

The logic lived inline in ``Engine.__init__``; it is extracted here so
it can be unit‑tested and reused by other components (e.g. future CLI tooling
or schema validators) without importing the full Engine.

Updated to work with simplified one-to-one schema/table mapping instead of glob patterns.
"""
from __future__ import annotations
from typing import Any, Dict, List, Tuple, Optional

__all__ = ["derive_sql_table_list"]

def derive_sql_table_list(schema: Dict[str, Any] | None) -> List[Tuple[str, str, Optional[str]]]:
    """Return a list of tables to process from SQL schema configuration.

    The function extracts table specifications from the x-sql.tables array.
    Each table is explicitly defined with schema and table name.

    :param schema: Parsed JSON schema dict (may be ``None``).
    :return: List of tuples (schema_name, table_name, output_name).
             Returns empty list if no tables specified.
    """
    if not schema:
        return []

    x_sql = schema.get("x-sql") or {}
    tables = x_sql.get("tables", [])

    table_list = []
    for table in tables:
        select = table.get("select", {})
        schema_name = select.get("schema", "default")
        table_name = select.get("name")
        output_name = table.get("outputName")

        if table_name:
            table_list.append((schema_name, table_name, output_name))

    return table_list
