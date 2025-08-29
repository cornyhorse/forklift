"""Utilities for deriving SQL include pattern lists from a schema.

The logic lived inline in ``Engine.__init__``; it is extracted here so
it can be unit‑tested and reused by other components (e.g. future CLI tooling
or schema validators) without importing the full Engine.
"""
from __future__ import annotations
from typing import Any, Dict, List

__all__ = ["derive_sql_include_patterns"]

def derive_sql_include_patterns(schema: Dict[str, Any] | None) -> List[str]:
    """Return an ordered, de‑duplicated list of SQL include patterns.

    The function merges patterns from three schema locations (all optional):

    * ``schema["include"]`` – root‑level list
    * ``schema["x-sql"]["include"]`` – extension block
    * ``schema["x-sql"]["tables"][*].select`` – table selection objects where
      either an explicit ``pattern`` or (``schema`` + ``name``) or bare ``name``
      is provided.

    Empty / missing structures are ignored. If the result is empty, the
    fallback pattern ``"*.*"`` is returned (matching all schemas and tables).

    :param schema: Parsed JSON schema dict (may be ``None``).
    :return: List of unique pattern strings preserving first‑seen order.
    """
    if not schema:
        return ["*.*"]

    include_patterns: List[str] = []

    root_include = schema.get("include") or []
    if isinstance(root_include, list):
        include_patterns.extend(root_include)

    x_sql = schema.get("x-sql") or {}
    xsql_include = x_sql.get("include") or []
    if isinstance(xsql_include, list):
        include_patterns.extend(xsql_include)

    for tbl in x_sql.get("tables", []) or []:
        sel = tbl.get("select") or {}
        schema_name = sel.get("schema")
        table_name = sel.get("name")
        pattern = sel.get("pattern")
        if pattern:
            include_patterns.append(pattern)
        elif schema_name and table_name:
            include_patterns.append(f"{schema_name}.{table_name}")
        elif table_name:
            include_patterns.append(table_name)

    if not include_patterns:
        include_patterns = ["*.*"]

    seen = set()
    deduped: List[str] = []
    for p in include_patterns:
        if p not in seen:
            seen.add(p)
            deduped.append(p)
    return deduped

