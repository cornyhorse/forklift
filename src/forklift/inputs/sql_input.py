from __future__ import annotations
from typing import Any, Iterable, List, Tuple
from .base import BaseInput
from forklift.inputs.db.mysql_input import MySQLInput
from forklift.inputs.db.oracle_input import OracleInput
from forklift.inputs.db.sqlite_input import SQLiteInput
from forklift.inputs.db.sqlserver_input import SQLServerInput
from forklift.inputs.db.postgres_input import PostgresInput
from forklift.inputs.base_sql_input import BaseSQLInput
from sqlalchemy import Table as _SA_Table, select as _sa_select  # noqa: F401

# Re-export for test monkeypatching (tests patch forklift.inputs.sql_input.Table)
Table = _SA_Table  # type: ignore
select = _sa_select  # type: ignore

class SQLInput(BaseInput):
    """Wrapper delegating to the appropriate concrete SQL input class.

    :param source: Database connection string.
    :param include: Optional list of table/view patterns.
    :param opts: Additional implementation options.
    """
    def __init__(self, source: str, include: List[str] = None, **opts: Any):
        super().__init__(source, **opts)
        self._delegate = get_sql_input(source, include, **opts)

    def _get_all_tables(self) -> List[Tuple[str, str]]:
        """Proxy to delegate implementation."""
        return self._delegate._get_all_tables()

    def iter_rows(self) -> Iterable:
        """Proxy row iterator from delegate."""
        return self._delegate.iter_rows()

    def get_tables(self) -> list:
        """Proxy table discovery from delegate."""
        return self._delegate.get_tables()

def get_sql_input(source: str, include: List[str] = None, **opts: Any) -> BaseSQLInput:
    """Factory returning a concrete SQL input based on URI prefix."""
    lower = source.lower()
    if lower.startswith("mssql"):
        return SQLServerInput(source, include, **opts)
    if lower.startswith("sqlite"):
        return SQLiteInput(source, include, **opts)
    if lower.startswith("mysql"):
        return MySQLInput(source, include, **opts)
    if lower.startswith("oracle"):
        return OracleInput(source, include, **opts)
    if lower.startswith("postgres") or lower.startswith("postgresql"):
        return PostgresInput(source, include, **opts)
    return BaseSQLInput(source, include, **opts)
