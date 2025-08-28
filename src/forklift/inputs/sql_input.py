from __future__ import annotations
from typing import Any, Iterable, List, Tuple
from .base import BaseInput
from sqlalchemy import create_engine, MetaData, inspect, text, Table, select
import re
from forklift.inputs.db.mysql_input import MySQLInput
from forklift.inputs.db.oracle_input import OracleInput
from forklift.inputs.db.sqlite_input import SQLiteInput
from forklift.inputs.db.sqlserver_input import SQLServerInput
from forklift.inputs.db.postgres_input import PostgresInput
from forklift.inputs.base_sql_input import BaseSQLInput

Table = Table
select = select

class SQLInput(BaseInput):
    """
    Wrapper for SQL input that delegates to the correct DB-specific subclass.
    Preserves the legacy interface for registry and tests.
    """
    def __init__(self, source: str, include: List[str] = None, **opts: Any):
        self._delegate = get_sql_input(source, include, **opts)

    def _get_all_tables(self) -> List[Tuple[str, str]]:
        return self._delegate._get_all_tables()

    def iter_rows(self) -> Iterable:
        return self._delegate.iter_rows()

    def get_tables(self) -> list:
        return self._delegate.get_tables()

def get_sql_input(source: str, include: List[str] = None, **opts: Any) -> BaseSQLInput:
    """
    Factory to select the correct SQL input class based on the connection string or engine dialect.
    """
    if source.lower().startswith("mssql"):
        return SQLServerInput(source, include, **opts)
    elif source.lower().startswith("sqlite"):
        return SQLiteInput(source, include, **opts)
    elif source.lower().startswith("mysql"):
        return MySQLInput(source, include, **opts)
    elif source.lower().startswith("oracle"):
        return OracleInput(source, include, **opts)
    elif source.lower().startswith("postgres") or source.lower().startswith("postgresql"):
        return PostgresInput(source, include, **opts)
    else:
        return BaseSQLInput(source, include, **opts)
