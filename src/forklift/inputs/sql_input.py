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

    :param source: Database connection string.
    :type source: str
    :param include: List of table/view patterns to include.
    :type include: List[str], optional
    :param opts: Additional options for the input type.
    :type opts: Any
    """
    def __init__(self, source: str, include: List[str] = None, **opts: Any):
        """
        Initialize SQLInput and delegate to the correct DB-specific subclass.

        :param source: Database connection string.
        :type source: str
        :param include: List of table/view patterns to include.
        :type include: List[str], optional
        :param opts: Additional options for the input type.
        :type opts: Any
        """
        self._delegate = get_sql_input(source, include, **opts)

    def _get_all_tables(self) -> List[Tuple[str, str]]:
        """
        Get all tables and views from the delegated input.

        :return: List of (schema, table/view) tuples.
        :rtype: List[Tuple[str, str]]
        """
        return self._delegate._get_all_tables()

    def iter_rows(self) -> Iterable:
        """
        Iterate over rows from the delegated input source.

        :return: An iterable of rows.
        :rtype: Iterable
        """
        return self._delegate.iter_rows()

    def get_tables(self) -> list:
        """
        Get tables matching the include patterns from the delegated input.

        :return: List of matched tables.
        :rtype: list
        """
        return self._delegate.get_tables()

def get_sql_input(source: str, include: List[str] = None, **opts: Any) -> BaseSQLInput:
    """
    Factory to select the correct SQL input class based on the connection string or engine dialect.

    :param source: Database connection string.
    :type source: str
    :param include: List of table/view patterns to include.
    :type include: List[str], optional
    :param opts: Additional options for the input type.
    :type opts: Any
    :return: An instance of the appropriate SQL input class.
    :rtype: BaseSQLInput
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
