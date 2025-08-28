from __future__ import annotations
from typing import Iterable, Dict, Any, List, Tuple
from .base import BaseInput
from sqlalchemy import create_engine, MetaData, Table, select, inspect
import re

class SQLInput(BaseInput):
    """
    SQL input class for Forklift. Supports glob-based schema/table selection using SQLAlchemy.

    :param source: SQLAlchemy connection string
    :type source: str
    :param include: List of glob patterns for schema/table selection
    :type include: List[str], optional
    :param opts: Additional options
    :type opts: Any
    """
    def __init__(self, source: str, include: List[str] = None, **opts: Any):
        """
        Initialize SQLInput.

        :param source: SQLAlchemy connection string
        :param include: List of glob patterns for schema/table selection
        :param opts: Additional options
        """
        super().__init__(source, **opts)
        self.engine = create_engine(source)
        self.metadata = MetaData()
        self.connection = self.engine.connect()
        if include is None:
            self.include = ["*.*"]
        else:
            self.include = include
        self.inspector = inspect(self.engine)
        self.is_sqlite = self.engine.dialect.name == "sqlite"

    def _get_all_tables(self) -> List[Tuple[str, str]]:
        """
        Discover all tables and views in the database.

        :return: List of (schema, table) tuples
        :rtype: List[Tuple[str, str]]
        """
        tables = []
        if self.is_sqlite:
            # SQLite: no schemas, just tables
            for tbl in self.inspector.get_table_names():
                tables.append((None, tbl))
            for view in self.inspector.get_view_names():
                tables.append((None, view))
        else:
            for schema in self.inspector.get_schema_names():
                for tbl in self.inspector.get_table_names(schema=schema):
                    tables.append((schema, tbl))
                for view in self.inspector.get_view_names(schema=schema):
                    tables.append((schema, view))
        return tables

    def _match_patterns(self, tables: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
        """
        Match tables/views against glob patterns.

        :param tables: List of (schema, table) tuples
        :return: List of matched (schema, table) tuples
        :rtype: List[Tuple[str, str]]
        """
        matched = set()
        for pat in self.include:
            if pat == "*.*":
                matched.update(tables)
            elif re.match(r"^[^.]+\.\*$", pat):
                schema = pat.split(".")[0]
                matched.update([(s, t) for s, t in tables if s == schema])
            elif re.match(r"^[^.]+\.[^.]+$", pat):
                schema, table = pat.split(".")
                matched.update([(s, t) for s, t in tables if s == schema and t == table])
            elif re.match(r"^[^.]+$", pat):
                matched.update([(s, t) for s, t in tables if t == pat])
        return list(matched)

    def iter_rows(self) -> Iterable[Dict[str, Any]]:
        """
        Iterate over all rows in matched tables/views.

        :return: Iterable of row dictionaries
        :rtype: Iterable[Dict[str, Any]]
        """
        tables = self._get_all_tables()
        matched_tables = self._match_patterns(tables)
        for schema, table in matched_tables:
            if self.is_sqlite:
                table_obj = Table(table, self.metadata, autoload_with=self.engine)
            else:
                table_obj = Table(table, self.metadata, schema=schema, autoload_with=self.engine)
            stmt = select(table_obj)
            result = self.connection.execute(stmt)
            for row in result:
                out = dict(row._mapping)
                out["_table"] = table
                if schema:
                    out["_schema"] = schema
                yield out

    def get_tables(self):
        """
        Get all matched tables/views and their rows.

        :return: List of dicts with table/view info and rows
        :rtype: List[Dict[str, Any]]
        """
        tables = self._get_all_tables()
        matched_tables = self._match_patterns(tables)
        out = []
        for schema, table in matched_tables:
            if self.is_sqlite:
                table_obj = Table(table, self.metadata, autoload_with=self.engine)
            else:
                table_obj = Table(table, self.metadata, schema=schema, autoload_with=self.engine)
            stmt = select(table_obj)
            result = self.connection.execute(stmt)
            rows = [dict(row._mapping) for row in result]
            out.append({
                "name": table,
                "schema": schema,
                "rows": rows
            })
        return out

    def __del__(self):
        """
        Destructor: close connection and dispose engine.
        """
        self.connection.close()
        self.engine.dispose()
