from typing import Any, Iterable, List, Tuple
from .base import BaseInput
from sqlalchemy import create_engine, MetaData, inspect

class BaseSQLInput(BaseInput):
    """
    Base class for SQL input. Handles generic DB logic.
    """
    def __init__(self, source: str, include: List[str] = None, **opts: Any):
        super().__init__(source, **opts)
        self.engine = create_engine(source)
        self.metadata = MetaData()
        try:
            self.connection = self.engine.connect()
        except Exception:
            self.connection = None
        self.include = include if include is not None else ["*.*"]
        self.inspector = inspect(self.engine)

    def _get_all_tables(self) -> List[Tuple[str, str]]:
        tables = []
        if getattr(self, "is_sqlite", False):
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

    def iter_rows(self) -> Iterable:
        raise NotImplementedError("iter_rows must be implemented in subclasses.")

    def get_tables(self) -> list:
        tables = []
        all_tables = self._get_all_tables()
        patterns = self.include if self.include is not None else ["*.*"]
        matched = set()
        for pattern in patterns:
            pattern = pattern.strip()
            if not pattern:
                continue
            if pattern == "*.*":
                matched.update(all_tables)
            elif ".*" in pattern:
                schema = pattern.split(".")[0]
                for t in all_tables:
                    if t[0] == schema:
                        matched.add(t)
            elif "." in pattern:
                schema, name = pattern.split(".", 1)
                for t in all_tables:
                    if t[0] == schema and t[1] == name:
                        matched.add(t)
            else:
                for t in all_tables:
                    if t[1] == pattern:
                        matched.add(t)
        for schema, name in matched:
            tables.append({"schema": schema, "name": name, "rows": []})
        return tables

