from typing import List, Tuple, Iterable, Any
from sqlalchemy import Table, select
from forklift.inputs.base_sql_input import BaseSQLInput

class SQLiteInput(BaseSQLInput):
    def __init__(self, source: str, include: List[str] = None, **opts: Any):
        super().__init__(source, include, **opts)

    def _get_all_tables(self) -> List[Tuple[str, str]]:
        tables = []
        for tbl in self.inspector.get_table_names():
            tables.append((None, tbl))
        for view in self.inspector.get_view_names():
            tables.append((None, view))
        return tables

    def iter_rows(self) -> Iterable:
        for schema, name in self._get_all_tables():
            try:
                table_obj = Table(name, self.metadata, autoload_with=self.engine)
                stmt = select(table_obj)
                result = self.connection.execute(stmt)
                for row in result:
                    yield dict(row._mapping)
            except Exception as e:
                raise e
