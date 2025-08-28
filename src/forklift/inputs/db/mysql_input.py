from typing import List, Tuple, Iterable
from forklift.inputs.base_sql_input import BaseSQLInput

class MySQLInput(BaseSQLInput):
    def _get_all_tables(self) -> List[Tuple[str, str]]:
        tables = []
        system_schemas = {"information_schema", "mysql", "performance_schema", "sys"}
        for schema in self.inspector.get_schema_names():
            if schema in system_schemas:
                continue
            for tbl in self.inspector.get_table_names(schema=schema):
                tables.append((schema, tbl))
            for view in self.inspector.get_view_names(schema=schema):
                tables.append((schema, view))
        return tables

    def iter_rows(self) -> Iterable:
        raise NotImplementedError("iter_rows must be implemented in MySQLInput.")

