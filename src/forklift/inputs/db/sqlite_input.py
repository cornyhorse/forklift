from typing import List, Tuple, Iterable, Any
from sqlalchemy import Table, select
from forklift.inputs.base_sql_input import BaseSQLInput

class SQLiteInput(BaseSQLInput):
    """
    SQLite-specific SQL input class. Handles table and view discovery and row iteration.

    :param source: Database connection string.
    :type source: str
    :param include: List of table/view patterns to include.
    :type include: List[str], optional
    :param opts: Additional options for the input type.
    :type opts: Any
    """
    def __init__(self, source: str, include: List[str] = None, **opts: Any):
        """
        Initialize SQLiteInput.

        :param source: Database connection string.
        :type source: str
        :param include: List of table/view patterns to include.
        :type include: List[str], optional
        :param opts: Additional options for the input type.
        :type opts: Any
        """
        super().__init__(source, include, **opts)

    def _get_all_tables(self) -> List[Tuple[str, str]]:
        """
        Get all tables and views from the SQLite database.

        :return: List of (schema, table/view) tuples. Schema is always None for SQLite.
        :rtype: List[Tuple[str, str]]
        """
        tables = []
        for tbl in self.inspector.get_table_names():
            tables.append((None, tbl))
        for view in self.inspector.get_view_names():
            tables.append((None, view))
        return tables

    def iter_rows(self) -> Iterable:
        """
        Iterate over rows from all tables/views in the SQLite database.

        :return: An iterable of row dictionaries.
        :rtype: Iterable
        :raises Exception: If row iteration fails for a table/view.
        """
        for schema, name in self._get_all_tables():
            try:
                table_obj = Table(name, self.metadata, autoload_with=self.engine)
                stmt = select(table_obj)
                result = self.connection.execute(stmt)
                for row in result:
                    yield dict(row._mapping)
            except Exception as e:
                raise e
