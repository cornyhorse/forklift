from typing import List, Tuple, Iterable
from forklift.inputs.base_sql_input import BaseSQLInput

class OracleInput(BaseSQLInput):
    """
    Oracle-specific SQL input class. Skips system schemas when discovering tables/views.
    """
    def _get_all_tables(self) -> List[Tuple[str, str]]:
        """
        Get all tables and views from non-system schemas in the Oracle database.

        :return: List of (schema, table/view) tuples.
        :rtype: List[Tuple[str, str]]
        """
        tables = []
        oracle_system_schemas = {"SYS", "SYSTEM", "OUTLN", "XDB", "DBSNMP", "APPQOSSYS", "AUDSYS", "CTXSYS", "DVSYS", "GGSYS", "GSMADMIN_INTERNAL", "LBACSYS", "MDSYS", "OJVMSYS", "OLAPSYS", "ORDDATA", "ORDPLUGINS", "ORDSYS", "SI_INFORMTN_SCHEMA", "WMSYS", "GSMCATUSER", "GSMUSER", "GSMROOTUSER", "GSMREGUSER", "ANONYMOUS", "XS$NULL", "DIP", "APEX_040000", "APEX_050000", "APEX_180200", "APEX_210100", "APEX_220100", "FLOWS_FILES", "SPATIAL_CSW_ADMIN_USR", "SPATIAL_WFS_ADMIN_USR", "PUBLIC"}
        for schema in self.inspector.get_schema_names():
            if schema.upper() in oracle_system_schemas:
                continue
            for tbl in self.inspector.get_table_names(schema=schema):
                tables.append((schema, tbl))
            for view in self.inspector.get_view_names(schema=schema):
                tables.append((schema, view))
        return tables

    def iter_rows(self) -> Iterable:
        """
        Not implemented for OracleInput. Use get_tables for table/view discovery.

        :raises NotImplementedError: Always, for this class.
        """
        raise NotImplementedError("iter_rows must be implemented in OracleInput.")
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
