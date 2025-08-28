from typing import List, Tuple, Iterable, Any
from forklift.inputs.base_sql_input import BaseSQLInput
from sqlalchemy import text
import re

class SQLServerInput(BaseSQLInput):
    """
    SQLServerInput handles SQL Server-specific quirks for connection string patching and table/view discovery.
    It ensures SSL is enabled, the correct driver is used, and all tables/views are discovered even with driver limitations.

    :param source: Database connection string.
    :type source: str
    :param include: List of table/view patterns to include.
    :type include: List[str], optional
    :param opts: Additional options for the input type.
    :type opts: Any
    """
    def __init__(self, source: str, include: List[str] = None, **opts: Any):
        """
        Initialize SQLServerInput, patching the connection string and passing options to BaseSQLInput.

        :param source: Database connection string.
        :type source: str
        :param include: List of table/view patterns to include.
        :type include: List[str], optional
        :param opts: Additional options for the input type.
        :type opts: Any
        """
        patched_source = self._patch_connection_string(source)
        super().__init__(patched_source, include, **opts)

    @staticmethod
    def _patch_connection_string(source: str) -> str:
        """
        Patch the connection string for SQL Server to ensure SSL and correct driver settings.
        Handles both ODBC connect string and driver param styles.
        """
        lower = source.lower()
        if lower.startswith('mssql'):
            from urllib.parse import quote_plus
            default_odbc = "DRIVER=ODBC Driver 18 for SQL Server;TrustServerCertificate=yes"
            # Bare odbc_connect (no '=') ?odbc_connect or &odbc_connect at param boundary
            bare_pattern = r'([?&])odbc_connect(?=(&|$))'
            if re.search(bare_pattern, source, flags=re.IGNORECASE) and 'odbc_connect=' not in lower:
                encoded = quote_plus(default_odbc)
                source = re.sub(bare_pattern,
                                lambda m: f"{m.group(1)}odbc_connect={encoded}{m.group(2) if m.group(2)=='&' else ''}",
                                source,
                                flags=re.IGNORECASE)
                # We inserted a fully formed odbc_connect param; no further patching needed
                return source
            has_odbc = 'odbc_connect=' in lower or re.search(bare_pattern, lower) is not None
            if has_odbc:
                return SQLServerInput._patch_odbc_connect_string(source)
            if 'driver=' in lower:
                return SQLServerInput._patch_driver_params(source)
        return source

    @staticmethod
    def _patch_odbc_connect_string(source: str) -> str:
        """
        Patch ODBC connect string to ensure driver and TrustServerCertificate are set.

        :param source: ODBC connection string.
        :type source: str
        :return: Patched ODBC connection string.
        :rtype: str
        """
        from urllib.parse import unquote_plus, quote_plus

        def _fix_driver_value(driver_value: str) -> str:
            """Sanitize driver value for ODBC connection string."""
            return driver_value.replace("{", "").replace("}", "").replace(" ", "+")

        def _patch_odbc_params(odbc_params: str) -> str:
            """
            Patch the semicolon-separated ODBC params string to ensure driver and TrustServerCertificate are set.

            :param odbc_params: ODBC parameters string.
            :type odbc_params: str
            :return: Patched ODBC parameters string.
            :rtype: str
            """
            param_parts = [param for param in odbc_params.split(";") if param]
            patched_parts = []
            has_driver = False
            has_trust_server_cert = False
            for param in param_parts:
                param_lower = param.lower()
                if param_lower.startswith("driver="):
                    has_driver = True
                    driver_value = param[len("driver="):].strip()
                    driver_value = _fix_driver_value(driver_value)
                    patched_parts.append(f"DRIVER={driver_value}")
                elif param_lower.startswith("trustservercertificate="):
                    has_trust_server_cert = True
                    patched_parts.append(param)
                else:
                    patched_parts.append(param)
            if not has_driver:
                patched_parts.append("DRIVER=ODBC Driver 18 for SQL Server")
            if not has_trust_server_cert:
                patched_parts.append("TrustServerCertificate=yes")
            return ";".join(patched_parts)

        if "?" in source:
            base_url, query_string = source.split("?", 1)
        else:
            base_url, query_string = source, ""
        # Robust handling: if empty query or sole/bare odbc_connect add default
        if not query_string or query_string.strip().lower() == 'odbc_connect':
            default_odbc = "DRIVER=ODBC Driver 18 for SQL Server;TrustServerCertificate=yes"
            return base_url + "?odbc_connect=" + quote_plus(default_odbc)
        query_params = [param for param in query_string.split("&") if param]
        cleaned_query_params = []
        found_odbc_connect = False
        for param in query_params:
            # Handle bare odbc_connect (no '=')
            if param.strip().lower() == 'odbc_connect':
                found_odbc_connect = True
                default_odbc = "DRIVER=ODBC Driver 18 for SQL Server;TrustServerCertificate=yes"
                cleaned_query_params.append(f"odbc_connect={quote_plus(default_odbc)}")
                continue
            key, eq, value = param.partition("=")
            if key.lower() == "odbc_connect":
                found_odbc_connect = True
                if eq == "=" and value:
                    decoded_odbc = unquote_plus(value)
                    patched_odbc = _patch_odbc_params(decoded_odbc)
                    cleaned_query_params.append(f"odbc_connect={quote_plus(patched_odbc)}")
                else:
                    default_odbc = "DRIVER=ODBC Driver 18 for SQL Server;TrustServerCertificate=yes"
                    cleaned_query_params.append(f"odbc_connect={quote_plus(default_odbc)}")
            else:
                cleaned_query_params.append(param)
        if not query_params or not found_odbc_connect:
            default_odbc = "DRIVER=ODBC Driver 18 for SQL Server;TrustServerCertificate=yes"
            cleaned_query_params.append(f"odbc_connect={quote_plus(default_odbc)}")
        # Always reconstruct output from cleaned_query_params
        return base_url + "?" + "&".join(cleaned_query_params)

    @staticmethod
    def _patch_driver_params(source: str) -> str:
        """
        Patch driver param style connection string to ensure driver and TrustServerCertificate are set.

        :param source: Driver param style connection string.
        :type source: str
        :return: Patched connection string.
        :rtype: str
        """
        def _fix_driver_value(dval: str) -> str:
            return dval.replace("{", "").replace("}", "").replace(" ", "+")
        source = source.replace("{", "").replace("}", "")
        if "?" in source:
            base, query = source.split("?", 1)
        else:
            base, query = source, ""
        raw_params = [p for p in re.split(r"[;&]", query) if p] if query else []
        new_params = []
        driver_found = False
        tsc_found = False
        for param in raw_params:
            lower_param = param.lower()
            if lower_param.startswith("driver="):
                driver_found = True
                driver_val = param[len("driver="):].strip()
                driver_val = _fix_driver_value(driver_val)
                param = f"driver={driver_val}"
            elif lower_param.startswith("trustservercertificate="):
                tsc_found = True
            new_params.append(param)
        if not driver_found:
            new_params.append("driver=ODBC+Driver+18+for+SQL+Server")
        if not tsc_found:
            new_params.append("TrustServerCertificate=yes")
        return base + ("?" + "&".join(new_params) if new_params else "")

    def _get_all_tables(self) -> List[Tuple[str, str]]:
        """
        Discover all tables and views in the database, using multiple fallbacks for SQL Server.
        Ensures all views are included, even if the driver/inspector misses some.

        :return: List of (schema, table/view) tuples.
        :rtype: List[Tuple[str, str]]
        """
        tables = []
        for schema in self.inspector.get_schema_names():
            for tbl in self.inspector.get_table_names(schema=schema):
                tables.append((schema, tbl))
            for view in self.inspector.get_view_names(schema=schema):
                tables.append((schema, view))
            if self.engine.dialect.name == "mssql":
                self._add_views_from_information_schema(tables, schema)
                self._add_views_from_sys_views(tables, schema)
                self._add_tables_and_views_from_information_schema_tables(tables, schema)
        if self.engine.dialect.name == "mssql":
            self._add_all_views_from_sys_views(tables)
        return tables

    def _add_views_from_information_schema(self, tables, schema):
        """
        Fallback: Add views from INFORMATION_SCHEMA.VIEWS for the given schema.
        Some drivers miss views, so this ensures they're included.

        :param tables: List of discovered tables/views.
        :type tables: list
        :param schema: Schema name to query.
        :type schema: str
        """
        try:
            conn = self.connection or self.engine.connect()
            try:
                result = conn.execute(
                    text("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = :schema"),
                    {"schema": schema},
                )
                for row in result:
                    v_schema = row[0]
                    v_name = row[1]
                    if (v_schema, v_name) not in tables:
                        tables.append((v_schema, v_name))
            finally:
                if conn is not self.connection:
                    conn.close()
        except Exception:
            pass

    def _add_views_from_sys_views(self, tables, schema):
        """
        Fallback: Add views from sys.views/sys.schemas for the given schema.

        :param tables: List of discovered tables/views.
        :type tables: list
        :param schema: Schema name to query.
        :type schema: str
        """
        try:
            conn = self.connection or self.engine.connect()
            try:
                result = conn.execute(
                    text(
                        """
                        SELECT s.name AS schema_name, v.name AS view_name
                        FROM sys.views v
                        JOIN sys.schemas s ON v.schema_id = s.schema_id
                        WHERE s.name = :schema
                        """
                    ),
                    {"schema": schema},
                )
                for row in result:
                    v_schema = row[0]
                    v_name = row[1]
                    if (v_schema, v_name) not in tables:
                        tables.append((v_schema, v_name))
            finally:
                if conn is not self.connection:
                    conn.close()
        except Exception:
            pass

    def _add_tables_and_views_from_information_schema_tables(self, tables, schema):
        """
        Fallback: Add both base tables and views from INFORMATION_SCHEMA.TABLES for the given schema.

        :param tables: List of discovered tables/views.
        :type tables: list
        :param schema: Schema name to query.
        :type schema: str
        """
        try:
            conn = self.connection or self.engine.connect()
            try:
                result = conn.execute(
                    text(
                        """
                        SELECT TABLE_SCHEMA, TABLE_NAME
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = :schema
                          AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')
                        """
                    ),
                    {"schema": schema},
                )
                for row in result:
                    t_schema = row[0]
                    t_name = row[1]
                    if (t_schema, t_name) not in tables:
                        tables.append((t_schema, t_name))
            finally:
                if conn is not self.connection:
                    conn.close()
        except Exception:
            pass

    def _add_all_views_from_sys_views(self, tables):
        """
        Fallback: Add all views across the DB from sys.views/sys.schemas.

        :param tables: List of discovered tables/views.
        :type tables: list
        """
        try:
            conn = self.connection or self.engine.connect()
            try:
                result = conn.execute(
                    text(
                        """
                        SELECT s.name AS schema_name, v.name AS view_name
                        FROM sys.views v
                        JOIN sys.schemas s ON v.schema_id = s.schema_id
                        """
                    )
                )
                for row in result:
                    v_schema = row[0]
                    v_name = row[1]
                    if (v_schema, v_name) not in tables:
                        tables.append((v_schema, v_name))
            finally:
                if conn is not self.connection:
                    conn.close()
        except Exception:
            pass

    def iter_rows(self) -> Iterable:
        """
        Not implemented for SQLServerInput. Use get_tables for table/view discovery.

        :raises NotImplementedError: Always, for this class.
        """
        raise NotImplementedError("iter_rows must be implemented in SQLServerInput.")
