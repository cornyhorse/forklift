from __future__ import annotations
from typing import Iterable, Dict, Any, List, Tuple
from .base import BaseInput
from sqlalchemy import create_engine, MetaData, Table, select, inspect, text
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
        # Patch connection string for SQL Server ODBC SSL issues
        patched_source = source
        if source.lower().startswith("mssql") and ("odbc_connect=" in source.lower() or "driver=" in source.lower()):
            def _fix_driver_value(dval: str) -> str:
                # Strip braces and normalize spaces to '+' for URL context
                return dval.replace("{", "").replace("}", "").replace(" ", "+")

            # Case 1: URL uses odbc_connect=... style (single param containing a semicolon-delimited ODBC string)
            if "odbc_connect=" in source.lower():
                # Split base and query once
                if "?" in patched_source:
                    base, query = patched_source.split("?", 1)
                else:
                    base, query = patched_source, ""

                # Split query into &-separated params (do not split on ';' here!)
                amp_params = [p for p in query.split("&") if p] if query else []
                new_amp_params = []
                oc_found = False

                from urllib.parse import unquote_plus, quote_plus

                for p in amp_params:
                    key, eq, val = p.partition("=")
                    if key.lower() == "odbc_connect" and eq == "=":
                        oc_found = True
                        # Decode the ODBC connect string, which itself is ';' separated
                        decoded = unquote_plus(val)
                        parts = [s for s in decoded.split(";") if s]
                        out_parts = []
                        has_driver = False
                        has_tsc = False
                        for part in parts:
                            lp = part.lower()
                            if lp.startswith("driver="):
                                has_driver = True
                                dval = part[len("driver="):].strip()
                                dval = _fix_driver_value(dval)
                                out_parts.append(f"DRIVER={dval}")
                            elif lp.startswith("trustservercertificate="):
                                has_tsc = True
                                out_parts.append(part)
                            else:
                                out_parts.append(part)
                        if not has_driver:
                            out_parts.append("DRIVER=ODBC Driver 18 for SQL Server")
                        if not has_tsc:
                            out_parts.append("TrustServerCertificate=yes")
                        rebuilt = ";".join(out_parts)
                        new_amp_params.append(f"odbc_connect={quote_plus(rebuilt)}")
                    else:
                        new_amp_params.append(p)

                # If no existing query, just set one with sane defaults
                if not amp_params:
                    from urllib.parse import quote_plus
                    rebuilt = "DRIVER=ODBC Driver 18 for SQL Server;TrustServerCertificate=yes"
                    new_amp_params.append(f"odbc_connect={quote_plus(rebuilt)}")

                patched_source = base + "?" + "&".join(new_amp_params)

            # Case 2: URL uses driver=... as a top-level query param alongside other params
            else:
                # Remove all curly braces from the connection string first to avoid driver lib parse issues
                patched_source = patched_source.replace("{", "").replace("}", "")
                if "?" in patched_source:
                    base, query = patched_source.split("?", 1)
                else:
                    base, query = patched_source, ""

                # Support both legacy ';' and proper '&' separators in the URL query string
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

                # Rebuild query using '&' (required for proper URL query semantics)
                patched_source = base + ("?" + "&".join(new_params) if new_params else "")
        super().__init__(patched_source, **opts)
        self.engine = create_engine(patched_source)
        self.metadata = MetaData()
        try:
            self.connection = self.engine.connect()
        except Exception:
            self.connection = None
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
        system_schemas = {"information_schema", "mysql", "performance_schema", "sys"}
        oracle_system_schemas = {"SYS", "SYSTEM", "OUTLN", "XDB", "DBSNMP", "APPQOSSYS", "AUDSYS", "CTXSYS", "DVSYS", "GGSYS", "GSMADMIN_INTERNAL", "LBACSYS", "MDSYS", "OJVMSYS", "OLAPSYS", "ORDDATA", "ORDPLUGINS", "ORDSYS", "SI_INFORMTN_SCHEMA", "WMSYS", "GSMCATUSER", "GSMUSER", "GSMROOTUSER", "GSMREGUSER", "ANONYMOUS", "XS$NULL", "DIP", "APEX_040000", "APEX_050000", "APEX_180200", "APEX_210100", "APEX_220100", "FLOWS_FILES", "SPATIAL_CSW_ADMIN_USR", "SPATIAL_WFS_ADMIN_USR", "PUBLIC"}
        if self.is_sqlite:
            # SQLite: no schemas, just tables
            for tbl in self.inspector.get_table_names():
                tables.append((None, tbl))
            for view in self.inspector.get_view_names():
                tables.append((None, view))
        else:
            for schema in self.inspector.get_schema_names():
                # Filter out system schemas for MySQL
                if self.engine.dialect.name == "mysql" and schema in system_schemas:
                    continue
                # Filter out system schemas for Oracle (case-insensitive)
                if self.engine.dialect.name == "oracle" and schema.upper() in oracle_system_schemas:
                    continue
                # Diagnostic logging for Oracle
                if self.engine.dialect.name == "oracle":
                    try:
                        print(f"[DEBUG] Inspecting Oracle schema: {schema}")
                        for tbl in self.inspector.get_table_names(schema=schema):
                            tables.append((schema, tbl))
                        for view in self.inspector.get_view_names(schema=schema):
                            tables.append((schema, view))
                    except Exception as e:
                        print(f"[ERROR] Exception in Oracle schema '{schema}': {e}")
                        continue
                else:
                    for tbl in self.inspector.get_table_names(schema=schema):
                        tables.append((schema, tbl))
                    for view in self.inspector.get_view_names(schema=schema):
                        tables.append((schema, view))
                # Fallback for SQL Server: some environments/drivers fail to list views via inspector
                # In that case, query INFORMATION_SCHEMA.VIEWS directly to ensure views are included
                if self.engine.dialect.name == "mssql":
                    try:
                        conn = self.connection or self.engine.connect()
                        try:
                            result = conn.execute(
                                text(
                                    "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = :schema"
                                ),
                                {"schema": schema},
                            )
                            for row in result:
                                # Avoid duplicates if inspector already returned it
                                v_schema = row[0]
                                v_name = row[1]
                                if (v_schema, v_name) not in tables:
                                    tables.append((v_schema, v_name))
                        finally:
                            if conn is not self.connection:
                                conn.close()
                    except Exception:
                        # Be conservative: ignore fallback errors and proceed with whatever inspector returned
                        pass
                # Secondary fallback for SQL Server: use sys.views/sys.schemas if INFORMATION_SCHEMA.VIEWS misses items
                if self.engine.dialect.name == "mssql":
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
                # Tertiary fallback for SQL Server: pull both BASE TABLE and VIEW from INFORMATION_SCHEMA.TABLES
                if self.engine.dialect.name == "mssql":
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
            # Final MSSQL-wide fallback: ensure all views across the DB are included
            if self.engine.dialect.name == "mssql":
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
        if hasattr(self, "connection") and self.connection:
            try:
                self.connection.close()
            except Exception:
                pass
        if hasattr(self, "engine") and self.engine:
            try:
                self.engine.dispose()
            except Exception:
                pass
