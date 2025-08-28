from typing import List, Tuple, Iterable, Any
from forklift.inputs.base_sql_input import BaseSQLInput
from sqlalchemy import text
import re

class SQLServerInput(BaseSQLInput):
    def __init__(self, source: str, include: List[str] = None, **opts: Any):
        patched_source = self._patch_connection_string(source)
        super().__init__(patched_source, include, **opts)

    @staticmethod
    def _patch_connection_string(source: str) -> str:
        def _fix_driver_value(dval: str) -> str:
            return dval.replace("{", "").replace("}", "").replace(" ", "+")
        patched_source = source
        if source.lower().startswith("mssql") and ("odbc_connect=" in source.lower() or "driver=" in source.lower()):
            if "odbc_connect=" in source.lower():
                if "?" in patched_source:
                    base, query = patched_source.split("?", 1)
                else:
                    base, query = patched_source, ""
                amp_params = [p for p in query.split("&") if p] if query else []
                new_amp_params = []
                oc_found = False
                from urllib.parse import unquote_plus, quote_plus
                for p in amp_params:
                    key, eq, val = p.partition("=")
                    if key.lower() == "odbc_connect" and eq == "=":
                        oc_found = True
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
                if not amp_params:
                    from urllib.parse import quote_plus
                    rebuilt = "DRIVER=ODBC Driver 18 for SQL Server;TrustServerCertificate=yes"
                    new_amp_params.append(f"odbc_connect={quote_plus(rebuilt)}")
                patched_source = base + "?" + "&".join(new_amp_params)
            else:
                patched_source = patched_source.replace("{", "").replace("}", "")
                if "?" in patched_source:
                    base, query = patched_source.split("?", 1)
                else:
                    base, query = patched_source, ""
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
                patched_source = base + ("?" + "&".join(new_params) if new_params else "")
        return patched_source

    def _get_all_tables(self) -> List[Tuple[str, str]]:
        tables = []
        for schema in self.inspector.get_schema_names():
            for tbl in self.inspector.get_table_names(schema=schema):
                tables.append((schema, tbl))
            for view in self.inspector.get_view_names(schema=schema):
                tables.append((schema, view))
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
                            v_schema = row[0]
                            v_name = row[1]
                            if (v_schema, v_name) not in tables:
                                tables.append((v_schema, v_name))
                    finally:
                        if conn is not self.connection:
                            conn.close()
                except Exception:
                    pass
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

    def iter_rows(self) -> Iterable:
        raise NotImplementedError("iter_rows must be implemented in SQLServerInput.")
