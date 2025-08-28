# gen_sql.py
from __future__ import annotations
from typing import List, Dict, Any, Tuple
from sqlalchemy import (
    Table, Column, MetaData, Integer, String, Boolean, Date, Numeric, Text
)
from sqlalchemy.dialects import postgresql, mysql, mssql, oracle
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import insert, text


# ---------- Helpers ----------

def map_type(col: Dict[str, Any]):
    """
    Minimal type mapper from a JSON-like spec to SQLAlchemy types.
    Extend as needed.
    """
    t = col["type"].lower()
    if t in ("int", "integer"):
        return Integer()
    if t in ("str", "string", "text"):
        # support length if provided
        if "length" in col and isinstance(col["length"], int):
            return String(col["length"])
        return Text() if col.get("long", False) else String()
    if t in ("bool", "boolean"):
        return Boolean()
    if t in ("date",):
        return Date()
    if t in ("number", "numeric", "decimal", "real", "float"):
        # optional precision/scale
        if "precision" in col or "scale" in col:
            return Numeric(col.get("precision", 18), col.get("scale", 6))
        return Numeric(18, 6)
    raise ValueError(f"Unmapped type: {t}")


def build_table(
        table_name: str,
        schema_name: str | None,
        columns: List[Dict[str, Any]],
        metadata: MetaData,
):
    cols = []
    for c in columns:
        coltype = map_type(c)
        kwargs = {
            "primary_key": bool(c.get("primary_key", False)),
            "nullable": not bool(c.get("not_null", False)),
        }
        cols.append(Column(c["name"], coltype, **kwargs))
    return Table(table_name, metadata, *cols, schema=schema_name)


def compile_ddl(stmt, dialect):
    return str(stmt.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))


def compile_insert(tbl: Table, rows: List[Dict[str, Any]], dialect):
    # Emit a multi-values INSERT for engines that support it; otherwise many VALUES
    ins = insert(tbl)
    if not rows:
        return ""
    if hasattr(dialect, "insert_executemany_returning") or True:
        # Generating static SQL with literal_binds:
        return compile_ddl(ins.values(rows), dialect)
    return "\n".join(compile_ddl(ins.values(r), dialect) for r in rows)


# ---------- Generators per dialect ----------

def gen_create_database(db_name: str, dialect_name: str) -> str:
    d = dialect_name.lower()
    if d in ("postgresql", "postgres"):
        return f"CREATE DATABASE {db_name};"
    if d in ("mysql", "mariadb"):
        return f"CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;"
    if d in ("mssql", "sqlserver"):
        return f"CREATE DATABASE [{db_name}];"
    if d in ("oracle",):
        # No CREATE DATABASE from a normal session. Optional (requires privileges):
        return (
            "-- Oracle has no CREATE DATABASE here. Optionally create a user/schema:\n"
            f"-- CREATE USER {db_name} IDENTIFIED BY <password>;\n"
            f"-- GRANT CONNECT, RESOURCE TO {db_name};"
        )
    raise ValueError(f"Unknown dialect for database: {dialect_name}")


def gen_create_schema(schema_name: str, dialect_name: str, db_name: str | None = None) -> str:
    d = dialect_name.lower()
    if not schema_name:
        return "-- No schema requested."
    if d in ("postgresql", "postgres"):
        return f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
    if d in ("mysql", "mariadb"):
        # In MySQL, schema == database. Prefer creating DB instead.
        target_db = schema_name or db_name
        return f"CREATE DATABASE IF NOT EXISTS `{target_db}`;"
    if d in ("mssql", "sqlserver"):
        return (
            f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'{schema_name}')\n"
            f"    EXEC('CREATE SCHEMA {schema_name}');"
        )
    if d in ("oracle",):
        # Schema == user. You typically CREATE USER (see above) and create objects under it.
        return f"-- Oracle: schema == user. Ensure USER '{schema_name}' exists and use it as owner."
    raise ValueError(f"Unknown dialect for schema: {dialect_name}")


def gen_create_view(view_name: str, select_sql: str, dialect_name: str, schema_name: str | None) -> str:
    qualified = f"{schema_name}.{view_name}" if schema_name and dialect_name != "mysql" else (
        f"`{view_name}`" if dialect_name in ("mysql", "mariadb") else view_name
    )
    if dialect_name in ("mssql", "sqlserver"):
        qualified = f"{schema_name}.{view_name}" if schema_name else f"dbo.{view_name}"
    return f"CREATE OR REPLACE VIEW {qualified} AS\n{select_sql};"


def pick_dialect(name: str):
    d = name.lower()
    if d in ("postgres", "postgresql"):
        return postgresql.dialect()
    if d in ("mysql", "mariadb"):
        return mysql.dialect()
    if d in ("mssql", "sqlserver"):
        return mssql.dialect()
    if d in ("oracle",):
        return oracle.dialect()
    raise ValueError(f"Unsupported dialect: {name}")


# ---------- Public API ----------

def generate_sql_bundle(
        dialect_name: str,
        *,
        database_name: str | None,
        schema_name: str | None,
        table_name: str,
        columns: List[Dict[str, Any]],
        view_name: str,
        view_select_sql: str,
        rows: List[Dict[str, Any]],
) -> Dict[str, str]:
    """
    Returns a dict of SQL strings for the requested dialect.
    """
    md = MetaData()
    tbl = build_table(table_name, schema_name if dialect_name not in ("mysql", "mariadb") else None, columns, md)
    dialect = pick_dialect(dialect_name)

    # Database & schema
    sql_create_db = gen_create_database(database_name,
                                        dialect_name) if database_name else "-- No database name provided."
    sql_create_schema = gen_create_schema(schema_name, dialect_name,
                                          database_name) if schema_name else "-- No schema requested."

    # Table DDL
    sql_create_table = compile_ddl(CreateTable(tbl), dialect)

    # View DDL (using provided SELECT)
    sql_create_view = gen_create_view(view_name, view_select_sql, dialect_name.lower(), schema_name)

    # Inserts
    sql_insert = compile_insert(tbl, rows, dialect)

    return {
        "create_database": sql_create_db,
        "create_schema": sql_create_schema,
        "create_table": sql_create_table + ";",
        "create_view": sql_create_view,
        "insert_rows": sql_insert + (";" if sql_insert and not sql_insert.strip().endswith(";") else ""),
    }


# ---------- Example usage (you can delete below) ----------

if __name__ == "__main__":
    # Example: your good_customers table
    columns = [
        {"name": "id", "type": "integer", "primary_key": True, "not_null": True},
        {"name": "name", "type": "string", "not_null": True, "length": 255},
        {"name": "email", "type": "string", "not_null": True, "length": 320},
        {"name": "signup_date", "type": "date", "not_null": True},
        {"name": "active", "type": "boolean", "not_null": True},
        {"name": "amount_usd", "type": "number", "not_null": True, "precision": 18, "scale": 2},
        {"name": "country", "type": "string", "not_null": True, "length": 2},
        {"name": "status", "type": "string", "not_null": True, "length": 16},
        {"name": "discount_pct", "type": "number", "precision": 5, "scale": 2},
        {"name": "notes", "type": "string"},
    ]

    rows = [
        {"id": 1, "name": "Amy Adams", "email": "amy.adams@example.com", "signup_date": "2024-01-05",
         "active": True, "amount_usd": 19.99, "country": "US", "status": "active", "discount_pct": 0,
         "notes": "First purchase"}
        # ... add more as needed
    ]

    view_select_sql = """
                      SELECT id,
                             name,
                             email,
                             country,
                             CASE WHEN active THEN 'Yes' ELSE 'No' END                      AS active_status,
                             status,
                             amount_usd,
                             COALESCE(discount_pct, 0)                                      AS discount_pct,
                             ROUND(amount_usd * (1 - COALESCE(discount_pct, 0) / 100.0), 2) AS net_amount,
                             signup_date,
                             notes
                      FROM good_customers \
                      """

    for d in ("postgresql", "mysql", "mssql", "oracle"):
        bundle = generate_sql_bundle(
            d,
            database_name="testdb" if d != "oracle" else None,  # Oracle: no database
            schema_name=("public" if d == "postgresql" else ("dbo" if d == "mssql" else None)),
            table_name="good_customers",
            columns=columns,
            view_name="v_good_customers",
            view_select_sql=view_select_sql.strip(),
            rows=rows,
        )
        print(f"\n--- {d.upper()} ---")
        for k, v in bundle.items():
            print(f"\n-- {k} --\n{v}")
