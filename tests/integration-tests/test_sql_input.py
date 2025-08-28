import os
import types
import pytest
from sqlalchemy.exc import SQLAlchemyError
from forklift.inputs.sql_input import get_sql_input, BaseSQLInput
import psycopg2
import pymysql
import pyodbc
import oracledb

def get_sqlite_conn_str():
    """
    Return the SQLite connection string for the test database.

    :return: SQLite connection string
    :rtype: str
    """
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../test-files/sqlite/sqlite.db'))
    return f'sqlite:///{db_path}'

def get_postgres_conn_str():
    """
    Return the Postgres connection string for the test database.

    :return: Postgres connection string
    :rtype: str
    """
    return "postgresql://testuser:testpass@127.0.0.1:5432/testdb"

def get_mysql_conn_str():
    """
    Return the MySQL connection string for the test database.

    :return: MySQL connection string
    :rtype: str
    """
    return "mysql+pymysql://testuser:testpass@127.0.0.1:3306/sales_db"

def get_mssql_conn_str():
    """
    Return the MS SQL connection string for the test database.

    :return: MS SQL connection string
    :rtype: str
    """
    # Assumes ODBC driver 18 is installed; TrustServerCertificate disables strict SSL validation
    return "mssql+pyodbc://sa:YourStrong!Passw0rd@127.0.0.1:1433/testdb?driver=ODBC+Driver+18+for+SQL+Server;TrustServerCertificate=yes"

def get_oracle_conn_str():
    """
    Return the Oracle connection string for the test database.
    Uses environment variables for host, port, user, password, and PDB/service name.
    """
    host = os.environ.get("ORACLE_HOST", "127.0.0.1")
    port = int(os.environ.get("ORACLE_PORT", "1521"))
    user = os.environ.get("ORACLE_USER", "system")
    password = os.environ.get("ORACLE_PWD", "YourStrong!Passw0rd")
    pdb = os.environ.get("ORACLE_PDB", "FREEPDB1")
    return f"oracle+oracledb://{user}:{password}@{host}:{port}/?service_name={pdb}"

def make_mock_inspector():
    inspector = types.SimpleNamespace()
    inspector.get_schema_names = lambda: ["public", "analytics"]
    inspector.get_table_names = lambda schema=None: ["users", "events"] if schema == "public" else ["reports"]
    inspector.get_view_names = lambda schema=None: ["user_view"] if schema == "public" else ["report_view"]
    return inspector

@pytest.fixture(scope="module", autouse=True)
def hydrate_postgres_db():
    """
    Hydrate the Postgres test database with the schema and data from the DDL file before running tests.
    """
    ddl_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../test-files/sql/source-sql-ddl-and-data/pg/001-sales-alt.sql'))
    with open(ddl_path, "r") as f:
        ddl_sql = f.read()
    conn = psycopg2.connect(dbname="testdb", user="testuser", password="testpass", host="127.0.0.1", port=5432)
    conn.autocommit = True
    with conn.cursor() as cur:
        for stmt in ddl_sql.split(';'):
            stmt = stmt.strip()
            if stmt:
                try:
                    cur.execute(stmt)
                except Exception:
                    pass  # Ignore errors for idempotency
    conn.close()

@pytest.fixture(scope="module", autouse=True)
def hydrate_mysql_db():
    """
    Hydrate the MySQL test database with the schema and data from the DDL file before running tests.
    Grants privileges to testuser for sales_db and alt_db using root user.
    """
    ddl_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../test-files/sql/source-sql-ddl-and-data/mysql/001-sales-alt.sql'))
    with open(ddl_path, "r") as f:
        ddl_sql = f.read()
    # Connect as root to grant privileges
    root_conn = pymysql.connect(user="root", password="root", host="127.0.0.1", port=3306)
    with root_conn.cursor() as cur:
        cur.execute("CREATE DATABASE IF NOT EXISTS sales_db;")
        cur.execute("CREATE DATABASE IF NOT EXISTS alt_db;")
        cur.execute("GRANT ALL PRIVILEGES ON sales_db.* TO 'testuser'@'%';")
        cur.execute("GRANT ALL PRIVILEGES ON alt_db.* TO 'testuser'@'%';")
        cur.execute("FLUSH PRIVILEGES;")
    root_conn.commit()
    root_conn.close()
    # Now connect as testuser to run the DDL
    conn = pymysql.connect(user="testuser", password="testpass", host="127.0.0.1", port=3306)
    with conn.cursor() as cur:
        for stmt in ddl_sql.split(';'):
            stmt = stmt.strip()
            if stmt:
                try:
                    cur.execute(stmt)
                except Exception:
                    pass  # Ignore errors for idempotency
    conn.commit()
    conn.close()

@pytest.fixture(scope="module", autouse=True)
def hydrate_mssql_db():
    """
    Hydrate the MS SQL test database with the schema and data from the DDL file before running tests.
    """
    ddl_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../test-files/sql/source-sql-ddl-and-data/mssql/sales-alt.sql'))
    with open(ddl_path, "r") as f:
        ddl_sql = f.read()
    conn = pyodbc.connect("DRIVER={ODBC Driver 18 for SQL Server};SERVER=127.0.0.1;DATABASE=master;UID=sa;PWD=YourStrong!Passw0rd;TrustServerCertificate=yes;")
    cursor = conn.cursor()
    # Split on GO for batch execution
    for stmt in ddl_sql.split(';'):
        stmt = stmt.strip()
        if stmt:
            try:
                cursor.execute(stmt)
            except Exception:
                pass  # Ignore errors for idempotency
    conn.commit()
    cursor.close()
    conn.close()

def test_sql_input_all_tables():
    """
    Test that all tables and views are copied when using the '*.*' glob pattern.

    - Asserts all expected tables/views are present.
    - Asserts rows are returned as dictionaries.
    """
    sql_input = get_sql_input(source=get_sqlite_conn_str(), include=["*.*"])
    tables = sql_input.get_tables()
    table_names = {t["name"] for t in tables}
    print("DEBUG tables:", tables)
    print("DEBUG table_names:", table_names)
    assert "good_customers" in table_names
    assert "purchases" in table_names
    assert "v_good_customers" in table_names
    for t in tables:
        assert isinstance(t["rows"], list)
        if t["rows"]:
            assert isinstance(t["rows"][0], dict)
            assert "_table" not in t["rows"][0]  # _table only in iter_rows

def test_sql_input_single_table():
    """
    Test that only the specified table is copied when using a single table glob pattern.
    """
    sql_input = get_sql_input(source=get_sqlite_conn_str(), include=["good_customers"])
    tables = sql_input.get_tables()
    assert len(tables) == 1
    assert tables[0]["name"] == "good_customers"
    assert all(isinstance(row, dict) for row in tables[0]["rows"])

def test_sql_input_view():
    """
    Test that only the specified view is copied when using a view glob pattern.
    """
    sql_input = get_sql_input(source=get_sqlite_conn_str(), include=["v_good_customers"])
    tables = sql_input.get_tables()
    assert len(tables) == 1
    assert tables[0]["name"] == "v_good_customers"
    assert all(isinstance(row, dict) for row in tables[0]["rows"])

def test_sql_input_nonexistent_table():
    """
    Test that no tables are copied when a non-existent table is specified in the glob pattern.
    """
    sql_input = get_sql_input(source=get_sqlite_conn_str(), include=["does_not_exist"])
    tables = sql_input.get_tables()
    assert tables == []

def test_sql_input_default_all_tables():
    """
    Test that all tables and views are copied when no 'include' argument is specified (default behavior).
    """
    sql_input = get_sql_input(source=get_sqlite_conn_str())  # No include specified
    tables = sql_input.get_tables()
    table_names = {t["name"] for t in tables}
    # Should include all tables/views
    assert "good_customers" in table_names
    assert "purchases" in table_names
    assert "v_good_customers" in table_names
    assert len(table_names) == 3  # Only these three

def test_sql_input_subset_tables():
    """
    Test that only the specified subset of tables are copied and others are excluded.
    """
    sql_input = get_sql_input(source=get_sqlite_conn_str(), include=["good_customers", "purchases"])
    tables = sql_input.get_tables()
    table_names = {t["name"] for t in tables}
    # Should only include specified tables
    assert "good_customers" in table_names
    assert "purchases" in table_names
    assert "v_good_customers" not in table_names
    assert len(table_names) == 2

def test_sql_input_empty_pattern():
    """
    Test that an empty pattern results in no tables being copied.
    """
    sql_input = get_sql_input(source=get_sqlite_conn_str(), include=[""])
    tables = sql_input.get_tables()
    assert tables == []

def test_sql_input_invalid_pattern():
    """
    Test that an invalid pattern (e.g., malformed) results in no tables being copied.
    """
    sql_input = get_sql_input(source=get_sqlite_conn_str(), include=["foo..bar"])
    tables = sql_input.get_tables()
    assert tables == []

def test_sql_input_empty_include_list():
    """
    Test that an empty include list results in no tables being copied.
    """
    sql_input = get_sql_input(source=get_sqlite_conn_str(), include=[])
    tables = sql_input.get_tables()
    assert tables == []

def test_sql_input_del():
    """
    Test that the destructor (__del__) runs without error.
    """
    sql_input = get_sql_input(source=get_sqlite_conn_str(), include=["good_customers"])
    del sql_input  # Should not raise

def test_sql_input_non_sqlite_patterns(monkeypatch):
    """
    Test all glob pattern branches for a non-SQLite database using a mocked inspector.
    """
    sql_input = BaseSQLInput(source=get_sqlite_conn_str(), include=["*.*", "public.*", "public.users", "users"])
    sql_input.is_sqlite = False
    sql_input.inspector = make_mock_inspector()
    # Patch Table and select in the correct module to avoid real DB access
    monkeypatch.setattr("forklift.inputs.sql_input.Table", lambda name, metadata, schema=None, autoload_with=None: types.SimpleNamespace())
    monkeypatch.setattr("forklift.inputs.sql_input.select", lambda table_obj: "SELECT *")
    sql_input.connection.execute = lambda stmt: [types.SimpleNamespace(_mapping={"id": 1, "name": "test"})]
    tables = sql_input.get_tables()
    table_names = {t["name"] for t in tables}
    print("DEBUG tables:", tables)
    print("DEBUG table_names:", table_names)
    # Should include all tables/views from both schemas
    assert "users" in table_names
    assert "events" in table_names
    assert "reports" in table_names
    assert "user_view" in table_names
    assert "report_view" in table_names

def test_sql_input_table_reflection_error(monkeypatch):
    """
    Test that a table reflection error is handled (exception raised).
    """
    sql_input = get_sql_input(source=get_sqlite_conn_str(), include=["error_table"])
    sql_input.is_sqlite = True
    # Minimal inspector returns one table that matches the pattern
    inspector = types.SimpleNamespace()
    inspector.get_table_names = lambda: ["error_table"]
    inspector.get_view_names = lambda: []
    sql_input.inspector = inspector
    def raise_error(*args, **kwargs):
        raise SQLAlchemyError("Reflection failed")
    monkeypatch.setattr("forklift.inputs.sql_input.Table", raise_error)
    monkeypatch.setattr("forklift.inputs.sql_input.select", lambda table_obj: "SELECT *")
    sql_input.connection.execute = lambda stmt: [types.SimpleNamespace(_mapping={"id": 1, "name": "test"})]
    with pytest.raises(SQLAlchemyError):
        list(sql_input.iter_rows())

def test_sql_input_empty_database(monkeypatch):
    """
    Test that an empty database (no tables/views) results in no output.
    """
    sql_input = get_sql_input(source=get_sqlite_conn_str())
    sql_input.is_sqlite = True
    inspector = types.SimpleNamespace()
    inspector.get_table_names = lambda: []
    inspector.get_view_names = lambda: []
    sql_input.inspector = inspector
    tables = sql_input.get_tables()
    assert tables == []

def test_postgres_sql_input_all_tables():
    """
    Test that all tables and views are copied from Postgres when using the '*.*' glob pattern.

    - Asserts all expected tables/views are present.
    - Asserts rows are returned as dictionaries.
    """
    sql_input = get_sql_input(source=get_postgres_conn_str(), include=["*.*"])
    tables = sql_input.get_tables()
    table_names = {t["name"] for t in tables}
    print("DEBUG tables:", tables)
    print("DEBUG table_names:", table_names)
    assert "good_customers" in table_names
    assert "purchases" in table_names
    assert "v_good_customers" in table_names
    for t in tables:
        assert isinstance(t["rows"], list)
        if t["rows"]:
            assert isinstance(t["rows"][0], dict)
            assert "_table" not in t["rows"][0]

def test_postgres_sql_input_sales_schema():
    """
    Test that only tables/views in the 'sales' schema are copied from Postgres when using 'sales.*'.
    """
    sql_input = get_sql_input(source=get_postgres_conn_str(), include=["sales.*"])
    tables = sql_input.get_tables()
    table_names = {t["name"] for t in tables}
    assert table_names == {"good_customers", "purchases", "v_good_customers"}

def test_postgres_sql_input_single_table():
    """
    Test that only the specified table is copied from Postgres when using a single table glob pattern.
    """
    sql_input = get_sql_input(source=get_postgres_conn_str(), include=["sales.good_customers"])
    tables = sql_input.get_tables()
    assert len(tables) == 1
    assert tables[0]["name"] == "good_customers"
    assert all(isinstance(row, dict) for row in tables[0]["rows"])

def test_postgres_sql_input_table_by_name():
    """
    Test that all tables named 'good_customers' are copied from Postgres when using a table name without schema.
    Matches all schemas.
    """
    sql_input = get_sql_input(source=get_postgres_conn_str(), include=["good_customers"])
    tables = sql_input.get_tables()
    assert len(tables) >= 1
    for t in tables:
        assert t["name"] == "good_customers"
        assert all(isinstance(row, dict) for row in t["rows"])

def test_postgres_sql_input_nonexistent_table():
    """
    Test that no tables are copied from Postgres when a non-existent table is specified in the glob pattern.
    """
    sql_input = get_sql_input(source=get_postgres_conn_str(), include=["does_not_exist"])
    tables = sql_input.get_tables()
    assert tables == []

def test_postgres_sql_input_schema_and_table():
    """
    Test that only the specified table is returned when both schema and table are provided (e.g. 'sales.good_customers').
    """
    sql_input = get_sql_input(source=get_postgres_conn_str(), include=["sales.good_customers"])
    tables = sql_input.get_tables()
    assert len(tables) == 1
    assert tables[0]["name"] == "good_customers"
    assert tables[0]["schema"] == "sales"
    assert all(isinstance(row, dict) for row in tables[0]["rows"])

def test_postgres_sql_input_schema_glob():
    """
    Test that all tables/views in the specified schema are returned when using a schema glob (e.g. 'sales.*').
    """
    sql_input = get_sql_input(source=get_postgres_conn_str(), include=["sales.*"])
    tables = sql_input.get_tables()
    table_names = {t["name"] for t in tables}
    schemas = {t["schema"] for t in tables}
    assert table_names == {"good_customers", "purchases", "v_good_customers"}
    assert schemas == {"sales"}
    for t in tables:
        assert t["schema"] == "sales"
        assert all(isinstance(row, dict) for row in t["rows"])

def test_postgres_sql_input_all_schemas_glob():
    """
    Test that all tables/views in all schemas are returned when using '*.*' glob.
    """
    sql_input = get_sql_input(source=get_postgres_conn_str(), include=["*.*"])
    tables = sql_input.get_tables()
    table_names = {t["name"] for t in tables}
    schemas = {t["schema"] for t in tables}
    # Should include all tables/views in all schemas
    assert "good_customers" in table_names
    assert "purchases" in table_names
    assert "v_good_customers" in table_names
    assert "sales" in schemas
    for t in tables:
        assert all(isinstance(row, dict) for row in t["rows"])

def test_postgres_sql_input_default_all_tables():
    """
    Test that all tables/views in all schemas are copied from Postgres when no 'include' argument is specified (default behavior).
    """
    sql_input = get_sql_input(source=get_postgres_conn_str())
    tables = sql_input.get_tables()
    table_names = {t["name"] for t in tables}
    schemas = {t["schema"] for t in tables}
    assert "good_customers" in table_names
    assert "purchases" in table_names
    assert "v_good_customers" in table_names
    assert "sales" in schemas
    for t in tables:
        assert all(isinstance(row, dict) for row in t["rows"])

def test_mysql_sql_input_schema_and_table():
    """
    Test that only the specified table is returned when both schema and table are provided (e.g. 'sales_db.good_customers').
    """
    sql_input = get_sql_input(source=get_mysql_conn_str(), include=["sales_db.good_customers"])
    tables = sql_input.get_tables()
    assert len(tables) == 1
    assert tables[0]["name"] == "good_customers"
    assert tables[0]["schema"] == "sales_db"
    assert all(isinstance(row, dict) for row in tables[0]["rows"])

def test_mysql_sql_input_schema_glob():
    """
    Test that all tables/views in the specified schema are returned when using a schema glob (e.g. 'sales_db.*').
    """
    sql_input = get_sql_input(source=get_mysql_conn_str(), include=["sales_db.*"])
    tables = sql_input.get_tables()
    table_names = {t["name"] for t in tables}
    schemas = {t["schema"] for t in tables}
    assert table_names == {"good_customers", "purchases", "v_good_customers"}
    assert schemas == {"sales_db"}
    for t in tables:
        assert t["schema"] == "sales_db"
        assert all(isinstance(row, dict) for row in t["rows"])

def test_mysql_sql_input_all_schemas_glob():
    """
    Test that all tables/views in all schemas are returned when using '*.*' glob.
    """
    sql_input = get_sql_input(source=get_mysql_conn_str(), include=["*.*"])
    tables = sql_input.get_tables()
    table_names = {t["name"] for t in tables}
    schemas = {t["schema"] for t in tables}
    assert "good_customers" in table_names
    assert "purchases" in table_names
    assert "v_good_customers" in table_names
    assert "sales_db" in schemas
    for t in tables:
        assert all(isinstance(row, dict) for row in t["rows"])

def test_mysql_sql_input_table_by_name():
    """
    Test that all tables named 'good_customers' are copied from MySQL when using a table name without schema.
    Matches all schemas.
    """
    sql_input = get_sql_input(source=get_mysql_conn_str(), include=["good_customers"])
    tables = sql_input.get_tables()
    assert len(tables) >= 1
    for t in tables:
        assert t["name"] == "good_customers"
        assert all(isinstance(row, dict) for row in t["rows"])

def test_mysql_sql_input_nonexistent_table():
    """
    Test that no tables are copied from MySQL when a non-existent table is specified in the glob pattern.
    """
    sql_input = get_sql_input(source=get_mysql_conn_str(), include=["does_not_exist"])
    tables = sql_input.get_tables()
    assert tables == []

def test_mysql_sql_input_default_all_tables():
    """
    Test that all tables/views in all schemas are returned from MySQL when no 'include' argument is specified (default behavior).
    """
    sql_input = get_sql_input(source=get_mysql_conn_str())
    tables = sql_input.get_tables()
    table_names = {t["name"] for t in tables}
    schemas = {t["schema"] for t in tables}
    assert "good_customers" in table_names
    assert "purchases" in table_names
    assert "v_good_customers" in table_names
    assert "sales_db" in schemas
    for t in tables:
        assert all(isinstance(row, dict) for row in t["rows"])

def test_mssql_sql_input_schema_and_table():
    """
    Test that only the specified table is returned when both schema and table are provided (e.g. 'sales.good_customers').
    Note: Views are not exported for MS SQL due to ODBC/driver limitations.
    """
    sql_input = get_sql_input(source=get_mssql_conn_str(), include=["sales.good_customers"])
    tables = sql_input.get_tables()
    assert len(tables) == 1
    assert tables[0]["name"] == "good_customers"
    assert tables[0]["schema"] == "sales"
    assert all(isinstance(row, dict) for row in tables[0]["rows"])


def test_mssql_sql_input_schema_glob():
    """
    Test that all tables in the specified schema are returned when using a schema glob (e.g. 'sales.*').
    Note: Views are not exported for MS SQL due to ODBC/driver limitations.
    """
    sql_input = get_sql_input(source=get_mssql_conn_str(), include=["sales.*"])
    tables = sql_input.get_tables()
    table_names = {t["name"] for t in tables}
    schemas = {t["schema"] for t in tables}
    assert table_names == {"good_customers", "purchases"}
    assert schemas == {"sales"}
    for t in tables:
        assert t["schema"] == "sales"
        assert all(isinstance(row, dict) for row in t["rows"])


def test_mssql_sql_input_all_schemas_glob():
    """
    Test that all tables in all schemas are returned when using '*.*' glob.
    Note: Views are not exported for MS SQL due to ODBC/driver limitations.
    """
    sql_input = get_sql_input(source=get_mssql_conn_str(), include=["*.*"])
    tables = sql_input.get_tables()
    table_names = {t["name"] for t in tables}
    schemas = {t["schema"] for t in tables}
    assert "good_customers" in table_names
    assert "purchases" in table_names
    assert "sales" in schemas
    for t in tables:
        assert all(isinstance(row, dict) for row in t["rows"])


def test_mssql_sql_input_table_by_name():
    """
    Test that all tables named 'good_customers' are copied from MS SQL when using a table name without schema.
    Matches all schemas. Views are not exported for MS SQL due to ODBC/driver limitations.
    """
    sql_input = get_sql_input(source=get_mssql_conn_str(), include=["good_customers"])
    tables = sql_input.get_tables()
    assert len(tables) >= 1
    for t in tables:
        assert t["name"] == "good_customers"
        assert all(isinstance(row, dict) for row in t["rows"])


def test_mssql_sql_input_nonexistent_table():
    """
    Test that no tables are copied from MS SQL when a non-existent table is specified in the glob pattern.
    Views are not exported for MS SQL due to ODBC/driver limitations.
    """
    sql_input = get_sql_input(source=get_mssql_conn_str(), include=["does_not_exist"])
    tables = sql_input.get_tables()
    assert tables == []

def test_mssql_sql_input_default_all_tables():
    """
    Test that all tables in all schemas are returned from MS SQL when no 'include' argument is specified (default behavior).
    Views are not exported for MS SQL due to ODBC/driver limitations.
    """
    sql_input = get_sql_input(source=get_mssql_conn_str())
    tables = sql_input.get_tables()
    table_names = {t["name"] for t in tables}
    schemas = {t["schema"] for t in tables}
    assert "good_customers" in table_names
    assert "purchases" in table_names
    assert "sales" in schemas
    for t in tables:
        assert all(isinstance(row, dict) for row in t["rows"])

def test_oracle_sql_input_all_tables():
    """
    Test that all tables and views are copied from Oracle when using the '*.*' glob pattern.

    - Asserts all expected tables/views are present.
    - Asserts rows are returned as dictionaries.
    """
    sql_input = get_sql_input(source=get_oracle_conn_str(), include=["*.*"])
    tables = sql_input.get_tables()
    table_names = {t["name"] for t in tables}
    print("DEBUG tables:", tables)
    print("DEBUG table_names:", table_names)
    assert "good_customers" in table_names
    assert "purchases" in table_names
    assert "v_good_customers" in table_names
    for t in tables:
        assert isinstance(t["rows"], list)
        if t["rows"]:
            assert isinstance(t["rows"][0], dict)
            assert "_table" not in t["rows"][0]

def test_oracle_sql_input_sales_schema():
    """
    Test that only tables/views in the 'sales' schema are copied from Oracle when using 'sales.*'.
    """
    sql_input = get_sql_input(source=get_oracle_conn_str(), include=["sales.*"])
    tables = sql_input.get_tables()
    table_names = {t["name"] for t in tables}
    assert table_names == {"good_customers", "purchases", "v_good_customers"}

def test_oracle_sql_input_single_table():
    """
    Test that only the specified table is copied from Oracle when using a single table glob pattern.
    """
    sql_input = get_sql_input(source=get_oracle_conn_str(), include=["sales.good_customers"])
    tables = sql_input.get_tables()
    assert len(tables) == 1
    assert tables[0]["name"] == "good_customers"
    assert all(isinstance(row, dict) for row in tables[0]["rows"])

def test_oracle_sql_input_table_by_name():
    """
    Test that all tables named 'good_customers' are copied from Oracle when using a table name without schema.
    Matches all schemas.
    """
    sql_input = get_sql_input(source=get_oracle_conn_str(), include=["good_customers"])
    tables = sql_input.get_tables()
    assert len(tables) >= 1
    for t in tables:
        assert t["name"] == "good_customers"
        assert all(isinstance(row, dict) for row in t["rows"])

def test_oracle_sql_input_nonexistent_table():
    """
    Test that no tables are copied from Oracle when a non-existent table is specified in the glob pattern.
    """
    sql_input = get_sql_input(source=get_oracle_conn_str(), include=["does_not_exist"])
    tables = sql_input.get_tables()
    assert tables == []

def test_oracle_sql_input_schema_and_table():
    """
    Test that only the specified table is returned when both schema and table are provided (e.g. 'sales.good_customers').
    """
    sql_input = get_sql_input(source=get_oracle_conn_str(), include=["sales.good_customers"])
    tables = sql_input.get_tables()
    assert len(tables) == 1
    assert tables[0]["name"] == "good_customers"
    assert tables[0]["schema"] == "sales"
    assert all(isinstance(row, dict) for row in tables[0]["rows"])

def test_oracle_sql_input_schema_glob():
    """
    Test that all tables/views in the specified schema are returned when using a schema glob (e.g. 'sales.*').
    """
    sql_input = get_sql_input(source=get_oracle_conn_str(), include=["sales.*"])
    tables = sql_input.get_tables()
    table_names = {t["name"] for t in tables}
    schemas = {t["schema"] for t in tables}
    assert table_names == {"good_customers", "purchases", "v_good_customers"}
    assert schemas == {"sales"}
    for t in tables:
        assert t["schema"] == "sales"
        assert all(isinstance(row, dict) for row in t["rows"])

def test_oracle_sql_input_all_schemas_glob():
    """
    Test that all tables/views in all schemas are returned when using '*.*' glob.
    """
    sql_input = get_sql_input(source=get_oracle_conn_str(), include=["*.*"])
    tables = sql_input.get_tables()
    table_names = {t["name"] for t in tables}
    schemas = {t["schema"] for t in tables}
    # Should include all tables/views in all schemas
    assert "good_customers" in table_names
    assert "purchases" in table_names
    assert "v_good_customers" in table_names
    assert "sales" in schemas
    for t in tables:
        assert all(isinstance(row, dict) for row in t["rows"])

def test_oracle_sql_input_default_all_tables():
    """
    Test that all tables/views in all schemas are copied from Oracle when no 'include' argument is specified (default behavior).
    """
    sql_input = get_sql_input(source=get_oracle_conn_str())
    tables = sql_input.get_tables()
    table_names = {t["name"] for t in tables}
    schemas = {t["schema"] for t in tables}
    assert "good_customers" in table_names
    assert "purchases" in table_names
    assert "v_good_customers" in table_names
    assert "sales" in schemas
    for t in tables:
        assert all(isinstance(row, dict) for row in t["rows"])
