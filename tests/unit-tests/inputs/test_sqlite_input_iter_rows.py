from forklift.inputs.db.sqlite_input import SQLiteInput
from sqlalchemy import text, inspect


def test_sqlite_iter_rows_full_coverage():
    """Ensure iter_rows yields rows for all tables/views (covers lines 54-57)."""
    si = SQLiteInput("sqlite:///:memory:")
    conn = si.connection
    # Create two tables and a view to exercise iteration across tables + views
    conn.execute(text("CREATE TABLE customers(id INTEGER PRIMARY KEY, name TEXT)"))
    conn.execute(text("INSERT INTO customers(name) VALUES ('Alice'), ('Bob')"))
    conn.execute(text("CREATE TABLE orders(id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)"))
    conn.execute(text("INSERT INTO orders(customer_id, amount) VALUES (1, 10.5), (2, 20.0)"))
    conn.execute(text("CREATE VIEW v_customers AS SELECT id, name FROM customers"))
    try:
        conn.commit()
    except Exception:
        # Some drivers autocommit DDL in SQLite; ignore if commit not needed
        pass
    # Refresh inspector so new tables/views are visible
    si.inspector = inspect(si.engine)

    rows = list(si.iter_rows())
    # Expect rows from customers (2), orders (2), and view v_customers (2)
    assert len(rows) == 6
    # Basic shape check
    assert all(isinstance(r, dict) for r in rows)
    assert any(r.get("name") == "Alice" for r in rows)
    assert any(r.get("amount") == 20.0 for r in rows)
    # Ensure view rows present (at least one duplicate name from customers)
    customer_names = [r.get("name") for r in rows if "name" in r]
    assert customer_names.count("Alice") >= 1
