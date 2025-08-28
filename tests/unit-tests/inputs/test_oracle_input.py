from forklift.inputs.db.oracle_input import OracleInput, MySQLInput
import pytest


class FakeOracleInspector:
    def __init__(self):
        # Include a mix of system and user schemas; also mixed case to exercise .upper() logic
        self._schemas = [
            "SYS",           # system (exact upper)
            "system",        # system (lowercase, should be skipped via upper())
            "Outln",         # system (mixed case)
            "HR",            # user
            "sales",         # user (lowercase)
            "Analytics"      # user (mixed case)
        ]
        self._tables = {
            "HR": ["EMPLOYEES"],
            "sales": ["ORDERS"],
            "Analytics": [],  # no tables, only a view
        }
        self._views = {
            "HR": ["V_EMP"],
            "sales": [],
            "Analytics": ["DASHBOARD"],
        }

    def get_schema_names(self):
        return self._schemas

    def get_table_names(self, schema=None):
        return self._tables.get(schema, [])

    def get_view_names(self, schema=None):
        return self._views.get(schema, [])


class FakeMySQLInspector:
    def __init__(self):
        self._schemas = [
            "information_schema",  # system
            "mysql",               # system
            "performance_schema",  # system
            "sys",                 # system
            "appdb",               # user
            "reporting"            # user
        ]
        self._tables = {
            "appdb": ["users"],
            "reporting": ["facts"],
        }
        self._views = {
            "appdb": ["v_users"],
            "reporting": [],
        }

    def get_schema_names(self):
        return self._schemas

    def get_table_names(self, schema=None):
        return self._tables.get(schema, [])

    def get_view_names(self, schema=None):
        return self._views.get(schema, [])


def test_oracle_get_all_tables_filters_system_and_collects_tables_and_views():
    oi = OracleInput("sqlite:///:memory:")  # engine type irrelevant; we'll replace inspector
    # Replace inspector with fake to avoid needing an Oracle backend
    oi.inspector = FakeOracleInspector()

    all_items = oi._get_all_tables()
    # Should not include any system schemas
    schemas_in_result = {s for s, _ in all_items}
    assert schemas_in_result.isdisjoint({"SYS", "SYSTEM", "OUTLN"})
    # Should include user schemas (case preserved as provided by inspector)
    assert ("HR", "EMPLOYEES") in all_items
    assert ("HR", "V_EMP") in all_items
    assert ("sales", "ORDERS") in all_items
    # View only schema still appears via its view
    assert ("Analytics", "DASHBOARD") in all_items


def test_oracle_iter_rows_not_implemented():
    oi = OracleInput("sqlite:///:memory:")
    with pytest.raises(NotImplementedError):
        list(oi.iter_rows())


def test_mysql_get_all_tables_filters_system_and_collects_tables_and_views():
    mi = MySQLInput("sqlite:///:memory:")
    mi.inspector = FakeMySQLInspector()
    all_items = mi._get_all_tables()
    schemas_in_result = {s for s, _ in all_items}
    assert schemas_in_result == {"appdb", "reporting"}
    assert ("appdb", "users") in all_items
    assert ("appdb", "v_users") in all_items
    assert ("reporting", "facts") in all_items


def test_mysql_iter_rows_not_implemented():
    mi = MySQLInput("sqlite:///:memory:")
    with pytest.raises(NotImplementedError):
        list(mi.iter_rows())

