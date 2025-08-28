from forklift.inputs.db.mysql_input import MySQLInput
import pytest


class FakeMySQLInspector:
    def __init__(self):
        # Mix of system and user schemas to exercise filtering logic
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


def test_mysql_get_all_tables_filters_system_and_collects_tables_and_views_module_import():
    mi = MySQLInput("sqlite:///:memory:")
    mi.inspector = FakeMySQLInspector()  # inject fake inspector
    all_items = mi._get_all_tables()
    # Only user schemas should appear
    assert set(s for s, _ in all_items) == {"appdb", "reporting"}
    # Tables
    assert ("appdb", "users") in all_items
    assert ("reporting", "facts") in all_items
    # View
    assert ("appdb", "v_users") in all_items


def test_mysql_iter_rows_not_implemented_module_import():
    mi = MySQLInput("sqlite:///:memory:")
    with pytest.raises(NotImplementedError):
        list(mi.iter_rows())

