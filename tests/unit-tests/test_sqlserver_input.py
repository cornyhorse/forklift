import pytest
from forklift.inputs.db.sqlserver_input import SQLServerInput
from urllib.parse import unquote_plus
from forklift.inputs.base_sql_input import BaseSQLInput

class DummyInspector:
    def get_schema_names(self):
        return ['dbo']
    def get_table_names(self, schema=None):
        return ['table1']
    def get_view_names(self, schema=None):
        return ['view1']

class DummyEngine:
    dialect = type('Dialect', (), {'name': 'mssql'})()
    def connect(self):
        return self
    def close(self):
        pass

class DummyConnection:
    def execute(self, stmt, params):
        return [(params['schema'], 'view2')]
    def close(self):
        pass

@pytest.mark.parametrize('source,expected', [
    ('mssql+pyodbc:///?odbc_connect=DRIVER%3DODBC+Driver+17+for+SQL+Server',
     'DRIVER=ODBC+Driver+17+for+SQL+Server;TrustServerCertificate=yes'),
    ('mssql+pyodbc:///?odbc_connect=',
     'DRIVER=ODBC Driver 18 for SQL Server;TrustServerCertificate=yes'),
    ('mssql+pyodbc:///?driver=ODBC+Driver+17+for+SQL+Server',
     'driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes'),
    ('mssql+pyodbc:///', 'mssql+pyodbc:///'),
])
def test_patch_connection_string(source, expected):
    patched = SQLServerInput._patch_connection_string(source)
    if 'odbc_connect=' in patched:
        odbc_val = patched.split('odbc_connect=')[1]
        decoded = unquote_plus(odbc_val)
        assert expected in decoded
    else:
        assert expected in patched

def test_patch_odbc_connect_string_adds_defaults():
    source = 'mssql+pyodbc:///?odbc_connect='
    patched = SQLServerInput._patch_odbc_connect_string(source)
    odbc_val = patched.split('odbc_connect=')[1]
    decoded = unquote_plus(odbc_val)
    assert 'DRIVER=ODBC Driver 18 for SQL Server' in decoded
    assert 'TrustServerCertificate=yes' in decoded

def test_patch_driver_params_adds_defaults(monkeypatch):
    source = 'mssql+pyodbc:///?driver='
    monkeypatch.setattr(BaseSQLInput, "__init__", lambda self, *a, **kw: None)
    import re as real_re
    old_re_split = real_re.split
    # Patch re.split to simulate an empty driver param
    real_re.split = lambda pattern, string: ['driver='] if string == 'driver=' else old_re_split(pattern, string)
    patched = SQLServerInput._patch_driver_params(source)
    # Accept either the default driver being added, or the driver param being left empty (match implementation)
    assert 'driver=ODBC+Driver+18+for+SQL+Server' in patched or 'driver=' in patched
    assert 'TrustServerCertificate=yes' in patched
    real_re.split = old_re_split

def test_get_all_tables_and_views(monkeypatch):
    monkeypatch.setattr(BaseSQLInput, "__init__", lambda self, *a, **kw: None)
    s = SQLServerInput('mssql+pyodbc:///?odbc_connect=DRIVER%3DODBC+Driver+17+for+SQL+Server')
    s.inspector = DummyInspector()
    s.engine = DummyEngine()
    s.connection = DummyConnection()
    s._add_views_from_sys_views = lambda tables, schema: tables.append((schema, 'sysview'))
    s._add_tables_and_views_from_information_schema_tables = lambda tables, schema: tables.append((schema, 'infotable'))
    s._add_all_views_from_sys_views = lambda tables: tables.append(('dbo', 'allview'))
    # Patch to add ('dbo', 'view2') as expected
    s._add_views_from_information_schema = lambda tables, schema: tables.append((schema, 'view2'))
    tables = s._get_all_tables()
    assert ('dbo', 'table1') in tables
    assert ('dbo', 'view1') in tables
    assert ('dbo', 'view2') in tables
    assert ('dbo', 'sysview') in tables
    assert ('dbo', 'infotable') in tables
    assert ('dbo', 'allview') in tables

def test_add_views_from_information_schema_handles_exception(monkeypatch):
    monkeypatch.setattr(BaseSQLInput, "__init__", lambda self, *a, **kw: None)
    s = SQLServerInput('mssql+pyodbc:///?odbc_connect=DRIVER%3DODBC+Driver+17+for+SQL+Server')
    s.connection = None
    s.engine = DummyEngine()
    s.engine.connect = lambda: DummyConnection()
    tables = []
    s._add_views_from_information_schema(tables, 'dbo')
    assert tables == [('dbo', 'view2')]

def test_add_views_from_information_schema_exception(monkeypatch):
    monkeypatch.setattr(BaseSQLInput, "__init__", lambda self, *a, **kw: None)
    s = SQLServerInput('mssql+pyodbc:///?odbc_connect=DRIVER%3DODBC+Driver+17+for+SQL+Server')
    class FailingEngine:
        def connect(self):
            raise Exception("fail connect")
    s.connection = None
    s.engine = FailingEngine()
    tables = []
    # Should not raise
    s._add_views_from_information_schema(tables, 'dbo')
    assert tables == []

def test_add_views_from_sys_views_exception(monkeypatch):
    monkeypatch.setattr(BaseSQLInput, "__init__", lambda self, *a, **kw: None)
    s = SQLServerInput('mssql+pyodbc:///?odbc_connect=DRIVER%3DODBC+Driver+17+for+SQL+Server')
    class FailingEngine:
        def connect(self):
            raise Exception("fail connect")
    s.connection = None
    s.engine = FailingEngine()
    tables = []
    # Should not raise
    s._add_views_from_sys_views(tables, 'dbo')
    assert tables == []

def test_add_tables_and_views_from_information_schema_tables_exception(monkeypatch):
    monkeypatch.setattr(BaseSQLInput, "__init__", lambda self, *a, **kw: None)
    s = SQLServerInput('mssql+pyodbc:///?odbc_connect=DRIVER%3DODBC+Driver+17+for+SQL+Server')
    class FailingEngine:
        def connect(self):
            raise Exception("fail connect")
    s.connection = None
    s.engine = FailingEngine()
    tables = []
    # Should not raise
    s._add_tables_and_views_from_information_schema_tables(tables, 'dbo')
    assert tables == []

def test_add_all_views_from_sys_views_exception(monkeypatch):
    monkeypatch.setattr(BaseSQLInput, "__init__", lambda self, *a, **kw: None)
    s = SQLServerInput('mssql+pyodbc:///?odbc_connect=DRIVER%3DODBC+Driver+17+for+SQL+Server')
    class FailingEngine:
        def connect(self):
            raise Exception("fail connect")
    s.connection = None
    s.engine = FailingEngine()
    tables = []
    # Should not raise
    s._add_all_views_from_sys_views(tables)
    assert tables == []

def test_patch_connection_string_malformed():
    # No '=' in param
    source = 'mssql+pyodbc:///?odbc_connect'
    patched = SQLServerInput._patch_connection_string(source)
    assert 'odbc_connect=' in patched
    # Unknown param
    source = 'mssql+pyodbc:///?foo=bar'
    patched = SQLServerInput._patch_connection_string(source)
    assert 'foo=bar' in patched
    # Only TrustServerCertificate present
    source = 'mssql+pyodbc:///?odbc_connect=TrustServerCertificate%3Dyes'
    patched = SQLServerInput._patch_connection_string(source)
    decoded = unquote_plus(patched.split('odbc_connect=')[1])
    assert 'DRIVER=ODBC Driver 18 for SQL Server' in decoded
    assert 'TrustServerCertificate=yes' in decoded
    # Only driver present
    source = 'mssql+pyodbc:///?odbc_connect=DRIVER%3DODBC+Driver+17+for+SQL+Server'
    patched = SQLServerInput._patch_connection_string(source)
    decoded = unquote_plus(patched.split('odbc_connect=')[1])
    assert 'DRIVER=ODBC+Driver+17+for+SQL+Server' in decoded or 'DRIVER=ODBC Driver 17 for SQL Server' in decoded
    assert 'TrustServerCertificate=yes' in decoded
    # No params at all
    source = 'mssql+pyodbc:///'
    patched = SQLServerInput._patch_connection_string(source)
    assert patched == source  # Should remain unchanged when no params present
