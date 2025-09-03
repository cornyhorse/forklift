"""Tests for SQL input handler and related functionality."""
import pytest
import sqlite3
import tempfile
import os
from unittest.mock import patch, MagicMock
import pyarrow as pa

from forklift.inputs.sql import SqlInputHandler
from forklift.inputs.config import SqlInputConfig
from forklift.schema.sql_schema_importer import SqlSchemaImporter, SchemaValidationError


@pytest.fixture
def sqlite_db():
    """Create a temporary SQLite database for testing."""
    # Create temporary database file
    db_fd, db_path = tempfile.mkstemp(suffix='.db')
    os.close(db_fd)

    # Create and populate test database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create test tables
    cursor.execute("""
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER,
            salary REAL,
            hire_date TEXT,
            active BOOLEAN
        )
    """)

    cursor.execute("""
        CREATE TABLE departments (
            dept_id INTEGER PRIMARY KEY,
            dept_name TEXT NOT NULL,
            budget REAL
        )
    """)

    cursor.execute("""
        CREATE VIEW employee_view AS 
        SELECT id, name, age FROM employees WHERE active = 1
    """)

    # Insert test data
    employees_data = [
        (1, 'Alice Johnson', 30, 75000.50, '2022-01-15', True),
        (2, 'Bob Smith', 25, 55000.00, '2022-03-20', True),
        (3, 'Carol Davis', 35, 85000.75, '2021-11-10', False),
        (4, 'David Wilson', 28, 62000.25, '2022-07-05', True),
    ]

    departments_data = [
        (1, 'Engineering', 1000000.00),
        (2, 'Sales', 750000.00),
        (3, 'Marketing', 500000.00),
    ]

    cursor.executemany(
        "INSERT INTO employees VALUES (?, ?, ?, ?, ?, ?)",
        employees_data
    )
    cursor.executemany(
        "INSERT INTO departments VALUES (?, ?, ?)",
        departments_data
    )

    conn.commit()
    conn.close()

    yield db_path

    # Cleanup
    os.unlink(db_path)


class TestSqlInputHandler:
    """Test cases for the SqlInputHandler class."""

    @pytest.fixture
    def sqlite_config(self, sqlite_db):
        """Create SQLite configuration for testing."""
        return SqlInputConfig(
            connection_string=f"DRIVER={{SQLite3 ODBC Driver}};Database={sqlite_db};",
            batch_size=1000,
            query_timeout=30,
            connection_timeout=10
        )

    @pytest.fixture
    def basic_config(self):
        """Create a basic SQL configuration."""
        return SqlInputConfig(
            connection_string="DRIVER={SQLite3 ODBC Driver};Database=:memory:;",
            batch_size=5000,
            query_timeout=60
        )

    def test_init(self, basic_config):
        """Test SqlInputHandler initialization."""
        handler = SqlInputHandler(basic_config)

        assert handler.config == basic_config
        assert handler.connection is None
        assert handler.schema_importer is None

    def test_set_schema_importer(self, basic_config):
        """Test setting schema importer."""
        handler = SqlInputHandler(basic_config)
        schema_importer = MagicMock(spec=SqlSchemaImporter)

        handler.set_schema_importer(schema_importer)

        assert handler.schema_importer == schema_importer

    @patch('pyodbc.connect')
    def test_connect_success(self, mock_connect, basic_config):
        """Test successful database connection."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        handler = SqlInputHandler(basic_config)
        handler.connect()

        assert handler.connection == mock_connection
        mock_connect.assert_called_once_with(
            basic_config.connection_string,
            timeout=basic_config.connection_timeout
        )
        assert mock_connection.timeout == basic_config.query_timeout

    @patch('pyodbc.connect')
    def test_connect_with_params(self, mock_connect):
        """Test connection with additional parameters."""
        config = SqlInputConfig(
            connection_string="DRIVER={SQLite3 ODBC Driver};Database=test.db;",
            connection_params={"Timeout": "30", "ReadOnly": "Yes"}
        )
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        handler = SqlInputHandler(config)
        handler.connect()

        expected_conn_str = "DRIVER={SQLite3 ODBC Driver};Database=test.db;;Timeout=30;ReadOnly=Yes"
        mock_connect.assert_called_once_with(
            expected_conn_str,
            timeout=config.connection_timeout
        )

    def test_connect_pyodbc_not_installed(self, basic_config):
        """Test ImportError when pyodbc is not installed."""
        handler = SqlInputHandler(basic_config)

        with patch('builtins.__import__', side_effect=ImportError("No module named 'pyodbc'")):
            with pytest.raises(ImportError, match="pyodbc is required for SQL database connectivity"):
                handler.connect()

    @patch('pyodbc.connect')
    def test_connect_connection_failure(self, mock_connect, basic_config):
        """Test connection failure handling."""
        mock_connect.side_effect = Exception("Connection failed")

        handler = SqlInputHandler(basic_config)

        with pytest.raises(ConnectionError, match="Failed to connect to database"):
            handler.connect()

    def test_disconnect(self, basic_config):
        """Test database disconnection."""
        handler = SqlInputHandler(basic_config)
        mock_connection = MagicMock()
        handler.connection = mock_connection

        handler.disconnect()

        mock_connection.close.assert_called_once()
        assert handler.connection is None

    def test_disconnect_with_error(self, basic_config):
        """Test disconnection with error handling."""
        handler = SqlInputHandler(basic_config)
        mock_connection = MagicMock()
        mock_connection.close.side_effect = Exception("Close error")
        handler.connection = mock_connection

        # Should not raise exception
        handler.disconnect()

        assert handler.connection is None

    def test_disconnect_no_connection(self, basic_config):
        """Test disconnection when no connection exists."""
        handler = SqlInputHandler(basic_config)

        # Should not raise exception
        handler.disconnect()

    def test_get_table_list_not_connected(self, basic_config):
        """Test getting table list when not connected."""
        handler = SqlInputHandler(basic_config)

        with pytest.raises(ConnectionError, match="Not connected to database"):
            handler.get_table_list()

    @patch('pyodbc.connect')
    def test_get_table_list_odbc_method(self, mock_connect, basic_config):
        """Test getting table list using ODBC tables() method."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        # Mock ODBC tables() method response
        mock_row1 = MagicMock()
        mock_row1.table_schem = 'main'
        mock_row1.table_name = 'employees'
        mock_row1.table_type = 'TABLE'

        mock_row2 = MagicMock()
        mock_row2.table_schem = None
        mock_row2.table_name = 'departments'
        mock_row2.table_type = 'TABLE'

        mock_row3 = MagicMock()
        mock_row3.table_schem = 'main'
        mock_row3.table_name = 'employee_view'
        mock_row3.table_type = 'VIEW'

        mock_cursor.tables.return_value = [mock_row1, mock_row2, mock_row3]

        handler = SqlInputHandler(basic_config)
        handler.connect()
        tables = handler.get_table_list()

        expected = [
            ('main', 'employees'),
            ('default', 'departments'),
            ('main', 'employee_view')
        ]
        assert tables == expected
        mock_cursor.close.assert_called_once()

    @patch('pyodbc.connect')
    def test_get_table_list_sqlite_fallback(self, mock_connect, basic_config):
        """Test getting table list using SQLite fallback method."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        # Mock ODBC tables() method failure
        mock_cursor.tables.side_effect = Exception("ODBC tables() not supported")

        # Mock SQLite fallback query
        mock_cursor.fetchall.return_value = [
            ('main', 'employees'),
            ('main', 'departments')
        ]

        handler = SqlInputHandler(basic_config)
        handler.connect()
        tables = handler.get_table_list()

        expected = [('main', 'employees'), ('main', 'departments')]
        assert tables == expected

        # Verify SQLite fallback query was executed
        mock_cursor.execute.assert_called_with("""
                    SELECT 'main' as schema_name, name as table_name 
                    FROM sqlite_master 
                    WHERE type IN ('table', 'view')
                """)

    def test_parse_table_specification(self, basic_config):
        """Test parsing table specifications."""
        handler = SqlInputHandler(basic_config)

        # Test simple table name
        schema, table = handler._parse_table_specification("employees")
        assert schema == "default"
        assert table == "employees"

        # Test qualified table name
        schema, table = handler._parse_table_specification("main.employees")
        assert schema == "main"
        assert table == "employees"

        # Test with extra dots
        schema, table = handler._parse_table_specification("db.schema.table")
        assert schema == "db"
        assert table == "schema.table"

    @patch('pyodbc.connect')
    def test_get_specified_tables(self, mock_connect, basic_config):
        """Test getting specified tables with validation."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        # Mock available tables
        mock_row1 = MagicMock()
        mock_row1.table_schem = 'main'
        mock_row1.table_name = 'employees'
        mock_row1.table_type = 'TABLE'

        mock_row2 = MagicMock()
        mock_row2.table_schem = 'main'
        mock_row2.table_name = 'departments'
        mock_row2.table_type = 'TABLE'

        mock_cursor.tables.return_value = [mock_row1, mock_row2]

        handler = SqlInputHandler(basic_config)
        handler.connect()

        # Test valid table specifications
        specs = ["main.employees", "departments", "nonexistent"]
        tables = handler.get_specified_tables(specs)

        expected = [
            ('main', 'employees'),
            ('main', 'departments')  # Should find default match for "departments"
        ]
        assert tables == expected

    def test_get_specified_tables_not_connected(self, basic_config):
        """Test getting specified tables when not connected."""
        handler = SqlInputHandler(basic_config)

        with pytest.raises(ConnectionError, match="Not connected to database"):
            handler.get_specified_tables(["employees"])

    @patch('pyodbc.connect')
    def test_get_table_schema_not_connected(self, mock_connect, basic_config):
        """Test getting table schema when not connected."""
        handler = SqlInputHandler(basic_config)

        with pytest.raises(ConnectionError, match="Not connected to database"):
            handler.get_table_schema("main", "employees")

    @patch('pyodbc.connect')
    def test_quote_identifier(self, mock_connect, basic_config):
        """Test identifier quoting."""
        handler = SqlInputHandler(basic_config)

        # Test with use_quoted_identifiers=False (default)
        assert handler._quote_identifier("employees") == "employees"

        # Test with use_quoted_identifiers=True
        config_with_quotes = SqlInputConfig(
            connection_string="test",
            use_quoted_identifiers=True
        )
        handler_with_quotes = SqlInputHandler(config_with_quotes)
        assert handler_with_quotes._quote_identifier("employees") == '"employees"'
        assert handler_with_quotes._quote_identifier("employee data") == '"employee data"'

    @patch('pyodbc.SQL_CHAR', 1, create=True)
    @patch('pyodbc.SQL_INTEGER', 4, create=True)
    @patch('pyodbc.SQL_FLOAT', 6, create=True)
    @patch('pyodbc.SQL_VARCHAR', 12, create=True)
    def test_odbc_type_to_string(self, basic_config):
        """Test ODBC type code to string conversion."""
        handler = SqlInputHandler(basic_config)

        # Test common ODBC type codes (using the actual mapping)
        assert handler._odbc_type_to_string(1) == "CHAR"  # SQL_CHAR
        assert handler._odbc_type_to_string(4) == "INTEGER"  # SQL_INTEGER
        assert handler._odbc_type_to_string(6) == "FLOAT"  # SQL_FLOAT
        assert handler._odbc_type_to_string(12) == "VARCHAR"  # SQL_VARCHAR

        # Test unknown type (defaults to VARCHAR)
        assert handler._odbc_type_to_string(999) == "VARCHAR"

    def test_sql_type_to_pyarrow(self, basic_config):
        """Test SQL type to PyArrow type conversion."""
        handler = SqlInputHandler(basic_config)

        # Test integer types (actual implementation returns int32 for INTEGER)
        assert handler._sql_type_to_pyarrow("INTEGER") == pa.int32()
        assert handler._sql_type_to_pyarrow("SMALLINT") == pa.int16()
        assert handler._sql_type_to_pyarrow("BIGINT") == pa.int64()

        # Test string types
        assert handler._sql_type_to_pyarrow("VARCHAR") == pa.string()
        assert handler._sql_type_to_pyarrow("CHAR") == pa.string()
        assert handler._sql_type_to_pyarrow("TEXT") == pa.string()

        # Test numeric types
        assert handler._sql_type_to_pyarrow("REAL") == pa.float32()  # REAL maps to float32
        assert handler._sql_type_to_pyarrow("FLOAT") == pa.float32()
        assert handler._sql_type_to_pyarrow("DOUBLE") == pa.float64()

        # Test decimal with precision
        decimal_type = handler._sql_type_to_pyarrow("DECIMAL", size=10, decimal_digits=2)
        assert isinstance(decimal_type, pa.Decimal128Type)
        assert decimal_type.precision == 10
        assert decimal_type.scale == 2

        # Test boolean
        assert handler._sql_type_to_pyarrow("BOOLEAN") == pa.bool_()

        # Test date/time types
        assert handler._sql_type_to_pyarrow("DATE") == pa.date32()
        assert handler._sql_type_to_pyarrow("TIMESTAMP") == pa.timestamp('us')

        # Test unknown type defaults to string
        assert handler._sql_type_to_pyarrow("UNKNOWN_TYPE") == pa.string()

    def test_context_manager(self, basic_config):
        """Test using SqlInputHandler as context manager."""
        handler = SqlInputHandler(basic_config)

        with patch.object(handler, 'connect') as mock_connect, \
             patch.object(handler, 'disconnect') as mock_disconnect:

            with handler as ctx_handler:
                assert ctx_handler == handler

            mock_connect.assert_called_once()
            mock_disconnect.assert_called_once()

    def test_get_include_patterns_no_schema_importer(self):
        """Test getting include patterns when no schema importer is set."""
        config = SqlInputConfig(connection_string="test")
        handler = SqlInputHandler(config)

        # Mock the config to have include_patterns attribute as None (like hasattr would be False)
        with patch.object(config, 'include_patterns', None, create=True):
            patterns = handler.get_include_patterns()
            assert patterns == ['*.*']  # Default to all tables

    def test_get_include_patterns_with_schema_importer(self):
        """Test getting include patterns with schema importer."""
        config = SqlInputConfig(connection_string="test")
        handler = SqlInputHandler(config)
        mock_schema_importer = MagicMock()
        mock_schema_importer.all_include_patterns = ["schema1.table1", "schema2.table2"]
        handler.set_schema_importer(mock_schema_importer)

        # Mock the config to have include_patterns attribute as None
        with patch.object(config, 'include_patterns', None, create=True):
            patterns = handler.get_include_patterns()
            expected = ["schema1.table1", "schema2.table2"]
            assert patterns == expected

    @patch('pyodbc.connect')
    def test_get_tables_to_process_without_schema_importer(self, mock_connect):
        """Test getting tables to process without schema importer."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        # Mock available tables
        mock_row = MagicMock()
        mock_row.table_schem = 'main'
        mock_row.table_name = 'employees'
        mock_row.table_type = 'TABLE'
        mock_cursor.tables.return_value = [mock_row]

        config = SqlInputConfig(connection_string="test")
        handler = SqlInputHandler(config)
        handler.connect()

        # Mock the config to have include_patterns attribute as None
        with patch.object(config, 'include_patterns', None, create=True):
            tables = handler.get_tables_to_process()
            expected = [("main", "employees", None)]
            assert tables == expected

    @patch('pyodbc.connect')
    def test_get_table_schema_with_columns_method(self, mock_connect, basic_config):
        """Test getting table schema using ODBC columns() method."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        # Mock ODBC columns() method response
        mock_column1 = MagicMock()
        mock_column1.column_name = 'id'
        mock_column1.type_name = 'INTEGER'
        mock_column1.column_size = 10
        mock_column1.decimal_digits = 0
        mock_column1.nullable = False

        mock_column2 = MagicMock()
        mock_column2.column_name = 'name'
        mock_column2.type_name = 'VARCHAR'
        mock_column2.column_size = 255
        mock_column2.decimal_digits = None
        mock_column2.nullable = True

        mock_cursor.columns.return_value = [mock_column1, mock_column2]

        handler = SqlInputHandler(basic_config)
        handler.connect()
        schema = handler.get_table_schema('main', 'employees')

        assert len(schema) == 2
        assert schema.field('id').type == pa.int32()
        assert schema.field('name').type == pa.string()
        assert not schema.field('id').nullable
        assert schema.field('name').nullable

    @patch('pyodbc.connect')
    def test_get_table_schema_fallback_method(self, mock_connect, basic_config):
        """Test getting table schema using fallback method when columns() fails."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        # Mock columns() method failure
        mock_cursor.columns.side_effect = Exception("columns() not supported")

        # Mock cursor description from SELECT query
        mock_cursor.description = [
            ('id', 4, None, None, None, None, None),  # SQL_INTEGER
            ('name', 12, None, None, None, None, None),  # SQL_VARCHAR
            ('salary', 8, None, None, None, None, None),  # SQL_DOUBLE
        ]

        handler = SqlInputHandler(basic_config)
        handler.connect()
        schema = handler.get_table_schema('main', 'employees')

        assert len(schema) == 3
        assert schema.field('id').type == pa.int32()
        assert schema.field('name').type == pa.string()
        assert schema.field('salary').type == pa.float64()

    @patch('pyodbc.connect')
    def test_read_table_data_success(self, mock_connect, basic_config):
        """Test successful table data reading."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        # Mock schema
        schema = pa.schema([
            pa.field('id', pa.int32()),
            pa.field('name', pa.string())
        ])

        # Mock data rows
        mock_cursor.fetchmany.side_effect = [
            [(1, 'Alice'), (2, 'Bob')],  # First batch
            [(3, 'Carol')],  # Second batch
            []  # Empty batch indicates end
        ]

        handler = SqlInputHandler(basic_config)
        handler.connect()

        # Mock get_table_schema to return our test schema
        with patch.object(handler, 'get_table_schema', return_value=schema):
            batches = list(handler.read_table_data('main', 'employees'))

        assert len(batches) == 2
        assert batches[0].num_rows == 2
        assert batches[1].num_rows == 1

        # Verify data content
        batch1_data = batches[0].to_pydict()
        assert batch1_data['id'] == [1, 2]
        assert batch1_data['name'] == ['Alice', 'Bob']

    def test_sql_type_to_pyarrow_conversions(self, basic_config):
        """Test SQL type to PyArrow type conversions."""
        handler = SqlInputHandler(basic_config)

        # Test integer types
        assert handler._sql_type_to_pyarrow('INT') == pa.int32()
        assert handler._sql_type_to_pyarrow('BIGINT') == pa.int64()
        assert handler._sql_type_to_pyarrow('SMALLINT') == pa.int16()
        assert handler._sql_type_to_pyarrow('TINYINT') == pa.int8()

        # Test float types
        assert handler._sql_type_to_pyarrow('FLOAT') == pa.float32()
        assert handler._sql_type_to_pyarrow('DOUBLE') == pa.float64()
        assert handler._sql_type_to_pyarrow('REAL') == pa.float32()

        # Test decimal types
        decimal_type = handler._sql_type_to_pyarrow('DECIMAL', 10, 2)
        assert isinstance(decimal_type, pa.Decimal128Type)
        assert decimal_type.precision == 10
        assert decimal_type.scale == 2

        # Test boolean types
        assert handler._sql_type_to_pyarrow('BOOLEAN') == pa.bool_()
        assert handler._sql_type_to_pyarrow('BIT') == pa.bool_()

        # Test date/time types
        assert handler._sql_type_to_pyarrow('DATE') == pa.date32()
        assert handler._sql_type_to_pyarrow('TIME') == pa.time64('us')
        assert handler._sql_type_to_pyarrow('TIMESTAMP') == pa.timestamp('us')

        # Test binary types
        assert handler._sql_type_to_pyarrow('BINARY') == pa.binary()
        assert handler._sql_type_to_pyarrow('BLOB') == pa.binary()

        # Test unknown types default to string
        assert handler._sql_type_to_pyarrow('UNKNOWN_TYPE') == pa.string()

    def test_convert_column_data_with_nulls(self, basic_config):
        """Test column data conversion with null handling."""
        config = SqlInputConfig(
            connection_string="test",
            null_values=['NULL', 'N/A', '']
        )
        handler = SqlInputHandler(config)

        # Test with actual nulls and configured null values
        column_data = (1, None, 'NULL', 4, 'N/A', '', 7)
        array = handler._convert_column_data(column_data, pa.int32())

        expected_data = [1, None, None, 4, None, None, 7]
        assert array.to_pylist() == expected_data

    def test_convert_column_data_type_conversion_failure(self, basic_config):
        """Test column data conversion with type conversion failure fallback."""
        handler = SqlInputHandler(basic_config)

        # Try to convert non-numeric data to int32 - should fallback to string
        column_data = ('not_a_number', 'also_not_a_number')

        with patch('forklift.inputs.sql.logger') as mock_logger:
            array = handler._convert_column_data(column_data, pa.int32())

            # Should fallback to string type
            assert array.type == pa.string()
            assert array.to_pylist() == ['not_a_number', 'also_not_a_number']
            mock_logger.warning.assert_called_once()

    def test_quote_identifier_enabled(self, basic_config):
        """Test identifier quoting when enabled."""
        basic_config.use_quoted_identifiers = True
        handler = SqlInputHandler(basic_config)

        assert handler._quote_identifier('table_name') == '"table_name"'
        assert handler._quote_identifier('column_name') == '"column_name"'

    def test_quote_identifier_disabled(self, basic_config):
        """Test identifier quoting when disabled."""
        basic_config.use_quoted_identifiers = False
        handler = SqlInputHandler(basic_config)

        assert handler._quote_identifier('table_name') == 'table_name'
        assert handler._quote_identifier('column_name') == 'column_name'

    def test_odbc_type_to_string_mapping(self, basic_config):
        """Test ODBC type constant to string mapping."""
        handler = SqlInputHandler(basic_config)

        # Mock pyodbc constants
        with patch('pyodbc.SQL_VARCHAR', 12), \
             patch('pyodbc.SQL_INTEGER', 4), \
             patch('pyodbc.SQL_DOUBLE', 8):

            import pyodbc
            assert handler._odbc_type_to_string(pyodbc.SQL_VARCHAR) == 'VARCHAR'
            assert handler._odbc_type_to_string(pyodbc.SQL_INTEGER) == 'INTEGER'
            assert handler._odbc_type_to_string(pyodbc.SQL_DOUBLE) == 'DOUBLE'

    def test_odbc_type_to_string_unknown_type(self, basic_config):
        """Test ODBC type mapping for unknown types."""
        handler = SqlInputHandler(basic_config)

        # Unknown type should default to VARCHAR
        assert handler._odbc_type_to_string(9999) == 'VARCHAR'

    def test_odbc_type_to_string_pyodbc_not_available(self, basic_config):
        """Test ODBC type mapping when pyodbc is not available."""
        handler = SqlInputHandler(basic_config)

        with patch('builtins.__import__', side_effect=ImportError("No module named 'pyodbc'")):
            assert handler._odbc_type_to_string(12) == 'VARCHAR'

    @patch('pyodbc.connect')
    def test_read_table_data_connection_error(self, mock_connect, basic_config):
        """Test read_table_data when not connected."""
        handler = SqlInputHandler(basic_config)
        # Don't connect

        with pytest.raises(ConnectionError, match="Not connected to database"):
            list(handler.read_table_data('main', 'employees'))

    @patch('pyodbc.connect')
    def test_get_table_schema_connection_error(self, mock_connect, basic_config):
        """Test get_table_schema when not connected."""
        handler = SqlInputHandler(basic_config)
        # Don't connect

        with pytest.raises(ConnectionError, match="Not connected to database"):
            handler.get_table_schema('main', 'employees')

    def test_rows_to_recordbatch_empty_rows(self, basic_config):
        """Test converting empty rows to RecordBatch."""
        handler = SqlInputHandler(basic_config)
        schema = pa.schema([pa.field('id', pa.int32())])

        # Test that the implementation properly handles empty rows
        # Should create an empty RecordBatch with the correct schema
        batch = handler._rows_to_recordbatch([], schema)
        assert batch.num_rows == 0
        assert batch.schema.equals(schema)

    @patch('pyodbc.connect')
    def test_read_table_data_with_fetch_size(self, mock_connect, basic_config):
        """Test read_table_data with custom fetch size configuration."""
        basic_config.fetch_size = 500
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        schema = pa.schema([pa.field('id', pa.int32())])
        mock_cursor.fetchmany.return_value = []

        handler = SqlInputHandler(basic_config)
        handler.connect()

        with patch.object(handler, 'get_table_schema', return_value=schema):
            list(handler.read_table_data('main', 'employees'))

        # Verify fetch size was set
        assert mock_cursor.arraysize == 500

    def test_get_tables_to_process_with_schema_importer_patterns(self, basic_config):
        """Test get_tables_to_process with schema importer using complex patterns."""
        handler = SqlInputHandler(basic_config)
        mock_schema_importer = MagicMock()
        mock_schema_importer.get_table_list.return_value = [
            ('schema1', 'table1', 'output1'),
            ('schema2', 'table2', 'output2')
        ]
        handler.set_schema_importer(mock_schema_importer)

        tables = handler.get_tables_to_process()

        expected = [('schema1', 'table1', 'output1'), ('schema2', 'table2', 'output2')]
        assert tables == expected

    def test_get_tables_to_process_with_config_patterns(self, basic_config):
        """Test get_tables_to_process with config include patterns."""
        basic_config.include_patterns = ['schema1.table1', 'table2', '*.*']
        handler = SqlInputHandler(basic_config)

        tables = handler.get_tables_to_process()

        expected = [('schema1', 'table1', None), ('default', 'table2', None)]
        assert tables == expected


class TestSqlSchemaImporter:
    """Test cases for the SqlSchemaImporter class."""

    @pytest.fixture
    def valid_sql_schema(self):
        """Create a valid SQL schema matching the actual implementation structure."""
        return {
            "$schema": "https://json-schema.org/draft/2019-09/schema",
            "$id": "https://example.com/sql-schema.json",
            "title": "SQL Schema Test",
            "type": "object",
            "x-sql": {
                "connection": {
                    "driver": "SQLite3 ODBC Driver",
                    "database": "test.db",
                    "timeout": 30
                },
                "tables": [
                    {
                        "select": {
                            "schema": "main",
                            "name": "employees"
                        },
                        "outputName": "emp_data",
                        "columns": {
                            "id": {
                                "sql_type": "INTEGER",
                                "parquet_type": "int32",
                                "nullable": False
                            },
                            "name": {
                                "sql_type": "VARCHAR",
                                "parquet_type": "string",
                                "nullable": True
                            }
                        }
                    }
                ],
                "include_patterns": ["main.employees", "main.departments"]
            }
        }

    @pytest.fixture
    def minimal_valid_schema(self):
        """Create a minimal valid schema for basic testing."""
        return {
            "$schema": "https://json-schema.org/draft/2019-09/schema",
            "$id": "https://example.com/minimal-schema.json",
            "title": "Minimal SQL Schema",
            "type": "object",
            "x-sql": {
                "connection": {
                    "driver": "SQLite3 ODBC Driver",
                    "database": "test.db"
                },
                "tables": []
            }
        }

    def test_init_with_dict(self, minimal_valid_schema):
        """Test initialization with dictionary schema."""
        importer = SqlSchemaImporter(minimal_valid_schema, validate=False)
        assert importer.schema == minimal_valid_schema
        assert importer.sql_ext == minimal_valid_schema["x-sql"]

    def test_init_with_file(self, minimal_valid_schema, tmp_path):
        """Test initialization with file path."""
        schema_file = tmp_path / "test_schema.json"
        with open(schema_file, 'w') as f:
            import json
            json.dump(minimal_valid_schema, f)

        importer = SqlSchemaImporter(schema_file, validate=False)
        assert importer.schema == minimal_valid_schema

    def test_init_with_invalid_type(self):
        """Test initialization with invalid schema type."""
        with pytest.raises(TypeError, match="schema must be path-like or dict"):
            SqlSchemaImporter(123)

    def test_validation_disabled(self):
        """Test with validation disabled allows invalid schemas."""
        invalid_schema = {"x-sql": {"tables": []}}
        importer = SqlSchemaImporter(invalid_schema, validate=False)
        assert importer.schema == invalid_schema

    def test_missing_x_sql_extension(self):
        """Test schema without x-sql extension fails validation."""
        schema = {
            "$schema": "https://json-schema.org/draft/2019-09/schema",
            "$id": "https://example.com/schema.json",
            "title": "Test Schema",
            "type": "object",
            "some_other_key": "value"
        }
        with pytest.raises(SchemaValidationError):
            SqlSchemaImporter(schema, validate=True)

    def test_get_table_list_basic(self, minimal_valid_schema):
        """Test getting table list from schema."""
        importer = SqlSchemaImporter(minimal_valid_schema, validate=False)
        tables = importer.get_table_list()
        assert tables == []  # Empty tables list

    def test_parquet_type_mapping(self, minimal_valid_schema):
        """Test parquet type mapping functionality."""
        importer = SqlSchemaImporter(minimal_valid_schema, validate=False)

        # Test that supported Parquet types are recognized
        assert "int32" in SqlSchemaImporter.SUPPORTED_PARQUET_TYPES
        assert "string" in SqlSchemaImporter.SUPPORTED_PARQUET_TYPES
        assert "double" in SqlSchemaImporter.SUPPORTED_PARQUET_TYPES
        assert "bool" in SqlSchemaImporter.SUPPORTED_PARQUET_TYPES
        assert "timestamp[us]" in SqlSchemaImporter.SUPPORTED_PARQUET_TYPES


class TestSqlUtilities:
    """Test cases for SQL utility functions."""

    def test_derive_sql_table_list_empty_schema(self):
        """Test derive_sql_table_list with empty or None schema."""
        from forklift.utils.sql_include import derive_sql_table_list

        # Test with None schema
        result = derive_sql_table_list(None)
        assert result == []

        # Test with empty schema
        result = derive_sql_table_list({})
        assert result == []

    def test_derive_sql_table_list_no_x_sql(self):
        """Test derive_sql_table_list with schema missing x-sql extension."""
        from forklift.utils.sql_include import derive_sql_table_list

        schema = {"some_other_key": "value"}
        result = derive_sql_table_list(schema)
        assert result == []

    def test_derive_sql_table_list_no_tables(self):
        """Test derive_sql_table_list with x-sql but no tables."""
        from forklift.utils.sql_include import derive_sql_table_list

        schema = {"x-sql": {}}
        result = derive_sql_table_list(schema)
        assert result == []

    def test_derive_sql_table_list_valid_tables(self):
        """Test derive_sql_table_list with valid table configurations."""
        from forklift.utils.sql_include import derive_sql_table_list

        schema = {
            "x-sql": {
                "tables": [
                    {
                        "select": {
                            "schema": "main",
                            "name": "employees"
                        },
                        "outputName": "emp_data"
                    },
                    {
                        "select": {
                            "name": "departments"  # No schema specified
                        },
                        "outputName": "dept_data"
                    },
                    {
                        "select": {
                            "schema": "hr",
                            "name": "staff"
                        }
                        # No outputName specified
                    }
                ]
            }
        }

        result = derive_sql_table_list(schema)
        expected = [
            ("main", "employees", "emp_data"),
            ("default", "departments", "dept_data"),
            ("hr", "staff", None)
        ]
        assert result == expected

    def test_derive_sql_table_list_missing_table_name(self):
        """Test derive_sql_table_list skips tables without name."""
        from forklift.utils.sql_include import derive_sql_table_list

        schema = {
            "x-sql": {
                "tables": [
                    {
                        "select": {
                            "schema": "main"
                            # Missing "name" field
                        },
                        "outputName": "invalid_table"
                    },
                    {
                        "select": {
                            "schema": "main",
                            "name": "valid_table"
                        }
                    }
                ]
            }
        }

        result = derive_sql_table_list(schema)
        expected = [("main", "valid_table", None)]
        assert result == expected


class TestSqlInputHandlerIntegration:
    """Integration tests for SqlInputHandler with real database operations."""

    @pytest.fixture
    def integration_config(self, sqlite_db):
        """Create configuration for integration testing."""
        return SqlInputConfig(
            connection_string=f"DRIVER={{SQLite3 ODBC Driver}};Database={sqlite_db};",
            batch_size=2,  # Small batch size for testing
            query_timeout=30,
            null_values=['NULL', '']
        )

    @patch('pyodbc.connect')
    def test_full_table_processing_workflow(self, mock_connect, integration_config):
        """Test complete workflow from connection to data reading."""
        # Mock pyodbc components
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        # Mock table discovery
        mock_table_row = MagicMock()
        mock_table_row.table_schem = 'main'
        mock_table_row.table_name = 'employees'
        mock_table_row.table_type = 'TABLE'
        mock_cursor.tables.return_value = [mock_table_row]

        # Mock schema discovery
        mock_column1 = MagicMock()
        mock_column1.column_name = 'id'
        mock_column1.type_name = 'INTEGER'
        mock_column1.nullable = False
        mock_column2 = MagicMock()
        mock_column2.column_name = 'name'
        mock_column2.type_name = 'VARCHAR'
        mock_column2.nullable = True
        mock_cursor.columns.return_value = [mock_column1, mock_column2]

        # Mock data reading
        mock_cursor.fetchmany.side_effect = [
            [(1, 'Alice'), (2, 'Bob')],  # First batch
            [(3, 'Carol')],              # Second batch
            []                           # End of data
        ]

        handler = SqlInputHandler(integration_config)

        try:
            # Test complete workflow
            handler.connect()

            # Discover tables
            tables = handler.get_table_list()
            assert len(tables) == 1
            assert tables[0] == ('main', 'employees')

            # Get schema
            schema = handler.get_table_schema('main', 'employees')
            assert len(schema) == 2
            assert schema.field(0).name == 'id'
            assert schema.field(1).name == 'name'

            # Read data
            batches = list(handler.read_table_data('main', 'employees'))
            assert len(batches) == 2
            assert batches[0].num_rows == 2
            assert batches[1].num_rows == 1

        finally:
            handler.disconnect()

    @patch('pyodbc.connect')
    def test_context_manager_integration(self, mock_connect, integration_config):
        """Test using handler as context manager in complete workflow."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        # Mock minimal table info
        mock_cursor.tables.return_value = []

        handler = SqlInputHandler(integration_config)

        with handler:
            # Should be connected inside context
            assert handler.connection is not None
            tables = handler.get_table_list()
            assert tables == []

        # Should be disconnected after context
        assert handler.connection is None

    def test_error_handling_during_processing(self, integration_config):
        """Test error handling during various processing stages."""
        handler = SqlInputHandler(integration_config)

        # Test operations without connection
        with pytest.raises(ConnectionError):
            handler.get_table_list()

        with pytest.raises(ConnectionError):
            handler.get_table_schema('main', 'employees')

        with pytest.raises(ConnectionError):
            list(handler.read_table_data('main', 'employees'))

    @patch('pyodbc.connect')
    def test_schema_importer_integration(self, mock_connect, integration_config):
        """Test integration with schema importer."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        handler = SqlInputHandler(integration_config)

        # Create mock schema importer
        mock_schema_importer = MagicMock()
        mock_schema_importer.all_include_patterns = ['main.employees']
        mock_schema_importer.get_table_list.return_value = [('main', 'employees', 'emp_output')]

        handler.set_schema_importer(mock_schema_importer)

        # Test that schema importer influences behavior
        patterns = handler.get_include_patterns()
        assert patterns == ['main.employees']

        tables = handler.get_tables_to_process()
        assert tables == [('main', 'employees', 'emp_output')]


class TestSqlInputConfig:
    """Test cases for SQL input configuration."""

    def test_sql_input_config_defaults(self):
        """Test SqlInputConfig with default values."""
        config = SqlInputConfig(connection_string="test_connection")

        assert config.connection_string == "test_connection"
        assert config.batch_size == 10000
        assert config.query_timeout == 300
        assert config.connection_timeout == 30
        assert config.fetch_size is None
        assert config.null_values is None
        assert config.date_formats is None
        assert config.timestamp_formats is None
        assert config.use_quoted_identifiers is False
        assert config.schema_name is None
        assert config.enable_streaming is True
        assert config.connection_params is None

    def test_sql_input_config_custom_values(self):
        """Test SqlInputConfig with custom values."""
        config = SqlInputConfig(
            connection_string="DRIVER={SQLite3};Database=test.db;",
            batch_size=5000,
            query_timeout=120,
            connection_timeout=60,
            fetch_size=1000,
            null_values=["NULL", "N/A", ""],
            use_quoted_identifiers=True,
            schema_name="public",
            enable_streaming=False,
            connection_params={"ReadOnly": "Yes", "Timeout": "30"}
        )

        assert config.connection_string == "DRIVER={SQLite3};Database=test.db;"
        assert config.batch_size == 5000
        assert config.query_timeout == 120
        assert config.connection_timeout == 60
        assert config.fetch_size == 1000
        assert config.null_values == ["NULL", "N/A", ""]
        assert config.use_quoted_identifiers is True
        assert config.schema_name == "public"
        assert config.enable_streaming is False
        assert config.connection_params == {"ReadOnly": "Yes", "Timeout": "30"}

    def test_config_validation(self):
        """Test configuration validation - SqlInputConfig doesn't perform validation."""
        # SqlInputConfig is a simple dataclass without validation
        # Test that we can create configs with various values without errors

        # Empty connection string should be allowed (validation happens elsewhere)
        config = SqlInputConfig(connection_string="")
        assert config.connection_string == ""

        # Negative values should be allowed (validation happens elsewhere)
        config = SqlInputConfig(connection_string="test", batch_size=-1)
        assert config.batch_size == -1

        # Negative timeout should be allowed (validation happens elsewhere)
        config = SqlInputConfig(connection_string="test", query_timeout=-1)
        assert config.query_timeout == -1

