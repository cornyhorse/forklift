"""Comprehensive tests for SQL input handler to achieve 100% code coverage."""

import pytest
from unittest.mock import Mock, MagicMock, patch, call
import pyarrow as pa
from forklift.inputs.sql import SqlInputHandler
from forklift.inputs.config import SqlInputConfig
from forklift.schema.sql_schema_importer import SqlSchemaImporter


class TestSqlInputHandlerComplete:
    """Complete test coverage for SqlInputHandler."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock SQL input configuration."""
        config = Mock(spec=SqlInputConfig)
        config.connection_string = "Driver={SQLite3};Database=test.db"
        config.connection_params = {"timeout": "30"}
        config.connection_timeout = 30
        config.query_timeout = 60
        config.use_quoted_identifiers = True
        config.batch_size = 1000
        config.fetch_size = 1000
        config.null_values = ["NULL", ""]
        # Removed include_patterns since glob patterns are no longer supported
        return config

    @pytest.fixture
    def sql_handler(self, mock_config):
        """Create a SQL input handler with mock configuration."""
        return SqlInputHandler(mock_config)

    def test_set_schema_importer(self, sql_handler):
        """Test setting schema importer."""
        mock_importer = Mock(spec=SqlSchemaImporter)
        sql_handler.set_schema_importer(mock_importer)
        assert sql_handler.schema_importer is mock_importer

    def test_connect_with_connection_params(self, sql_handler):
        """Test connection with additional parameters."""
        with patch('pyodbc.connect') as mock_connect:
            mock_connection = Mock()
            mock_connection.timeout = 60
            mock_connect.return_value = mock_connection

            with patch('pyodbc.pooling', new=False):
                sql_handler.connect()

            expected_conn_str = "Driver={SQLite3};Database=test.db;timeout=30"
            mock_connect.assert_called_once_with(expected_conn_str, timeout=30)
            assert sql_handler.connection.timeout == 60

    def test_connect_pyodbc_import_error(self, sql_handler):
        """Test connection when pyodbc is not available."""
        with patch('builtins.__import__', side_effect=ImportError("No module named 'pyodbc'")):
            with pytest.raises(ImportError, match="pyodbc is required for SQL database connectivity"):
                sql_handler.connect()

    def test_connect_connection_error(self, sql_handler):
        """Test connection failure."""
        with patch('pyodbc.connect', side_effect=Exception("Connection failed")):
            with pytest.raises(ConnectionError, match="Failed to connect to database"):
                sql_handler.connect()

    def test_disconnect_with_connection(self, sql_handler):
        """Test disconnecting when connected."""
        mock_connection = Mock()
        sql_handler.connection = mock_connection

        sql_handler.disconnect()

        mock_connection.close.assert_called_once()
        assert sql_handler.connection is None

    def test_disconnect_with_error(self, sql_handler):
        """Test disconnecting with error during close."""
        mock_connection = Mock()
        mock_connection.close.side_effect = Exception("Close error")
        sql_handler.connection = mock_connection

        sql_handler.disconnect()

        mock_connection.close.assert_called_once()
        assert sql_handler.connection is None

    def test_disconnect_no_connection(self, sql_handler):
        """Test disconnecting when not connected."""
        sql_handler.connection = None
        sql_handler.disconnect()  # Should not raise any exception

    def test_get_table_list_not_connected(self, sql_handler):
        """Test get_table_list when not connected."""
        with pytest.raises(ConnectionError, match="Not connected to database"):
            sql_handler.get_table_list()

    def test_get_table_list_success(self, sql_handler):
        """Test successful table list retrieval."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        # Mock table rows
        mock_row1 = Mock()
        mock_row1.table_schem = "public"
        mock_row1.table_name = "users"
        mock_row1.table_type = "TABLE"

        mock_row2 = Mock()
        mock_row2.table_schem = None
        mock_row2.table_name = "products"
        mock_row2.table_type = "VIEW"

        mock_row3 = Mock()
        mock_row3.table_schem = "admin"
        mock_row3.table_name = "logs"
        mock_row3.table_type = "SYSTEM TABLE"  # Should be excluded

        mock_cursor.tables.return_value = [mock_row1, mock_row2, mock_row3]
        sql_handler.connection = mock_connection

        tables = sql_handler.get_table_list()

        expected = [("public", "users"), ("default", "products")]
        assert tables == expected
        mock_cursor.close.assert_called_once()

    def test_get_table_list_odbc_fallback(self, sql_handler):
        """Test table list retrieval with ODBC fallback."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        # First call to tables() fails
        mock_cursor.tables.side_effect = Exception("ODBC error")

        # Fallback SQLite query works
        mock_cursor.execute.return_value = None
        mock_cursor.fetchall.return_value = [("main", "users"), ("main", "orders")]

        sql_handler.connection = mock_connection

        tables = sql_handler.get_table_list()

        expected = [("main", "users"), ("main", "orders")]
        assert tables == expected
        mock_cursor.execute.assert_called_once_with("""
                    SELECT 'main' as schema_name, name as table_name 
                    FROM sqlite_master 
                    WHERE type IN ('table', 'view')
                """)

    def test_get_table_list_all_fallbacks_fail(self, sql_handler):
        """Test table list retrieval when all methods fail."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        # Both ODBC and SQLite fallback fail
        mock_cursor.tables.side_effect = Exception("ODBC error")
        mock_cursor.execute.side_effect = Exception("SQLite error")

        sql_handler.connection = mock_connection

        tables = sql_handler.get_table_list()

        assert tables == []
        mock_cursor.close.assert_called_once()

    def test_get_specified_tables_not_connected(self, sql_handler):
        """Test get_specified_tables when not connected."""
        with pytest.raises(ConnectionError, match="Not connected to database"):
            sql_handler.get_specified_tables(["users"])

    def test_get_specified_tables_exact_match(self, sql_handler):
        """Test get_specified_tables with exact matches."""
        sql_handler.connection = Mock()

        with patch.object(sql_handler, 'get_table_list') as mock_get_tables:
            mock_get_tables.return_value = [("public", "users"), ("public", "orders")]

            result = sql_handler.get_specified_tables(["public.users", "public.orders"])

            expected = [("public", "users"), ("public", "orders")]
            assert result == expected

    def test_get_specified_tables_default_schema_fallback(self, sql_handler):
        """Test get_specified_tables with default schema fallback."""
        sql_handler.connection = Mock()

        with patch.object(sql_handler, 'get_table_list') as mock_get_tables:
            mock_get_tables.return_value = [("main", "users"), ("public", "orders")]

            result = sql_handler.get_specified_tables(["users", "orders"])

            expected = [("main", "users"), ("public", "orders")]
            assert result == expected

    def test_get_specified_tables_not_found(self, sql_handler):
        """Test get_specified_tables with tables not found."""
        sql_handler.connection = Mock()

        with patch.object(sql_handler, 'get_table_list') as mock_get_tables:
            mock_get_tables.return_value = [("public", "users")]

            result = sql_handler.get_specified_tables(["nonexistent"])

            assert result == []

    def test_parse_table_specification_with_schema(self, sql_handler):
        """Test parsing table specification with schema."""
        schema, table = sql_handler._parse_table_specification("public.users")
        assert schema == "public"
        assert table == "users"

    def test_parse_table_specification_without_schema(self, sql_handler):
        """Test parsing table specification without schema."""
        schema, table = sql_handler._parse_table_specification("users")
        assert schema == "default"
        assert table == "users"

    def test_get_table_schema_not_connected(self, sql_handler):
        """Test get_table_schema when not connected."""
        with pytest.raises(ConnectionError, match="Not connected to database"):
            sql_handler.get_table_schema("public", "users")

    def test_get_table_schema_columns_method_success(self, sql_handler):
        """Test get_table_schema using ODBC columns method."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        # Mock column info
        mock_col1 = Mock()
        mock_col1.column_name = "id"
        mock_col1.type_name = "INTEGER"
        mock_col1.column_size = None
        mock_col1.decimal_digits = None
        mock_col1.nullable = False

        mock_col2 = Mock()
        mock_col2.column_name = "name"
        mock_col2.type_name = "VARCHAR"
        mock_col2.column_size = 255
        mock_col2.decimal_digits = None
        mock_col2.nullable = True

        mock_cursor.columns.return_value = [mock_col1, mock_col2]
        sql_handler.connection = mock_connection

        with patch.object(sql_handler, '_quote_identifier', side_effect=lambda x: f'"{x}"'):
            with patch.object(sql_handler, '_sql_type_to_pyarrow') as mock_convert:
                mock_convert.side_effect = [pa.int32(), pa.string()]

                schema = sql_handler.get_table_schema("public", "users")

                assert len(schema) == 2
                assert schema.field(0).name == "id"
                assert schema.field(0).nullable == False
                assert schema.field(1).name == "name"
                assert schema.field(1).nullable == True

    def test_get_table_schema_fallback_method(self, sql_handler):
        """Test get_table_schema using fallback SELECT method."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        # columns() method fails
        mock_cursor.columns.side_effect = Exception("ODBC error")

        # Fallback SELECT method
        mock_cursor.description = [
            ("id", 4, None, None, None, None, None),  # INTEGER type
            ("name", 12, 255, None, None, 0, None)    # VARCHAR type
        ]

        sql_handler.connection = mock_connection

        with patch.object(sql_handler, '_quote_identifier', side_effect=lambda x: f'"{x}"'):
            with patch.object(sql_handler, '_odbc_type_to_string', side_effect=["INTEGER", "VARCHAR"]):
                with patch.object(sql_handler, '_sql_type_to_pyarrow', side_effect=[pa.int32(), pa.string()]):
                    schema = sql_handler.get_table_schema("public", "users")

                    assert len(schema) == 2
                    mock_cursor.execute.assert_called_once_with('SELECT * FROM "public"."users" LIMIT 1')

    def test_get_table_schema_default_schema(self, sql_handler):
        """Test get_table_schema with default schema."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        mock_cursor.columns.side_effect = Exception("ODBC error")
        mock_cursor.description = [("id", 4, None, None, None, None, None)]

        sql_handler.connection = mock_connection

        with patch.object(sql_handler, '_quote_identifier', side_effect=lambda x: f'"{x}"'):
            with patch.object(sql_handler, '_odbc_type_to_string', return_value="INTEGER"):
                with patch.object(sql_handler, '_sql_type_to_pyarrow', return_value=pa.int32()):
                    sql_handler.get_table_schema("default", "users")

                    mock_cursor.execute.assert_called_once_with('SELECT * FROM "users" LIMIT 1')

    def test_get_table_schema_all_methods_fail(self, sql_handler):
        """Test get_table_schema when all methods fail."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        mock_cursor.columns.side_effect = Exception("ODBC error")
        mock_cursor.execute.side_effect = Exception("SELECT error")

        sql_handler.connection = mock_connection

        with pytest.raises(RuntimeError, match="Could not determine schema"):
            sql_handler.get_table_schema("public", "users")

    def test_quote_identifier_enabled(self, sql_handler):
        """Test identifier quoting when enabled."""
        sql_handler.config.use_quoted_identifiers = True
        result = sql_handler._quote_identifier("table_name")
        assert result == '"table_name"'

    def test_quote_identifier_disabled(self, sql_handler):
        """Test identifier quoting when disabled."""
        sql_handler.config.use_quoted_identifiers = False
        result = sql_handler._quote_identifier("table_name")
        assert result == "table_name"

    def test_odbc_type_to_string_with_pyodbc(self, sql_handler):
        """Test ODBC type conversion with pyodbc available."""
        with patch('builtins.__import__') as mock_import:
            mock_pyodbc = Mock()
            mock_pyodbc.SQL_VARCHAR = 12
            mock_pyodbc.SQL_INTEGER = 4
            mock_pyodbc.SQL_UNKNOWN = 999
            mock_import.return_value = mock_pyodbc

            assert sql_handler._odbc_type_to_string(12) == "VARCHAR"
            assert sql_handler._odbc_type_to_string(4) == "INTEGER"
            assert sql_handler._odbc_type_to_string(999) == "VARCHAR"  # Unknown type

    def test_odbc_type_to_string_without_pyodbc(self, sql_handler):
        """Test ODBC type conversion without pyodbc available."""
        with patch('builtins.__import__', side_effect=ImportError("No module named 'pyodbc'")):
            result = sql_handler._odbc_type_to_string(12)
            assert result == "VARCHAR"

    def test_sql_type_to_pyarrow_integer_types(self, sql_handler):
        """Test SQL to PyArrow type conversion for integer types."""
        assert sql_handler._sql_type_to_pyarrow("INT") == pa.int32()
        assert sql_handler._sql_type_to_pyarrow("INTEGER") == pa.int32()
        assert sql_handler._sql_type_to_pyarrow("BIGINT") == pa.int64()
        assert sql_handler._sql_type_to_pyarrow("SMALLINT") == pa.int16()
        assert sql_handler._sql_type_to_pyarrow("TINYINT") == pa.int8()

    def test_sql_type_to_pyarrow_float_types(self, sql_handler):
        """Test SQL to PyArrow type conversion for float types."""
        assert sql_handler._sql_type_to_pyarrow("FLOAT") == pa.float32()
        assert sql_handler._sql_type_to_pyarrow("DOUBLE") == pa.float64()
        assert sql_handler._sql_type_to_pyarrow("REAL") == pa.float32()

    def test_sql_type_to_pyarrow_decimal_with_precision(self, sql_handler):
        """Test SQL to PyArrow type conversion for decimal with precision."""
        result = sql_handler._sql_type_to_pyarrow("DECIMAL", size=10, decimal_digits=2)
        assert isinstance(result, pa.Decimal128Type)
        assert result.precision == 10
        assert result.scale == 2

    def test_sql_type_to_pyarrow_decimal_without_precision(self, sql_handler):
        """Test SQL to PyArrow type conversion for decimal without precision."""
        result = sql_handler._sql_type_to_pyarrow("DECIMAL")
        assert result == pa.float64()

    def test_sql_type_to_pyarrow_boolean_types(self, sql_handler):
        """Test SQL to PyArrow type conversion for boolean types."""
        assert sql_handler._sql_type_to_pyarrow("BOOLEAN") == pa.bool_()
        assert sql_handler._sql_type_to_pyarrow("BOOL") == pa.bool_()
        assert sql_handler._sql_type_to_pyarrow("BIT") == pa.bool_()

    def test_sql_type_to_pyarrow_date_time_types(self, sql_handler):
        """Test SQL to PyArrow type conversion for date/time types."""
        assert sql_handler._sql_type_to_pyarrow("DATE") == pa.date32()
        assert sql_handler._sql_type_to_pyarrow("TIME") == pa.time64('us')
        assert sql_handler._sql_type_to_pyarrow("TIMESTAMP") == pa.timestamp('us')
        assert sql_handler._sql_type_to_pyarrow("TIMESTAMPTZ") == pa.timestamp('us', tz='UTC')

    def test_sql_type_to_pyarrow_binary_types(self, sql_handler):
        """Test SQL to PyArrow type conversion for binary types."""
        assert sql_handler._sql_type_to_pyarrow("BINARY") == pa.binary()
        assert sql_handler._sql_type_to_pyarrow("VARBINARY") == pa.binary()
        assert sql_handler._sql_type_to_pyarrow("BLOB") == pa.binary()

    def test_sql_type_to_pyarrow_string_default(self, sql_handler):
        """Test SQL to PyArrow type conversion defaults to string."""
        assert sql_handler._sql_type_to_pyarrow("VARCHAR") == pa.string()
        assert sql_handler._sql_type_to_pyarrow("UNKNOWN_TYPE") == pa.string()

    def test_read_table_data_not_connected(self, sql_handler):
        """Test read_table_data when not connected."""
        with pytest.raises(ConnectionError, match="Not connected to database"):
            list(sql_handler.read_table_data("public", "users"))

    def test_read_table_data_success(self, sql_handler):
        """Test successful table data reading."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        # Mock data rows
        mock_cursor.fetchmany.side_effect = [
            [(1, "Alice"), (2, "Bob")],  # First batch
            [(3, "Charlie")],             # Second batch
            []                            # End of data
        ]

        # Mock schema
        mock_schema = pa.schema([
            pa.field("id", pa.int32()),
            pa.field("name", pa.string())
        ])

        sql_handler.connection = mock_connection

        # Mock the data reader's read_table_data method directly to avoid the schema retrieval issue
        with patch.object(sql_handler.data_reader, 'read_table_data') as mock_read_data:
            mock_batch_1 = pa.record_batch([[1, 2], ["Alice", "Bob"]], schema=mock_schema)
            mock_batch_2 = pa.record_batch([[3], ["Charlie"]], schema=mock_schema)
            mock_read_data.return_value = iter([mock_batch_1, mock_batch_2])

            batches = list(sql_handler.read_table_data("public", "users"))

            assert len(batches) == 2
            assert batches[0].num_rows == 2
            assert batches[1].num_rows == 1
            mock_read_data.assert_called_once_with("public", "users")

    def test_rows_to_recordbatch_empty(self, sql_handler):
        """Test converting empty rows to RecordBatch."""
        schema = pa.schema([pa.field("id", pa.int32())])

        # For empty rows, the method directly returns pa.record_batch([], schema)
        # No need to mock _convert_column_data as it's not called for empty rows
        batch = sql_handler._rows_to_recordbatch([], schema)
        assert batch.num_rows == 0
        assert batch.schema.equals(schema)

    def test_rows_to_recordbatch_with_data(self, sql_handler):
        """Test converting rows to RecordBatch with data."""
        schema = pa.schema([
            pa.field("id", pa.int32()),
            pa.field("name", pa.string())
        ])
        rows = [(1, "Alice"), (2, "Bob")]

        # Mock the data reader's _rows_to_recordbatch method since that's where the logic moved
        with patch.object(sql_handler.data_reader, '_rows_to_recordbatch') as mock_rows_to_batch:
            expected_batch = pa.record_batch([[1, 2], ["Alice", "Bob"]], schema=schema)
            mock_rows_to_batch.return_value = expected_batch

            batch = sql_handler._rows_to_recordbatch(rows, schema)

            assert batch.num_rows == 2
            # The method should be called once with the rows and schema
            mock_rows_to_batch.assert_called_once_with(rows, schema)

    def test_convert_column_data_with_nulls(self, sql_handler):
        """Test converting column data with null values."""
        sql_handler.config.null_values = ["NULL", ""]
        column_data = (1, None, "NULL", "", 5)

        result = sql_handler._convert_column_data(column_data, pa.int32())

        # Check that nulls are properly handled
        assert result.to_pylist() == [1, None, None, None, 5]

    def test_convert_column_data_type_conversion_error(self, sql_handler):
        """Test converting column data with type conversion error."""
        column_data = ("invalid", "data")

        # This should fallback to string type
        result = sql_handler._convert_column_data(column_data, pa.int32())

        assert result.to_pylist() == ["invalid", "data"]

    def test_get_tables_to_process_with_schema_importer(self, sql_handler):
        """Test getting tables to process from schema importer."""
        mock_importer = Mock()
        mock_importer.get_table_list.return_value = [
            ("public", "users", "users_output"),
            ("public", "orders", None)
        ]
        sql_handler.schema_importer = mock_importer

        tables = sql_handler.get_tables_to_process()
        assert tables == [("public", "users", "users_output"), ("public", "orders", None)]

    def test_connect_without_connection_params(self, sql_handler):
        """Test connection without additional parameters."""
        sql_handler.config.connection_params = None

        with patch('pyodbc.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection

            sql_handler.connect()

            # Should use connection string as-is without additional params
            mock_connect.assert_called_once_with(
                "Driver={SQLite3};Database=test.db",
                timeout=30
            )
