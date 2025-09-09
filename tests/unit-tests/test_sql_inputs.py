"""Tests for SQL input handler with explicit table specification."""

import pytest
from unittest.mock import Mock, MagicMock, patch
import pyarrow as pa

from forklift.inputs.sql import SqlInputHandler
from forklift.inputs.config import SqlInputConfig


class TestSqlInputHandler:
    """Test cases for SqlInputHandler with explicit table specification."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock SQL input configuration."""
        config = Mock(spec=SqlInputConfig)
        config.connection_string = "Driver={SQLite3};Database=test.db"
        config.connection_params = {}
        config.connection_timeout = 30
        config.query_timeout = 60
        config.use_quoted_identifiers = False
        config.batch_size = 1000
        config.fetch_size = 1000
        config.null_values = None
        config.include_patterns = None
        return config

    @pytest.fixture
    def sql_handler(self, mock_config):
        """Create a SQL input handler with mock configuration."""
        return SqlInputHandler(mock_config)

    def test_init(self, mock_config):
        """Test SQL handler initialization."""
        handler = SqlInputHandler(mock_config)
        assert handler.config == mock_config
        assert handler.connection is None
        assert handler.schema_importer is None

    def test_connect_success(self, sql_handler):
        """Test successful database connection."""
        with patch('pyodbc.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection

            sql_handler.connect()

            assert sql_handler.connection == mock_connection
            mock_connect.assert_called_once()

    def test_connect_import_error(self, sql_handler):
        """Test connection failure when pyodbc is not available."""
        with patch.dict('sys.modules', {'pyodbc': None}):
            with pytest.raises(ImportError, match="pyodbc is required"):
                sql_handler.connect()

    def test_parse_table_specification(self, sql_handler):
        """Test parsing of table specifications."""
        # Test fully qualified table name
        schema, table = sql_handler._parse_table_specification("sales.customers")
        assert schema == "sales"
        assert table == "customers"

        # Test table name only (should use default schema)
        schema, table = sql_handler._parse_table_specification("products")
        assert schema == "default"
        assert table == "products"

        # Test with whitespace
        schema, table = sql_handler._parse_table_specification(" inventory.items ")
        assert schema == "inventory"
        assert table == "items"

    def test_get_table_list(self, sql_handler):
        """Test getting list of available tables."""
        # Setup mock connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        # Mock table rows
        mock_table_row_1 = Mock()
        mock_table_row_1.table_schem = "public"
        mock_table_row_1.table_name = "users"
        mock_table_row_1.table_type = "TABLE"

        mock_table_row_2 = Mock()
        mock_table_row_2.table_schem = "sales"
        mock_table_row_2.table_name = "orders"
        mock_table_row_2.table_type = "VIEW"

        mock_cursor.tables.return_value = [mock_table_row_1, mock_table_row_2]

        sql_handler.connection = mock_connection

        tables = sql_handler.get_table_list()

        expected_tables = [("public", "users"), ("sales", "orders")]
        assert tables == expected_tables

    def test_get_table_list_sqlite_fallback(self, sql_handler):
        """Test fallback method for SQLite when ODBC tables() fails."""
        # Setup mock connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        # Make tables() method raise an exception
        mock_cursor.tables.side_effect = Exception("tables() not supported")

        # Setup fallback SQLite query result
        mock_cursor.fetchall.return_value = [("main", "users"), ("main", "products")]

        sql_handler.connection = mock_connection

        tables = sql_handler.get_table_list()

        expected_tables = [("main", "users"), ("main", "products")]
        assert tables == expected_tables

    def test_get_specified_tables(self, sql_handler):
        """Test getting tables based on explicit specifications."""
        # Mock get_table_list to return available tables
        available_tables = [
            ("public", "users"),
            ("sales", "orders"),
            ("inventory", "products"),
            ("default", "logs")
        ]

        with patch.object(sql_handler, 'get_table_list', return_value=available_tables):
            with patch.object(sql_handler, 'connection', Mock()):
                # Test exact matches
                specs = ["public.users", "sales.orders"]
                result = sql_handler.get_specified_tables(specs)
                expected = [("public", "users"), ("sales", "orders")]
                assert result == expected

                # Test table name only (should find in default schema)
                specs = ["logs"]
                result = sql_handler.get_specified_tables(specs)
                expected = [("default", "logs")]
                assert result == expected

                # Test table name only that exists in multiple schemas (should pick first match)
                specs = ["users"]
                result = sql_handler.get_specified_tables(specs)
                expected = [("public", "users")]  # First match
                assert result == expected

    def test_get_specified_tables_not_found(self, sql_handler):
        """Test handling of table specifications that don't exist."""
        # Mock get_table_list to return available tables
        available_tables = [("public", "users"), ("sales", "orders")]

        with patch.object(sql_handler, 'get_table_list', return_value=available_tables):
            with patch.object(sql_handler, 'connection', Mock()):
                # Test table that doesn't exist
                specs = ["nonexistent.table", "public.users"]
                result = sql_handler.get_specified_tables(specs)
                expected = [("public", "users")]  # Only the valid table
                assert result == expected

    def test_sql_type_to_pyarrow_mapping(self, sql_handler):
        """Test SQL type to PyArrow type conversion."""
        # Test integer types
        assert sql_handler._sql_type_to_pyarrow("INTEGER") == pa.int32()
        assert sql_handler._sql_type_to_pyarrow("BIGINT") == pa.int64()
        assert sql_handler._sql_type_to_pyarrow("SMALLINT") == pa.int16()

        # Test float types
        assert sql_handler._sql_type_to_pyarrow("FLOAT") == pa.float32()
        assert sql_handler._sql_type_to_pyarrow("DOUBLE") == pa.float64()

        # Test decimal with precision
        decimal_type = sql_handler._sql_type_to_pyarrow("DECIMAL", size=10, decimal_digits=2)
        assert isinstance(decimal_type, pa.Decimal128Type)
        assert decimal_type.precision == 10
        assert decimal_type.scale == 2

        # Test boolean
        assert sql_handler._sql_type_to_pyarrow("BOOLEAN") == pa.bool_()

        # Test date/time types
        assert sql_handler._sql_type_to_pyarrow("DATE") == pa.date32()
        assert sql_handler._sql_type_to_pyarrow("TIME") == pa.time64('us')
        assert sql_handler._sql_type_to_pyarrow("TIMESTAMP") == pa.timestamp('us')

        # Test default to string for unknown types
        assert sql_handler._sql_type_to_pyarrow("UNKNOWN_TYPE") == pa.string()

    def test_quote_identifier(self, sql_handler):
        """Test identifier quoting functionality."""
        # Test without quoting enabled
        sql_handler.config.use_quoted_identifiers = False
        assert sql_handler._quote_identifier("table_name") == "table_name"

        # Test with quoting enabled
        sql_handler.config.use_quoted_identifiers = True
        assert sql_handler._quote_identifier("table_name") == '"table_name"'

    def test_get_tables_to_process_with_schema_importer(self, sql_handler):
        """Test getting tables to process when schema importer is available."""
        # Mock schema importer
        mock_schema_importer = Mock()
        mock_schema_importer.get_table_list.return_value = [
            ("sales", "customers", "customers_output"),
            ("inventory", "products", "products_output")
        ]

        sql_handler.schema_importer = mock_schema_importer

        result = sql_handler.get_tables_to_process()
        expected = [
            ("sales", "customers", "customers_output"),
            ("inventory", "products", "products_output")
        ]
        assert result == expected

    def test_get_tables_to_process_discover_all(self, sql_handler):
        """Test discovering all tables when no specific configuration is provided."""
        # Mock get_table_list
        mock_tables = [("public", "users"), ("sales", "orders")]

        # Mock the connection and schema manager's get_table_list method to prevent connection errors
        sql_handler.connection = Mock()

        with patch.object(sql_handler.schema_manager, 'get_table_list', return_value=mock_tables):
            result = sql_handler.get_tables_to_process()
            expected = [("public", "users", None), ("sales", "orders", None)]
            assert result == expected

    def test_context_manager(self, sql_handler):
        """Test context manager functionality."""
        with patch.object(sql_handler, 'connect') as mock_connect:
            with patch.object(sql_handler, 'disconnect') as mock_disconnect:
                with sql_handler:
                    pass

                mock_connect.assert_called_once()
                mock_disconnect.assert_called_once()

    def test_convert_column_data(self, sql_handler):
        """Test conversion of column data to PyArrow arrays."""
        # Test successful conversion
        column_data = (1, 2, 3, None)
        pa_type = pa.int32()

        result = sql_handler._convert_column_data(column_data, pa_type)

        assert isinstance(result, pa.Array)
        assert result.type == pa_type
        assert result.to_pylist() == [1, 2, 3, None]

    def test_convert_column_data_with_null_values(self, sql_handler):
        """Test conversion with custom null values."""
        sql_handler.config.null_values = ["NULL", "N/A"]

        column_data = ("value1", "NULL", "N/A", None, "value2")
        pa_type = pa.string()

        result = sql_handler._convert_column_data(column_data, pa_type)

        expected = ["value1", None, None, None, "value2"]
        assert result.to_pylist() == expected

    def test_convert_column_data_fallback_to_string(self, sql_handler):
        """Test fallback to string type when conversion fails."""
        # Use incompatible data that will fail conversion
        column_data = ("not_a_number", "also_not_a_number")
        pa_type = pa.int32()

        with patch('forklift.inputs.sql.types.logger') as mock_logger:
            result = sql_handler._convert_column_data(column_data, pa_type)

            # Should fallback to string representation
            assert result.type == pa.string()
            assert result.to_pylist() == ["not_a_number", "also_not_a_number"]
            mock_logger.warning.assert_called_once()


class TestSqlInputHandlerIntegration:
    """Integration tests for SQL input handler."""

    def test_no_globbing_functionality(self):
        """Test that globbing functionality has been completely removed."""
        # Verify that globbing-related methods don't exist
        handler = SqlInputHandler(Mock())

        assert not hasattr(handler, 'filter_tables')
        assert not hasattr(handler, '_glob_to_regex')

        # Verify import statements don't include regex
        import forklift.inputs.sql as sql_module
        import inspect

        source = inspect.getsource(sql_module)
        assert 'import re' not in source
        assert 'from re import' not in source


if __name__ == "__main__":
    pytest.main([__file__])
