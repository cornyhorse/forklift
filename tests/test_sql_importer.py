"""Tests for SQL importer functionality."""

import pytest
import tempfile
import json
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, call

import pyarrow as pa

from forklift.engine.importers.sql_importer import SqlImporter
from forklift.engine.exceptions import ProcessingError


class TestSqlImporter:
    """Test cases for SqlImporter class."""

    @pytest.fixture
    def sample_schema_file(self):
        """Create a sample SQL schema file for testing."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
            schema_content = {
                "tables": [
                    {
                        "schema": "public",
                        "name": "users",
                        "outputName": "users_table",
                        "select": ["id", "name", "email"],
                        "where": "active = 1"
                    },
                    {
                        "schema": "public",
                        "name": "orders",
                        "outputName": "orders_table"
                    }
                ],
                "connectionTimeout": 30,
                "queryTimeout": 300,
                "batchSize": 5000
            }
            json.dump(schema_content, tmp_file)

            yield Path(tmp_file.name)
            Path(tmp_file.name).unlink()

    @pytest.fixture
    def output_directory(self):
        """Create a temporary output directory."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            yield Path(tmp_dir)

    def test_import_sql_basic_functionality(self, sample_schema_file, output_directory):
        """Test basic SQL import functionality."""
        connection_string = "driver={ODBC Driver 17 for SQL Server};server=localhost;database=test"

        with patch('forklift.schema.sql_schema_importer.SqlSchemaImporter') as mock_schema_importer_class:
            with patch('forklift.inputs.sql.SqlInputHandler') as mock_handler_class:
                with patch('forklift.io.create_parquet_writer') as mock_writer:
                    # Mock schema importer
                    mock_schema_importer = Mock()
                    mock_schema_importer_class.return_value = mock_schema_importer
                    mock_schema_importer.get_table_list.return_value = [
                        ('public', 'users', 'users_table'),
                        ('public', 'orders', 'orders_table')
                    ]

                    # Mock SQL handler with context manager support
                    mock_handler = Mock()
                    mock_handler_class.return_value = mock_handler
                    mock_handler.__enter__ = Mock(return_value=mock_handler)
                    mock_handler.__exit__ = Mock(return_value=None)

                    # Create real PyArrow schema and batches
                    schema = pa.schema([
                        ('id', pa.int64()),
                        ('name', pa.string()),
                        ('email', pa.string())
                    ])

                    # Create batches for each table
                    batch1 = pa.record_batch([
                        [1, 2, 3],
                        ['Alice', 'Bob', 'Charlie'],
                        ['alice@test.com', 'bob@test.com', 'charlie@test.com']
                    ], schema=schema)

                    batch2 = pa.record_batch([
                        [4, 5, 6],
                        ['David', 'Eve', 'Frank'],
                        ['david@test.com', 'eve@test.com', 'frank@test.com']
                    ], schema=schema)

                    mock_handler.get_table_schema.return_value = schema
                    # Mock read_table_data to return different batches for each table
                    mock_handler.read_table_data.side_effect = [
                        [batch1],  # First table: users
                        [batch2]   # Second table: orders
                    ]

                    # Mock parquet writer
                    mock_writer_instance = Mock()
                    mock_writer.return_value = mock_writer_instance

                    result = SqlImporter.import_sql(
                        connection_string=connection_string,
                        output_path=output_directory,
                        schema_file=sample_schema_file
                    )

                    # Verify results
                    assert result.total_rows == 6  # 3 rows × 2 tables
                    assert result.valid_rows == 6
                    assert result.invalid_rows == 0
                    assert len(result.output_files) == 2
                    assert result.execution_time > 0

                    # Verify handler was called for each table
                    assert mock_handler.read_table_data.call_count == 2
                    assert mock_handler.get_table_schema.call_count == 2

    def test_import_sql_no_schema_file(self, output_directory):
        """Test SQL import without schema file raises error."""
        connection_string = "driver={ODBC Driver 17 for SQL Server};server=localhost;database=test"

        with pytest.raises(ProcessingError, match="Schema file is required"):
            SqlImporter.import_sql(
                connection_string=connection_string,
                output_path=output_directory
            )

    def test_import_sql_invalid_schema(self, output_directory):
        """Test SQL import with invalid schema file."""
        connection_string = "driver={ODBC Driver 17 for SQL Server};server=localhost;database=test"

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
            tmp_file.write('invalid json content')
            invalid_schema_path = Path(tmp_file.name)

        try:
            with patch('forklift.schema.sql_schema_importer.SqlSchemaImporter') as mock_schema_importer_class:
                mock_schema_importer_class.side_effect = Exception("Invalid schema format")

                with pytest.raises(ProcessingError, match="Schema validation failed"):
                    SqlImporter.import_sql(
                        connection_string=connection_string,
                        output_path=output_directory,
                        schema_file=invalid_schema_path
                    )
        finally:
            invalid_schema_path.unlink()

    def test_import_sql_empty_table_list(self, output_directory):
        """Test SQL import with schema that has no tables."""
        connection_string = "driver={ODBC Driver 17 for SQL Server};server=localhost;database=test"

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
            schema_content = {"tables": []}
            json.dump(schema_content, tmp_file)
            empty_schema_path = Path(tmp_file.name)

        try:
            with patch('forklift.schema.sql_schema_importer.SqlSchemaImporter') as mock_schema_importer_class:
                mock_schema_importer = Mock()
                mock_schema_importer_class.return_value = mock_schema_importer
                mock_schema_importer.get_table_list.return_value = []

                with pytest.raises(ValueError, match="Schema file must specify at least one table"):
                    SqlImporter.import_sql(
                        connection_string=connection_string,
                        output_path=output_directory,
                        schema_file=empty_schema_path
                    )
        finally:
            empty_schema_path.unlink()

    def test_import_sql_with_custom_kwargs(self, sample_schema_file, output_directory):
        """Test SQL import with custom configuration parameters."""
        connection_string = "driver={ODBC Driver 17 for SQL Server};server=localhost;database=test"

        with patch('forklift.schema.sql_schema_importer.SqlSchemaImporter') as mock_schema_importer_class:
            with patch('forklift.inputs.sql.SqlInputHandler') as mock_handler_class:
                with patch('forklift.inputs.config.SqlInputConfig') as mock_config_class:
                    with patch('forklift.io.create_parquet_writer'):
                        # Mock schema importer
                        mock_schema_importer = Mock()
                        mock_schema_importer_class.return_value = mock_schema_importer
                        mock_schema_importer.get_table_list.return_value = [
                            ('public', 'users', 'users_table')
                        ]

                        # Mock SQL handler with context manager support
                        mock_handler = Mock()
                        mock_handler_class.return_value = mock_handler
                        mock_handler.__enter__ = Mock(return_value=mock_handler)
                        mock_handler.__exit__ = Mock(return_value=None)

                        # Create real PyArrow schema and batch
                        schema = pa.schema([('id', pa.int64()), ('name', pa.string())])
                        batch = pa.record_batch([[1], ['Alice']], schema=schema)

                        mock_handler.get_table_schema.return_value = schema
                        mock_handler.read_table_data.return_value = [batch]

                        # Create a proper mock config that can be serialized
                        mock_config = Mock()
                        mock_config_class.return_value = mock_config

                        # Mock the config attributes as regular values, not Mocks
                        mock_config.batch_size = 1000
                        mock_config.query_timeout = 600
                        mock_config.connection_timeout = 60
                        mock_config.use_quoted_identifiers = True
                        mock_config.schema_name = 'custom_schema'
                        mock_config.enable_streaming = False
                        mock_config.null_values = ['NULL', '']
                        mock_config.connection_string = connection_string

                        SqlImporter.import_sql(
                            connection_string=connection_string,
                            output_path=output_directory,
                            schema_file=sample_schema_file,
                            batch_size=1000,
                            query_timeout=600,
                            connection_timeout=60,
                            use_quoted_identifiers=True,
                            schema_name='custom_schema',
                            enable_streaming=False,
                            null_values=['NULL', '']
                        )

                        # Verify config was created with custom parameters
                        config_call = mock_config_class.call_args[1]
                        assert config_call['connection_string'] == connection_string
                        assert config_call['batch_size'] == 1000
                        assert config_call['query_timeout'] == 600
                        assert config_call['connection_timeout'] == 60
                        assert config_call['use_quoted_identifiers'] == True
                        assert config_call['schema_name'] == 'custom_schema'
                        assert config_call['enable_streaming'] == False
                        assert config_call['null_values'] == ['NULL', '']

    def test_import_sql_table_processing_error(self, sample_schema_file, output_directory):
        """Test SQL import with table processing error."""
        connection_string = "driver={ODBC Driver 17 for SQL Server};server=localhost;database=test"

        with patch('forklift.schema.sql_schema_importer.SqlSchemaImporter') as mock_schema_importer_class:
            with patch('forklift.inputs.sql.SqlInputHandler') as mock_handler_class:
                with patch('forklift.io.create_parquet_writer') as mock_writer:
                    # Mock schema importer
                    mock_schema_importer = Mock()
                    mock_schema_importer_class.return_value = mock_schema_importer
                    mock_schema_importer.get_table_list.return_value = [
                        ('public', 'users', 'users_table'),
                        ('public', 'orders', 'orders_table')
                    ]

                    # Mock SQL handler with context manager support
                    mock_handler = Mock()
                    mock_handler_class.return_value = mock_handler
                    mock_handler.__enter__ = Mock(return_value=mock_handler)
                    mock_handler.__exit__ = Mock(return_value=None)

                    # Create real PyArrow schema and batch for successful table
                    schema = pa.schema([('id', pa.int64()), ('name', pa.string())])
                    batch = pa.record_batch([[1, 2], ['Alice', 'Bob']], schema=schema)

                    # First table succeeds, second table fails
                    mock_handler.get_table_schema.side_effect = [
                        schema,  # First table succeeds
                        Exception("Table processing failed")  # Second table fails
                    ]
                    mock_handler.read_table_data.return_value = [batch]

                    # Mock parquet writer
                    mock_writer_instance = Mock()
                    mock_writer.return_value = mock_writer_instance

                    # The SQL importer should handle the error gracefully and continue
                    result = SqlImporter.import_sql(
                        connection_string=connection_string,
                        output_path=output_directory,
                        schema_file=sample_schema_file
                    )

                    # Verify error handling - first table processed, second failed
                    assert result.total_rows == 2  # Only successful table
                    assert result.valid_rows == 2
                    assert result.invalid_rows == 1  # One table failed
                    assert len(result.output_files) == 1  # Only one file created

    def test_import_sql_connection_error(self, sample_schema_file, output_directory):
        """Test SQL import with database connection error."""
        connection_string = "driver={ODBC Driver 17 for SQL Server};server=localhost;database=test"

        with patch('forklift.schema.sql_schema_importer.SqlSchemaImporter') as mock_schema_importer_class:
            with patch('forklift.inputs.sql.SqlInputHandler') as mock_handler_class:
                # Mock schema importer
                mock_schema_importer = Mock()
                mock_schema_importer_class.return_value = mock_schema_importer
                mock_schema_importer.get_table_list.return_value = [
                    ('public', 'users', 'users_table')
                ]

                # Mock SQL handler to raise connection error on enter
                mock_handler = Mock()
                mock_handler_class.return_value = mock_handler
                mock_handler.__enter__ = Mock(side_effect=Exception("Connection failed"))
                mock_handler.__exit__ = Mock(return_value=None)

                with pytest.raises(Exception, match="Connection failed"):
                    SqlImporter.import_sql(
                        connection_string=connection_string,
                        output_path=output_directory,
                        schema_file=sample_schema_file
                    )

    def test_import_sql_output_filename_generation(self, output_directory):
        """Test different output filename generation scenarios."""
        connection_string = "driver={ODBC Driver 17 for SQL Server};server=localhost;database=test"

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
            schema_content = {
                "tables": [
                    {"schema": "public", "name": "users", "outputName": "custom_users"},
                    {"schema": "sales", "name": "orders", "outputName": None},
                    {"schema": "default", "name": "logs", "outputName": None},
                    {"schema": None, "name": "system", "outputName": None}
                ]
            }
            json.dump(schema_content, tmp_file)
            schema_path = Path(tmp_file.name)

        try:
            with patch('forklift.schema.sql_schema_importer.SqlSchemaImporter') as mock_schema_importer_class:
                with patch('forklift.inputs.sql.SqlInputHandler') as mock_handler_class:
                    with patch('forklift.io.create_parquet_writer') as mock_writer:
                        # Mock schema importer
                        mock_schema_importer = Mock()
                        mock_schema_importer_class.return_value = mock_schema_importer
                        mock_schema_importer.get_table_list.return_value = [
                            ('public', 'users', 'custom_users'),
                            ('sales', 'orders', None),
                            ('default', 'logs', None),
                            (None, 'system', None)
                        ]

                        # Mock SQL handler with context manager support
                        mock_handler = Mock()
                        mock_handler_class.return_value = mock_handler
                        mock_handler.__enter__ = Mock(return_value=mock_handler)
                        mock_handler.__exit__ = Mock(return_value=None)

                        # Create real PyArrow schema and batch
                        schema = pa.schema([('id', pa.int64())])
                        batch = pa.record_batch([[1]], schema=schema)

                        mock_handler.get_table_schema.return_value = schema
                        mock_handler.read_table_data.return_value = [batch]

                        # Mock parquet writer
                        mock_writer_instance = Mock()
                        mock_writer.return_value = mock_writer_instance

                        result = SqlImporter.import_sql(
                            connection_string=connection_string,
                            output_path=output_directory,
                            schema_file=schema_path
                        )

                        # Verify correct output filenames were generated
                        expected_files = [
                            str(output_directory / "custom_users.parquet"),
                            str(output_directory / "sales_orders.parquet"),
                            str(output_directory / "logs.parquet"),
                            str(output_directory / "system.parquet")
                        ]

                        assert len(result.output_files) == 4
                        for expected_file in expected_files:
                            assert expected_file in result.output_files
        finally:
            schema_path.unlink()

    def test_import_sql_with_table_errors(self, sample_schema_file, output_directory):
        """Test SQL import handling tables with validation errors."""
        connection_string = "driver={ODBC Driver 17 for SQL Server};server=localhost;database=test"

        with patch('forklift.schema.sql_schema_importer.SqlSchemaImporter') as mock_schema_importer_class:
            with patch('forklift.inputs.sql.SqlInputHandler') as mock_handler_class:
                with patch('forklift.io.create_parquet_writer') as mock_writer:
                    # Mock schema importer
                    mock_schema_importer = Mock()
                    mock_schema_importer_class.return_value = mock_schema_importer
                    mock_schema_importer.get_table_list.return_value = [
                        ('public', 'users', 'users_table'),
                        ('public', 'bad_table', 'bad_table')
                    ]

                    # Mock SQL handler with context manager support
                    mock_handler = Mock()
                    mock_handler_class.return_value = mock_handler
                    mock_handler.__enter__ = Mock(return_value=mock_handler)
                    mock_handler.__exit__ = Mock(return_value=None)

                    # Create real PyArrow schema and batch
                    schema = pa.schema([('id', pa.int64()), ('name', pa.string())])
                    batch = pa.record_batch([[1, 2], ['Alice', 'Bob']], schema=schema)

                    # First table succeeds, second table fails
                    mock_handler.get_table_schema.side_effect = [
                        schema,  # First table succeeds
                        Exception("Table processing failed")  # Second table fails
                    ]
                    mock_handler.read_table_data.return_value = [batch]

                    # Mock parquet writer
                    mock_writer_instance = Mock()
                    mock_writer.return_value = mock_writer_instance

                    result = SqlImporter.import_sql(
                        connection_string=connection_string,
                        output_path=output_directory,
                        schema_file=sample_schema_file
                    )

                    # Verify error handling - first table processed, second failed
                    assert result.total_rows == 2  # Only successful table
                    assert result.valid_rows == 2
                    assert result.invalid_rows == 1  # One table failed
                    assert len(result.output_files) == 1  # Only one file created

    def test_import_sql_string_and_path_inputs(self, output_directory):
        """Test SQL import with string and Path inputs."""
        connection_string = "driver={ODBC Driver 17 for SQL Server};server=localhost;database=test"

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
            schema_content = {
                "tables": [{"schema": "public", "name": "users", "outputName": "users_table"}]
            }
            json.dump(schema_content, tmp_file)
            schema_path_str = tmp_file.name

        try:
            with patch('forklift.schema.sql_schema_importer.SqlSchemaImporter') as mock_schema_importer_class:
                with patch('forklift.inputs.sql.SqlInputHandler') as mock_handler_class:
                    with patch('forklift.io.create_parquet_writer'):
                        # Mock schema importer
                        mock_schema_importer = Mock()
                        mock_schema_importer_class.return_value = mock_schema_importer
                        mock_schema_importer.get_table_list.return_value = [
                            ('public', 'users', 'users_table')
                        ]

                        # Mock SQL handler with context manager support
                        mock_handler = Mock()
                        mock_handler_class.return_value = mock_handler
                        mock_handler.__enter__ = Mock(return_value=mock_handler)
                        mock_handler.__exit__ = Mock(return_value=None)

                        # Create real PyArrow schema and batch
                        schema = pa.schema([('id', pa.int64())])
                        batch = pa.record_batch([[1]], schema=schema)

                        mock_handler.get_table_schema.return_value = schema
                        mock_handler.read_table_data.return_value = [batch]

                        # Test with string paths
                        result = SqlImporter.import_sql(
                            connection_string=connection_string,
                            output_path=str(output_directory),  # String path
                            schema_file=schema_path_str  # String path
                        )

                        assert result.total_rows == 1
                        assert len(result.output_files) == 1
        finally:
            Path(schema_path_str).unlink()
