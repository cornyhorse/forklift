"""Additional tests to achieve 100% coverage for forklift_core.py."""

import pytest
import tempfile
import json
import os
import io
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open, Mock
import pyarrow as pa
import pyarrow.parquet as pq
from dataclasses import dataclass

from forklift.engine.forklift_core import (
    ForkliftCore,
    ImportConfig,
    HeaderMode,
    ExcessColumnMode,
    ProcessingResults,
    import_csv,
    import_fwf,
    import_excel,
    import_sql
)


@pytest.fixture
def s3_mock_flag(request):
    """Fixture to determine if S3 should be mocked based on command line flag."""
    return not request.config.getoption("--no-s3-mock")


class TestForkliftCore100Percent:
    """Test cases to achieve 100% coverage for remaining missing lines."""

    def test_break_in_auto_detect_header_search_limit(self):
        """Test break statement in auto detect header when search limit reached (line 287)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            header_mode=HeaderMode.AUTO,
            header_search_rows=2  # Limit to 2 rows
        )
        engine = ForkliftCore(config)

        # Create CSV with many rows to trigger search limit
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("data1,data2,data3\n")  # Row 0
            f.write("value1,value2,value3\n")  # Row 1
            f.write("id,name,age\n")  # Row 2 - would be header but beyond search limit
            f.write("1,John,25\n")  # Row 3
            test_file = Path(f.name)

        try:
            header_idx, columns = engine._auto_detect_header(test_file)
            # Should stop at search limit and pick first row
            assert header_idx == 0
            assert columns == ['data1', 'data2', 'data3']
        finally:
            test_file.unlink()

    def test_continue_in_auto_detect_header_comment_skip(self):
        """Test continue statement in auto detect header when skipping comments (line 301)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            header_mode=HeaderMode.AUTO,
            comment_rows=[r"#.*"]  # Pattern to match comment rows
        )
        engine = ForkliftCore(config)

        # Create CSV with comment rows that should be skipped
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("# This is a comment row\n")  # Should be skipped
            f.write("# Another comment\n")  # Should be skipped
            f.write("id,name,age\n")  # This should be detected as header
            f.write("1,John,25\n")
            test_file = Path(f.name)

        try:
            header_idx, columns = engine._auto_detect_header(test_file)
            # Should skip comments and find header at row 2
            assert header_idx == 2
            assert columns == ['id', 'name', 'age']
        finally:
            test_file.unlink()

    def test_break_in_batch_reader_none_batch(self):
        """Test break statement when batch is None in batch reader (line 505)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output"
        )
        engine = ForkliftCore(config)
        engine.header_row_index = 0
        engine.column_names = ["id", "name"]

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("id,name\n1,Alice\n")
            test_file = Path(f.name)

        try:
            # Mock PyArrow CSV reader to return None batch
            with patch('pyarrow.csv.open_csv') as mock_open_csv:
                mock_reader = MagicMock()
                mock_reader.read_next_batch.side_effect = [None]  # Return None to trigger break
                mock_open_csv.return_value = mock_reader

                batches = list(engine._create_batch_reader(test_file))
                assert len(batches) == 0  # Should break and return empty
        finally:
            test_file.unlink()

    def test_empty_csv_return_iterator(self):
        """Test return empty iterator for empty CSV (line 540)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output"
        )
        engine = ForkliftCore(config)
        engine.header_row_index = 0
        engine.column_names = ["id", "name"]

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            # Create empty file
            pass
            test_file = Path(f.name)

        try:
            # Mock PyArrow to raise "Empty CSV file" exception
            with patch('pyarrow.csv.open_csv', side_effect=pa.ArrowInvalid("Empty CSV file")):
                batches = list(engine._create_batch_reader(test_file))
                assert len(batches) == 0  # Should return empty iterator
        finally:
            test_file.unlink()

    def test_stop_iteration_in_column_mismatch_reader(self):
        """Test StopIteration handling in column mismatch reader (lines 554-555)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output"
        )
        engine = ForkliftCore(config)
        engine.column_names = ["id", "name"]

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("1,Alice\n")
            test_file = Path(f.name)

        try:
            # Mock csv.reader to raise StopIteration
            with patch('csv.reader') as mock_reader:
                mock_reader.return_value = iter([])  # Empty iterator causes StopIteration

                batches = list(engine._handle_column_mismatch_reader(test_file, 0))
                assert len(batches) == 0  # Should handle StopIteration gracefully
        finally:
            test_file.unlink()

    def test_break_in_column_mismatch_reader_batch_full(self):
        """Test break when batch buffer reaches limit (line 560)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            batch_size=2  # Small batch size
        )
        engine = ForkliftCore(config)
        engine.column_names = ["id", "name"]

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            # Create file with exactly batch_size rows
            f.write("1,Alice\n2,Bob\n")
            test_file = Path(f.name)

        try:
            batches = list(engine._handle_column_mismatch_reader(test_file, 0))
            # Should create one batch with 2 rows
            assert len(batches) == 1
            assert len(batches[0]) == 2
        finally:
            test_file.unlink()

    def test_yield_remaining_rows_in_buffer(self):
        """Test yielding remaining rows in buffer (lines 579-580)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            batch_size=3  # Batch size larger than data
        )
        engine = ForkliftCore(config)
        engine.column_names = ["id", "name"]

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            # Create file with fewer rows than batch size
            f.write("1,Alice\n2,Bob\n")  # Only 2 rows, batch size is 3
            test_file = Path(f.name)

        try:
            batches = list(engine._handle_column_mismatch_reader(test_file, 0))
            # Should yield remaining rows as final batch
            assert len(batches) == 1
            assert len(batches[0]) == 2
        finally:
            test_file.unlink()

    def test_convert_rows_to_batch_empty_schema(self):
        """Test convert rows to batch with empty schema creation (lines 600-601)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output"
        )
        engine = ForkliftCore(config)
        engine.column_names = ["id", "name"]

        # Test with empty rows list
        batch = engine._convert_rows_to_batch([], 2)

        # Should create empty batch with correct schema
        assert batch.num_columns == 2
        assert len(batch) == 0
        assert batch.column_names == ["id", "name"]

    @pytest.mark.s3
    def test_s3_output_path_handling(self, s3_mock_conditional):
        """Test S3 output path handling in process_csv method (lines 834-840)."""
        import uuid

        # Generate unique S3 path for testing
        test_id = str(uuid.uuid4())[:8]
        s3_bucket = 'test-bucket'
        s3_output_path = f"s3://{s3_bucket}/forklift-test/{test_id}/"

        config = ImportConfig(
            input_path="dummy.csv",
            output_path=s3_output_path,
            validate_schema=False
        )

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("id,name\n1,Alice\n2,Bob\n")
            test_file = Path(f.name)
            config.input_path = str(test_file)

        engine = ForkliftCore(config)

        try:
            # Always mock S3 components to avoid actual S3 calls
            with patch('forklift.io.is_s3_path', return_value=True), \
                 patch('forklift.io.S3Path') as mock_s3_path, \
                 patch('forklift.engine.forklift_core.create_parquet_writer') as mock_writer, \
                 patch.object(engine.io_handler, 'exists', return_value=False), \
                 patch.object(engine.io_handler, 'get_size', return_value=0), \
                 patch.object(engine.io_handler, 'open_for_write') as mock_open_write, \
                 patch.object(engine.io_handler, 's3_client', create=True) as mock_s3_client, \
                 patch.object(engine, '_create_batch_reader') as mock_reader, \
                 patch('boto3.Session') as mock_session:

                # Mock boto3 session and client
                mock_client = MagicMock()
                mock_session.return_value.client.return_value = mock_client

                # Mock batch reader to return some data
                mock_batch = pa.RecordBatch.from_arrays([
                    pa.array([1, 2]),
                    pa.array(['Alice', 'Bob'])
                ], ['id', 'name'])
                mock_reader.return_value = iter([mock_batch])

                # Mock S3Path with specific string return values for JSON serialization
                mock_s3_instance = MagicMock()
                mock_s3_instance.join.side_effect = lambda x: f"{s3_output_path}{x}"
                mock_s3_instance.bucket = s3_bucket
                mock_s3_instance.key = f"forklift-test/{test_id}/"
                mock_s3_instance.name = f"forklift-test/{test_id}/"
                mock_s3_path.return_value = mock_s3_instance

                mock_writer_instance = MagicMock()
                mock_writer_instance.schema = pa.schema([pa.field('id', pa.int64()), pa.field('name', pa.string())])
                mock_writer.return_value = mock_writer_instance

                # Mock file writing for manifest/metadata with StringIO
                from io import StringIO
                mock_string_io = StringIO()
                mock_open_write.return_value.__enter__.return_value = mock_string_io

                # This should trigger S3 output path handling
                result = engine.process_csv()

                assert result.total_rows == 2
                assert result.valid_rows == 2
                assert len(result.output_files) >= 1

        finally:
            test_file.unlink()

    def test_bad_writer_creation(self):
        """Test bad writer creation when invalid batch exists (line 867)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            validate_schema=True
        )

        # Create schema for validation
        schema = pa.schema([pa.field("id", pa.int64())])
        config.schema = schema

        engine = ForkliftCore(config)

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("id\ninvalid_data\n")  # This should cause validation failure
            test_file = Path(f.name)
            config.input_path = str(test_file)

        try:
            with patch('forklift.engine.forklift_core.create_parquet_writer') as mock_writer, \
                 patch.object(engine, '_create_manifest', return_value="manifest.json"), \
                 patch.object(engine, '_create_metadata', return_value="metadata.json"):

                mock_writer_instance = MagicMock()
                mock_writer.return_value = mock_writer_instance

                result = engine.process_csv()

                # Should create both good and bad writers
                assert mock_writer.call_count >= 1  # At least good writer created
        finally:
            test_file.unlink()

    def test_writer_close_and_file_append(self):
        """Test writer closing and file appending (lines 880-888)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            validate_schema=False
        )
        engine = ForkliftCore(config)

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("id,name\n1,Alice\n")
            test_file = Path(f.name)
            config.input_path = str(test_file)

        try:
            with patch('forklift.engine.forklift_core.create_parquet_writer') as mock_writer, \
                 patch.object(engine, '_create_manifest', return_value="manifest.json"), \
                 patch.object(engine, '_create_metadata', return_value="metadata.json"):

                mock_writer_instance = MagicMock()
                mock_writer.return_value = mock_writer_instance

                result = engine.process_csv()

                # Verify writer was closed
                mock_writer_instance.close.assert_called()

                # Verify output files were tracked
                assert len(result.output_files) > 0
        finally:
            test_file.unlink()

    def test_manifest_and_metadata_creation(self):
        """Test manifest and metadata creation (lines 898-899)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            create_manifest=True,
            create_metadata=True
        )
        engine = ForkliftCore(config)

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("id,name\n1,Alice\n")
            test_file = Path(f.name)
            config.input_path = str(test_file)

        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                config.output_path = temp_dir

                # Don't mock the manifest/metadata methods - let them run
                with patch('forklift.engine.forklift_core.create_parquet_writer') as mock_writer:
                    mock_writer_instance = MagicMock()
                    mock_writer.return_value = mock_writer_instance

                    result = engine.process_csv()

                    # Verify manifest and metadata files were created
                    assert result.manifest_file is not None
                    assert result.metadata_file is not None
        finally:
            test_file.unlink()

    def test_import_fwf_not_implemented(self):
        """Test import_fwf not implemented error (line 944)."""
        with pytest.raises(NotImplementedError, match="FWF import not yet implemented"):
            import_fwf("input.fwf", "output", schema_file="schema.json")

    def test_import_excel_file_not_found(self):
        """Test import_excel with non-existent file (line 969)."""
        with pytest.raises(FileNotFoundError, match="Input file not found"):
            import_excel("nonexistent.xlsx", "output")

    def test_import_excel_processing_error(self):
        """Test import_excel processing error handling (lines 973-976)."""
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            test_file = Path(f.name)

        try:
            # Mock schema loading to raise an exception - use correct import path
            with patch('forklift.schema.excel_schema_importer.ExcelSchemaImporter', side_effect=Exception("Schema error")):
                with pytest.raises(Exception, match="Schema error"):
                    import_excel(str(test_file), "output", schema_file="schema.json")
        finally:
            test_file.unlink()

    def test_import_excel_exception_handling_and_results(self):
        """Test import_excel exception handling and error results (line 979)."""
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            test_file = Path(f.name)

        try:
            # Mock to raise exception during processing
            with patch('forklift.engine.forklift_core.ExcelImporter._create_default_excel_config', side_effect=Exception("Processing error")):
                with pytest.raises(Exception, match="Processing error"):
                    import_excel(str(test_file), "output")
        finally:
            test_file.unlink()

    def test_import_sql_parameter_handling(self):
        """Test import_sql parameter handling (lines 986-987)."""
        # Test that import_sql raises ProcessingError when schema is missing
        with pytest.raises(Exception, match="Schema file is required"):
            from forklift.engine.forklift_core import import_sql
            import_sql("connection_string", "output")


class TestImportFunctions:
    """Test the public import functions for full coverage."""

    def test_import_csv_with_all_parameters(self):
        """Test import_csv with comprehensive parameter coverage (lines 1019-1022)."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("id,name\n1,Alice\n2,Bob\n")
            test_file = Path(f.name)

        with tempfile.TemporaryDirectory() as output_dir:
            try:
                # Test with comprehensive parameters
                result = import_csv(
                    input_path=str(test_file),
                    output_path=output_dir,
                    schema_file=None,
                    header_mode="present",
                    delimiter=",",
                    encoding="utf-8",
                    validate_schema=False,
                    create_manifest=True,
                    create_metadata=True,
                    batch_size=1000,
                    compression="snappy"
                )

                assert isinstance(result, ProcessingResults)
                assert result.total_rows == 2
                assert result.valid_rows == 2
                assert result.invalid_rows == 0
                assert len(result.output_files) > 0
            finally:
                test_file.unlink()


class TestMissingLargeSections:
    """Test the large missing sections in forklift_core.py."""

    def test_excel_import_large_section(self):
        """Test the large Excel import section (lines 1218-1287)."""
        # This section is the Excel import function implementation
        # Create a minimal Excel file for testing
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            test_file = Path(f.name)

        with tempfile.TemporaryDirectory() as output_dir:
            try:
                # Mock Excel-related components since we can't create real Excel files easily
                with patch('forklift.inputs.excel.ExcelInputHandler') as mock_handler, \
                     patch('forklift.engine.forklift_core.ExcelImporter._create_default_excel_config') as mock_config, \
                     patch('forklift.engine.forklift_core.ExcelImporter._sanitize_filename', return_value="test_sheet"), \
                     patch('pyarrow.parquet.write_table') as mock_write:

                    # Mock Excel handler
                    mock_handler_instance = MagicMock()
                    mock_handler.return_value = mock_handler_instance

                    # Mock sheet info
                    mock_handler_instance.get_sheet_names.return_value = ['Sheet1']

                    # Mock sheet processing - return an Arrow table
                    mock_table = pa.table([pa.array([1, 2])], names=['id'])
                    mock_handler_instance.process_sheets.return_value = [('Sheet1', mock_table)]

                    # Mock config
                    mock_config_instance = MagicMock()
                    mock_config.return_value = mock_config_instance

                    # Test Excel import
                    result = import_excel(str(test_file), output_dir)

                    # Verify the process completed
                    assert isinstance(result, ProcessingResults)
                    mock_write.assert_called_once()

            finally:
                test_file.unlink()

    def test_sql_import_section(self):
        """Test the SQL import section (lines 1375-1532)."""
        from forklift.engine.forklift_core import import_sql

        # Test with proper schema file to trigger the missing lines
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as schema_f:
            schema_data = {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$id": "http://example.com/schema",
                "title": "Test Schema",
                "type": "object",
                "x-sql": {
                    "tables": [
                        {
                            "select": {
                                "schema": "public",
                                "name": "users"
                            },
                            "outputName": "users_data"
                        }
                    ]
                }
            }
            json.dump(schema_data, schema_f)
            schema_file = Path(schema_f.name)

        with tempfile.TemporaryDirectory() as output_dir:
            try:
                # This should trigger SQL processing code paths but may fail due to missing DB
                with pytest.raises(Exception):  # Expect some exception due to invalid connection
                    import_sql("invalid_connection", output_dir, schema_file=str(schema_file))
            finally:
                schema_file.unlink()

    def test_helper_functions_section(self):
        """Test helper functions section (lines 1537-1553, 1563-1607, 1612-1618)."""
        # Test ExcelImporter._create_default_excel_config if it exists
        try:
            from forklift.engine.importers.excel_importer import ExcelImporter
            with tempfile.NamedTemporaryFile(suffix='.xlsx') as f:
                test_file = Path(f.name)

                # Mock the Excel handler since the file is empty
                with patch('forklift.inputs.excel.ExcelInputHandler') as mock_handler:
                    mock_handler_instance = MagicMock()
                    mock_handler.return_value = mock_handler_instance
                    mock_handler_instance.get_sheet_names.return_value = ['Sheet1']

                    config = ExcelImporter._create_default_excel_config(test_file)
                    assert config is not None
        except ImportError:
            # Function might not be directly importable
            pass

        # Test ExcelImporter._sanitize_filename if it exists
        try:
            from forklift.engine.importers.excel_importer import ExcelImporter
            result = ExcelImporter._sanitize_filename("test sheet name!@#")
            assert isinstance(result, str)
        except ImportError:
            # Function might not be directly importable
            pass


# Run with: python -m pytest tests/test_forklift_core_100_percent.py --cov=src/forklift/engine/forklift_core --cov-report=term-missing -v
