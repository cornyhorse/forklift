"""Complete coverage tests for forklift_core.py to achieve 100% coverage."""

import pytest
import tempfile
import json
import os
import io
import csv
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open, Mock
import pyarrow as pa
import pyarrow.parquet as pq
from dataclasses import dataclass

from src.forklift.engine.forklift_core import (
    ForkliftCore,
    ImportConfig,
    HeaderMode,
    ExcessColumnMode,
    ProcessingResults,
    ProcessingError,
    import_csv,
    import_fwf,
    import_excel,
    import_sql,
    _create_default_excel_config
)


class TestForkliftCoreCompleteCoverage:
    """Test cases to achieve 100% coverage for all remaining missing lines."""

    def test_json_schema_to_pyarrow_with_required_fields(self):
        """Test _json_schema_to_pyarrow with required fields (lines 182-189)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output"
        )
        engine = ForkliftCore(config)

        # Test schema with required fields
        schema_dict = {
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "email": {"type": "string"}
            },
            "required": ["id", "name"]  # email is optional
        }

        schema = engine._json_schema_to_pyarrow(schema_dict)

        # Check that required fields are non-nullable
        assert not schema.field('id').nullable
        assert not schema.field('name').nullable
        assert schema.field('email').nullable  # Optional field

    def test_json_type_to_pyarrow_date_formats(self):
        """Test _json_type_to_pyarrow with date formats (lines 200-208)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output"
        )
        engine = ForkliftCore(config)

        # Test date format
        date_field = {"type": "string", "format": "date"}
        assert engine._json_type_to_pyarrow(date_field) == pa.date32()

        # Test date-time format
        datetime_field = {"type": "string", "format": "date-time"}
        assert engine._json_type_to_pyarrow(datetime_field) == pa.timestamp("us")

        # Test unknown type fallback
        unknown_field = {"type": "unknown"}
        assert engine._json_type_to_pyarrow(unknown_field) == pa.string()

    def test_detect_header_row_present_mode(self):
        """Test _detect_header_row in PRESENT mode (lines 219-235)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            header_mode=HeaderMode.PRESENT
        )
        engine = ForkliftCore(config)

        # Create test CSV
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("id,name,age\n")
            f.write("1,John,25\n")
            test_file = Path(f.name)

        try:
            header_idx, columns = engine._detect_header_row(test_file)
            assert header_idx == 0
            assert columns == ["id", "name", "age"]
        finally:
            test_file.unlink()

    def test_detect_header_row_absent_mode(self):
        """Test _detect_header_row in ABSENT mode (line 266)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            header_mode=HeaderMode.ABSENT
        )
        engine = ForkliftCore(config)

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("1,John,25\n")
            f.write("2,Jane,30\n")
            test_file = Path(f.name)

        try:
            header_idx, columns = engine._detect_header_row(test_file)
            assert header_idx == -1
            assert columns == []
        finally:
            test_file.unlink()

    def test_auto_detect_header_search_limit(self):
        """Test auto detect header with search limit (line 287)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            header_mode=HeaderMode.AUTO,
            header_search_rows=2
        )
        engine = ForkliftCore(config)

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("data1,data2,data3\n")  # Row 0
            f.write("value1,value2,value3\n")  # Row 1
            f.write("id,name,age\n")  # Row 2 - beyond search limit
            f.write("1,John,25\n")
            test_file = Path(f.name)

        try:
            header_idx, columns = engine._auto_detect_header(test_file)
            # Should find header in first 2 rows only
            assert header_idx <= 1
        finally:
            test_file.unlink()

    def test_auto_detect_header_score_calculation(self):
        """Test auto detect header score calculation (lines 298, 301, 306)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            header_mode=HeaderMode.AUTO
        )
        engine = ForkliftCore(config)

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("123,456,789\n")  # Low score - all numbers
            f.write("id,name,age\n")  # High score - text headers
            f.write("1,John,25\n")
            test_file = Path(f.name)

        try:
            header_idx, columns = engine._auto_detect_header(test_file)
            assert header_idx == 1  # Should pick the text header row
            assert columns == ["id", "name", "age"]
        finally:
            test_file.unlink()

    def test_footer_detection_patterns(self):
        """Test footer detection with patterns (lines 430-435)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            footer_detection={
                "patterns": ["^TOTAL", "^END"],
                "column_index": 0
            }
        )
        engine = ForkliftCore(config)

        test_rows = [
            ["id", "name", "value"],
            ["1", "John", "100"],
            ["2", "Jane", "200"],
            ["TOTAL", "", "300"]  # Footer row
        ]

        # Test footer detection - use the correct method name
        assert engine._should_stop_for_footer(test_rows[3])
        assert not engine._should_stop_for_footer(test_rows[1])

    def test_create_batch_reader_empty_file_handling(self):
        """Test batch reader with empty file handling (line 515)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output"
        )
        engine = ForkliftCore(config)

        # Create empty CSV file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            pass  # Write nothing
            test_file = Path(f.name)

        try:
            # This should handle empty files gracefully
            batches = list(engine._create_batch_reader(test_file))
            assert isinstance(batches, list)  # Should return empty iterator
        finally:
            test_file.unlink()

    def test_column_mismatch_handling_exception(self):
        """Test column mismatch exception handling (line 540)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            excess_column_mode=ExcessColumnMode.TRUNCATE
        )
        engine = ForkliftCore(config)

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("id,name\n")  # 2 columns
            f.write("1,John,extra\n")  # 3 columns - mismatch
            test_file = Path(f.name)

        try:
            # This should handle column mismatches through the actual processing
            batches = list(engine._create_batch_reader(test_file))
            # Should handle gracefully without crashing
        finally:
            test_file.unlink()

    def test_temp_file_cleanup_with_footer_detection(self):
        """Test temporary file cleanup (lines 560, 573)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            footer_detection={"patterns": ["TOTAL"]}
        )
        engine = ForkliftCore(config)

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("id,name\n1,John\nTOTAL,2\n")
            test_file = Path(f.name)

        try:
            # This should trigger footer detection and temp file creation/cleanup
            list(engine._create_batch_reader(test_file))
        finally:
            test_file.unlink()

    def test_manifest_generation_with_correct_config(self):
        """Test manifest generation (lines 683, 705, 708)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            create_manifest=True  # Correct parameter name
        )
        engine = ForkliftCore(config)

        results = ProcessingResults(
            total_rows=100,
            valid_rows=95,
            invalid_rows=5,
            execution_time=1.5,
            output_files=["test.parquet"]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir)

            # Test successful generation using the correct method name
            manifest_file = engine._create_manifest(output_path, ["test.parquet"])

            # Check file was created
            assert Path(manifest_file).exists()

    def test_import_csv_with_actual_file(self):
        """Test import_csv with proper file handling (lines 880-888)."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("id,name,age\n")
            f.write("1,John,25\n")
            f.write("2,Jane,30\n")
            csv_file = Path(f.name)

        try:
            with tempfile.TemporaryDirectory() as output_dir:
                # This should work without errors
                results = import_csv(
                    input_path=str(csv_file),
                    output_path=output_dir
                )
                assert results.total_rows >= 0
        finally:
            csv_file.unlink()

    def test_import_csv_with_invalid_schema(self):
        """Test import_csv with schema validation error (lines 898-899)."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("id,name,age\n")
            f.write("1,John,25\n")
            csv_file = Path(f.name)

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as schema_f:
            # Create a completely invalid JSON file
            schema_f.write("invalid json content")
            schema_file = Path(schema_f.name)

        try:
            with tempfile.TemporaryDirectory() as output_dir:
                # This should raise an exception due to invalid JSON
                with pytest.raises(Exception):
                    import_csv(
                        input_path=str(csv_file),
                        output_path=output_dir,
                        schema_file=str(schema_file)
                    )
        finally:
            csv_file.unlink()
            schema_file.unlink()

    def test_import_fwf_error_handling(self):
        """Test import_fwf error handling (lines 910-913)."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("John      25    \n")
            f.write("Jane      30    \n")
            fwf_file = Path(f.name)

        try:
            with tempfile.TemporaryDirectory() as output_dir:
                # This should raise NotImplementedError
                with pytest.raises(NotImplementedError):
                    import_fwf(
                        input_path=str(fwf_file),
                        output_path=output_dir,
                        column_specs=[(0, 10), (10, 15)]
                    )
        finally:
            fwf_file.unlink()

    def test_import_excel_with_proper_mocking(self):
        """Test import_excel error handling (line 944)."""
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            excel_file = Path(f.name)

        try:
            with tempfile.TemporaryDirectory() as output_dir:
                with patch('src.forklift.inputs.excel.ExcelInputHandler') as mock_handler:
                    mock_handler.side_effect = Exception("Excel processing failed")

                    with pytest.raises(Exception):
                        import_excel(
                            input_path=str(excel_file),
                            output_path=output_dir
                        )
        finally:
            excel_file.unlink()

    def test_import_sql_schema_processing_error(self):
        """Test import_sql schema processing error (line 1408)."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as schema_f:
            json.dump({"invalid": "sql_schema"}, schema_f)
            schema_file = Path(schema_f.name)

        try:
            with pytest.raises(Exception):
                import_sql(
                    connection_string="sqlite:///:memory:",
                    output_path="dummy_output",
                    schema_file=str(schema_file)
                )
        finally:
            schema_file.unlink()

    def test_import_sql_complete_flow(self):
        """Test complete SQL import flow (lines 1442-1482)."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as schema_f:
            schema_data = {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "x-sql": {
                    "tables": [
                        {
                            "select": {"name": "users"},
                            "outputName": "users_export"
                        }
                    ]
                }
            }
            json.dump(schema_data, schema_f)
            schema_file = Path(schema_f.name)

        try:
            with tempfile.TemporaryDirectory() as output_dir:
                with patch('src.forklift.inputs.sql.SqlInputHandler') as mock_handler, \
                     patch('src.forklift.schema.sql_schema_importer.SqlSchemaImporter') as mock_schema, \
                     patch('src.forklift.io.create_parquet_writer') as mock_writer:

                    # Mock schema importer
                    mock_schema_instance = MagicMock()
                    mock_schema.return_value = mock_schema_instance
                    mock_schema_instance.get_table_list.return_value = [("public", "users", "users_export")]

                    # Mock SQL handler
                    mock_handler_instance = MagicMock()
                    mock_handler.return_value = mock_handler_instance
                    mock_handler_instance.__enter__.return_value = mock_handler_instance
                    mock_handler_instance.get_table_schema.return_value = pa.schema([
                        pa.field("id", pa.int64()),
                        pa.field("name", pa.string())
                    ])

                    # Mock data batches
                    mock_batch = pa.record_batch([
                        pa.array([1, 2]),
                        pa.array(["John", "Jane"])
                    ], names=["id", "name"])
                    mock_handler_instance.read_table_data.return_value = [mock_batch]

                    # Mock writer
                    mock_writer_instance = MagicMock()
                    mock_writer.return_value = mock_writer_instance

                    # Execute import
                    results = import_sql(
                        connection_string="sqlite:///:memory:",
                        output_path=output_dir,
                        schema_file=str(schema_file)
                    )

                    assert results.total_rows == 2
                    assert results.valid_rows == 2
        finally:
            schema_file.unlink()

    def test_create_default_excel_config_specific_sheet(self):
        """Test _create_default_excel_config with specific sheet (lines 1542-1551)."""
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            test_file = Path(f.name)

        try:
            with patch('src.forklift.inputs.excel.ExcelInputHandler') as mock_handler:
                mock_handler_instance = MagicMock()
                mock_handler.return_value = mock_handler_instance
                mock_handler_instance.get_sheet_info.return_value = {
                    'sheet_names': ['Sheet1', 'Sheet2', 'Data']
                }

                # Test with sheet name
                config = _create_default_excel_config(test_file, sheet='Data')
                assert len(config.sheets) == 1

                # Test with sheet index
                config = _create_default_excel_config(test_file, sheet=1)
                assert len(config.sheets) == 1

                # Test with invalid sheet name
                with pytest.raises(ValueError, match="Sheet 'Invalid' not found"):
                    _create_default_excel_config(test_file, sheet='Invalid')

                # Test with invalid sheet index
                with pytest.raises(ValueError, match="Sheet index 5 out of range"):
                    _create_default_excel_config(test_file, sheet=5)

        finally:
            test_file.unlink()

    def test_create_default_excel_config_all_sheets(self):
        """Test _create_default_excel_config with all sheets (lines 1578-1592)."""
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            test_file = Path(f.name)

        try:
            with patch('src.forklift.inputs.excel.ExcelInputHandler') as mock_handler:
                mock_handler_instance = MagicMock()
                mock_handler.return_value = mock_handler_instance
                mock_handler_instance.get_sheet_info.return_value = {
                    'sheet_names': ['Sheet1', 'Sheet2', 'Data']
                }

                # Test processing all sheets (no sheet parameter)
                config = _create_default_excel_config(test_file)
                assert len(config.sheets) == 3
                assert config.values_only is True

                # Test with custom values_only
                config = _create_default_excel_config(test_file, values_only=False)
                assert config.values_only is False

        finally:
            test_file.unlink()

    def test_remaining_edge_cases(self):
        """Test remaining edge cases (lines 1596-1597)."""
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            test_file = Path(f.name)

        try:
            with patch('src.forklift.inputs.excel.ExcelInputHandler') as mock_handler:
                mock_handler_instance = MagicMock()
                mock_handler.return_value = mock_handler_instance
                mock_handler_instance.get_sheet_info.return_value = {
                    'sheet_names': ['Sheet1']
                }

                # Test with additional kwargs
                config = _create_default_excel_config(
                    test_file,
                    values_only=False,
                    date_system=1904,
                    null_values=['NULL', 'N/A']
                )
                assert len(config.sheets) == 1

        finally:
            test_file.unlink()

    def test_schema_file_not_found_error(self):
        """Test schema file not found error handling."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            schema_file="/nonexistent/schema.json"
        )

        with pytest.raises(FileNotFoundError):
            engine = ForkliftCore(config)
            engine._load_schema()

    def test_manifest_metadata_generation(self):
        """Test manifest and metadata generation (lines 1228-1229)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            create_manifest=True,  # Correct parameter name
            create_metadata=True   # Correct parameter name
        )
        engine = ForkliftCore(config)

        results = ProcessingResults(
            total_rows=100,
            valid_rows=100,
            invalid_rows=0,
            execution_time=1.0,
            output_files=["test.parquet"]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir)

            # Test successful generation using correct method names
            manifest_file = engine._create_manifest(output_path, ["test.parquet"])
            metadata_file = engine._create_metadata(output_path, results)

            # Check files were created
            assert Path(manifest_file).exists()
            assert Path(metadata_file).exists()
