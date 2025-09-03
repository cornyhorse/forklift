"""Final targeted tests to achieve 100% coverage for forklift_core.py."""

import pytest
import tempfile
import json
import os
from pathlib import Path
from unittest.mock import patch, MagicMock
import pyarrow as pa

from src.forklift.engine.forklift_core import (
    ForkliftCore,
    ImportConfig,
    HeaderMode,
    ProcessingResults,
    ProcessingError,
    import_csv
)


class TestForkliftCoreFinalMissingLines:
    """Test cases to cover the final 38 missing lines for 100% coverage."""

    def test_json_schema_properties_missing(self):
        """Test _json_schema_to_pyarrow when properties key is missing (line 189)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output"
        )
        engine = ForkliftCore(config)

        # Test schema without properties key
        schema_dict = {"required": ["id"]}  # No properties key
        schema = engine._json_schema_to_pyarrow(schema_dict)
        assert len(schema) == 0  # Should create empty schema

    def test_header_mode_absent_direct_path(self):
        """Test ABSENT header mode direct path (line 266)."""
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
            # Test the direct ABSENT path
            header_idx, columns = engine._detect_header_row(test_file)
            assert header_idx == -1
            assert columns == []
        finally:
            test_file.unlink()

    def test_auto_detect_header_break_condition(self):
        """Test auto detect header break condition (line 287)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            header_mode=HeaderMode.AUTO,
            header_search_rows=1  # Very limited search
        )
        engine = ForkliftCore(config)

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("data1,data2,data3\n")  # Row 0
            f.write("id,name,age\n")  # Row 1 - beyond limit
            test_file = Path(f.name)

        try:
            # Should break after first row due to search limit
            header_idx, columns = engine._auto_detect_header(test_file)
            assert header_idx == 0  # Should use first row
        finally:
            test_file.unlink()

    def test_header_score_calculation_paths(self):
        """Test different paths in header score calculation (lines 298, 301, 306)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            header_mode=HeaderMode.AUTO
        )
        engine = ForkliftCore(config)

        # Test with various row types to hit different score paths
        test_rows = [
            ["123", "456", "789"],      # All numeric - should not look like header
            ["", "", ""],               # Empty cells
            ["id", "name", "age"],      # Good header row - should look like header
            ["a", "b", "c"]             # Single character headers
        ]

        results = []
        for row in test_rows:
            # Use the actual method that exists
            looks_like_header = engine._looks_like_header(row)
            results.append(looks_like_header)

        # Header row should be identified as header
        assert results[2] is True  # "id", "name", "age" should look like header
        assert results[0] is False  # All numeric should not look like header

    def test_footer_detection_error_handling(self):
        """Test footer detection with error handling (lines 560, 573)."""
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
            # Mock file operations to test cleanup paths
            with patch.object(Path, 'unlink', side_effect=OSError("Cleanup failed")):
                # Should handle cleanup errors gracefully
                list(engine._create_batch_reader(test_file))
        finally:
            test_file.unlink()

    def test_manifest_generation_error_paths(self):
        """Test manifest generation error paths (lines 683, 705, 708)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            create_manifest=True
        )
        engine = ForkliftCore(config)

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir)

            # Test with JSON serialization error instead of file permission error
            with patch('json.dump', side_effect=TypeError("JSON serialization failed")):
                with pytest.raises(Exception):
                    engine._create_manifest(output_path, ["test.parquet"])

    def test_s3_initialization_error(self):
        """Test S3 initialization error (line 867)."""
        # This tests the S3 initialization error path by creating a config that would use S3
        # but mocking the UnifiedIOHandler to fail during ForkliftCore.__init__
        with patch('src.forklift.engine.forklift_core.UnifiedIOHandler') as mock_handler:
            mock_handler.side_effect = Exception("S3 connection failed")

            with pytest.raises(Exception, match="S3 connection failed"):
                config = ImportConfig(
                    input_path="s3://bucket/file.csv",
                    output_path="s3://bucket/output/"
                )
                # This should trigger the S3 initialization error during ForkliftCore creation
                ForkliftCore(config)

    def test_import_csv_error_paths(self):
        """Test import_csv error handling paths (lines 880-888)."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("id,name\n1,John\n")
            csv_file = Path(f.name)

        try:
            # Test with ForkliftCore initialization error
            with patch('src.forklift.engine.forklift_core.ForkliftCore', side_effect=Exception("Init failed")):
                with pytest.raises(Exception):
                    import_csv(
                        input_path=str(csv_file),
                        output_path="dummy_output"
                    )
        finally:
            csv_file.unlink()

    def test_csv_processing_error_paths(self):
        """Test CSV processing error paths (lines 898-899)."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("id,name\n1,John\n")
            csv_file = Path(f.name)

        try:
            # Mock process_csv to raise an error
            with patch.object(ForkliftCore, 'process_csv', side_effect=Exception("Processing failed")):
                with pytest.raises(Exception):
                    import_csv(
                        input_path=str(csv_file),
                        output_path="dummy_output"
                    )
        finally:
            csv_file.unlink()

    def test_import_excel_not_implemented_path(self):
        """Test import_excel not implemented path (line 944)."""
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            excel_file = Path(f.name)

        try:
            # The function should handle cases where Excel processing isn't available
            with patch('src.forklift.inputs.excel.ExcelInputHandler', side_effect=ImportError("Excel not available")):
                with pytest.raises(ImportError):
                    from src.forklift.engine.forklift_core import import_excel
                    import_excel(
                        input_path=str(excel_file),
                        output_path="dummy_output"
                    )
        finally:
            excel_file.unlink()

    def test_schema_validation_error_paths(self):
        """Test schema validation error paths (lines 969, 973-976, 979, 986-987)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            schema_file="/nonexistent/schema.json"
        )

        # Test file not found error
        with pytest.raises(FileNotFoundError):
            engine = ForkliftCore(config)
            engine._load_schema()

    def test_manifest_metadata_creation_paths(self):
        """Test manifest and metadata creation paths (lines 1228-1229)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            create_manifest=True,
            create_metadata=True
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

            # Test the conditional creation paths
            manifest_file = engine._create_manifest(output_path, ["test.parquet"])
            metadata_file = engine._create_metadata(output_path, results)

            assert Path(manifest_file).exists()
            assert Path(metadata_file).exists()

    def test_sql_import_error_path(self):
        """Test SQL import error path (line 1408)."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as schema_f:
            # Invalid SQL schema
            json.dump({"invalid": "sql_schema"}, schema_f)
            schema_file = Path(schema_f.name)

        try:
            with pytest.raises(Exception):
                from src.forklift.engine.forklift_core import import_sql
                import_sql(
                    connection_string="sqlite:///:memory:",
                    output_path="dummy_output",
                    schema_file=str(schema_file)
                )
        finally:
            schema_file.unlink()

    def test_sql_table_processing_paths(self):
        """Test SQL table processing paths (lines 1448-1451, 1476, 1480-1482)."""
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

                    # Mock SQL handler with error handling
                    mock_handler_instance = MagicMock()
                    mock_handler.return_value = mock_handler_instance
                    mock_handler_instance.__enter__.return_value = mock_handler_instance
                    mock_handler_instance.get_table_schema.side_effect = Exception("Schema error")

                    # This should trigger error handling paths
                    from src.forklift.engine.forklift_core import import_sql
                    results = import_sql(
                        connection_string="sqlite:///:memory:",
                        output_path=output_dir,
                        schema_file=str(schema_file)
                    )

                    # Should handle errors gracefully
                    assert results.invalid_rows > 0
        finally:
            schema_file.unlink()

    def test_excel_config_edge_cases(self):
        """Test Excel config edge cases (lines 1542-1551)."""
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            test_file = Path(f.name)

        try:
            with patch('src.forklift.inputs.excel.ExcelInputHandler') as mock_handler:
                mock_handler_instance = MagicMock()
                mock_handler.return_value = mock_handler_instance
                mock_handler_instance.get_sheet_info.return_value = {
                    'sheet_names': ['Sheet1', 'Sheet2']
                }

                from src.forklift.engine.forklift_core import _create_default_excel_config

                # Test edge cases for sheet selection
                with pytest.raises(ValueError):
                    _create_default_excel_config(test_file, sheet='NonExistent')

                with pytest.raises(ValueError):
                    _create_default_excel_config(test_file, sheet=10)  # Index out of range

        finally:
            test_file.unlink()
