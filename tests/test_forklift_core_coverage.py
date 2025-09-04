"""Tests to achieve 100% coverage for forklift_core.py missing lines."""

import pytest
import tempfile
import json
import os
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open
import pyarrow as pa

from forklift.engine.forklift_core import (
    ForkliftCore,
    ImportConfig,
    HeaderMode,
    ExcessColumnMode,
    import_csv,
    import_fwf,
    import_excel
)


class TestForkliftCoreMissingCoverage:
    """Test cases specifically targeting missing coverage lines in forklift_core.py."""

    def test_auto_detect_header_no_suitable_header(self):
        """Test auto header detection when no suitable header found (line 314-317)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            header_mode=HeaderMode.AUTO
        )
        engine = ForkliftCore(config)

        # Create a CSV file with only numeric data (no good header candidates)
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("1,2,3\n4,5,6\n7,8,9\n")
            test_file = Path(f.name)

        try:
            # This should default to first row when no header-like row is found
            header_idx, columns = engine._auto_detect_header(test_file)
            assert header_idx == 0
            assert columns == ['1', '2', '3']
        finally:
            test_file.unlink()

    def test_auto_detect_header_no_rows(self):
        """Test auto header detection with no rows (line 317)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            header_mode=HeaderMode.AUTO
        )
        engine = ForkliftCore(config)

        # Create an empty CSV file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("")
            test_file = Path(f.name)

        try:
            with pytest.raises(ValueError, match="Could not detect header row"):
                engine._auto_detect_header(test_file)
        finally:
            test_file.unlink()

    def test_looks_like_header_empty_row(self):
        """Test _looks_like_header with empty row (line 332)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output"
        )
        engine = ForkliftCore(config)

        result = engine._looks_like_header([])
        assert result is False

    def test_looks_like_header_numeric_row(self):
        """Test _looks_like_header with mostly numeric content (line 340, 344)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output"
        )
        engine = ForkliftCore(config)

        # Test row with only numbers (should not look like header)
        result = engine._looks_like_header(["1", "2", "3"])
        assert result is False

        # Test row with mix but more numbers than text
        result = engine._looks_like_header(["1", "2", "name"])
        assert result is False

    def test_column_mismatch_reader_empty_result(self):
        """Test column mismatch reader with empty result."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output"
        )
        engine = ForkliftCore(config)
        engine.column_names = ["id", "name"]

        # Create an empty CSV file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("")  # Empty file
            test_file = Path(f.name)

        try:
            batches = list(engine._handle_column_mismatch_reader(test_file, 0))
            # Should handle empty files gracefully
            assert len(batches) == 0
        finally:
            test_file.unlink()

    def test_column_mismatch_reader_excess_columns_truncate(self):
        """Test column mismatch reader with excess columns in truncate mode."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            excess_column_mode=ExcessColumnMode.TRUNCATE
        )
        engine = ForkliftCore(config)
        engine.column_names = ["id", "name"]

        # Create CSV with excess columns
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("1,John,Extra,Data\n2,Jane,More,Extra\n")
            test_file = Path(f.name)

        try:
            batches = list(engine._handle_column_mismatch_reader(test_file, 0))
            assert len(batches) >= 1
            # Should truncate excess columns
            batch = batches[0]
            assert batch.num_columns == 2
            assert len(batch) == 2
        finally:
            test_file.unlink()

    def test_column_mismatch_reader_excess_columns_reject(self):
        """Test column mismatch reader with excess columns in reject mode."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            excess_column_mode=ExcessColumnMode.REJECT
        )
        engine = ForkliftCore(config)
        engine.column_names = ["id", "name"]

        # Create CSV with excess columns
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("1,John,Extra\n2,Jane\n")  # First row has excess, second is normal
            test_file = Path(f.name)

        try:
            batches = list(engine._handle_column_mismatch_reader(test_file, 0))
            # Should only process the second row
            total_rows = sum(len(batch) for batch in batches)
            assert total_rows == 1  # Only the row without excess columns
        finally:
            test_file.unlink()

    def test_validate_batch_missing_field_attributes(self):
        """Test batch validation with missing field attributes (line 571-572)."""
        # Create a schema where we can test nullable attribute access
        schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=True)
        ])

        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            validate_schema=True
        )
        engine = ForkliftCore(config)
        engine.schema = schema

        # Create a batch with more columns than schema fields to test the bounds check
        batch = pa.RecordBatch.from_arrays([
            pa.array([1, 2]),
            pa.array(["Alice", "Bob"]),
            pa.array(["Extra", "Column"])  # Extra column beyond schema
        ], ["id", "name", "extra"])

        valid_batch, invalid_batch = engine._validate_batch(batch)

        # Should handle the extra column gracefully
        assert len(valid_batch) > 0

    def test_create_manifest_nonexistent_file(self):
        """Test manifest creation with nonexistent file (line 609)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output"
        )
        engine = ForkliftCore(config)

        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            # List includes a file that doesn't exist
            files = [str(output_dir / "nonexistent.parquet")]

            manifest_path = engine._create_manifest(output_dir, files)

            # Should create manifest even with nonexistent files (size will be 0)
            assert Path(manifest_path).exists()

            with open(manifest_path, 'r') as f:
                manifest = json.load(f)
                assert len(manifest["files"]) == 1
                assert manifest["files"][0]["file_size"] == 0

    def test_header_mode_enum_conversion(self):
        """Test string to enum conversion for header_mode (line 631)."""
        # Test with string header_mode that gets converted to enum
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            header_mode="auto"  # String instead of enum
        )
        engine = ForkliftCore(config)

        # Should convert string to enum
        assert isinstance(engine.config.header_mode, HeaderMode)
        assert engine.config.header_mode == HeaderMode.AUTO

    def test_create_batch_reader_empty_file(self):
        """Test batch reader with empty file (line 474)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output"
        )
        engine = ForkliftCore(config)
        engine.header_row_index = 0
        engine.column_names = ["id", "name"]

        # Create an empty file (size 0)
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            # Don't write anything - file will be empty
            test_file = Path(f.name)

        try:
            # The file is already empty (size 0), so this should trigger the empty file check
            batches = list(engine._create_batch_reader(test_file))
            assert len(batches) == 0
        finally:
            test_file.unlink()

    def test_pyarrow_csv_other_exceptions(self):
        """Test PyArrow CSV parsing with other exceptions (line 481, 486)."""
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
            # Mock PyArrow to raise a different exception
            with patch('pyarrow.csv.open_csv', side_effect=pa.ArrowInvalid("Unknown error")):
                with pytest.raises(pa.ArrowInvalid, match="Unknown error"):
                    list(engine._create_batch_reader(test_file))
        finally:
            test_file.unlink()

    def test_filtered_file_cleanup_exception(self):
        """Test filtered file cleanup with exception (line 492-493)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            footer_detection={"stop_on_blank": True}
        )
        engine = ForkliftCore(config)
        engine.header_row_index = 0
        engine.column_names = ["id", "name"]

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("id,name\n1,Alice\n\n")  # Has blank line for footer detection
            test_file = Path(f.name)

        try:
            # Mock Path.unlink to raise an exception during cleanup
            with patch.object(Path, 'unlink', side_effect=Exception("Cleanup failed")):
                # Should handle cleanup exception gracefully
                batches = list(engine._create_batch_reader(test_file))
                # Test should complete without raising the cleanup exception
                assert isinstance(batches, list)
        finally:
            test_file.unlink()

    def test_detect_header_row_absent_no_schema(self):
        """Test header detection with absent mode and no schema (line 263)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            header_mode=HeaderMode.ABSENT
        )
        engine = ForkliftCore(config)
        engine.schema = None  # No schema provided

        test_file = Path("dummy.csv")  # File won't be read in this case

        header_idx, columns = engine._detect_header_row(test_file)

        assert header_idx == -1
        assert columns == []

    def test_detect_header_row_absent_with_schema(self):
        """Test header detection with absent mode and schema (line 259)."""
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string())
        ])

        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            header_mode=HeaderMode.ABSENT
        )
        engine = ForkliftCore(config)
        engine.schema = schema

        test_file = Path("dummy.csv")  # File won't be read in this case

        header_idx, columns = engine._detect_header_row(test_file)

        assert header_idx == -1
        assert columns == ["id", "name"]

    def test_find_first_data_row_empty_row_check(self):
        """Test find_first_data_row with empty row handling (line 273)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output"
        )
        engine = ForkliftCore(config)

        # Create a CSV file with completely empty rows (not just whitespace)
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("\n")  # Completely empty line
            f.write("id,name\n")
            f.write("1,Alice\n")
            test_file = Path(f.name)

        try:
            header_idx, columns = engine._find_first_data_row(test_file)
            assert header_idx == 1  # Should skip the empty line
            assert columns == ['id', 'name']
        finally:
            test_file.unlink()

    def test_auto_detect_header_with_comments(self):
        """Test auto header detection with comments (line 304)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            header_mode=HeaderMode.AUTO,
            comment_rows=[r"#.*"]
        )
        engine = ForkliftCore(config)

        # Create a CSV file with comments that should be skipped
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("# This is a comment\n")
            f.write("id,name\n")
            f.write("1,Alice\n")
            test_file = Path(f.name)

        try:
            header_idx, columns = engine._auto_detect_header(test_file)
            assert header_idx == 1  # Should skip the comment
            assert columns == ['id', 'name']
        finally:
            test_file.unlink()

    def test_create_filtered_file_stop_iteration(self):
        """Test filtered file creation with StopIteration (line 387)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            footer_detection={"stop_on_blank": True}
        )
        engine = ForkliftCore(config)

        # Create a file where we'll mock the reader to raise StopIteration
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("id,name\n")
            test_file = Path(f.name)

        try:
            # Mock csv.reader to simulate StopIteration during skip_rows
            with patch('csv.reader') as mock_reader:
                mock_reader.return_value = iter([])  # Empty iterator causes StopIteration

                filtered_path = engine._create_filtered_file(test_file, 1)

                # Should handle StopIteration gracefully
                assert filtered_path.exists()
                filtered_path.unlink()  # Clean up
        finally:
            test_file.unlink()


class TestPublicAPIFunctions:
    """Test the public API functions for missing coverage."""

    def test_import_fwf_not_implemented(self):
        """Test import_fwf raises NotImplementedError (line 901)."""
        with pytest.raises(NotImplementedError, match="FWF import not yet implemented"):
            import_fwf("input.fwf", "output/")

    def test_import_excel_file_not_found(self):
        """Test import_excel raises FileNotFoundError when file doesn't exist."""
        with pytest.raises(FileNotFoundError, match="Input file not found"):
            import_excel("input.xlsx", "output/")

    def test_metadata_creation_with_enum_header_mode(self):
        """Test metadata creation with enum header_mode (line 791)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            header_mode=HeaderMode.PRESENT  # Use enum directly
        )
        engine = ForkliftCore(config)

        from src.forklift.engine.forklift_core import ProcessingResults
        results = ProcessingResults(total_rows=10, valid_rows=8, invalid_rows=2)

        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)

            metadata_path = engine._create_metadata(output_dir, results)

            # Should handle enum header_mode properly
            assert Path(metadata_path).exists()

            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
                assert metadata["input_config"]["header_mode"] == "present"


class TestRemainingMissingLines:
    """Additional tests to cover the last 9 missing lines in forklift_core.py."""

    def test_looks_like_header_empty_cells(self):
        """Test _looks_like_header with cells that are empty strings (line 340)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output"
        )
        engine = ForkliftCore(config)

        # Test row with empty cells that should be skipped in the counting logic
        result = engine._looks_like_header(["", "", ""])
        assert result is False

    def test_find_first_data_row_completely_empty_list(self):
        """Test find_first_data_row when CSV reader returns completely empty list (line 273)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output"
        )
        engine = ForkliftCore(config)

        # Create a CSV file that when read with csv.reader will have a completely empty row (not just empty strings)
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            # Write a line that will be parsed as an empty list by csv.reader
            f.write("id,name\n")  # Header
            test_file = Path(f.name)

        try:
            # Mock csv.reader to return an empty list for one iteration
            import csv
            original_reader = csv.reader

            def mock_reader(*args, **kwargs):
                reader = original_reader(*args, **kwargs)
                # Convert to list and inject an empty row
                rows = list(reader)
                return iter([[], *rows])  # Empty list first, then actual rows

            with patch('csv.reader', side_effect=mock_reader):
                header_idx, columns = engine._find_first_data_row(test_file)
                assert header_idx == 1  # Should skip the empty row and find the header
                assert columns == ['id', 'name']
        finally:
            test_file.unlink()

    def test_create_filtered_file_os_close_in_finally(self):
        """Test create_filtered_file finally block os.close call (line 387)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            footer_detection={"stop_on_blank": True}
        )
        engine = ForkliftCore(config)

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("id,name\n1,Alice\n")
            test_file = Path(f.name)

        try:
            # Mock tempfile.mkstemp to return a file descriptor
            with patch('tempfile.mkstemp') as mock_mkstemp:
                mock_fd = 123  # Mock file descriptor
                mock_path = "/tmp/test.csv"
                mock_mkstemp.return_value = (mock_fd, mock_path)

                # Mock the file operations
                with patch('builtins.open', mock_open(read_data="id,name\n1,Alice\n")):
                    with patch('csv.reader') as mock_reader:
                        mock_reader.return_value = iter([["id", "name"], ["1", "Alice"]])
                        with patch('csv.writer'):
                            with patch('os.close') as mock_close:
                                filtered_path = engine._create_filtered_file(test_file, 0)
                                # Verify os.close was called in the finally block
                                mock_close.assert_called_once_with(mock_fd)
        finally:
            test_file.unlink()

    def test_empty_csv_file_check_with_zero_size(self):
        """Test empty file check that triggers the early return (line 474)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output"
        )
        engine = ForkliftCore(config)
        engine.header_row_index = 0
        engine.column_names = ["id", "name"]

        # Create an empty file with zero bytes
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            # Don't write anything to ensure size is 0
            pass

        test_file = Path(f.name)

        try:
            # This should hit the st_size == 0 check and return empty iterator
            batches = list(engine._create_batch_reader(test_file))
            assert len(batches) == 0
        finally:
            test_file.unlink()

    def test_pyarrow_empty_csv_exception(self):
        """Test PyArrow CSV parsing with 'Empty CSV file' exception (line 481)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output"
        )
        engine = ForkliftCore(config)
        engine.header_row_index = 0
        engine.column_names = ["id", "name"]

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("id,name\n")
            test_file = Path(f.name)

        try:
            # Mock PyArrow to raise "Empty CSV file" exception
            with patch('pyarrow.csv.open_csv', side_effect=pa.ArrowInvalid("Empty CSV file")):
                batches = list(engine._create_batch_reader(test_file))
                # Should return empty iterator when "Empty CSV file" exception occurs
                assert len(batches) == 0
        finally:
            test_file.unlink()

    def test_manifest_creation_file_exists_check(self):
        """Test manifest creation when file exists vs doesn't exist (line 609)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output"
        )
        engine = ForkliftCore(config)

        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)

            # Create one file that exists and one that doesn't
            existing_file = output_dir / "existing.parquet"
            existing_file.write_text("dummy content")

            files = [
                str(existing_file),  # This exists
                str(output_dir / "nonexistent.parquet")  # This doesn't exist
            ]

            manifest_path = engine._create_manifest(output_dir, files)

            with open(manifest_path, 'r') as f:
                manifest = json.load(f)
                assert len(manifest["files"]) == 2
                # First file should have actual size, second should have 0
                assert manifest["files"][0]["file_size"] > 0
                assert manifest["files"][1]["file_size"] == 0

    def test_header_mode_string_not_enum(self):
        """Test when header_mode is already a string and doesn't need conversion (line 631)."""
        # Create config where header_mode is already a string but gets set as enum
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            header_mode="present"
        )

        # Manually override to test the isinstance check path
        engine = ForkliftCore(config)
        # The constructor converts string to enum, but let's test the check
        assert isinstance(engine.config.header_mode, HeaderMode)

    def test_metadata_hasattr_value_path(self):
        """Test metadata creation hasattr(header_mode_value, 'value') path (line 791)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            header_mode=HeaderMode.AUTO  # Use enum that has .value attribute
        )
        engine = ForkliftCore(config)

        from src.forklift.engine.forklift_core import ProcessingResults
        results = ProcessingResults(total_rows=5, valid_rows=5, invalid_rows=0)

        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)

            metadata_path = engine._create_metadata(output_dir, results)

            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
                # Should use the .value of the enum
                assert metadata["input_config"]["header_mode"] == "auto"

    def test_absent_header_mode_with_schema_field_access(self):
        """Test header detection absent mode accessing schema fields (line 259)."""
        schema = pa.schema([
            pa.field("col1", pa.int64()),
            pa.field("col2", pa.string()),
            pa.field("col3", pa.float64())
        ])

        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            header_mode=HeaderMode.ABSENT
        )
        engine = ForkliftCore(config)
        engine.schema = schema

        test_file = Path("dummy.csv")  # Won't be read

        header_idx, columns = engine._detect_header_row(test_file)

        # Should extract field names from schema
        assert header_idx == -1
        assert columns == ["col1", "col2", "col3"]


class TestFinalMissingLines:
    """Tests to cover the final 7 missing lines in forklift_core.py."""

    def test_schema_field_name_extraction_direct(self):
        """Test direct field.name access for schema fields (line 259)."""
        schema = pa.schema([
            pa.field("user_id", pa.int64()),
            pa.field("username", pa.string()),
            pa.field("email", pa.string())
        ])

        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            header_mode=HeaderMode.ABSENT
        )
        engine = ForkliftCore(config)
        engine.schema = schema

        # This should trigger the field.name access for each field in the schema
        header_idx, columns = engine._detect_header_row(Path("dummy.csv"))

        assert header_idx == -1
        assert columns == ["user_id", "username", "email"]

    def test_csv_reader_empty_row_detection(self):
        """Test CSV reader encountering a truly empty row (line 273)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output"
        )
        engine = ForkliftCore(config)

        # Create a file with an empty line that csv.reader will parse as []
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("\n")  # First line is empty (will be parsed as [])
            f.write("col1,col2\n")  # Second line has content
            test_file = Path(f.name)

        try:
            header_idx, columns = engine._find_first_data_row(test_file)
            assert header_idx == 1  # Should skip empty row and find header at index 1
            assert columns == ['col1', 'col2']
        finally:
            test_file.unlink()

    def test_os_close_finally_block_execution(self):
        """Test that os.close is called in the finally block (line 387)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            footer_detection={"stop_on_blank": True}
        )
        engine = ForkliftCore(config)

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("data\n")
            test_file = Path(f.name)

        try:
            with patch('tempfile.mkstemp') as mock_mkstemp:
                mock_fd = 456
                mock_mkstemp.return_value = (mock_fd, "/tmp/filtered.csv")

                with patch('builtins.open', mock_open(read_data="data\n")):
                    with patch('csv.reader', return_value=iter([["data"]])):
                        with patch('csv.writer'):
                            with patch('os.close') as mock_close:
                                filtered_path = engine._create_filtered_file(test_file, 0)
                                # The os.close should be called in the finally block
                                mock_close.assert_called_once_with(mock_fd)
        finally:
            test_file.unlink()

    def test_file_stat_size_zero_check(self):
        """Test file.stat().st_size == 0 check (line 474)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output"
        )
        engine = ForkliftCore(config)
        engine.header_row_index = 0
        engine.column_names = ["col1"]

        # Create a truly empty file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as f:
            test_file = Path(f.name)

        try:
            # Verify the file is actually empty
            assert test_file.stat().st_size == 0

            # This should trigger the early return for empty files
            batches = list(engine._create_batch_reader(test_file))
            assert len(batches) == 0
        finally:
            test_file.unlink()

    def test_path_exists_check_in_manifest(self):
        """Test Path(f).exists() check in manifest creation (line 609)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output"
        )
        engine = ForkliftCore(config)

        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)

            # Create a file that exists
            real_file = output_dir / "real.parquet"
            real_file.write_bytes(b"test data")

            # Reference both existing and non-existing files
            files = [str(real_file), str(output_dir / "fake.parquet")]

            manifest_path = engine._create_manifest(output_dir, files)

            with open(manifest_path, 'r') as f:
                manifest = json.load(f)
                # Should have both files, one with size > 0, one with size 0
                assert len(manifest["files"]) == 2
                assert manifest["files"][0]["file_size"] > 0  # real file
                assert manifest["files"][1]["file_size"] == 0  # fake file

    def test_isinstance_header_mode_string_check(self):
        """Test isinstance(self.config.header_mode, str) check (line 631)."""
        # Create a config and manually set header_mode to a string after construction
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            header_mode=HeaderMode.PRESENT
        )

        # Manually override the header_mode to be a string to test the isinstance check
        config.header_mode = "auto"  # Set as string

        engine = ForkliftCore(config)
        # The constructor should convert the string to enum
        assert isinstance(engine.config.header_mode, HeaderMode)
        assert engine.config.header_mode == HeaderMode.AUTO

    def test_hasattr_header_mode_value_attribute(self):
        """Test hasattr(header_mode_value, 'value') check (line 791)."""
        config = ImportConfig(
            input_path="dummy.csv",
            output_path="dummy_output",
            header_mode=HeaderMode.ABSENT  # Use enum with .value attribute
        )
        engine = ForkliftCore(config)

        from src.forklift.engine.forklift_core import ProcessingResults
        results = ProcessingResults(total_rows=3, valid_rows=3, invalid_rows=0)

        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)

            metadata_path = engine._create_metadata(output_dir, results)

            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
                # Should access the .value attribute of the enum
                assert metadata["input_config"]["header_mode"] == "absent"

