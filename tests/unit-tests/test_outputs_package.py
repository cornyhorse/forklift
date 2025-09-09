"""Comprehensive tests for the outputs package to achieve 100% coverage."""

import json
import tempfile
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
import pyarrow as pa
import pyarrow.parquet as pq

from forklift.outputs.config import OutputConfig
from forklift.outputs.manifest import ManifestGenerator
from forklift.outputs.metadata import MetadataGenerator
from forklift.outputs.parquet import ParquetOutputHandler


class TestOutputConfig:
    """Test the OutputConfig dataclass."""

    def test_output_config_defaults(self):
        """Test OutputConfig with default values."""
        config = OutputConfig()

        assert config.compression == "snappy"
        assert config.create_manifest is True
        assert config.create_metadata is True
        assert config.row_group_size == 50000

    def test_output_config_custom_values(self):
        """Test OutputConfig with custom values."""
        config = OutputConfig(
            compression="gzip",
            create_manifest=False,
            create_metadata=False,
            row_group_size=100000
        )

        assert config.compression == "gzip"
        assert config.create_manifest is False
        assert config.create_metadata is False
        assert config.row_group_size == 100000


class TestManifestGenerator:
    """Test the ManifestGenerator class."""

    def test_create_manifest_with_valid_files(self):
        """Test creating manifest with valid parquet files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create test parquet files
            test_files = []
            for i in range(2):
                file_path = temp_path / f"test_file_{i}.parquet"

                # Create test data
                schema = pa.schema([
                    pa.field("id", pa.int64()),
                    pa.field("name", pa.string()),
                ])

                data = pa.table({
                    "id": [1, 2, 3],
                    "name": ["Alice", "Bob", "Charlie"]
                })

                # Write parquet file
                pq.write_table(data, file_path)
                test_files.append(str(file_path))

            # Create manifest
            manifest_path = ManifestGenerator.create_manifest(temp_path, test_files)

            # Verify manifest was created
            assert Path(manifest_path).exists()
            assert Path(manifest_path).name == "manifest.json"

            # Verify manifest contents
            with open(manifest_path, 'r') as f:
                manifest = json.load(f)

            assert manifest["format_version"] == "1.0"
            assert manifest["total_files"] == 2
            assert len(manifest["files"]) == 2
            assert "created_at" in manifest
            assert "total_size" in manifest

            # Check each file entry
            for file_entry in manifest["files"]:
                assert "file_path" in file_entry
                assert "file_size" in file_entry
                assert "record_count" in file_entry
                assert file_entry["record_count"] == 3  # 3 rows in our test data

    def test_create_manifest_with_nonexistent_files(self):
        """Test creating manifest with non-existent files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Use non-existent files
            test_files = [
                str(temp_path / "nonexistent1.parquet"),
                str(temp_path / "nonexistent2.parquet")
            ]

            # Create manifest
            manifest_path = ManifestGenerator.create_manifest(temp_path, test_files)

            # Verify manifest was created
            assert Path(manifest_path).exists()

            # Verify manifest contents
            with open(manifest_path, 'r') as f:
                manifest = json.load(f)

            assert manifest["total_files"] == 2
            assert manifest["total_size"] == 0  # No files exist

            # Check file entries have zero size and record count
            for file_entry in manifest["files"]:
                assert file_entry["file_size"] == 0
                assert file_entry["record_count"] == 0

    def test_get_parquet_row_count_valid_file(self):
        """Test _get_parquet_row_count with valid parquet file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "test.parquet"

            # Create test parquet file
            data = pa.table({
                "id": [1, 2, 3, 4, 5],
                "value": ["a", "b", "c", "d", "e"]
            })
            pq.write_table(data, file_path)

            # Test row count extraction
            row_count = ManifestGenerator._get_parquet_row_count(str(file_path))
            assert row_count == 5

    def test_get_parquet_row_count_invalid_file(self):
        """Test _get_parquet_row_count with invalid/non-existent file."""
        # Test with non-existent file
        row_count = ManifestGenerator._get_parquet_row_count("nonexistent.parquet")
        assert row_count == 0

        # Test with invalid parquet file
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            f.write(b"not a parquet file")
            f.flush()

            row_count = ManifestGenerator._get_parquet_row_count(f.name)
            assert row_count == 0

    def test_get_parquet_row_count_exception_handling(self):
        """Test _get_parquet_row_count exception handling."""
        with patch('pyarrow.parquet.ParquetFile') as mock_parquet_file:
            mock_parquet_file.side_effect = Exception("Simulated error")

            row_count = ManifestGenerator._get_parquet_row_count("any_file.parquet")
            assert row_count == 0


class TestMetadataGenerator:
    """Test the MetadataGenerator class."""

    def test_create_metadata_basic(self):
        """Test creating metadata with basic processing stats."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            processing_stats = {
                "processing_summary": {
                    "total_records": 1000,
                    "valid_records": 950,
                    "invalid_records": 50
                },
                "input_config": {
                    "source_file": "input.csv",
                    "delimiter": ","
                },
                "output_files": []
            }

            # Create metadata
            metadata_path = MetadataGenerator.create_metadata(temp_path, processing_stats)

            # Verify metadata was created
            assert Path(metadata_path).exists()
            assert Path(metadata_path).name == "metadata.json"

            # Verify metadata contents
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)

            assert metadata["processing_summary"] == processing_stats["processing_summary"]
            assert metadata["input_config"] == processing_stats["input_config"]
            assert metadata["output_files"] == []
            assert metadata["column_statistics"] == {}
            assert metadata["metadata_version"] == "1.0"
            assert "created_at" in metadata

    def test_create_metadata_with_output_files(self):
        """Test creating metadata with actual parquet output files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create test parquet file
            output_file = temp_path / "output.parquet"
            data = pa.table({
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35]
            })
            pq.write_table(data, output_file)

            processing_stats = {
                "processing_summary": {"total_records": 3},
                "input_config": {"source": "test.csv"},
                "output_files": [str(output_file)]
            }

            # Create metadata
            metadata_path = MetadataGenerator.create_metadata(temp_path, processing_stats)

            # Verify metadata contents
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)

            assert metadata["output_files"] == [str(output_file)]
            assert "column_statistics" in metadata

            # Verify column statistics for the parquet file
            file_stats = metadata["column_statistics"]["output.parquet"]
            assert file_stats["num_columns"] == 3
            assert file_stats["num_rows"] == 3
            assert file_stats["column_names"] == ["id", "name", "age"]
            assert len(file_stats["column_types"]) == 3

    def test_create_metadata_with_nonexistent_files(self):
        """Test creating metadata with non-existent output files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            processing_stats = {
                "processing_summary": {"total_records": 0},
                "input_config": {},
                "output_files": [
                    str(temp_path / "nonexistent.parquet"),
                    str(temp_path / "also_missing.parquet")
                ]
            }

            # Create metadata
            metadata_path = MetadataGenerator.create_metadata(temp_path, processing_stats)

            # Verify metadata contents
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)

            # Should still list the files even if they don't exist
            assert len(metadata["output_files"]) == 2
            # But no column statistics since files don't exist
            assert metadata["column_statistics"] == {}

    def test_create_metadata_with_invalid_parquet_files(self):
        """Test creating metadata with invalid parquet files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create invalid parquet file
            invalid_file = temp_path / "invalid.parquet"
            with open(invalid_file, 'w') as f:
                f.write("not a parquet file")

            processing_stats = {
                "processing_summary": {},
                "input_config": {},
                "output_files": [str(invalid_file)]
            }

            # Create metadata - should handle the invalid file gracefully
            metadata_path = MetadataGenerator.create_metadata(temp_path, processing_stats)

            # Verify metadata contents
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)

            # Should list the file but skip column statistics due to read error
            assert len(metadata["output_files"]) == 1
            assert metadata["column_statistics"] == {}

    def test_create_metadata_with_non_parquet_files(self):
        """Test creating metadata with non-parquet files in output list."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create non-parquet file
            csv_file = temp_path / "output.csv"
            with open(csv_file, 'w') as f:
                f.write("id,name\n1,Alice\n2,Bob\n")

            processing_stats = {
                "processing_summary": {},
                "input_config": {},
                "output_files": [str(csv_file)]
            }

            # Create metadata
            metadata_path = MetadataGenerator.create_metadata(temp_path, processing_stats)

            # Verify metadata contents
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)

            # Should list the file but skip column statistics for non-parquet files
            assert len(metadata["output_files"]) == 1
            assert metadata["column_statistics"] == {}

    def test_create_metadata_missing_fields(self):
        """Test creating metadata with missing fields in processing_stats."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Empty processing stats
            processing_stats = {}

            # Create metadata
            metadata_path = MetadataGenerator.create_metadata(temp_path, processing_stats)

            # Verify metadata contents with defaults
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)

            assert metadata["processing_summary"] == {}
            assert metadata["input_config"] == {}
            assert metadata["output_files"] == []
            assert metadata["column_statistics"] == {}
            assert metadata["metadata_version"] == "1.0"
            assert "created_at" in metadata


class TestParquetOutputHandler:
    """Test the ParquetOutputHandler class."""

    def test_parquet_handler_initialization(self):
        """Test ParquetOutputHandler initialization."""
        config = OutputConfig(compression="gzip", row_group_size=10000)
        handler = ParquetOutputHandler(config)

        assert handler.config == config
        assert handler.writers == {}

    def test_create_writer(self):
        """Test creating a ParquetWriter."""
        config = OutputConfig()
        handler = ParquetOutputHandler(config)

        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "test.parquet"
            schema = pa.schema([
                pa.field("id", pa.int64()),
                pa.field("name", pa.string())
            ])

            # Create writer
            writer = handler.create_writer(file_path, schema)

            # Verify writer was created and stored
            assert isinstance(writer, pq.ParquetWriter)
            assert str(file_path) in handler.writers
            assert handler.writers[str(file_path)] == writer

            # Clean up
            writer.close()

    def test_write_batch_with_data(self):
        """Test writing a batch with data."""
        config = OutputConfig()
        handler = ParquetOutputHandler(config)

        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "test.parquet"
            schema = pa.schema([
                pa.field("id", pa.int64()),
                pa.field("name", pa.string())
            ])

            # Create writer
            writer = handler.create_writer(file_path, schema)

            # Create test batch
            batch = pa.record_batch([
                pa.array([1, 2, 3]),
                pa.array(["Alice", "Bob", "Charlie"])
            ], schema=schema)

            # Write batch
            handler.write_batch(writer, batch)

            # Close writer to finalize file
            writer.close()

            # Verify file was written
            assert file_path.exists()

            # Verify contents
            table = pq.read_table(file_path)
            assert table.num_rows == 3
            assert table.num_columns == 2

    def test_write_batch_empty_batch(self):
        """Test writing an empty batch (should be skipped)."""
        config = OutputConfig()
        handler = ParquetOutputHandler(config)

        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "test.parquet"
            schema = pa.schema([
                pa.field("id", pa.int64()),
                pa.field("name", pa.string())
            ])

            # Create writer
            writer = handler.create_writer(file_path, schema)

            # Create empty batch
            empty_batch = pa.record_batch([
                pa.array([], type=pa.int64()),
                pa.array([], type=pa.string())
            ], schema=schema)

            # Write empty batch (should be skipped)
            handler.write_batch(writer, empty_batch)

            # Close writer
            writer.close()

            # File should exist but be empty
            assert file_path.exists()
            table = pq.read_table(file_path)
            assert table.num_rows == 0

    def test_close_all_writers(self):
        """Test closing all writers."""
        config = OutputConfig()
        handler = ParquetOutputHandler(config)

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create multiple writers
            writers = []
            for i in range(3):
                file_path = Path(temp_dir) / f"test_{i}.parquet"
                schema = pa.schema([pa.field("id", pa.int64())])
                writer = handler.create_writer(file_path, schema)
                writers.append(writer)

            # Verify writers are stored
            assert len(handler.writers) == 3

            # Close all writers
            handler.close_all_writers()

            # Verify writers dictionary is cleared
            assert len(handler.writers) == 0

    def test_parquet_writer_with_custom_config(self):
        """Test ParquetWriter creation with custom configuration."""
        config = OutputConfig(
            compression="gzip",
            row_group_size=1000
        )
        handler = ParquetOutputHandler(config)

        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "test.parquet"
            schema = pa.schema([pa.field("id", pa.int64())])

            # Mock ParquetWriter to verify config is passed correctly
            with patch('pyarrow.parquet.ParquetWriter') as mock_writer:
                mock_instance = MagicMock()
                mock_writer.return_value = mock_instance

                writer = handler.create_writer(file_path, schema)

                # Verify ParquetWriter was called with correct parameters
                mock_writer.assert_called_once_with(
                    file_path,
                    schema,
                    compression="gzip"
                )

                assert writer == mock_instance
                assert str(file_path) in handler.writers
