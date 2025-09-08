"""Comprehensive tests for forklift core engine."""

import pytest
import pyarrow as pa
from unittest.mock import patch, MagicMock, mock_open
from pathlib import Path

from forklift.engine.forklift_core import (
    ForkliftCore,
    ImportConfig,
    HeaderMode
)
from forklift.engine.config.processing_results import ProcessingResults


class TestHeaderMode:
    """Test HeaderMode enum."""

    def test_header_mode_values(self):
        """Test HeaderMode enum values."""
        assert HeaderMode.PRESENT.value == "present"
        assert HeaderMode.AUTO.value == "auto"
        assert HeaderMode.ABSENT.value == "absent"


class TestForkliftCore:
    """Test ForkliftCore class."""

    def test_init(self):
        """Test ForkliftCore initialization."""
        core = ForkliftCore()
        assert core is not None

    @patch('forklift.engine.forklift_core.CsvImporter')
    def test_process_csv_basic(self, mock_csv_importer):
        """Test basic CSV processing."""
        # Mock the CSV importer
        mock_importer_instance = MagicMock()
        mock_importer_instance.process.return_value = ProcessingResults(
            total_rows=100,
            valid_rows=95,
            invalid_rows=5,
            output_files=['output.parquet']
        )
        mock_csv_importer.return_value = mock_importer_instance

        core = ForkliftCore()

        config = ImportConfig(
            source_path="test.csv",
            dest_path="output/",
            input_kind="csv"
        )

        result = core.process(config)

        assert isinstance(result, ProcessingResults)
        assert result.total_rows == 100
        assert result.valid_rows == 95
        mock_csv_importer.assert_called_once()
        mock_importer_instance.process.assert_called_once()

    @patch('forklift.engine.forklift_core.ExcelImporter')
    def test_process_excel_basic(self, mock_excel_importer):
        """Test basic Excel processing."""
        mock_importer_instance = MagicMock()
        mock_importer_instance.process.return_value = ProcessingResults(
            total_rows=50,
            valid_rows=50,
            invalid_rows=0,
            output_files=['output.parquet']
        )
        mock_excel_importer.return_value = mock_importer_instance

        core = ForkliftCore()

        config = ImportConfig(
            source_path="test.xlsx",
            dest_path="output/",
            input_kind="excel"
        )

        result = core.process(config)

        assert isinstance(result, ProcessingResults)
        assert result.total_rows == 50
        mock_excel_importer.assert_called_once()

    @patch('forklift.engine.forklift_core.SqlImporter')
    def test_process_sql_basic(self, mock_sql_importer):
        """Test basic SQL processing."""
        mock_importer_instance = MagicMock()
        mock_importer_instance.process.return_value = ProcessingResults(
            total_rows=200,
            valid_rows=200,
            invalid_rows=0,
            output_files=['output.parquet']
        )
        mock_sql_importer.return_value = mock_importer_instance

        core = ForkliftCore()

        config = ImportConfig(
            source_path="SELECT * FROM table",
            dest_path="output/",
            input_kind="sql"
        )

        result = core.process(config)

        assert isinstance(result, ProcessingResults)
        assert result.total_rows == 200
        mock_sql_importer.assert_called_once()

    def test_process_unsupported_input_kind(self):
        """Test processing with unsupported input kind."""
        core = ForkliftCore()

        config = ImportConfig(
            source_path="test.xml",
            dest_path="output/",
            input_kind="xml"  # Unsupported
        )

        with pytest.raises(ValueError, match="Unsupported input kind"):
            core.process(config)

    @patch('forklift.engine.forklift_core.CsvImporter')
    def test_process_with_schema(self, mock_csv_importer):
        """Test processing with schema configuration."""
        mock_importer_instance = MagicMock()
        mock_importer_instance.process.return_value = ProcessingResults(
            total_rows=100,
            valid_rows=100,
            invalid_rows=0,
            output_files=['output.parquet']
        )
        mock_csv_importer.return_value = mock_importer_instance

        core = ForkliftCore()

        config = ImportConfig(
            source_path="test.csv",
            dest_path="output/",
            input_kind="csv",
            schema_path="schema.json"
        )

        result = core.process(config)

        assert isinstance(result, ProcessingResults)
        mock_csv_importer.assert_called_once()

        # Verify schema was passed to importer
        call_args = mock_csv_importer.call_args
        assert 'schema_path' in call_args[1] or call_args[0]

    @patch('forklift.engine.forklift_core.CsvImporter')
    def test_process_with_header_modes(self, mock_csv_importer):
        """Test processing with different header modes."""
        mock_importer_instance = MagicMock()
        mock_importer_instance.process.return_value = ProcessingResults(
            total_rows=100,
            valid_rows=100,
            invalid_rows=0,
            output_files=['output.parquet']
        )
        mock_csv_importer.return_value = mock_importer_instance

        core = ForkliftCore()

        header_modes = [HeaderMode.PRESENT, HeaderMode.AUTO, HeaderMode.ABSENT]

        for header_mode in header_modes:
            config = ImportConfig(
                source_path="test.csv",
                dest_path="output/",
                input_kind="csv",
                header_mode=header_mode
            )

            result = core.process(config)
            assert isinstance(result, ProcessingResults)

    @patch('forklift.engine.forklift_core.CsvImporter')
    def test_process_with_preprocessors(self, mock_csv_importer):
        """Test processing with preprocessors."""
        mock_importer_instance = MagicMock()
        mock_importer_instance.process.return_value = ProcessingResults(
            total_rows=100,
            valid_rows=100,
            invalid_rows=0,
            output_files=['output.parquet']
        )
        mock_csv_importer.return_value = mock_importer_instance

        core = ForkliftCore()

        config = ImportConfig(
            source_path="test.csv",
            dest_path="output/",
            input_kind="csv",
            preprocessors=["clean", "validate"]
        )

        result = core.process(config)

        assert isinstance(result, ProcessingResults)
        mock_csv_importer.assert_called_once()

    @patch('forklift.engine.forklift_core.CsvImporter')
    def test_process_with_encoding_priority(self, mock_csv_importer):
        """Test processing with encoding priority."""
        mock_importer_instance = MagicMock()
        mock_importer_instance.process.return_value = ProcessingResults(
            total_rows=100,
            valid_rows=100,
            invalid_rows=0,
            output_files=['output.parquet']
        )
        mock_csv_importer.return_value = mock_importer_instance

        core = ForkliftCore()

        config = ImportConfig(
            source_path="test.csv",
            dest_path="output/",
            input_kind="csv",
            encoding_priority=["utf-8", "latin-1", "cp1252"]
        )

        result = core.process(config)

        assert isinstance(result, ProcessingResults)
        mock_csv_importer.assert_called_once()

    @patch('forklift.engine.forklift_core.CsvImporter')
    def test_process_with_delimiter(self, mock_csv_importer):
        """Test processing with custom delimiter."""
        mock_importer_instance = MagicMock()
        mock_importer_instance.process.return_value = ProcessingResults(
            total_rows=100,
            valid_rows=100,
            invalid_rows=0,
            output_files=['output.parquet']
        )
        mock_csv_importer.return_value = mock_importer_instance

        core = ForkliftCore()

        config = ImportConfig(
            source_path="test.csv",
            dest_path="output/",
            input_kind="csv",
            delimiter="|"
        )

        result = core.process(config)

        assert isinstance(result, ProcessingResults)
        mock_csv_importer.assert_called_once()

    @patch('forklift.engine.forklift_core.ExcelImporter')
    def test_process_excel_with_sheet(self, mock_excel_importer):
        """Test Excel processing with specific sheet."""
        mock_importer_instance = MagicMock()
        mock_importer_instance.process.return_value = ProcessingResults(
            total_rows=75,
            valid_rows=75,
            invalid_rows=0,
            output_files=['output.parquet']
        )
        mock_excel_importer.return_value = mock_importer_instance

        core = ForkliftCore()

        config = ImportConfig(
            source_path="test.xlsx",
            dest_path="output/",
            input_kind="excel",
            sheet="Sheet2"
        )

        result = core.process(config)

        assert isinstance(result, ProcessingResults)
        mock_excel_importer.assert_called_once()

    @patch('forklift.engine.forklift_core.CsvImporter')
    def test_process_error_handling(self, mock_csv_importer):
        """Test error handling during processing."""
        mock_csv_importer.side_effect = Exception("Processing failed")

        core = ForkliftCore()

        config = ImportConfig(
            source_path="test.csv",
            dest_path="output/",
            input_kind="csv"
        )

        with pytest.raises(Exception, match="Processing failed"):
            core.process(config)

    @patch('forklift.engine.forklift_core.is_s3_path')
    @patch('forklift.engine.forklift_core.CsvImporter')
    def test_process_s3_paths(self, mock_csv_importer, mock_is_s3_path):
        """Test processing with S3 paths."""
        mock_is_s3_path.return_value = True

        mock_importer_instance = MagicMock()
        mock_importer_instance.process.return_value = ProcessingResults(
            total_rows=100,
            valid_rows=100,
            invalid_rows=0,
            output_files=['s3://bucket/output.parquet']
        )
        mock_csv_importer.return_value = mock_importer_instance

        core = ForkliftCore()

        config = ImportConfig(
            source_path="s3://bucket/test.csv",
            dest_path="s3://bucket/output/",
            input_kind="csv"
        )

        result = core.process(config)

        assert isinstance(result, ProcessingResults)
        mock_is_s3_path.assert_called()
        mock_csv_importer.assert_called_once()

    def test_validate_config_valid(self):
        """Test config validation with valid configuration."""
        core = ForkliftCore()

        config = ImportConfig(
            source_path="test.csv",
            dest_path="output/",
            input_kind="csv"
        )

        # Should not raise exception
        core._validate_config(config)

    def test_validate_config_missing_source(self):
        """Test config validation with missing source path."""
        core = ForkliftCore()

        config = ImportConfig(
            source_path="",
            dest_path="output/",
            input_kind="csv"
        )

        with pytest.raises(ValueError, match="Source path is required"):
            core._validate_config(config)

    def test_validate_config_missing_dest(self):
        """Test config validation with missing destination path."""
        core = ForkliftCore()

        config = ImportConfig(
            source_path="test.csv",
            dest_path="",
            input_kind="csv"
        )

        with pytest.raises(ValueError, match="Destination path is required"):
            core._validate_config(config)

    def test_validate_config_missing_input_kind(self):
        """Test config validation with missing input kind."""
        core = ForkliftCore()

        config = ImportConfig(
            source_path="test.csv",
            dest_path="output/",
            input_kind=""
        )

        with pytest.raises(ValueError, match="Input kind is required"):
            core._validate_config(config)

    @patch('forklift.engine.forklift_core.Path.exists')
    def test_validate_config_schema_file_not_found(self, mock_exists):
        """Test config validation with non-existent schema file."""
        mock_exists.return_value = False

        core = ForkliftCore()

        config = ImportConfig(
            source_path="test.csv",
            dest_path="output/",
            input_kind="csv",
            schema_path="nonexistent_schema.json"
        )

        with pytest.raises(ValueError, match="Schema file not found"):
            core._validate_config(config)

    @patch('forklift.engine.forklift_core.is_s3_path')
    @patch('forklift.engine.forklift_core.Path.exists')
    def test_validate_config_s3_schema_path(self, mock_exists, mock_is_s3_path):
        """Test config validation with S3 schema path."""
        mock_is_s3_path.return_value = True
        mock_exists.return_value = False  # Shouldn't be called for S3 paths

        core = ForkliftCore()

        config = ImportConfig(
            source_path="s3://bucket/test.csv",
            dest_path="s3://bucket/output/",
            input_kind="csv",
            schema_path="s3://bucket/schema.json"
        )

        # Should not raise exception for S3 schema paths
        core._validate_config(config)
        mock_exists.assert_not_called()


class TestImportConfig:
    """Test ImportConfig dataclass."""

    def test_default_config(self):
        """Test default import configuration."""
        config = ImportConfig(
            source_path="test.csv",
            dest_path="output/",
            input_kind="csv"
        )

        assert config.source_path == "test.csv"
        assert config.dest_path == "output/"
        assert config.input_kind == "csv"
        assert config.schema_path is None
        assert config.preprocessors == []
        assert config.encoding_priority == ["utf-8-sig", "utf-8", "latin-1"]
        assert config.delimiter is None
        assert config.sheet is None
        assert config.fwf_spec is None
        assert config.header_mode == HeaderMode.PRESENT

    def test_custom_config(self):
        """Test custom import configuration."""
        config = ImportConfig(
            source_path="data.xlsx",
            dest_path="processed/",
            input_kind="excel",
            schema_path="schema.json",
            preprocessors=["clean", "validate"],
            encoding_priority=["utf-8", "latin-1"],
            delimiter=";",
            sheet="DataSheet",
            fwf_spec="fwf.json",
            header_mode=HeaderMode.AUTO
        )

        assert config.source_path == "data.xlsx"
        assert config.dest_path == "processed/"
        assert config.input_kind == "excel"
        assert config.schema_path == "schema.json"
        assert config.preprocessors == ["clean", "validate"]
        assert config.encoding_priority == ["utf-8", "latin-1"]
        assert config.delimiter == ";"
        assert config.sheet == "DataSheet"
        assert config.fwf_spec == "fwf.json"
        assert config.header_mode == HeaderMode.AUTO


class TestForkliftCoreIntegration:
    """Test ForkliftCore integration scenarios."""

    @patch('forklift.engine.forklift_core.CsvImporter')
    def test_complete_csv_workflow(self, mock_csv_importer):
        """Test complete CSV processing workflow."""
        mock_importer_instance = MagicMock()
        mock_importer_instance.process.return_value = ProcessingResults(
            total_rows=1000,
            valid_rows=950,
            invalid_rows=50,
            output_files=['output.parquet'],
            processing_time=5.2,
            warnings=['Some data quality issues detected']
        )
        mock_csv_importer.return_value = mock_importer_instance

        core = ForkliftCore()

        config = ImportConfig(
            source_path="large_dataset.csv",
            dest_path="processed/",
            input_kind="csv",
            schema_path="validation_schema.json",
            preprocessors=["clean", "validate", "transform"],
            encoding_priority=["utf-8", "latin-1"],
            delimiter=",",
            header_mode=HeaderMode.PRESENT
        )

        result = core.process(config)

        assert result.total_rows == 1000
        assert result.valid_rows == 950
        assert result.invalid_rows == 50
        assert len(result.output_files) == 1
        assert result.processing_time == 5.2
        assert len(result.warnings) == 1

    def test_module_imports(self):
        """Test that all components can be imported."""
        from forklift.engine.forklift_core import (
            ForkliftCore,
            ImportConfig,
            HeaderMode
        )

        assert ForkliftCore is not None
        assert ImportConfig is not None
        assert HeaderMode is not None

    def test_module_docstring(self):
        """Test module documentation."""
        import forklift.engine.forklift_core as core_module

        assert core_module.__doc__ is not None

    @patch('forklift.engine.forklift_core.CsvImporter')
    @patch('forklift.engine.forklift_core.ExcelImporter')
    @patch('forklift.engine.forklift_core.SqlImporter')
    def test_all_input_types(self, mock_sql, mock_excel, mock_csv):
        """Test that all input types can be processed."""
        # Mock all importers
        for mock_importer in [mock_sql, mock_excel, mock_csv]:
            mock_instance = MagicMock()
            mock_instance.process.return_value = ProcessingResults(
                total_rows=100,
                valid_rows=100,
                invalid_rows=0,
                output_files=['output.parquet']
            )
            mock_importer.return_value = mock_instance

        core = ForkliftCore()

        input_types = [
            ("test.csv", "csv"),
            ("test.xlsx", "excel"),
            ("SELECT * FROM table", "sql")
        ]

        for source_path, input_kind in input_types:
            config = ImportConfig(
                source_path=source_path,
                dest_path="output/",
                input_kind=input_kind
            )

            result = core.process(config)
            assert isinstance(result, ProcessingResults)

    def test_error_recovery_scenarios(self):
        """Test error recovery in various scenarios."""
        core = ForkliftCore()

        # Test with invalid configuration
        invalid_configs = [
            ImportConfig("", "output/", "csv"),  # Empty source
            ImportConfig("test.csv", "", "csv"),  # Empty dest
            ImportConfig("test.csv", "output/", ""),  # Empty input_kind
        ]

        for config in invalid_configs:
            with pytest.raises(ValueError):
                core.process(config)
