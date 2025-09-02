"""Tests for the CLI module."""
import pytest
from unittest.mock import patch, MagicMock
from io import StringIO

from forklift.cli import main
from forklift.engine.forklift_core import HeaderMode


class TestCLI:
    """Test cases for the CLI functionality."""

    def test_main_no_args(self):
        """Test that main() exits with error when no arguments provided."""
        with patch('sys.argv', ['forklift']):
            with pytest.raises(SystemExit):
                main()

    def test_main_help(self):
        """Test that main() shows help when --help is provided."""
        with patch('sys.argv', ['forklift', '--help']):
            with pytest.raises(SystemExit):
                main()

    def test_ingest_help(self):
        """Test that ingest subcommand shows help."""
        with patch('sys.argv', ['forklift', 'ingest', '--help']):
            with pytest.raises(SystemExit):
                main()

    @patch('forklift.cli.ForkliftCore')
    @patch('forklift.cli.is_s3_path')
    def test_ingest_csv_local_paths(self, mock_is_s3_path, mock_forklift_core):
        """Test CSV ingest with local file paths."""
        # Setup mocks
        mock_is_s3_path.return_value = False
        mock_core_instance = MagicMock()
        mock_result = MagicMock()
        mock_result.total_rows = 100
        mock_result.valid_rows = 95
        mock_result.invalid_rows = 5
        mock_result.output_files = ['output.parquet']
        mock_result.manifest_file = 'manifest.json'
        mock_result.metadata_file = 'metadata.json'
        mock_core_instance.process_csv.return_value = mock_result
        mock_forklift_core.return_value = mock_core_instance

        # Test arguments
        test_args = [
            'forklift', 'ingest',
            'test.csv',
            '--dest', 'output/',
            '--input-kind', 'csv'
        ]

        with patch('sys.argv', test_args):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                main()

        # Verify ForkliftCore was called correctly
        mock_forklift_core.assert_called_once()
        config_arg = mock_forklift_core.call_args[0][0]
        assert config_arg.input_path == 'test.csv'
        assert config_arg.output_path == 'output/'
        assert config_arg.header_mode == HeaderMode.PRESENT
        assert config_arg.encoding == 'utf-8-sig'
        assert config_arg.delimiter == ','

        # Verify process_csv was called
        mock_core_instance.process_csv.assert_called_once()

        # Verify output messages
        output = mock_stdout.getvalue()
        assert 'Processing complete. Processed 100 rows.' in output
        assert 'Valid rows: 95, Invalid rows: 5' in output
        assert 'Output files: output.parquet' in output
        assert 'Manifest file: manifest.json' in output
        assert 'Metadata file: metadata.json' in output

    @patch('forklift.cli.ForkliftCore')
    @patch('forklift.cli.is_s3_path')
    def test_ingest_csv_s3_paths(self, mock_is_s3_path, mock_forklift_core):
        """Test CSV ingest with S3 paths."""
        # Setup mocks - return True for S3 paths
        mock_is_s3_path.side_effect = lambda path: path.startswith('s3://')
        mock_core_instance = MagicMock()
        mock_result = MagicMock()
        mock_result.total_rows = 50
        mock_result.valid_rows = 50
        mock_result.invalid_rows = 0
        mock_result.output_files = []
        mock_result.manifest_file = None
        mock_result.metadata_file = None
        mock_core_instance.process_csv.return_value = mock_result
        mock_forklift_core.return_value = mock_core_instance

        test_args = [
            'forklift', 'ingest',
            's3://bucket/input.csv',
            '--dest', 's3://bucket/output/',
            '--input-kind', 'csv'
        ]

        with patch('sys.argv', test_args):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                main()

        # Verify S3 feedback messages
        output = mock_stdout.getvalue()
        assert 'Reading from S3: s3://bucket/input.csv' in output
        assert 'Writing to S3: s3://bucket/output/' in output

    @patch('forklift.cli.ForkliftCore')
    @patch('forklift.cli.is_s3_path')
    def test_ingest_with_all_options(self, mock_is_s3_path, mock_forklift_core):
        """Test CSV ingest with all CLI options."""
        mock_is_s3_path.return_value = False
        mock_core_instance = MagicMock()
        mock_result = MagicMock()
        mock_result.total_rows = 100
        mock_result.valid_rows = 100
        mock_result.invalid_rows = 0
        mock_result.output_files = []
        mock_result.manifest_file = None
        mock_result.metadata_file = None
        mock_core_instance.process_csv.return_value = mock_result
        mock_forklift_core.return_value = mock_core_instance

        test_args = [
            'forklift', 'ingest',
            'test.csv',
            '--dest', 'output/',
            '--input-kind', 'csv',
            '--schema', 'schema.json',
            '--encoding-priority', 'latin-1', 'utf-8',
            '--delimiter', '|',
            '--header-mode', 'absent',
            '--pre', 'preprocessor1', 'preprocessor2',
            '--sheet', 'Sheet1',
            '--fwf-spec', 'fwf.json'
        ]

        with patch('sys.argv', test_args):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                main()

        # Verify config was created with correct values
        config_arg = mock_forklift_core.call_args[0][0]
        assert config_arg.input_path == 'test.csv'
        assert config_arg.output_path == 'output/'
        assert config_arg.schema_file == 'schema.json'
        assert config_arg.header_mode == HeaderMode.ABSENT
        assert config_arg.encoding == 'latin-1'  # First in priority list
        assert config_arg.delimiter == '|'

        # Verify warning messages for unimplemented features
        output = mock_stdout.getvalue()
        assert 'Warning: FWF spec processing not yet implemented' in output
        assert 'Warning: Preprocessors not yet implemented' in output
        assert 'Warning: Excel sheet processing not yet implemented' in output

    @patch('forklift.cli.ForkliftCore')
    @patch('forklift.cli.is_s3_path')
    def test_ingest_unsupported_input_kind(self, mock_is_s3_path, mock_forklift_core):
        """Test ingest with unsupported input kind."""
        mock_is_s3_path.return_value = False
        mock_core_instance = MagicMock()
        mock_forklift_core.return_value = mock_core_instance

        test_args = [
            'forklift', 'ingest',
            'test.xlsx',
            '--dest', 'output/',
            '--input-kind', 'excel'
        ]

        with patch('sys.argv', test_args):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                main()

        # Verify error message for unsupported input kind
        output = mock_stdout.getvalue()
        assert "Error: Input kind 'excel' not yet implemented" in output
        assert "Only 'csv' is currently supported" in output

        # Verify process_csv was not called
        mock_core_instance.process_csv.assert_not_called()

    @patch('forklift.cli.ForkliftCore')
    @patch('forklift.cli.is_s3_path')
    def test_ingest_fwf_unsupported(self, mock_is_s3_path, mock_forklift_core):
        """Test ingest with FWF input kind (unsupported)."""
        mock_is_s3_path.return_value = False
        mock_core_instance = MagicMock()
        mock_forklift_core.return_value = mock_core_instance

        test_args = [
            'forklift', 'ingest',
            'test.fwf',
            '--dest', 'output/',
            '--input-kind', 'fwf'
        ]

        with patch('sys.argv', test_args):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                main()

        output = mock_stdout.getvalue()
        assert "Error: Input kind 'fwf' not yet implemented" in output

    def test_ingest_missing_required_args(self):
        """Test that missing required arguments cause exit."""
        # Missing --dest
        with patch('sys.argv', ['forklift', 'ingest', 'test.csv', '--input-kind', 'csv']):
            with pytest.raises(SystemExit):
                main()

        # Missing --input-kind
        with patch('sys.argv', ['forklift', 'ingest', 'test.csv', '--dest', 'output/']):
            with pytest.raises(SystemExit):
                main()

    @patch('forklift.cli.ForkliftCore')
    @patch('forklift.cli.is_s3_path')
    def test_ingest_default_encoding_priority(self, mock_is_s3_path, mock_forklift_core):
        """Test that default encoding priority is used when not specified."""
        mock_is_s3_path.return_value = False
        mock_core_instance = MagicMock()
        mock_result = MagicMock()
        mock_result.total_rows = 0
        mock_result.valid_rows = 0
        mock_result.invalid_rows = 0
        mock_result.output_files = []
        mock_result.manifest_file = None
        mock_result.metadata_file = None
        mock_core_instance.process_csv.return_value = mock_result
        mock_forklift_core.return_value = mock_core_instance

        test_args = [
            'forklift', 'ingest',
            'test.csv',
            '--dest', 'output/',
            '--input-kind', 'csv'
        ]

        with patch('sys.argv', test_args):
            main()

        # Verify default encoding is utf-8-sig (first in default priority)
        config_arg = mock_forklift_core.call_args[0][0]
        assert config_arg.encoding == 'utf-8-sig'

    @patch('forklift.cli.ForkliftCore')
    @patch('forklift.cli.is_s3_path')
    def test_ingest_empty_encoding_priority(self, mock_is_s3_path, mock_forklift_core):
        """Test behavior when encoding priority list is empty."""
        mock_is_s3_path.return_value = False
        mock_core_instance = MagicMock()
        mock_result = MagicMock()
        mock_result.total_rows = 0
        mock_result.valid_rows = 0
        mock_result.invalid_rows = 0
        mock_result.output_files = []
        mock_result.manifest_file = None
        mock_result.metadata_file = None
        mock_core_instance.process_csv.return_value = mock_result
        mock_forklift_core.return_value = mock_core_instance

        test_args = [
            'forklift', 'ingest',
            'test.csv',
            '--dest', 'output/',
            '--input-kind', 'csv',
            '--encoding-priority'  # Empty list
        ]

        with patch('sys.argv', test_args):
            main()

        # Verify fallback encoding is utf-8
        config_arg = mock_forklift_core.call_args[0][0]
        assert config_arg.encoding == 'utf-8'
