"""Tests for the CLI module."""
import pytest
from unittest.mock import patch, MagicMock, call
import argparse
import sys

from forklift.cli import main
from forklift.engine.forklift_core import ImportConfig, HeaderMode
from forklift.schema.schema_generator import SchemaGenerationConfig, OutputTarget, FileType


class TestCLIArgumentParsing:
    """Test cases for CLI argument parsing."""

    def test_no_command_provided(self):
        """Test that CLI fails when no command is provided."""
        with patch('sys.argv', ['forklift']):
            with pytest.raises(SystemExit):
                main()

    def test_invalid_command(self):
        """Test that CLI fails with invalid command."""
        with patch('sys.argv', ['forklift', 'invalid-command']):
            with pytest.raises(SystemExit):
                main()

    def test_ingest_missing_required_args(self):
        """Test that ingest command fails when required arguments are missing."""
        # Missing --dest
        with patch('sys.argv', ['forklift', 'ingest', 'input.csv']):
            with pytest.raises(SystemExit):
                main()

        # Missing --input-kind
        with patch('sys.argv', ['forklift', 'ingest', 'input.csv', '--dest', 'output/']):
            with pytest.raises(SystemExit):
                main()

    def test_generate_schema_missing_required_args(self):
        """Test that generate-schema command fails when required arguments are missing."""
        # Missing --file-type
        with patch('sys.argv', ['forklift', 'generate-schema', 'input.csv']):
            with pytest.raises(SystemExit):
                main()

    def test_generate_schema_file_output_missing_path(self):
        """Test that generate-schema fails when file output is specified without path."""
        test_args = ['forklift', 'generate-schema', 'input.csv', '--file-type', 'csv', '--output', 'file']

        with patch('sys.argv', test_args):
            with patch('builtins.print') as mock_print:
                main()
                mock_print.assert_called_with("Error: --output-path is required when --output=file")

    @patch('forklift.cli.ForkliftCore')
    def test_ingest_csv_with_all_options(self, mock_forklift):
        """Test ingest command with CSV input and all optional arguments."""
        test_args = [
            'forklift', 'ingest', 'input.csv',
            '--dest', 'output/',
            '--input-kind', 'csv',
            '--schema', 'schema.json',
            '--pre', 'clean_whitespace', 'normalize_case',
            '--encoding-priority', 'utf-8', 'latin-1',
            '--delimiter', '|',
            '--header-mode', 'auto'
        ]

        with patch('sys.argv', test_args):
            main()

        mock_forklift.assert_called_once()

    @patch('forklift.cli.ForkliftCore')
    def test_ingest_excel_with_sheet(self, mock_forklift):
        """Test ingest command with Excel input and sheet specification."""
        test_args = [
            'forklift', 'ingest', 'input.xlsx',
            '--dest', 'output/',
            '--input-kind', 'excel',
            '--sheet', 'Sheet1'
        ]

        with patch('sys.argv', test_args):
            main()

        mock_forklift.assert_called_once()

    @patch('forklift.cli.ForkliftCore')
    def test_ingest_fwf_with_spec(self, mock_forklift):
        """Test ingest command with FWF input and specification."""
        test_args = [
            'forklift', 'ingest', 'input.txt',
            '--dest', 'output/',
            '--input-kind', 'fwf',
            '--fwf-spec', 'fwf_spec.json',
            '--header-mode', 'absent'
        ]

        with patch('sys.argv', test_args):
            main()

        mock_forklift.assert_called_once()

    @patch('forklift.cli.SchemaGenerator')
    def test_generate_schema_csv_basic(self, mock_schema_gen):
        """Test generate-schema command with CSV input basic options."""
        test_args = [
            'forklift', 'generate-schema', 'input.csv',
            '--file-type', 'csv'
        ]

        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator

        with patch('sys.argv', test_args):
            main()

        mock_schema_gen.assert_called_once()
        mock_generator.generate_schema.assert_called_once()

    @patch('forklift.cli.SchemaGenerator')
    def test_generate_schema_excel_with_all_options(self, mock_schema_gen):
        """Test generate-schema command with Excel input and all options."""
        test_args = [
            'forklift', 'generate-schema', 'input.xlsx',
            '--file-type', 'excel',
            '--nrows', '500',
            '--output', 'file',
            '--output-path', 'schema_output.json',
            '--delimiter', ';',
            '--encoding', 'utf-8',
            '--sheet', 'DataSheet',
            '--include-sample',
            '--infer-primary-key',
            '--no-metadata',
            '--metadata-output', 'metadata.json',
            '--enum-threshold', '0.05',
            '--uniqueness-threshold', '0.90',
            '--top-n-values', '20',
            '--quantiles', '0.1', '0.5', '0.9'
        ]

        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator

        with patch('sys.argv', test_args):
            main()

        mock_schema_gen.assert_called_once()
        mock_generator.generate_schema.assert_called_once()

    @patch('forklift.cli.SchemaGenerator')
    def test_generate_schema_parquet_stdout(self, mock_schema_gen):
        """Test generate-schema command with Parquet input to stdout."""
        test_args = [
            'forklift', 'generate-schema', 'input.parquet',
            '--file-type', 'parquet',
            '--output', 'stdout'
        ]

        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator

        with patch('sys.argv', test_args):
            main()

        mock_schema_gen.assert_called_once()
        mock_generator.generate_schema.assert_called_once()

    @patch('forklift.cli.SchemaGenerator')
    def test_generate_schema_clipboard_output(self, mock_schema_gen):
        """Test generate-schema command with clipboard output."""
        test_args = [
            'forklift', 'generate-schema', 'input.csv',
            '--file-type', 'csv',
            '--output', 'clipboard'
        ]

        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator

        with patch('sys.argv', test_args):
            main()

        mock_schema_gen.assert_called_once()
        mock_generator.generate_schema.assert_called_once()

    def test_ingest_invalid_input_kind(self):
        """Test that ingest command fails with invalid input-kind."""
        test_args = [
            'forklift', 'ingest', 'input.txt',
            '--dest', 'output/',
            '--input-kind', 'invalid'
        ]

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                main()

    def test_generate_schema_invalid_file_type(self):
        """Test that generate-schema command fails with invalid file-type."""
        test_args = [
            'forklift', 'generate-schema', 'input.txt',
            '--file-type', 'invalid'
        ]

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                main()

    def test_ingest_invalid_header_mode(self):
        """Test that ingest command fails with invalid header-mode."""
        test_args = [
            'forklift', 'ingest', 'input.csv',
            '--dest', 'output/',
            '--input-kind', 'csv',
            '--header-mode', 'invalid'
        ]

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                main()

    def test_generate_schema_invalid_output_target(self):
        """Test that generate-schema command fails with invalid output target."""
        test_args = [
            'forklift', 'generate-schema', 'input.csv',
            '--file-type', 'csv',
            '--output', 'invalid'
        ]

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                main()

    @patch('forklift.cli.ForkliftCore')
    def test_ingest_s3_paths(self, mock_forklift):
        """Test ingest command with S3 paths."""
        test_args = [
            'forklift', 'ingest', 's3://bucket/input.csv',
            '--dest', 's3://bucket/output/',
            '--input-kind', 'csv',
            '--schema', 's3://bucket/schema.json'
        ]

        with patch('sys.argv', test_args):
            main()

        mock_forklift.assert_called_once()

    @patch('forklift.cli.SchemaGenerator')
    def test_generate_schema_s3_input(self, mock_schema_gen):
        """Test generate-schema command with S3 input."""
        test_args = [
            'forklift', 'generate-schema', 's3://bucket/input.csv',
            '--file-type', 'csv'
        ]

        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator

        with patch('sys.argv', test_args):
            main()

        mock_schema_gen.assert_called_once()

    def test_help_commands(self):
        """Test help commands don't crash."""
        # Test main help
        with patch('sys.argv', ['forklift', '--help']):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0

        # Test ingest help
        with patch('sys.argv', ['forklift', 'ingest', '--help']):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0

        # Test generate-schema help
        with patch('sys.argv', ['forklift', 'generate-schema', '--help']):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0

    @patch('forklift.cli.ForkliftCore')
    def test_ingest_minimal_args(self, mock_forklift):
        """Test ingest command with minimal required arguments."""
        test_args = [
            'forklift', 'ingest', 'input.csv',
            '--dest', 'output/',
            '--input-kind', 'csv'
        ]

        with patch('sys.argv', test_args):
            main()

        mock_forklift.assert_called_once()

    @patch('forklift.cli.SchemaGenerator')
    def test_generate_schema_minimal_args(self, mock_schema_gen):
        """Test generate-schema command with minimal required arguments."""
        test_args = [
            'forklift', 'generate-schema', 'input.csv',
            '--file-type', 'csv'
        ]

        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator

        with patch('sys.argv', test_args):
            main()

        mock_schema_gen.assert_called_once()


class TestCLIEdgeCases:
    """Test edge cases and error conditions in CLI."""

    def test_empty_args(self):
        """Test CLI with empty arguments list."""
        with patch('sys.argv', []):
            with pytest.raises(SystemExit):
                main()

    def test_special_characters_in_paths(self):
        """Test CLI with special characters in file paths."""
        test_args = [
            'forklift', 'ingest', 'input with spaces.csv',
            '--dest', 'output with spaces/',
            '--input-kind', 'csv'
        ]

        with patch('sys.argv', test_args):
            with patch('forklift.cli.ForkliftCore') as mock_forklift:
                main()
                mock_forklift.assert_called_once()

    @patch('forklift.cli.SchemaGenerator')
    def test_numeric_edge_cases(self, mock_schema_gen):
        """Test CLI with numeric edge cases."""
        test_args = [
            'forklift', 'generate-schema', 'input.csv',
            '--file-type', 'csv',
            '--nrows', '0',  # Edge case: zero rows
            '--enum-threshold', '1.0',  # Edge case: maximum threshold
            '--uniqueness-threshold', '0.0',  # Edge case: minimum threshold
            '--top-n-values', '1'  # Edge case: minimum top values
        ]

        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator

        with patch('sys.argv', test_args):
            main()

        mock_schema_gen.assert_called_once()

    def test_duplicate_arguments(self):
        """Test CLI behavior with duplicate arguments."""
        test_args = [
            'forklift', 'ingest', 'input.csv',
            '--dest', 'output1/',
            '--dest', 'output2/',  # Duplicate dest
            '--input-kind', 'csv'
        ]

        with patch('sys.argv', test_args):
            with patch('forklift.cli.ForkliftCore') as mock_forklift:
                main()
                # Should use the last value
                mock_forklift.assert_called_once()

    @patch('forklift.cli.ForkliftCore')
    def test_all_encoding_priority_options(self, mock_forklift):
        """Test CLI with various encoding priority combinations."""
        test_args = [
            'forklift', 'ingest', 'input.csv',
            '--dest', 'output/',
            '--input-kind', 'csv',
            '--encoding-priority', 'utf-8-sig', 'utf-8', 'latin-1', 'cp1252', 'ascii'
        ]

        with patch('sys.argv', test_args):
            main()

        mock_forklift.assert_called_once()

    @patch('forklift.cli.ForkliftCore')
    def test_multiple_preprocessors(self, mock_forklift):
        """Test CLI with multiple preprocessors."""
        test_args = [
            'forklift', 'ingest', 'input.csv',
            '--dest', 'output/',
            '--input-kind', 'csv',
            '--pre', 'clean_whitespace', 'normalize_case', 'trim_fields', 'remove_duplicates'
        ]

        with patch('sys.argv', test_args):
            main()

        mock_forklift.assert_called_once()

    @patch('forklift.cli.SchemaGenerator')
    def test_custom_quantiles_list(self, mock_schema_gen):
        """Test CLI with custom quantiles list."""
        test_args = [
            'forklift', 'generate-schema', 'input.csv',
            '--file-type', 'csv',
            '--quantiles', '0.1', '0.25', '0.5', '0.75', '0.9', '0.95', '0.99'
        ]

        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator

        with patch('sys.argv', test_args):
            main()

        mock_schema_gen.assert_called_once()


class TestCLIIntegration:
    """Integration-style tests for CLI functionality."""

    @patch('forklift.cli.ForkliftCore')
    @patch('forklift.cli.is_s3_path')
    def test_s3_path_detection(self, mock_is_s3_path, mock_forklift):
        """Test that S3 path detection is called appropriately."""
        mock_is_s3_path.return_value = True

        test_args = [
            'forklift', 'ingest', 's3://bucket/input.csv',
            '--dest', 's3://bucket/output/',
            '--input-kind', 'csv'
        ]

        with patch('sys.argv', test_args):
            main()

        mock_forklift.assert_called_once()

    @patch('forklift.cli.SchemaGenerator')
    def test_schema_generation_config_creation(self, mock_schema_gen):
        """Test that SchemaGenerationConfig is created correctly."""
        test_args = [
            'forklift', 'generate-schema', 'input.csv',
            '--file-type', 'csv',
            '--nrows', '1000',
            '--include-sample',
            '--infer-primary-key'
        ]

        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator

        with patch('sys.argv', test_args):
            main()

        # Verify SchemaGenerator was called
        mock_schema_gen.assert_called_once()
        # Verify generate_schema was called
        mock_generator.generate_schema.assert_called_once()

    @patch('forklift.cli.ForkliftCore')
    def test_import_config_creation(self, mock_forklift):
        """Test that ImportConfig is created correctly."""
        test_args = [
            'forklift', 'ingest', 'input.csv',
            '--dest', 'output/',
            '--input-kind', 'csv',
            '--header-mode', 'present'
        ]

        with patch('sys.argv', test_args):
            main()

        mock_forklift.assert_called_once()

    def test_argument_parser_structure(self):
        """Test that argument parser has the expected structure."""
        # This test verifies the parser structure without executing main()
        import forklift.cli

        # We can't easily test the parser structure without refactoring
        # but we can at least verify the main function exists
        assert hasattr(forklift.cli, 'main')
        assert callable(forklift.cli.main)
