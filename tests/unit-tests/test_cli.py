"""Tests for the CLI module."""
import pytest
from unittest.mock import patch, MagicMock, call

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


class TestIngestCommand:
    """Test cases for the ingest command."""

    @patch('forklift.cli.ForkliftCore')
    @patch('forklift.cli.is_s3_path')
    @patch('builtins.print')
    def test_ingest_csv_basic(self, mock_print, mock_is_s3, mock_forklift_core):
        """Test basic CSV ingest command."""
        # Setup mocks
        mock_is_s3.return_value = False
        mock_core_instance = MagicMock()
        mock_results = MagicMock()
        mock_results.total_rows = 100
        mock_results.valid_rows = 95
        mock_results.invalid_rows = 5
        mock_results.output_files = ['output.parquet']
        mock_results.manifest_file = 'manifest.json'
        mock_results.metadata_file = 'metadata.json'
        mock_core_instance.process_csv.return_value = mock_results
        mock_forklift_core.return_value = mock_core_instance

        test_args = ['forklift', 'ingest', 'input.csv', '--dest', 'output/', '--input-kind', 'csv']

        with patch('sys.argv', test_args):
            main()

        # Verify ForkliftCore was created with correct config
        mock_forklift_core.assert_called_once()
        config = mock_forklift_core.call_args[0][0]
        assert isinstance(config, ImportConfig)
        assert config.input_path == 'input.csv'
        assert config.output_path == 'output/'
        assert config.schema_file is None
        assert config.header_mode == HeaderMode.PRESENT
        assert config.encoding == 'utf-8-sig'  # First in default priority list
        assert config.delimiter == ','

        # Verify process_csv was called
        mock_core_instance.process_csv.assert_called_once()

        # Verify output messages
        expected_calls = [
            call("Processing complete. Processed 100 rows."),
            call("Valid rows: 95, Invalid rows: 5"),
            call("Output files: output.parquet"),
            call("Manifest file: manifest.json"),
            call("Metadata file: metadata.json")
        ]
        mock_print.assert_has_calls(expected_calls)

    @patch('forklift.cli.ForkliftCore')
    @patch('forklift.cli.is_s3_path')
    @patch('builtins.print')
    def test_ingest_csv_with_all_options(self, mock_print, mock_is_s3, mock_forklift_core):
        """Test CSV ingest with all optional parameters."""
        # Setup mocks
        mock_is_s3.return_value = False
        mock_core_instance = MagicMock()
        mock_results = MagicMock()
        mock_results.total_rows = 50
        mock_results.valid_rows = 50
        mock_results.invalid_rows = 0
        mock_results.output_files = []
        mock_results.manifest_file = None
        mock_results.metadata_file = None
        mock_core_instance.process_csv.return_value = mock_results
        mock_forklift_core.return_value = mock_core_instance

        test_args = [
            'forklift', 'ingest', 'input.csv',
            '--dest', 'output/',
            '--input-kind', 'csv',
            '--schema', 'schema.json',
            '--encoding-priority', 'utf-8', 'latin-1',
            '--delimiter', ';',
            '--header-mode', 'absent',
            '--pre', 'preprocessor1', 'preprocessor2',
            '--sheet', 'Sheet1',
            '--fwf-spec', 'fwf.json'
        ]

        with patch('sys.argv', test_args):
            main()

        # Verify config
        config = mock_forklift_core.call_args[0][0]
        assert config.input_path == 'input.csv'
        assert config.output_path == 'output/'
        assert config.schema_file == 'schema.json'
        assert config.header_mode == HeaderMode.ABSENT
        assert config.encoding == 'utf-8'
        assert config.delimiter == ';'

        # Verify warning messages for unimplemented features
        warning_calls = [call for call in mock_print.call_args_list if 'Warning:' in str(call)]
        assert len(warning_calls) == 3  # preprocessors, sheet, fwf-spec

    @patch('forklift.cli.ForkliftCore')
    @patch('forklift.cli.is_s3_path')
    @patch('builtins.print')
    def test_ingest_s3_paths(self, mock_print, mock_is_s3, mock_forklift_core):
        """Test ingest with S3 paths."""
        # Setup mocks
        def s3_side_effect(path):
            return path.startswith('s3://')

        mock_is_s3.side_effect = s3_side_effect
        mock_core_instance = MagicMock()
        mock_results = MagicMock()
        mock_results.total_rows = 10
        mock_results.valid_rows = 10
        mock_results.invalid_rows = 0
        mock_results.output_files = None
        mock_results.manifest_file = None
        mock_results.metadata_file = None
        mock_core_instance.process_csv.return_value = mock_results
        mock_forklift_core.return_value = mock_core_instance

        test_args = [
            'forklift', 'ingest',
            's3://bucket/input.csv',
            '--dest', 's3://bucket/output/',
            '--input-kind', 'csv'
        ]

        with patch('sys.argv', test_args):
            main()

        # Verify S3 feedback messages
        s3_messages = [call for call in mock_print.call_args_list if 'S3:' in str(call)]
        assert len(s3_messages) == 2  # Reading from S3 and Writing to S3

    @patch('forklift.cli.ForkliftCore')
    @patch('forklift.cli.is_s3_path')
    @patch('builtins.print')
    def test_ingest_unsupported_input_kind(self, mock_print, mock_is_s3, mock_forklift_core):
        """Test ingest with unsupported input kind."""
        mock_is_s3.return_value = False
        mock_core_instance = MagicMock()
        mock_forklift_core.return_value = mock_core_instance

        test_args = ['forklift', 'ingest', 'input.xlsx', '--dest', 'output/', '--input-kind', 'excel']

        with patch('sys.argv', test_args):
            main()

        # Should not call process_csv for non-CSV input
        mock_core_instance.process_csv.assert_not_called()

        # Should print error message
        error_calls = [call for call in mock_print.call_args_list if 'Error:' in str(call)]
        assert len(error_calls) == 1
        assert 'not yet implemented' in str(error_calls[0])

    def test_ingest_header_mode_validation(self):
        """Test that header mode accepts valid values."""
        valid_modes = ['present', 'auto', 'absent']

        for mode in valid_modes:
            with patch('forklift.cli.ForkliftCore') as mock_core:
                mock_instance = MagicMock()
                mock_core.return_value = mock_instance
                mock_instance.process_csv.return_value = MagicMock(
                    total_rows=0, valid_rows=0, invalid_rows=0,
                    output_files=None, manifest_file=None, metadata_file=None
                )

                with patch('forklift.cli.is_s3_path', return_value=False):
                    test_args = [
                        'forklift', 'ingest', 'input.csv',
                        '--dest', 'output/',
                        '--input-kind', 'csv',
                        '--header-mode', mode
                    ]

                    with patch('sys.argv', test_args):
                        main()  # Should not raise exception

                    config = mock_core.call_args[0][0]
                    assert config.header_mode == HeaderMode(mode)


class TestGenerateSchemaCommand:
    """Test cases for the generate-schema command."""

    @patch('forklift.cli.SchemaGenerator')
    @patch('builtins.print')
    def test_generate_schema_basic(self, mock_print, mock_schema_generator):
        """Test basic schema generation command."""
        # Setup mocks
        mock_generator_instance = MagicMock()
        mock_schema = {'type': 'object', 'properties': {}}
        mock_generator_instance.generate_schema.return_value = mock_schema
        mock_schema_generator.return_value = mock_generator_instance

        test_args = ['forklift', 'generate-schema', 'input.csv', '--file-type', 'csv']

        with patch('sys.argv', test_args):
            main()

        # Verify SchemaGenerator was created with correct config
        mock_schema_generator.assert_called_once()
        config = mock_schema_generator.call_args[0][0]
        assert isinstance(config, SchemaGenerationConfig)
        assert config.input_path == 'input.csv'
        assert config.file_type == FileType.CSV
        assert config.output_target == OutputTarget.STDOUT
        assert config.delimiter == ','
        assert config.encoding == 'utf-8'
        assert config.generate_metadata is True

        # Verify methods were called
        mock_generator_instance.generate_schema.assert_called_once()
        mock_generator_instance.output_schema.assert_called_once_with(mock_schema)

    @patch('forklift.cli.SchemaGenerator')
    @patch('builtins.print')
    def test_generate_schema_with_all_options(self, mock_print, mock_schema_generator):
        """Test schema generation with all optional parameters."""
        # Setup mocks
        mock_generator_instance = MagicMock()
        mock_schema = {'type': 'object'}
        mock_generator_instance.generate_schema.return_value = mock_schema
        mock_schema_generator.return_value = mock_generator_instance

        test_args = [
            'forklift', 'generate-schema', 'input.xlsx',
            '--file-type', 'excel',
            '--nrows', '500',
            '--output', 'file',
            '--output-path', 'schema.json',
            '--delimiter', '|',
            '--encoding', 'latin-1',
            '--sheet', 'Data',
            '--include-sample',
            '--infer-primary-key',
            '--no-metadata',
            '--metadata-output', 'metadata.json',
            '--enum-threshold', '0.2',
            '--uniqueness-threshold', '0.8',
            '--top-n-values', '5',
            '--quantiles', '0.1', '0.5', '0.9'
        ]

        with patch('sys.argv', test_args):
            main()

        # Verify config
        config = mock_schema_generator.call_args[0][0]
        assert config.input_path == 'input.xlsx'
        assert config.file_type == FileType.EXCEL
        assert config.nrows == 500
        assert config.output_target == OutputTarget.FILE
        assert config.output_path == 'schema.json'
        assert config.delimiter == '|'
        assert config.encoding == 'latin-1'
        assert config.sheet_name == 'Data'
        assert config.include_sample_data is True
        assert config.infer_primary_key_from_metadata is True
        assert config.generate_metadata is False  # --no-metadata
        assert config.metadata_output_path == 'metadata.json'
        assert config.enum_threshold == 0.2
        assert config.uniqueness_threshold == 0.8
        assert config.top_n_values == 5
        assert config.quantiles == [0.1, 0.5, 0.9]

    @patch('forklift.cli.SchemaGenerator')
    @patch('builtins.print')
    def test_generate_schema_with_metadata_output(self, mock_print, mock_schema_generator):
        """Test schema generation with separate metadata file output."""
        # Setup mocks
        mock_generator_instance = MagicMock()
        mock_schema = {'type': 'object'}
        mock_table = MagicMock()
        mock_generator_instance.generate_schema.return_value = mock_schema
        mock_generator_instance._read_csv_sample.return_value = mock_table
        mock_generator_instance.generate_and_save_metadata.return_value = 'metadata.json'
        mock_schema_generator.return_value = mock_generator_instance

        test_args = [
            'forklift', 'generate-schema', 'input.csv',
            '--file-type', 'csv',
            '--metadata-output', 'metadata.json'
        ]

        with patch('sys.argv', test_args):
            main()

        # Verify metadata generation was called
        mock_generator_instance._read_csv_sample.assert_called_once()
        mock_generator_instance.generate_and_save_metadata.assert_called_once_with(mock_table)

        # Verify metadata output message
        metadata_calls = [call for call in mock_print.call_args_list if 'Metadata file written' in str(call)]
        assert len(metadata_calls) == 1

    @patch('forklift.cli.SchemaGenerator')
    @patch('builtins.print')
    def test_generate_schema_excel_metadata_output(self, mock_print, mock_schema_generator):
        """Test schema generation for Excel with metadata output."""
        # Setup mocks
        mock_generator_instance = MagicMock()
        mock_schema = {'type': 'object'}
        mock_table = MagicMock()
        mock_generator_instance.generate_schema.return_value = mock_schema
        mock_generator_instance._read_excel_sample.return_value = mock_table
        mock_generator_instance.generate_and_save_metadata.return_value = 'metadata.json'
        mock_schema_generator.return_value = mock_generator_instance

        test_args = [
            'forklift', 'generate-schema', 'input.xlsx',
            '--file-type', 'excel',
            '--metadata-output', 'metadata.json'
        ]

        with patch('sys.argv', test_args):
            main()

        # Verify Excel-specific metadata generation
        mock_generator_instance._read_excel_sample.assert_called_once()
        mock_generator_instance.generate_and_save_metadata.assert_called_once_with(mock_table)

    @patch('forklift.cli.SchemaGenerator')
    @patch('builtins.print')
    def test_generate_schema_parquet_metadata_output(self, mock_print, mock_schema_generator):
        """Test schema generation for Parquet with metadata output."""
        # Setup mocks
        mock_generator_instance = MagicMock()
        mock_schema = {'type': 'object'}
        mock_table = MagicMock()
        mock_generator_instance.generate_schema.return_value = mock_schema
        mock_generator_instance._read_parquet_sample.return_value = mock_table
        mock_generator_instance.generate_and_save_metadata.return_value = 'metadata.json'
        mock_schema_generator.return_value = mock_generator_instance

        test_args = [
            'forklift', 'generate-schema', 'input.parquet',
            '--file-type', 'parquet',
            '--metadata-output', 'metadata.json'
        ]

        with patch('sys.argv', test_args):
            main()

        # Verify Parquet-specific metadata generation
        mock_generator_instance._read_parquet_sample.assert_called_once()
        mock_generator_instance.generate_and_save_metadata.assert_called_once_with(mock_table)

    @patch('forklift.cli.SchemaGenerator')
    @patch('builtins.print')
    def test_generate_schema_error_handling(self, mock_print, mock_schema_generator):
        """Test error handling in schema generation."""
        # Setup mocks to raise exception
        mock_generator_instance = MagicMock()
        mock_generator_instance.generate_schema.side_effect = Exception("Test error")
        mock_schema_generator.return_value = mock_generator_instance

        test_args = ['forklift', 'generate-schema', 'input.csv', '--file-type', 'csv']

        with patch('sys.argv', test_args):
            main()

        # Verify error message was printed
        error_calls = [call for call in mock_print.call_args_list if 'Error generating schema:' in str(call)]
        assert len(error_calls) == 1
        assert 'Test error' in str(error_calls[0])

    def test_generate_schema_file_type_validation(self):
        """Test that file type accepts valid values."""
        valid_types = ['csv', 'excel', 'parquet']

        for file_type in valid_types:
            with patch('forklift.cli.SchemaGenerator') as mock_generator:
                mock_instance = MagicMock()
                mock_instance.generate_schema.return_value = {}
                mock_generator.return_value = mock_instance

                test_args = [
                    'forklift', 'generate-schema', 'input.file',
                    '--file-type', file_type
                ]

                with patch('sys.argv', test_args):
                    main()  # Should not raise exception

                config = mock_generator.call_args[0][0]
                assert config.file_type == FileType(file_type)

    def test_generate_schema_output_target_validation(self):
        """Test that output target accepts valid values."""
        valid_targets = ['stdout', 'file', 'clipboard']

        for target in valid_targets:
            if target == 'file':
                # File output requires output-path
                test_args = [
                    'forklift', 'generate-schema', 'input.csv',
                    '--file-type', 'csv',
                    '--output', target,
                    '--output-path', 'schema.json'
                ]
            else:
                test_args = [
                    'forklift', 'generate-schema', 'input.csv',
                    '--file-type', 'csv',
                    '--output', target
                ]

            with patch('forklift.cli.SchemaGenerator') as mock_generator:
                mock_instance = MagicMock()
                mock_instance.generate_schema.return_value = {}
                mock_generator.return_value = mock_instance

                with patch('sys.argv', test_args):
                    main()  # Should not raise exception

                config = mock_generator.call_args[0][0]
                assert config.output_target == OutputTarget(target)


class TestCLIEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_encoding_priority_list(self):
        """Test behavior when encoding priority list is empty."""
        with patch('forklift.cli.ForkliftCore') as mock_core:
            mock_instance = MagicMock()
            mock_core.return_value = mock_instance
            mock_instance.process_csv.return_value = MagicMock(
                total_rows=0, valid_rows=0, invalid_rows=0,
                output_files=None, manifest_file=None, metadata_file=None
            )

            with patch('forklift.cli.is_s3_path', return_value=False):
                test_args = [
                    'forklift', 'ingest', 'input.csv',
                    '--dest', 'output/',
                    '--input-kind', 'csv',
                    '--encoding-priority'  # Empty list
                ]

                with patch('sys.argv', test_args):
                    main()

                config = mock_core.call_args[0][0]
                assert config.encoding == 'utf-8'  # Fallback default

    def test_none_delimiter_fallback(self):
        """Test that None delimiter falls back to comma."""
        with patch('forklift.cli.ForkliftCore') as mock_core:
            mock_instance = MagicMock()
            mock_core.return_value = mock_instance
            mock_instance.process_csv.return_value = MagicMock(
                total_rows=0, valid_rows=0, invalid_rows=0,
                output_files=None, manifest_file=None, metadata_file=None
            )

            with patch('forklift.cli.is_s3_path', return_value=False):
                test_args = [
                    'forklift', 'ingest', 'input.csv',
                    '--dest', 'output/',
                    '--input-kind', 'csv'
                    # No --delimiter specified
                ]

                with patch('sys.argv', test_args):
                    main()

                config = mock_core.call_args[0][0]
                assert config.delimiter == ','  # Default fallback


if __name__ == "__main__":
    pytest.main([__file__])
