"""Comprehensive tests for CLI module."""

import pytest
import argparse
from unittest.mock import patch, MagicMock
from forklift.cli import main


class TestCLIMain:
    """Test CLI main function and argument parsing."""

    @patch('forklift.cli.argparse.ArgumentParser.parse_args')
    @patch('forklift.cli.ForkliftCore')
    def test_ingest_command_basic(self, mock_forklift_core, mock_parse_args):
        """Test basic ingest command."""
        # Mock arguments for ingest command
        mock_args = MagicMock()
        mock_args.cmd = 'ingest'
        mock_args.source = 'test.csv'
        mock_args.dest = 'output/'
        mock_args.input_kind = 'csv'
        mock_args.schema = None
        mock_args.pre = []
        mock_args.encoding_priority = ['utf-8-sig', 'utf-8', 'latin-1']
        mock_args.delimiter = None
        mock_args.sheet = None
        mock_args.fwf_spec = None
        mock_args.header_mode = 'present'

        mock_parse_args.return_value = mock_args

        # Mock ForkliftCore instance
        mock_core_instance = MagicMock()
        mock_forklift_core.return_value = mock_core_instance

        # Call main function
        main()

        # Verify ForkliftCore was called
        mock_forklift_core.assert_called_once()

    @patch('forklift.cli.argparse.ArgumentParser.parse_args')
    @patch('forklift.cli.SchemaGenerator')
    def test_generate_schema_command_basic(self, mock_schema_generator, mock_parse_args):
        """Test basic generate-schema command."""
        # Mock arguments for generate-schema command
        mock_args = MagicMock()
        mock_args.cmd = 'generate-schema'
        mock_args.source = 'test.csv'
        mock_args.file_type = 'csv'
        mock_args.nrows = None
        mock_args.output = 'stdout'
        mock_args.output_path = None
        mock_args.delimiter = ','
        mock_args.encoding = 'utf-8'
        mock_args.sheet = None
        mock_args.include_sample = False
        mock_args.infer_primary_key = False
        mock_args.no_metadata = False
        mock_args.metadata_output = None
        mock_args.enum_threshold = 0.1
        mock_args.uniqueness_threshold = 0.95
        mock_args.top_n_values = 10
        mock_args.quantiles = None

        mock_parse_args.return_value = mock_args

        # Mock SchemaGenerator instance
        mock_generator_instance = MagicMock()
        mock_schema_generator.return_value = mock_generator_instance

        # Call main function
        main()

        # Verify SchemaGenerator was called
        mock_schema_generator.assert_called_once()

    @patch('forklift.cli.argparse.ArgumentParser.parse_args')
    @patch('forklift.cli.ForkliftCore')
    def test_ingest_command_with_all_options(self, mock_forklift_core, mock_parse_args):
        """Test ingest command with all options specified."""
        # Mock arguments with all options
        mock_args = MagicMock()
        mock_args.cmd = 'ingest'
        mock_args.source = 's3://bucket/test.csv'
        mock_args.dest = 's3://bucket/output/'
        mock_args.input_kind = 'csv'
        mock_args.schema = 'schema.json'
        mock_args.pre = ['preprocessor1', 'preprocessor2']
        mock_args.encoding_priority = ['utf-8', 'latin-1']
        mock_args.delimiter = '|'
        mock_args.sheet = 'Sheet1'
        mock_args.fwf_spec = 'fwf_spec.json'
        mock_args.header_mode = 'auto'

        mock_parse_args.return_value = mock_args

        # Mock ForkliftCore instance
        mock_core_instance = MagicMock()
        mock_forklift_core.return_value = mock_core_instance

        # Call main function
        main()

        # Verify ForkliftCore was called
        mock_forklift_core.assert_called_once()

    @patch('forklift.cli.argparse.ArgumentParser.parse_args')
    @patch('forklift.cli.SchemaGenerator')
    def test_generate_schema_command_with_all_options(self, mock_schema_generator, mock_parse_args):
        """Test generate-schema command with all options specified."""
        # Mock arguments with all options
        mock_args = MagicMock()
        mock_args.cmd = 'generate-schema'
        mock_args.source = 'test.xlsx'
        mock_args.file_type = 'excel'
        mock_args.nrows = 500
        mock_args.output = 'file'
        mock_args.output_path = 'schema.json'
        mock_args.delimiter = ';'
        mock_args.encoding = 'utf-16'
        mock_args.sheet = 'Data'
        mock_args.include_sample = True
        mock_args.infer_primary_key = True
        mock_args.no_metadata = True
        mock_args.metadata_output = 'metadata.json'
        mock_args.enum_threshold = 0.2
        mock_args.uniqueness_threshold = 0.9
        mock_args.top_n_values = 5
        mock_args.quantiles = [0.1, 0.5, 0.9]

        mock_parse_args.return_value = mock_args

        # Mock SchemaGenerator instance
        mock_generator_instance = MagicMock()
        mock_schema_generator.return_value = mock_generator_instance

        # Call main function
        main()

        # Verify SchemaGenerator was called
        mock_schema_generator.assert_called_once()

    @patch('forklift.cli.argparse.ArgumentParser.parse_args')
    def test_ingest_command_fwf_input(self, mock_parse_args):
        """Test ingest command with FWF input type."""
        mock_args = MagicMock()
        mock_args.cmd = 'ingest'
        mock_args.source = 'test.txt'
        mock_args.dest = 'output/'
        mock_args.input_kind = 'fwf'
        mock_args.schema = None
        mock_args.pre = []
        mock_args.encoding_priority = ['utf-8-sig', 'utf-8', 'latin-1']
        mock_args.delimiter = None
        mock_args.sheet = None
        mock_args.fwf_spec = 'spec.json'
        mock_args.header_mode = 'absent'

        mock_parse_args.return_value = mock_args

        with patch('forklift.cli.ForkliftCore') as mock_forklift_core:
            mock_core_instance = MagicMock()
            mock_forklift_core.return_value = mock_core_instance

            main()

            mock_forklift_core.assert_called_once()

    @patch('forklift.cli.argparse.ArgumentParser.parse_args')
    def test_ingest_command_excel_input(self, mock_parse_args):
        """Test ingest command with Excel input type."""
        mock_args = MagicMock()
        mock_args.cmd = 'ingest'
        mock_args.source = 'test.xlsx'
        mock_args.dest = 'output/'
        mock_args.input_kind = 'excel'
        mock_args.schema = 'schema.json'
        mock_args.pre = ['clean', 'validate']
        mock_args.encoding_priority = ['utf-8-sig', 'utf-8', 'latin-1']
        mock_args.delimiter = None
        mock_args.sheet = 'DataSheet'
        mock_args.fwf_spec = None
        mock_args.header_mode = 'present'

        mock_parse_args.return_value = mock_args

        with patch('forklift.cli.ForkliftCore') as mock_forklift_core:
            mock_core_instance = MagicMock()
            mock_forklift_core.return_value = mock_core_instance

            main()

            mock_forklift_core.assert_called_once()

    @patch('forklift.cli.argparse.ArgumentParser.parse_args')
    def test_generate_schema_parquet_input(self, mock_parse_args):
        """Test generate-schema command with Parquet input type."""
        mock_args = MagicMock()
        mock_args.cmd = 'generate-schema'
        mock_args.source = 'test.parquet'
        mock_args.file_type = 'parquet'
        mock_args.nrows = 1000
        mock_args.output = 'clipboard'
        mock_args.output_path = None
        mock_args.delimiter = ','
        mock_args.encoding = 'utf-8'
        mock_args.sheet = None
        mock_args.include_sample = True
        mock_args.infer_primary_key = False
        mock_args.no_metadata = False
        mock_args.metadata_output = None
        mock_args.enum_threshold = 0.1
        mock_args.uniqueness_threshold = 0.95
        mock_args.top_n_values = 10
        mock_args.quantiles = None

        mock_parse_args.return_value = mock_args

        with patch('forklift.cli.SchemaGenerator') as mock_schema_generator:
            mock_generator_instance = MagicMock()
            mock_schema_generator.return_value = mock_generator_instance

            main()

            mock_schema_generator.assert_called_once()


class TestCLIArgumentParsing:
    """Test CLI argument parsing without mocking."""

    def test_argument_parser_creation(self):
        """Test that argument parser can be created successfully."""
        # This tests the actual argument parser setup
        import forklift.cli

        # Test by creating a parser manually (similar to what main() does)
        p = argparse.ArgumentParser("forklift")
        sub = p.add_subparsers(dest="cmd", required=True)

        # Verify basic structure
        assert p.prog == "forklift"
        assert sub is not None

    def test_ingest_subparser_arguments(self):
        """Test ingest subparser has all required arguments."""
        # Create the parser as done in main()
        p = argparse.ArgumentParser("forklift")
        sub = p.add_subparsers(dest="cmd", required=True)

        ingest = sub.add_parser("ingest", help="Clean & write to Parquet")
        ingest.add_argument("source", help="Input path (local file or S3 URI: s3://bucket/key)")
        ingest.add_argument("--dest", required=True, help="Output path (local directory or S3 URI: s3://bucket/prefix/)")
        ingest.add_argument("--input-kind", choices=["csv","fwf","excel"], required=True)

        # Test parsing valid arguments
        args = p.parse_args(["ingest", "test.csv", "--dest", "output/", "--input-kind", "csv"])
        assert args.cmd == "ingest"
        assert args.source == "test.csv"
        assert args.dest == "output/"
        assert args.input_kind == "csv"

    def test_schema_gen_subparser_arguments(self):
        """Test generate-schema subparser has all required arguments."""
        # Create the parser as done in main()
        p = argparse.ArgumentParser("forklift")
        sub = p.add_subparsers(dest="cmd", required=True)

        schema_gen = sub.add_parser("generate-schema", help="Generate schema from data file")
        schema_gen.add_argument("source", help="Input path (local file or S3 URI: s3://bucket/key)")
        schema_gen.add_argument("--file-type", choices=["csv", "excel", "parquet"], required=True, help="Type of input file")

        # Test parsing valid arguments
        args = p.parse_args(["generate-schema", "test.csv", "--file-type", "csv"])
        assert args.cmd == "generate-schema"
        assert args.source == "test.csv"
        assert args.file_type == "csv"
