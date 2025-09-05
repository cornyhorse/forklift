"""Final tests to achieve 100% CLI coverage - targeting the last 2 missing branches."""
import pytest
from unittest.mock import patch, MagicMock
from forklift.cli import main


class TestCLIFinalBranches:
    """Test suite targeting the final 2 missing branches for 100% coverage."""

    def test_generate_schema_file_output_validation_exit_branch(self):
        """Test the exit branch after file output validation error (covers 101->exit)."""
        test_args = [
            'forklift', 'generate-schema', 'input.csv',
            '--file-type', 'csv',
            '--output', 'file'
            # Missing --output-path intentionally
        ]

        with patch('sys.argv', test_args):
            with patch('builtins.print') as mock_print:
                # Mock sys.exit to capture the exit call instead of actually exiting
                with patch('sys.exit') as mock_exit:
                    try:
                        main()
                    except SystemExit:
                        pass  # Expected when return is called

                # Verify the error message is printed
                mock_print.assert_called_with("Error: --output-path is required when --output=file")

    @patch('forklift.cli.SchemaGenerator')
    def test_generate_schema_metadata_conditional_branch(self, mock_schema_gen):
        """Test the conditional branch in metadata generation (covers 141->144)."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator

        # Mock methods - metadata generation succeeds and returns file path
        mock_generator.generate_schema.return_value = {"test": "schema"}
        mock_generator._read_excel_sample.return_value = "mock_table"
        mock_generator.generate_and_save_metadata.return_value = "metadata_file.json"

        test_args = [
            'forklift', 'generate-schema', 'input.xlsx',
            '--file-type', 'excel',
            '--metadata-output', 'metadata_output.json'
        ]

        with patch('sys.argv', test_args):
            with patch('builtins.print') as mock_print:
                main()

                # This should trigger the conditional branch where metadata_file is truthy
                # and the success message is printed
                mock_print.assert_any_call("Metadata file written to: metadata_file.json")

                # Verify the metadata methods were called
                mock_generator._read_excel_sample.assert_called_once()
                mock_generator.generate_and_save_metadata.assert_called_once_with("mock_table")

    @patch('forklift.cli.SchemaGenerator')
    def test_generate_schema_metadata_no_output_path_conditional(self, mock_schema_gen):
        """Test metadata generation without output path specified."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator

        # Mock methods
        mock_generator.generate_schema.return_value = {"test": "schema"}

        test_args = [
            'forklift', 'generate-schema', 'input.csv',
            '--file-type', 'csv'
            # No --metadata-output specified
        ]

        with patch('sys.argv', test_args):
            main()

            # Should not call metadata generation methods when no metadata output is specified
            assert not hasattr(mock_generator, '_read_csv_sample') or not mock_generator._read_csv_sample.called
            mock_generator.generate_schema.assert_called_once()
            mock_generator.output_schema.assert_called_once()
