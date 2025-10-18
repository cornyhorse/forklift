"""Additional tests for CLI module to achieve 100% code coverage."""

from unittest.mock import MagicMock, patch

import pytest

from forklift.cli import main


class TestCLI100PercentCoverage:
    """Test suite specifically targeting the remaining missing branch coverage."""

    @patch("forklift.cli.ForkliftCore")
    def test_ingest_csv_processing_no_optional_files(self, mock_forklift):
        """Test ingest CSV when optional files are None/empty (covers branches 93->95, 95->97)."""
        # Setup mock to return results with None/empty optional fields
        mock_results = MagicMock()
        mock_results.total_rows = 500
        mock_results.valid_rows = 500
        mock_results.invalid_rows = 0
        mock_results.output_files = None  # This should not print output files
        mock_results.manifest_file = None  # This should not print manifest file
        mock_results.metadata_file = None  # This should not print metadata file

        mock_core_instance = MagicMock()
        mock_core_instance.process_csv.return_value = mock_results
        mock_forklift.return_value = mock_core_instance

        test_args = ["forklift", "ingest", "input.csv", "--dest", "output/", "--input-kind", "csv"]

        with patch("sys.argv", test_args):
            with patch("builtins.print") as mock_print:
                main()
                # Verify basic output is printed
                mock_print.assert_any_call("Processing complete. Processed 500 rows.")
                mock_print.assert_any_call("Valid rows: 500, Invalid rows: 0")

                # Verify that optional file messages are NOT printed
                print_calls = [str(call) for call in mock_print.call_args_list]
                assert not any("Output files:" in call for call in print_calls)
                assert not any("Manifest file:" in call for call in print_calls)
                assert not any("Metadata file:" in call for call in print_calls)

    @patch("forklift.cli.ForkliftCore")
    def test_ingest_csv_processing_empty_output_files(self, mock_forklift):
        """Test ingest CSV when output_files is empty list (covers edge case)."""
        # Setup mock to return results with empty output files list
        mock_results = MagicMock()
        mock_results.total_rows = 100
        mock_results.valid_rows = 100
        mock_results.invalid_rows = 0
        mock_results.output_files = []  # Empty list should not print output files
        mock_results.manifest_file = "manifest.json"
        mock_results.metadata_file = "metadata.json"

        mock_core_instance = MagicMock()
        mock_core_instance.process_csv.return_value = mock_results
        mock_forklift.return_value = mock_core_instance

        test_args = ["forklift", "ingest", "input.csv", "--dest", "output/", "--input-kind", "csv"]

        with patch("sys.argv", test_args):
            with patch("builtins.print") as mock_print:
                main()
                # Verify basic output is printed
                mock_print.assert_any_call("Processing complete. Processed 100 rows.")
                mock_print.assert_any_call("Valid rows: 100, Invalid rows: 0")
                mock_print.assert_any_call("Manifest file: manifest.json")
                mock_print.assert_any_call("Metadata file: metadata.json")

                # Verify that empty output files message is NOT printed
                print_calls = [str(call) for call in mock_print.call_args_list]
                assert not any("Output files:" in call for call in print_calls)

    @patch("forklift.cli.SchemaGenerator")
    def test_generate_schema_metadata_output_no_file_returned(self, mock_schema_gen):
        """Test generate-schema when metadata generation returns None (covers branch 141->144)."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator

        # Mock methods - metadata generation returns None
        mock_generator.generate_schema.return_value = {"test": "schema"}
        mock_generator._read_csv_sample.return_value = "mock_table"
        mock_generator.generate_and_save_metadata.return_value = None

        test_args = [
            "forklift",
            "generate-schema",
            "input.csv",
            "--file-type",
            "csv",
            "--metadata-output",
            "metadata_output.json",
        ]

        with patch("sys.argv", test_args):
            with patch("builtins.print") as mock_print:
                main()
                # Should call the metadata methods but not print success message
                mock_generator._read_csv_sample.assert_called_once()
                mock_generator.generate_and_save_metadata.assert_called_once_with("mock_table")

                # Should not print metadata file message when None is returned
                print_calls = [str(call) for call in mock_print.call_args_list]
                assert not any("Metadata file written to:" in call for call in print_calls)

    @patch("forklift.cli.ForkliftCore")
    def test_ingest_csv_processing_mixed_optional_files(self, mock_forklift):
        """Test ingest CSV with mixed optional file conditions."""
        # Setup mock with some files present, some None
        mock_results = MagicMock()
        mock_results.total_rows = 250
        mock_results.valid_rows = 240
        mock_results.invalid_rows = 10
        mock_results.output_files = ["output1.parquet"]  # Has files
        mock_results.manifest_file = None  # No manifest
        mock_results.metadata_file = "metadata.json"  # Has metadata

        mock_core_instance = MagicMock()
        mock_core_instance.process_csv.return_value = mock_results
        mock_forklift.return_value = mock_core_instance

        test_args = ["forklift", "ingest", "input.csv", "--dest", "output/", "--input-kind", "csv"]

        with patch("sys.argv", test_args):
            with patch("builtins.print") as mock_print:
                main()
                # Verify basic output is printed
                mock_print.assert_any_call("Processing complete. Processed 250 rows.")
                mock_print.assert_any_call("Valid rows: 240, Invalid rows: 10")
                mock_print.assert_any_call("Output files: output1.parquet")
                mock_print.assert_any_call("Metadata file: metadata.json")

                # Verify that manifest file message is NOT printed
                print_calls = [str(call) for call in mock_print.call_args_list]
                assert not any("Manifest file:" in call for call in print_calls)

    @patch("forklift.cli.SchemaGenerator")
    def test_generate_schema_metadata_output_with_success_message(self, mock_schema_gen):
        """Test generate-schema when metadata generation succeeds (covers positive branch)."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator

        # Mock methods - metadata generation returns a file path
        mock_generator.generate_schema.return_value = {"test": "schema"}
        mock_generator._read_parquet_sample.return_value = "mock_table"
        mock_generator.generate_and_save_metadata.return_value = "metadata_output.json"

        test_args = [
            "forklift",
            "generate-schema",
            "input.parquet",
            "--file-type",
            "parquet",
            "--metadata-output",
            "metadata_output.json",
        ]

        with patch("sys.argv", test_args):
            with patch("builtins.print") as mock_print:
                main()
                # Should call the metadata methods and print success message
                mock_generator._read_parquet_sample.assert_called_once()
                mock_generator.generate_and_save_metadata.assert_called_once_with("mock_table")

                # Should print metadata file message when file is returned
                mock_print.assert_any_call("Metadata file written to: metadata_output.json")

    def test_generate_schema_file_output_validation_early_return(self):
        """Test generate-schema early return for file output validation (covers return at line 101)."""
        test_args = [
            "forklift",
            "generate-schema",
            "input.csv",
            "--file-type",
            "csv",
            "--output",
            "file",
            # Missing --output-path
        ]

        with patch("sys.argv", test_args):
            with patch("builtins.print") as mock_print:
                # The function should return early without proceeding further
                result = main()
                # Verify the error message is printed
                mock_print.assert_called_with(
                    "Error: --output-path is required when --output=file"
                )
                # The function should return None (early return)
                assert result is None

    @patch("forklift.cli.ForkliftCore")
    def test_ingest_csv_processing_falsy_output_files(self, mock_forklift):
        """Test ingest CSV when output_files is falsy but not None (covers additional edge case)."""
        # Setup mock to return results with falsy output files
        mock_results = MagicMock()
        mock_results.total_rows = 50
        mock_results.valid_rows = 50
        mock_results.invalid_rows = 0
        mock_results.output_files = ""  # Falsy string should not print output files
        mock_results.manifest_file = "manifest.json"
        mock_results.metadata_file = ""  # Falsy string should not print metadata file

        mock_core_instance = MagicMock()
        mock_core_instance.process_csv.return_value = mock_results
        mock_forklift.return_value = mock_core_instance

        test_args = ["forklift", "ingest", "input.csv", "--dest", "output/", "--input-kind", "csv"]

        with patch("sys.argv", test_args):
            with patch("builtins.print") as mock_print:
                main()
                # Verify basic output is printed
                mock_print.assert_any_call("Processing complete. Processed 50 rows.")
                mock_print.assert_any_call("Valid rows: 50, Invalid rows: 0")
                mock_print.assert_any_call("Manifest file: manifest.json")

                # Verify that falsy file messages are NOT printed
                print_calls = [str(call) for call in mock_print.call_args_list]
                assert not any("Output files:" in call for call in print_calls)
                assert not any("Metadata file:" in call for call in print_calls)
