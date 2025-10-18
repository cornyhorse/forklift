"""Final tests to achieve 100% CLI coverage - targeting the last 2 missing branches."""

from unittest.mock import MagicMock, patch

import pytest

from forklift.cli import main


class TestCLIFinalBranches:
    """Test suite targeting the final 2 missing branches for 100% coverage."""

    def test_generate_schema_file_output_validation_early_return(self):
        """Test the early return after file output validation error (covers 101->exit)."""
        test_args = [
            "forklift",
            "generate-schema",
            "input.csv",
            "--file-type",
            "csv",
            "--output",
            "file",
            # Missing --output-path intentionally to trigger validation error
        ]

        with patch("sys.argv", test_args):
            with patch("builtins.print") as mock_print:
                # This should trigger the early return without any exception
                result = main()

                # Verify the error message is printed
                mock_print.assert_called_with("Error: --output-path is required when --output=file")
                # The function should return None (early return)
                assert result is None

    @patch("forklift.cli.SchemaGenerator")
    def test_generate_schema_metadata_file_none_condition(self, mock_schema_gen):
        """Test when metadata generation returns None (covers 141->144 negative branch)."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator

        # Mock methods - metadata generation returns None (no file written)
        mock_generator.generate_schema.return_value = {"test": "schema"}
        mock_generator._read_csv_sample.return_value = "mock_table"
        mock_generator.generate_and_save_metadata.return_value = None  # This is key!

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

                # Verify the metadata methods were called
                mock_generator._read_csv_sample.assert_called_once()
                mock_generator.generate_and_save_metadata.assert_called_once_with("mock_table")

                # Verify that NO metadata success message is printed (None branch)
                print_calls = [str(call) for call in mock_print.call_args_list]
                assert not any("Metadata file written to:" in call for call in print_calls)

    @patch("forklift.cli.SchemaGenerator")
    def test_generate_schema_metadata_file_truthy_condition(self, mock_schema_gen):
        """Test when metadata generation returns a file path (covers 141->144 positive branch)."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator

        # Mock methods - metadata generation returns a file path
        mock_generator.generate_schema.return_value = {"test": "schema"}
        mock_generator._read_parquet_sample.return_value = "mock_table"
        mock_generator.generate_and_save_metadata.return_value = (
            "metadata_file.json"  # Truthy value
        )

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

                # Verify the metadata methods were called
                mock_generator._read_parquet_sample.assert_called_once()
                mock_generator.generate_and_save_metadata.assert_called_once_with("mock_table")

                # Verify that the metadata success message IS printed (truthy branch)
                mock_print.assert_any_call("Metadata file written to: metadata_file.json")

    @patch("forklift.cli.SchemaGenerator")
    def test_generate_schema_metadata_no_output_path_conditional(self, mock_schema_gen):
        """Test metadata generation without output path specified."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator

        # Mock methods
        mock_generator.generate_schema.return_value = {"test": "schema"}

        test_args = [
            "forklift",
            "generate-schema",
            "input.csv",
            "--file-type",
            "csv",
            # No --metadata-output specified
        ]

        with patch("sys.argv", test_args):
            main()

            # Should not call metadata generation methods when no metadata output is specified
            assert (
                not hasattr(mock_generator, "_read_csv_sample")
                or not mock_generator._read_csv_sample.called
            )
            mock_generator.generate_schema.assert_called_once()
            mock_generator.output_schema.assert_called_once()
