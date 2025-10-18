"""Ultra-precise tests to hit the exact missing branches for 100% CLI coverage."""

from unittest.mock import MagicMock, patch

import pytest

from forklift.cli import main


class TestCLIPreciseBranches:
    """Test suite with surgical precision for the exact missing branches."""

    def test_file_output_validation_return_branch_101_exit(self):
        """Test the exact return statement after file output validation (line 101->exit)."""
        test_args = [
            "forklift",
            "generate-schema",
            "input.csv",
            "--file-type",
            "csv",
            "--output",
            "file",
            # No --output-path to trigger validation failure
        ]

        with patch("sys.argv", test_args):
            with patch("builtins.print") as mock_print:
                # Call main and capture the result
                result = main()

                # Verify error message and early return
                mock_print.assert_called_with("Error: --output-path is required when --output=file")
                # This should hit the return statement at line 101
                assert result is None

    @patch("forklift.cli.SchemaGenerator")
    def test_metadata_file_conditional_branch_141_144(self, mock_schema_gen):
        """Test the exact conditional at line 141->144: if metadata_file:"""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator

        # Test the NEGATIVE branch (metadata_file is falsy)
        mock_generator.generate_schema.return_value = {"schema": "data"}
        mock_generator.output_schema.return_value = None
        mock_generator._read_csv_sample.return_value = "table_data"
        mock_generator.generate_and_save_metadata.return_value = ""  # Empty string (falsy)

        test_args = [
            "forklift",
            "generate-schema",
            "input.csv",
            "--file-type",
            "csv",
            "--metadata-output",
            "metadata.json",
        ]

        with patch("sys.argv", test_args):
            with patch("builtins.print") as mock_print:
                main()

                # Verify metadata methods were called
                mock_generator._read_csv_sample.assert_called_once()
                mock_generator.generate_and_save_metadata.assert_called_once()

                # The key test: NO success message should be printed (falsy branch)
                all_calls = [str(call) for call in mock_print.call_args_list]
                metadata_messages = [
                    call for call in all_calls if "Metadata file written to:" in call
                ]
                assert (
                    len(metadata_messages) == 0
                ), f"Expected no metadata messages, but got: {metadata_messages}"

    @patch("forklift.cli.SchemaGenerator")
    def test_metadata_file_conditional_branch_141_144_positive(self, mock_schema_gen):
        """Test the POSITIVE branch of the metadata conditional."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator

        # Test the POSITIVE branch (metadata_file is truthy)
        mock_generator.generate_schema.return_value = {"schema": "data"}
        mock_generator.output_schema.return_value = None
        mock_generator._read_excel_sample.return_value = "table_data"
        mock_generator.generate_and_save_metadata.return_value = "actual_file.json"  # Truthy value

        test_args = [
            "forklift",
            "generate-schema",
            "input.xlsx",
            "--file-type",
            "excel",
            "--metadata-output",
            "metadata.json",
        ]

        with patch("sys.argv", test_args):
            with patch("builtins.print") as mock_print:
                main()

                # Verify metadata methods were called
                mock_generator._read_excel_sample.assert_called_once()
                mock_generator.generate_and_save_metadata.assert_called_once()

                # The key test: success message SHOULD be printed (truthy branch)
                mock_print.assert_any_call("Metadata file written to: actual_file.json")

    @patch("forklift.cli.SchemaGenerator")
    def test_metadata_file_conditional_branch_with_none_value(self, mock_schema_gen):
        """Test the conditional with explicit None return value."""
        mock_generator = MagicMock()
        mock_schema_gen.return_value = mock_generator

        # Test with explicit None (falsy)
        mock_generator.generate_schema.return_value = {"schema": "data"}
        mock_generator.output_schema.return_value = None
        mock_generator._read_parquet_sample.return_value = "table_data"
        mock_generator.generate_and_save_metadata.return_value = None  # Explicit None

        test_args = [
            "forklift",
            "generate-schema",
            "input.parquet",
            "--file-type",
            "parquet",
            "--metadata-output",
            "metadata.json",
        ]

        with patch("sys.argv", test_args):
            with patch("builtins.print") as mock_print:
                main()

                # Verify metadata methods were called
                mock_generator._read_parquet_sample.assert_called_once()
                mock_generator.generate_and_save_metadata.assert_called_once()

                # The key test: NO success message should be printed (None/falsy branch)
                all_calls = [str(call) for call in mock_print.call_args_list]
                metadata_messages = [
                    call for call in all_calls if "Metadata file written to:" in call
                ]
                assert (
                    len(metadata_messages) == 0
                ), f"Expected no metadata messages, but got: {metadata_messages}"
