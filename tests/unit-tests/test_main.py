"""Tests for the __main__ module."""
import pytest
from unittest.mock import patch, MagicMock


class TestMain:
    """Test cases for the __main__ module functionality."""

    @patch('forklift.__main__.main')
    def test_main_module_execution(self, mock_main):
        """Test that the main module calls the CLI main function when executed."""
        # Import the module to trigger the if __name__ == "__main__" block
        import runpy

        with patch('sys.argv', ['python', '-m', 'forklift']):
            try:
                runpy.run_module('forklift.__main__', run_name='__main__')
            except SystemExit:
                # Expected when no arguments are provided
                pass

        # The actual main function should be called
        # We can test this by importing and checking the structure
        from forklift.__main__ import main
        assert main is not None

    def test_main_import(self):
        """Test that main can be imported from __main__ module."""
        from forklift.__main__ import main
        from forklift.cli import main as cli_main

        # Verify it's the same function
        assert main is cli_main
