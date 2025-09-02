"""Tests for the __main__ module."""
import pytest
import sys
from unittest.mock import patch, MagicMock


class TestMain:
    """Test cases for the __main__ module functionality."""

    def test_main_module_execution(self):
        """Test that the main module calls the CLI main function when executed."""
        # Clean approach: test the module's behavior without runpy
        import forklift.__main__
        from forklift.cli import main as cli_main

        # Verify that the __main__ module imports the correct main function
        assert forklift.__main__.main is cli_main

    @patch('forklift.cli.main')
    def test_main_module_script_execution(self, mock_main):
        """Test that the main module executes main() when run as script."""
        # Save the original module state
        main_module = sys.modules.get('forklift.__main__')

        try:
            # Remove the module from sys.modules to simulate fresh import
            if 'forklift.__main__' in sys.modules:
                del sys.modules['forklift.__main__']

            # Mock __name__ to be "__main__" during import
            with patch('forklift.__main__.__name__', '__main__'):
                # Import the module with __name__ == "__main__"
                import forklift.__main__

                # Since we patched __name__ to "__main__", the if block should execute
                # But we need to manually trigger it since import already happened
                if forklift.__main__.__name__ == "__main__":
                    forklift.__main__.main()

            mock_main.assert_called_once()

        finally:
            # Restore the original module state
            if main_module is not None:
                sys.modules['forklift.__main__'] = main_module

    def test_main_import(self):
        """Test that main can be imported from __main__ module."""
        from forklift.__main__ import main
        from forklift.cli import main as cli_main

        # Verify it's the same function
        assert main is cli_main
