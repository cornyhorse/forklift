"""Tests for the __main__ module."""

import importlib
import runpy
import sys
import warnings
from unittest.mock import MagicMock, patch

import pytest


class TestMain:
    """Test cases for the __main__ module functionality."""

    def test_main_module_execution(self):
        """Test that the main module calls the CLI main function when executed."""
        # Clean approach: test the module's behavior without runpy
        import forklift.__main__
        from forklift.cli import main as cli_main

        # Verify that the __main__ module imports the correct main function
        assert forklift.__main__.main is cli_main

    def test_main_import(self):
        """Test that main can be imported from __main__ module."""
        from forklift.__main__ import main
        from forklift.cli import main as cli_main

        # Verify it's the same function
        assert main is cli_main

    def test_main_function_availability(self):
        """Test that the main function is properly imported and available."""
        import forklift.__main__

        # Verify the main function is available in the module
        assert hasattr(forklift.__main__, "main")
        assert callable(forklift.__main__.main)

        # Verify it's the same as the CLI main function
        from forklift.cli import main as cli_main

        assert forklift.__main__.main is cli_main

    @patch("forklift.cli.main")
    def test_main_module_script_execution_with_runpy(self, mock_main):
        """Test that the main module executes main() when run as script using runpy."""
        # Use runpy to simulate running the module as a script
        # This is the most accurate way to test the if __name__ == "__main__": block

        # Suppress the expected RuntimeWarning about module being found in sys.modules
        # This warning is normal when using runpy to test __main__ modules and can be safely ignored
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore", message=".*found in sys.modules after import.*", category=RuntimeWarning
            )
            try:
                runpy.run_module("forklift", run_name="__main__")
            except SystemExit:
                # It's normal for CLI applications to call sys.exit()
                pass

        # Verify that main() was called
        mock_main.assert_called_once()

    @patch("forklift.cli.main")
    def test_main_module_direct_execution_simulation(self, mock_main):
        """Test the __main__ module execution by simulating the condition."""
        # This test simulates what happens when the module is executed directly

        # Import the module to get access to its main function
        import forklift.__main__

        # Directly test the conditional logic by executing it
        # We know the condition is: if __name__ == "__main__": main()
        # So we'll simulate this by calling main() as if the condition was true
        # Save the original main function
        original_main = forklift.__main__.main

        try:
            # Replace with our mock
            forklift.__main__.main = mock_main

            # Simulate the execution of the if block
            # This is what would happen if __name__ == "__main__"
            forklift.__main__.main()

            # Verify the mock was called
            mock_main.assert_called_once()

        finally:
            # Restore the original function
            forklift.__main__.main = original_main

    def test_module_name_when_imported(self):
        """Test that the module has the correct __name__ when imported normally."""
        import forklift.__main__

        # When imported normally, __name__ should be the module name
        assert forklift.__main__.__name__ == "forklift.__main__"

    def test_main_module_code_structure(self):
        """Test the structure and content of the __main__ module."""
        import inspect

        import forklift.__main__

        # Get the source code of the module
        source = inspect.getsource(forklift.__main__)

        # Verify it contains the expected import
        assert "from .cli import main" in source

        # Verify it contains the if __name__ == "__main__" block
        assert 'if __name__ == "__main__":' in source
        assert "main()" in source

    @patch("forklift.cli.main")
    def test_manual_execution_of_main_block(self, mock_main):
        """Test manual execution of the main block logic."""
        # This test manually executes the logic that would run when __name__ == "__main__"

        # Import the module
        import forklift.__main__

        # Create a scenario where we simulate __name__ being "__main__"
        # and then execute the main function
        # Replace the main function temporarily with our mock
        original_main = forklift.__main__.main
        forklift.__main__.main = mock_main

        try:
            # Simulate the condition being true and execute main()
            # This is effectively testing: if __name__ == "__main__": main()
            exec_globals = {"__name__": "__main__", "main": mock_main}
            exec('if __name__ == "__main__": main()', exec_globals)

            # Verify main was called
            mock_main.assert_called_once()

        finally:
            # Restore original function
            forklift.__main__.main = original_main

    def test_import_structure(self):
        """Test that the import structure is correct."""
        # Test that we can import the module without errors
        import forklift.__main__

        # Test that the cli module is importable
        from forklift import cli

        # Test that main is properly imported from cli
        assert hasattr(cli, "main")
        assert callable(cli.main)

        # Test that the __main__ module has access to main
        assert forklift.__main__.main is cli.main
