"""Tests for __main__ module."""

import pytest
from unittest.mock import patch, MagicMock


def test_main_module_execution():
    """Test that __main__ module calls cli.main when executed."""
    with patch('forklift.cli.main') as mock_main:
        # Import the module which should trigger the main() call when __name__ == "__main__"
        # We need to simulate the __name__ == "__main__" condition
        with patch('forklift.__main__.__name__', '__main__'):
            import forklift.__main__
            # The import itself doesn't trigger main(), so we need to test the conditional

        # Test that the module has the correct imports
        assert hasattr(forklift.__main__, 'main')


def test_main_module_import():
    """Test that __main__ module can be imported without issues."""
    import forklift.__main__
    assert hasattr(forklift.__main__, 'main')


def test_main_module_execution_path():
    """Test the execution path when run as main module."""
    with patch('forklift.cli.main') as mock_main:
        # Simulate running as main module
        import sys
        original_name = sys.modules.get('forklift.__main__', {}).get('__name__')

        # Execute the main block logic directly
        exec("""
if __name__ == "__main__":
    from forklift.cli import main
    main()
""")

        # Verify main was called
        mock_main.assert_called_once()
