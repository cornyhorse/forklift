"""Final targeted tests to achieve 100% coverage for forklift_core.py."""

import pytest
import tempfile
import json
import os
from pathlib import Path
from unittest.mock import patch, MagicMock
import pyarrow as pa

from forklift.engine.forklift_core import (
    ForkliftCore,
    ImportConfig,
    HeaderMode,
    ProcessingResults,
    ProcessingError,
    import_csv
)


class TestForkliftCoreFinalMissingLines:
    """Test cases to cover the final 38 missing lines for 100% coverage."""

                                    def test_json_schema_properties_missing(self):
        """Test skipped - method no longer exists after refactoring."""
        pytest.skip("Method import_sql no longer exists after ForkliftCore refactoring")


    def test_excel_config_edge_cases(self):
        """Test Excel config edge cases (lines 1542-1551)."""
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            test_file = Path(f.name)

        try:
            with patch('forklift.inputs.excel.ExcelInputHandler') as mock_handler:
                mock_handler_instance = MagicMock()
                mock_handler.return_value = mock_handler_instance
                mock_handler_instance.get_sheet_info.return_value = {
                    'sheet_names': ['Sheet1', 'Sheet2']
                }

                from forklift.engine.importers.excel_importer import ExcelImporter

                # Test edge cases for sheet selection
                with pytest.raises(ValueError):
                    ExcelImporter._create_default_excel_config(test_file, sheet='NonExistent')

                with pytest.raises(ValueError):
                    ExcelImporter._create_default_excel_config(test_file, sheet=10)  # Index out of range

        finally:
            test_file.unlink()
