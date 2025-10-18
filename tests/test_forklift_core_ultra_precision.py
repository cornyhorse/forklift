"""Ultra-precise tests to hit the final 33 missing lines for 100% coverage."""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pyarrow as pa
import pytest

from forklift.engine.forklift_core import (
    ForkliftCore,
    HeaderMode,
    ImportConfig,
    ProcessingError,
    ProcessingResults,
    import_csv,
    import_excel,
    import_sql,
)
from forklift.engine.importers.excel_importer import ExcelImporter


class TestForkliftCoreUltraPrecision:
    """Ultra-precise tests targeting the exact 33 remaining missing lines."""

    def test_line_189_exact_properties_get(self):
        """Test skipped - method no longer exists after refactoring."""
        pytest.skip(
            "Method _should_stop_for_footer no longer exists after ForkliftCore refactoring"
        )
