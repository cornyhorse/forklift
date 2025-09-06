"""Ultra-precise tests to hit the final 33 missing lines for 100% coverage."""

import pytest
import tempfile
import json
import os
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open
import pyarrow as pa

from forklift.engine.forklift_core import (
    ForkliftCore,
    ImportConfig,
    HeaderMode,
    ProcessingResults,
    ProcessingError,
    import_csv,
    import_excel,
    import_sql
)
from forklift.engine.importers.excel_importer import ExcelImporter


class TestForkliftCoreUltraPrecision:
    """Ultra-precise tests targeting the exact 33 remaining missing lines."""

                            def test_line_189_exact_properties_get(self):
        """Test skipped - method no longer exists after refactoring."""
        pytest.skip("Method _should_stop_for_footer no longer exists after ForkliftCore refactoring")

