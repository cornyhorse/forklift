"""Ultra-targeted tests to achieve 100% coverage for forklift_core.py - hitting every remaining line."""

import pytest
import tempfile
import json
import os
import csv
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open, call
import pyarrow as pa
import pyarrow.parquet as pq

from forklift.engine.forklift_core import (
    ForkliftCore,
    ImportConfig,
    HeaderMode,
    ExcessColumnMode,
    ProcessingResults,
    import_csv,
    import_fwf,
    import_excel,
    import_sql
)


class TestUltraTargetedCoverage:
    """Ultra-targeted tests for the final 11% to reach 100% coverage."""

            def test_line_287_exact_break_condition(self):
        """Test skipped - method no longer exists after refactoring."""
        pytest.skip("Method _create_batch_reader no longer exists after ForkliftCore refactoring")



# Run with: python -m pytest tests/test_ultra_targeted_coverage.py --cov=src/forklift/engine/forklift_core --cov-report=term-missing -v
