"""Ultra-targeted tests to achieve 100% coverage for forklift_core.py - hitting every remaining line."""

import csv
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, call, mock_open, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from forklift.engine.forklift_core import (
    ExcessColumnMode,
    ForkliftCore,
    HeaderMode,
    ImportConfig,
    ProcessingResults,
    import_csv,
    import_excel,
    import_fwf,
    import_sql,
)


class TestUltraTargetedCoverage:
    """Ultra-targeted tests for the final 11% to reach 100% coverage."""

    def test_line_287_exact_break_condition(self):
        """Test skipped - method no longer exists after refactoring."""
        pytest.skip("Method _create_batch_reader no longer exists after ForkliftCore refactoring")


# Run with: python -m pytest tests/test_ultra_targeted_coverage.py --cov=src/forklift/engine/forklift_core --cov-report=term-missing -v
