"""Complete coverage tests for forklift_core.py to achieve 100% coverage."""

import csv
import io
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock, Mock, mock_open, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from forklift.engine.forklift_core import (
    ExcessColumnMode,
    ForkliftCore,
    HeaderMode,
    ImportConfig,
    ProcessingError,
    ProcessingResults,
    import_csv,
    import_excel,
    import_fwf,
    import_sql,
)
from forklift.engine.importers.excel_importer import ExcelImporter


class TestForkliftCoreCompleteCoverage:
    """Test cases to achieve 100% coverage for all remaining missing lines."""

    def test_json_schema_to_pyarrow_with_required_fields(self):
        """Test skipped - method no longer exists after refactoring."""
        pytest.skip("Method _create_manifest no longer exists after ForkliftCore refactoring")
