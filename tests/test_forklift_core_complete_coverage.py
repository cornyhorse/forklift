"""Complete coverage tests for forklift_core.py to achieve 100% coverage."""

import pytest
import tempfile
import json
import os
import io
import csv
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open, Mock
import pyarrow as pa
import pyarrow.parquet as pq
from dataclasses import dataclass

from forklift.engine.forklift_core import (
    ForkliftCore,
    ImportConfig,
    HeaderMode,
    ExcessColumnMode,
    ProcessingResults,
    ProcessingError,
    import_csv,
    import_fwf,
    import_excel,
    import_sql
)
from forklift.engine.importers.excel_importer import ExcelImporter


class TestForkliftCoreCompleteCoverage:
    """Test cases to achieve 100% coverage for all remaining missing lines."""

                                    def test_json_schema_to_pyarrow_with_required_fields(self):
        """Test skipped - method no longer exists after refactoring."""
        pytest.skip("Method _create_manifest no longer exists after ForkliftCore refactoring")

