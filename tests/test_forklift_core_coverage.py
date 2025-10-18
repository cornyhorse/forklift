"""Tests to achieve 100% coverage for forklift_core.py missing lines."""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pyarrow as pa
import pytest

from forklift.engine.forklift_core import (ExcessColumnMode, ForkliftCore,
                                           HeaderMode, ImportConfig,
                                           import_csv, import_excel,
                                           import_fwf)


class TestForkliftCoreMissingCoverage:
    """Test cases specifically targeting missing coverage lines in forklift_core.py."""

    def test_auto_detect_header_no_suitable_header(self):
        """Test skipped - method no longer exists after refactoring."""
        pytest.skip("Method _create_metadata no longer exists after ForkliftCore refactoring")
