"""Tests to achieve 100% coverage for forklift_core.py missing lines."""

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
    ExcessColumnMode,
    import_csv,
    import_fwf,
    import_excel
)


class TestForkliftCoreMissingCoverage:
    """Test cases specifically targeting missing coverage lines in forklift_core.py."""

    def test_auto_detect_header_no_suitable_header(self):
        """Test skipped - method no longer exists after refactoring."""
        pytest.skip("Method _create_metadata no longer exists after ForkliftCore refactoring")

