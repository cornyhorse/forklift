"""Targeted test for forklift_core.py missing line 288 - empty row handling in _detect_header_row method."""
import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

from forklift.engine.forklift_core import ForkliftCore, ImportConfig, HeaderMode


class TestForkliftCoreLine288Coverage:
    """Test suite targeting the specific missing line 288 in _detect_header_row method."""

    def test_detect_header_row_with_empty_rows(self):
        """Test _detect_header_row method with empty rows to cover line 288."""
        # Create a CSV file with empty rows followed by a header
        csv_content = """

# Comment line
,,,
name,age,city
John,25,NYC
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            f.flush()

            try:
                config = ImportConfig(
                    input_path=f.name,
                    output_path="dummy_output",
                    header_mode=HeaderMode.PRESENT,
                    skip_blank_lines=True
                )

                core = ForkliftCore(config)

                # This should trigger the _detect_header_row method and hit line 288
                # The method should skip empty rows and find the actual header
                header_idx, header_cols = core._detect_header_row(f.name)

                # Verify that it found the header correctly
                assert header_idx >= 0  # Should find a header row
                assert len(header_cols) == 3  # Should have 3 columns
                assert 'name' in header_cols
                assert 'age' in header_cols
                assert 'city' in header_cols

            finally:
                os.unlink(f.name)

    def test_detect_header_row_all_empty_rows(self):
        """Test _detect_header_row method with only empty rows to cover line 288 and return path."""
        # Create a CSV file with only empty rows
        csv_content = """


,,
  ,  ,  

"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            f.flush()

            try:
                config = ImportConfig(
                    input_path=f.name,
                    output_path="dummy_output",
                    header_mode=HeaderMode.PRESENT,
                    skip_blank_lines=True,
                    header_search_rows=10  # Search more rows
                )

                core = ForkliftCore(config)

                # This should trigger line 288 multiple times and eventually return (-1, [])
                header_idx, header_cols = core._detect_header_row(f.name)

                # Should return empty result for file with no valid header
                assert header_idx == -1
                assert header_cols == []

            finally:
                os.unlink(f.name)

    def test_detect_header_row_with_comment_starting_with_hash(self):
        """Test _detect_header_row method with rows starting with # to cover line 295."""
        # Create a CSV file with comment rows starting with #
        csv_content = """# This is a comment
# Another comment
#Third comment
name,age,city
John,25,NYC
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            f.flush()

            try:
                config = ImportConfig(
                    input_path=f.name,
                    output_path="dummy_output",
                    header_mode=HeaderMode.PRESENT
                )

                core = ForkliftCore(config)

                # This should skip the comment lines and find the actual header
                header_idx, header_cols = core._detect_header_row(f.name)

                # Should find the header after skipping comments
                assert header_idx >= 0
                assert len(header_cols) == 3
                assert 'name' in header_cols

            finally:
                os.unlink(f.name)
