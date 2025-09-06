"""
Test coverage for missing lines in forklift_core.py - Batch 1
Targeting lines: 288, 432->438, 725-739
"""

import tempfile
import json
import os
import shutil
from pathlib import Path
import pytest
import pyarrow as pa

from forklift.engine.forklift_core import ForkliftCore, ImportConfig, HeaderMode


class TestForkliftCoreMissingLinesBatch1:
    """Test class to cover specific missing lines in forklift_core.py"""

            def test_line_288_empty_row_handling_in_header_detection(self):
        """Test skipped - method no longer exists after refactoring."""
        pytest.skip("Method _detect_header_row no longer exists after ForkliftCore refactoring")


    def test_footer_detection_edge_cases(self):
        """Test edge cases in footer detection to cover more lines"""

        csv_content = """name,age,city
John,25,NYC

Jane,30,Chicago
"""

        temp_dir = tempfile.mkdtemp()
        csv_path = os.path.join(temp_dir, "test_footer_edge.csv")
        output_path = os.path.join(temp_dir, "output")

        try:
            with open(csv_path, 'w') as f:
                f.write(csv_content)

            config = ImportConfig(
                input_path=csv_path,
                output_path=output_path,
                header_mode=HeaderMode.PRESENT,
                footer_detection={
                    "stop_on_blank": True,  # This should stop on blank rows
                    "patterns": []
                }
            )

            core = ForkliftCore(config)
            result = core.process_csv()

            # Should stop at the blank row
            assert result.total_rows <= 2

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
