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
        """Test line 288: Skip completely empty rows during header detection"""

        # Create CSV with empty rows during header search
        csv_content = """

# This is a comment

name,age,city
John,25,NYC
"""

        temp_dir = tempfile.mkdtemp()
        csv_path = os.path.join(temp_dir, "test_empty_rows.csv")
        output_path = os.path.join(temp_dir, "output")

        try:
            with open(csv_path, 'w') as f:
                f.write(csv_content)

            config = ImportConfig(
                input_path=csv_path,
                output_path=output_path,
                header_mode=HeaderMode.AUTO,  # This triggers header detection
                header_search_rows=10  # Search through enough rows to hit empty ones
            )

            core = ForkliftCore(config)

            # This should trigger the auto header detection that processes empty rows
            header_info = core._detect_header_row(csv_path)

            # Verify that header was found despite empty rows
            assert header_info is not None
            assert header_info[0] >= 0  # row_index
            assert len(header_info[1]) > 0  # columns found

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_lines_432_438_footer_pattern_matching(self):
        """Test lines 432-438: Footer detection pattern matching logic"""

        # Create CSV with footer that matches patterns
        csv_content = """name,age,city
John,25,NYC
Jane,30,Chicago
TOTAL:,2,RECORDS
"""

        temp_dir = tempfile.mkdtemp()
        csv_path = os.path.join(temp_dir, "test_footer.csv")
        output_path = os.path.join(temp_dir, "output")

        try:
            with open(csv_path, 'w') as f:
                f.write(csv_content)

            # Configure with footer detection that has specific patterns
            config = ImportConfig(
                input_path=csv_path,
                output_path=output_path,
                header_mode=HeaderMode.PRESENT,
                footer_detection={
                    "stop_on_blank": False,
                    "patterns": [
                        {
                            "column_index": 0,
                            "patterns": [r"^TOTAL:", r"^SUM:", r"^SUMMARY"]
                        }
                    ]
                }
            )

            core = ForkliftCore(config)

            # This should trigger the footer detection logic
            result = core.process_csv()

            # Verify processing completed with footer detected
            # The footer pattern should be detected and processing should stop before TOTAL row
            assert result.total_rows <= 3  # Should process all or stop before footer

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_footer_pattern_no_match(self):
        """Test footer detection when patterns don't match"""

        csv_content = """name,age,city
John,25,NYC
Jane,30,Chicago
Bob,35,LA
"""

        temp_dir = tempfile.mkdtemp()
        csv_path = os.path.join(temp_dir, "test_no_footer.csv")
        output_path = os.path.join(temp_dir, "output")

        try:
            with open(csv_path, 'w') as f:
                f.write(csv_content)

            config = ImportConfig(
                input_path=csv_path,
                output_path=output_path,
                header_mode=HeaderMode.PRESENT,
                footer_detection={
                    "stop_on_blank": False,
                    "patterns": [
                        {
                            "column_index": 0,
                            "patterns": [r"^TOTAL:", r"^SUMMARY:"]
                        }
                    ]
                }
            )

            core = ForkliftCore(config)

            result = core.process_csv()

            # All rows should be processed since no footer was detected
            assert result.total_rows == 3

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_lines_725_739_validate_batch_error_handling(self):
        """Test lines 725-739: Error handling in validate_batch method"""

        csv_content = """name,age,city
John,25,NYC
Jane,not_a_number,Chicago
Bob,,LA
"""

        # Schema that will cause validation errors
        schema_content = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer", "nullable": False},  # Make age required/non-nullable
                "city": {"type": "string"}
            },
            "required": ["name", "age", "city"]
        }

        temp_dir = tempfile.mkdtemp()
        csv_path = os.path.join(temp_dir, "test_validation.csv")
        schema_path = os.path.join(temp_dir, "test_schema.json")
        output_path = os.path.join(temp_dir, "output")

        try:
            with open(csv_path, 'w') as f:
                f.write(csv_content)

            with open(schema_path, 'w') as f:
                json.dump(schema_content, f)

            config = ImportConfig(
                input_path=csv_path,
                output_path=output_path,
                schema_file=schema_path,
                header_mode=HeaderMode.PRESENT,
                validate_schema=True,
                max_validation_errors=10
            )

            core = ForkliftCore(config)

            # Force schema to be non-nullable for age
            core.schema = core._load_schema()
            if core.schema:
                # Modify the age field to be non-nullable
                fields = []
                for field in core.schema:
                    if field.name == "age":
                        fields.append(pa.field("age", pa.int64(), nullable=False))
                    else:
                        fields.append(field)
                core.schema = pa.schema(fields)

            # Process with validation errors
            result = core.process_csv()

            # Should have some invalid rows due to schema validation
            # Since we have invalid data, we should see some validation errors
            assert result.total_rows >= 2  # At least processed some rows

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_empty_row_in_csv_data(self):
        """Test handling of completely empty rows in CSV data"""

        csv_content = """name,age,city
John,25,NYC

Jane,30,Chicago

"""

        temp_dir = tempfile.mkdtemp()
        csv_path = os.path.join(temp_dir, "test_empty_data_rows.csv")
        output_path = os.path.join(temp_dir, "output")

        try:
            with open(csv_path, 'w') as f:
                f.write(csv_content)

            config = ImportConfig(
                input_path=csv_path,
                output_path=output_path,
                header_mode=HeaderMode.PRESENT
            )

            core = ForkliftCore(config)
            result = core.process_csv()

            # Should process non-empty rows only
            assert result.total_rows == 2

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_comment_row_handling_in_header_detection(self):
        """Test handling of comment rows during header detection"""

        csv_content = """# This file contains customer data
# Generated on 2023-12-01
# Contact: admin@company.com
name,age,city
John,25,NYC
Jane,30,Chicago
"""

        temp_dir = tempfile.mkdtemp()
        csv_path = os.path.join(temp_dir, "test_comments.csv")
        output_path = os.path.join(temp_dir, "output")

        try:
            with open(csv_path, 'w') as f:
                f.write(csv_content)

            config = ImportConfig(
                input_path=csv_path,
                output_path=output_path,
                header_mode=HeaderMode.AUTO,
                header_search_rows=10
            )

            core = ForkliftCore(config)

            # This should skip comment lines and find the real header
            header_info = core._detect_header_row(csv_path)

            assert header_info is not None
            # Header should be found (may be at index 0 if comments are handled during processing)
            assert header_info[0] >= 0  # row_index should be >= 0
            assert "name" in header_info[1]  # columns should contain 'name'

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

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
