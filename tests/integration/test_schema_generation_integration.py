"""Integration tests for schema generation functionality.

These tests verify that generated schemas can be used to successfully
process the original files, ensuring the schema generation is accurate
and complete.
"""

import json
import tempfile
from pathlib import Path
from typing import Any, Dict

import pytest

from forklift.api import (generate_and_save_schema, generate_schema_from_csv,
                          generate_schema_from_excel)
from forklift.engine.forklift_core import ForkliftCore, ImportConfig


class TestSchemaGenerationIntegration:
    """Integration tests for schema generation and file processing."""

    def test_generate_and_process_csv_full_file(self, tmp_path):
        """Test generating schema from CSV and using it to process the same file."""
        # Create test CSV file
        csv_content = """id,name,age,salary,is_active
1,John Doe,30,50000.50,true
2,Jane Smith,25,45000.00,false
3,Bob Johnson,35,60000.75,true"""

        csv_file = tmp_path / "test_data.csv"
        csv_file.write_text(csv_content)

        # Generate schema from the CSV
        schema = generate_schema_from_csv(
            input_path=str(csv_file),
            include_sample_data=False,  # Use new default (no sample data)
            infer_primary_key=True,
        )

        # Validate schema structure
        assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
        assert "properties" in schema
        assert "x-csv" in schema
        assert "x-generation" in schema

        # Check that all columns are present
        expected_columns = ["id", "name", "age", "salary", "is_active"]
        assert all(col in schema["properties"] for col in expected_columns)

        # Check data types are reasonable
        assert schema["properties"]["id"]["type"] == "integer"
        assert schema["properties"]["name"]["type"] == "string"
        assert schema["properties"]["age"]["type"] == "integer"
        assert schema["properties"]["salary"]["type"] == "number"
        assert schema["properties"]["is_active"]["type"] == "boolean"

        # Check primary key inference
        assert "x-primaryKey" in schema
        assert schema["x-primaryKey"]["columns"] == ["id"]

        # Check that sample data is NOT included by default (new behavior)
        assert "x-sample" not in schema

        # Save schema to file
        schema_file = tmp_path / "generated_schema.json"
        with open(schema_file, "w") as f:
            json.dump(schema, f, indent=2)

        # Now use the generated schema to process the original file
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        config = ImportConfig(
            input_path=str(csv_file), output_path=str(output_dir), schema_file=str(schema_file)
        )

        core = ForkliftCore(config)
        results = core.process_csv()

        # Verify processing was successful
        assert results.total_rows == 3
        assert results.valid_rows == 3
        assert results.invalid_rows == 0
        assert len(results.output_files) > 0

    def test_generate_and_process_csv_limited_rows(self, tmp_path):
        """Test generating schema from limited rows and processing full file."""
        # Create larger test CSV file
        csv_content = """id,name,age,department,hire_date
1,Alice Johnson,28,Engineering,2023-01-15
2,Bob Smith,32,Marketing,2022-06-20
3,Carol Davis,29,Engineering,2023-03-10
4,David Wilson,35,Sales,2021-11-05
5,Eva Brown,31,HR,2022-09-12
6,Frank Miller,27,Engineering,2023-05-18"""

        csv_file = tmp_path / "test_data.csv"
        csv_file.write_text(csv_content)

        # Generate schema using only first 3 rows
        schema = generate_schema_from_csv(
            input_path=str(csv_file),
            nrows=3,
            include_sample_data=True,  # Explicitly request sample data
        )

        # Validate schema was generated from limited data
        assert schema["x-generation"]["rows_analyzed"] == 3
        assert "x-sample" in schema  # Now checking for presence since we requested it
        assert len(schema["x-sample"]["rows"]) == 3

        # Save schema and process full file
        schema_file = tmp_path / "limited_schema.json"
        with open(schema_file, "w") as f:
            json.dump(schema, f, indent=2)

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        config = ImportConfig(
            input_path=str(csv_file), output_path=str(output_dir), schema_file=str(schema_file)
        )

        core = ForkliftCore(config)
        results = core.process_csv()

        # Should process all 6 rows even though schema was generated from 3
        assert results.total_rows == 6
        assert results.valid_rows == 6

    def test_generate_schema_with_different_delimiters(self, tmp_path):
        """Test schema generation with different CSV delimiters."""
        # Create pipe-delimited CSV
        csv_content = """id|name|score|category
1|Product A|95.5|Electronics
2|Product B|87.2|Clothing
3|Product C|92.8|Electronics"""

        csv_file = tmp_path / "pipe_delimited.csv"
        csv_file.write_text(csv_content)

        # Generate schema with pipe delimiter
        schema = generate_schema_from_csv(input_path=str(csv_file), delimiter="|")

        # Check that delimiter is captured in CSV extension
        assert schema["x-csv"]["delimiter"] == "|"

        # Verify correct column parsing
        expected_columns = ["id", "name", "score", "category"]
        assert all(col in schema["properties"] for col in expected_columns)

    def test_generate_and_save_schema_api(self, tmp_path):
        """Test the convenience API for generating and saving schemas."""
        # Create test data
        csv_content = """user_id,username,email,created_at
1,johndoe,john@example.com,2023-01-01T10:00:00
2,janedoe,jane@example.com,2023-01-02T11:30:00"""

        csv_file = tmp_path / "users.csv"
        csv_file.write_text(csv_content)

        schema_file = tmp_path / "users_schema.json"

        # Use API to generate and save schema
        generate_and_save_schema(
            input_path=str(csv_file), output_path=str(schema_file), file_type="csv", nrows=None
        )

        # Verify schema file was created and is valid JSON
        assert schema_file.exists()

        with open(schema_file, "r") as f:
            schema = json.load(f)

        assert schema["title"] == "Forklift CSV Schema - Generated"
        assert "user_id" in schema["properties"]
        assert "username" in schema["properties"]

    def test_schema_generation_handles_mixed_types(self, tmp_path):
        """Test schema generation with mixed data types."""
        csv_content = """id,mixed_field,number_field,bool_field
1,text_value,100,true
2,123,200.5,false
3,another_text,300,true"""

        csv_file = tmp_path / "mixed_types.csv"
        csv_file.write_text(csv_content)

        schema = generate_schema_from_csv(str(csv_file))

        # Check that mixed field is handled appropriately
        # (PyArrow will infer the most general type)
        assert "mixed_field" in schema["properties"]
        assert "number_field" in schema["properties"]
        assert "bool_field" in schema["properties"]

        # Verify the schema can be used for processing
        schema_file = tmp_path / "mixed_schema.json"
        with open(schema_file, "w") as f:
            json.dump(schema, f, indent=2)

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        config = ImportConfig(
            input_path=str(csv_file), output_path=str(output_dir), schema_file=str(schema_file)
        )

        core = ForkliftCore(config)
        results = core.process_csv()

        assert results.valid_rows == 3

    def test_excel_schema_generation(self, tmp_path):
        """Test schema generation from Excel files."""
        # This test requires pandas and openpyxl for Excel support
        pytest.importorskip("openpyxl")

        import pandas as pd

        # Create test Excel file
        data = {
            "id": [1, 2, 3],
            "product_name": ["Widget A", "Widget B", "Widget C"],
            "price": [19.99, 29.99, 39.99],
            "in_stock": [True, False, True],
        }

        df = pd.DataFrame(data)
        excel_file = tmp_path / "products.xlsx"
        df.to_excel(excel_file, index=False)

        # Generate schema from Excel
        from forklift.api import generate_schema_from_excel

        schema = generate_schema_from_excel(input_path=str(excel_file), include_sample_data=True)

        # Validate Excel schema
        assert schema["title"] == "Forklift EXCEL Schema - Generated"
        assert "x-excel" in schema
        assert all(
            col in schema["properties"] for col in ["id", "product_name", "price", "in_stock"]
        )

    def test_error_handling_invalid_file(self, tmp_path):
        """Test error handling for invalid files."""
        non_existent_file = tmp_path / "does_not_exist.csv"

        with pytest.raises(Exception):
            generate_schema_from_csv(str(non_existent_file))

    def test_error_handling_malformed_csv(self, tmp_path):
        """Test error handling for malformed CSV files."""
        # Create malformed CSV
        malformed_content = """id,name,age
1,John,30
2,Jane  # Missing field
3,Bob,35,extra_field"""

        csv_file = tmp_path / "malformed.csv"
        csv_file.write_text(malformed_content)

        # Should still generate a schema but might have some inconsistencies
        schema = generate_schema_from_csv(str(csv_file))

        # Basic validation that schema was generated
        assert "properties" in schema
        assert len(schema["properties"]) > 0

    def test_encoding_handling(self, tmp_path):
        """Test schema generation with different encodings."""
        # Create CSV with UTF-8 content including special characters
        csv_content = """id,name,description
1,José García,Café con leche
2,François Müller,Naïve résumé
3,李小明,中文描述"""

        csv_file = tmp_path / "utf8_data.csv"
        csv_file.write_text(csv_content, encoding="utf-8")

        schema = generate_schema_from_csv(input_path=str(csv_file), encoding="utf-8")

        # Verify schema generation handled encoding correctly
        assert "name" in schema["properties"]
        assert "description" in schema["properties"]
        assert schema["x-csv"]["encodingPriority"][0] == "utf-8"
