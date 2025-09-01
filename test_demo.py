"""Simple test to demonstrate the forklift CSV import functionality."""

import tempfile
import json
from pathlib import Path
import forklift as fl

# Create a sample CSV file for testing
def create_test_csv():
    """Create a sample CSV file with header and some data."""
    csv_content = """# This is a comment line
# Another comment line
Name,Age,Salary,Department
John Doe,30,50000.50,Engineering
Jane Smith,25,45000.75,Marketing
Bob Johnson,35,60000.00,Engineering
Alice Brown,28,52000.25,Sales
Charlie Wilson,32,55000.00,Engineering
"""

    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
    temp_file.write(csv_content)
    temp_file.flush()
    return temp_file.name

# Create a sample schema file
def create_test_schema():
    """Create a JSON schema file for the test CSV."""
    schema = {
        "type": "object",
        "properties": {
            "Name": {"type": "string"},
            "Age": {"type": "integer"},
            "Salary": {"type": "number"},
            "Department": {"type": "string"}
        },
        "required": ["Name", "Age", "Department"]
    }

    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
    json.dump(schema, temp_file, indent=2)
    temp_file.flush()
    return temp_file.name

def test_csv_import():
    """Test the CSV import functionality."""
    print("Testing Forklift CSV Import...")

    # Create test files
    csv_file = create_test_csv()
    schema_file = create_test_schema()
    output_dir = tempfile.mkdtemp()

    print(f"Input CSV: {csv_file}")
    print(f"Schema file: {schema_file}")
    print(f"Output directory: {output_dir}")

    try:
        # Test basic CSV import with schema
        results = fl.import_csv(
            input_path=csv_file,
            output_path=output_dir,
            schema_file=schema_file,
            header_mode="present",
            comment_rows=[r"^#"],  # Skip lines starting with #
            batch_size=1000
        )

        print(f"\nImport Results:")
        print(f"Total rows processed: {results.total_rows}")
        print(f"Valid rows: {results.valid_rows}")
        print(f"Invalid rows: {results.invalid_rows}")
        print(f"Execution time: {results.execution_time:.2f} seconds")
        print(f"Output files: {results.output_files}")

        if results.manifest_file:
            print(f"Manifest file: {results.manifest_file}")

        if results.metadata_file:
            print(f"Metadata file: {results.metadata_file}")

        if results.errors:
            print(f"Errors encountered: {results.errors}")

        # Check output files exist
        output_path = Path(output_dir)
        data_file = output_path / "data.parquet"
        if data_file.exists():
            print(f"\nData file created successfully: {data_file}")
            print(f"File size: {data_file.stat().st_size} bytes")

        # Test auto header detection
        print("\n" + "="*50)
        print("Testing auto header detection...")

        output_dir2 = tempfile.mkdtemp()
        results2 = fl.import_csv(
            input_path=csv_file,
            output_path=output_dir2,
            header_mode="auto",
            comment_rows=[r"^#"],
            batch_size=1000
        )

        print(f"Auto-detect results - Valid rows: {results2.valid_rows}")

        print("\nTest completed successfully!")

    except Exception as e:
        print(f"Error during testing: {str(e)}")
        import traceback
        traceback.print_exc()

    finally:
        # Cleanup
        import os
        try:
            os.unlink(csv_file)
            os.unlink(schema_file)
        except:
            pass

if __name__ == "__main__":
    test_csv_import()
