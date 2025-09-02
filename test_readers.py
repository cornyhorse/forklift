"""Test the new forklift reader functionality."""

import forklift as fl
import tempfile
import os
from pathlib import Path

def test_reader_functionality():
    """Test the new read_csv functionality with DataFrame conversion."""

    # Create a simple test CSV
    test_csv_content = """name,age,city
Alice,25,New York
Bob,30,San Francisco
Charlie,35,Chicago"""

    # Write test CSV to temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(test_csv_content)
        csv_path = f.name

    try:
        print("Testing forklift readers...")

        # Test reading CSV and converting to different formats
        reader = fl.read_csv(csv_path)

        # Test Polars DataFrame (if available)
        try:
            df_polars = reader.as_polars()
            print(f"✅ Polars DataFrame: {df_polars.shape} rows/cols")
            print("Polars data preview:")
            print(df_polars.head())
        except ImportError:
            print("⚠️  Polars not available - install with: pip install polars")

        # Test Pandas DataFrame
        try:
            df_pandas = reader.as_pandas()
            print(f"✅ Pandas DataFrame: {df_pandas.shape} rows/cols")
            print("Pandas data preview:")
            print(df_pandas.head())
        except ImportError:
            print("⚠️  Pandas not available")

        # Test PyArrow Table
        try:
            table_arrow = reader.as_pyarrow()
            print(f"✅ PyArrow Table: {table_arrow.num_rows} rows, {table_arrow.num_columns} cols")
            print("PyArrow schema:")
            print(table_arrow.schema)
        except ImportError:
            print("⚠️  PyArrow not available")

        # Test lazy Polars (if available)
        try:
            lf_polars = reader.as_polars(lazy=True)
            print(f"✅ Polars LazyFrame created")
            print("LazyFrame preview:")
            print(lf_polars.collect().head())
        except ImportError:
            print("⚠️  Polars not available for lazy evaluation")

        # Clean up
        reader.cleanup()
        print("✅ Cleanup completed")

    finally:
        # Remove test CSV
        os.unlink(csv_path)

if __name__ == "__main__":
    test_reader_functionality()
