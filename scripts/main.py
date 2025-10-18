from __future__ import annotations
from pathlib import Path
import forklift as fl
import pandas as pd

def display_parquet_head(file_path: str, num_rows: int = 5) -> None:
    """Display the first few rows of a Parquet file."""
    try:
        df = pd.read_parquet(file_path)
        print(f"\n=== First {num_rows} rows of {Path(file_path).name} ===")
        print(df.head(num_rows).to_string(index=False, max_cols=10))
        print(f"Total shape: {df.shape}")
    except Exception as e:
        print(f"❌ Error reading Parquet file {file_path}: {str(e)}")

def main() -> None:
    # Hardcoded absolute paths for demo purposes
    schema_file = '/tests/test-files/largecsv/parquet_types.json'
    csv_file = '/tests/test-files/largecsv/parquet_types.txt'
    output_dir = '/Users/matt/PycharmProjects/forklift/output/largecsv'

    print("=== Forklift Large CSV Processing ===")
    print(f"Input CSV: {csv_file}")
    print(f"Schema: {schema_file}")
    print(f"Output directory: {output_dir}")

    # Check if files exist
    if not Path(csv_file).exists():
        print(f"❌ CSV file not found: {csv_file}")
        print("Run the CSV generator first if needed.")
        return

    if not Path(schema_file).exists():
        print(f"❌ Schema file not found: {schema_file}")
        return

    from forklift.schema.csv_schema_importer import CsvSchemaImporter
    importer = CsvSchemaImporter(schema_file)
    schema_dict = importer.as_dict()

    try:
        results = fl.import_csv(
            input_path=csv_file,
            output_path=output_dir,
            schema_file=schema_file
        )
        print("CSV import completed.")
        print(results)

        # Calculate processing rate
        if results.execution_time > 0:
            rows_per_second = results.total_rows / results.execution_time
            print(f"\n🚀 Processing rate: {rows_per_second:,.0f} rows/second")

        # Display head of the first Parquet file
        if results.output_files:
            parquet_files = [f for f in results.output_files if f.endswith('.parquet')]
            if parquet_files:
                display_parquet_head(parquet_files[0], num_rows=10)

    except Exception as e:
        print(f"❌ Error processing CSV: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":  # simple manual smoke test
    main()
