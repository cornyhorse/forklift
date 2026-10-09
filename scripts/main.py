from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq

import forklift as fl

# Paths are derived from this file's location, so the demo works from any checkout
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def display_parquet_head(file_path: str, num_rows: int = 5) -> None:
    """Display the first few rows of a Parquet file."""
    try:
        table = pq.read_table(file_path)
        print(f"\n=== First {num_rows} rows of {Path(file_path).name} ===")
        print(", ".join(table.schema.names))
        for row in table.slice(0, num_rows).to_pylist():
            print(row)
        print(f"Total shape: ({table.num_rows}, {table.num_columns})")
    except Exception as e:
        print(f"❌ Error reading Parquet file {file_path}: {str(e)}")


def main() -> None:
    schema_file = str(PROJECT_ROOT / "tests" / "test-files" / "largecsv" / "parquet_types.json")
    csv_file = str(PROJECT_ROOT / "tests" / "test-files" / "largecsv" / "parquet_types.txt")
    output_dir = str(PROJECT_ROOT / "output" / "largecsv")

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
            input_path=csv_file, output_path=output_dir, schema_file=schema_file
        )
        print("CSV import completed.")
        print(results)

        # Calculate processing rate
        if results.execution_time > 0:
            rows_per_second = results.total_rows / results.execution_time
            print(f"\n🚀 Processing rate: {rows_per_second:,.0f} rows/second")

        # Display head of the first Parquet file
        if results.output_files:
            parquet_files = [f for f in results.output_files if f.endswith(".parquet")]
            if parquet_files:
                display_parquet_head(parquet_files[0], num_rows=10)

    except Exception as e:
        print(f"❌ Error processing CSV: {str(e)}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":  # simple manual smoke test
    main()
