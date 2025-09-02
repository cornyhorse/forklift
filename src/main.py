from __future__ import annotations
from pathlib import Path
import forklift as fl

def main() -> None:
    # Hardcoded absolute paths for your demo
    schema_file = '/Users/matt/PycharmProjects/forklift/tests/test-files/largecsv/parquet_types.json'
    csv_file = '/Users/matt/PycharmProjects/forklift/tests/test-files/largecsv/parquet_types.txt'
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

    # Display schema information first
    print("\n=== Schema Information ===")
    from forklift.schema.csv_schema_importer import CsvSchemaImporter
    importer = CsvSchemaImporter(schema_file)
    schema_dict = importer.as_dict()
    print(f"Schema title: {schema_dict.get('title', 'Unknown')}")
    print(f"Number of columns: {len(schema_dict.get('properties', {}))}")
    print("Column types:")
    for col_name, col_def in schema_dict.get('properties', {}).items():
        col_type = col_def.get('type', 'unknown')
        col_format = col_def.get('format', '')
        type_str = f"{col_type}" + (f" ({col_format})" if col_format else "")
        print(f"  {col_name}: {type_str}")

    print("\n=== Processing CSV with Forklift ===")
    try:
        # Process the large CSV file using the new PyArrow engine
        results = fl.import_csv(
            input_path=csv_file,
            output_path=output_dir,
            schema_file=schema_file,
            header_mode="present",  # CSV has headers
            batch_size=50000,       # Process in 50k row batches for efficiency
            encoding="utf-8",
            validate_schema=True,
            create_manifest=True,
            create_metadata=True,
            compression="snappy"
        )

        print("✅ Processing completed successfully!")
        print(f"\n=== Results ===")
        print(f"Total rows processed: {results.total_rows:,}")
        print(f"Valid rows: {results.valid_rows:,}")
        print(f"Invalid rows: {results.invalid_rows:,}")
        print(f"Execution time: {results.execution_time:.2f} seconds")

        print(f"\n=== Output Files ===")
        for file_path in results.output_files:
            file_size = Path(file_path).stat().st_size if Path(file_path).exists() else 0
            print(f"📄 {Path(file_path).name}: {file_size:,} bytes")

        if results.manifest_file:
            print(f"📋 Manifest: {Path(results.manifest_file).name}")

        if results.metadata_file:
            print(f"📊 Metadata: {Path(results.metadata_file).name}")

        if results.errors:
            print(f"\n⚠️  Errors encountered:")
            for error in results.errors:
                print(f"  - {error}")

        # Calculate processing rate
        if results.execution_time > 0:
            rows_per_second = results.total_rows / results.execution_time
            print(f"\n🚀 Processing rate: {rows_per_second:,.0f} rows/second")

    except Exception as e:
        print(f"❌ Error processing CSV: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":  # simple manual smoke test
    main()
