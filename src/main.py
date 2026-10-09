"""Minimal runnable example: import a CSV with a schema into Parquet.

Run from the repository root:

    python src/main.py

The input, schema and output locations are relative to the repository, so the example works on
any machine. Output goes to ``output/largecsv`` (created if needed).
"""

from __future__ import annotations

import sys
from pathlib import Path

import forklift as fl
from forklift.engine import HeaderMode

REPO_ROOT = Path(__file__).resolve().parent.parent
EXAMPLE_DIR = REPO_ROOT / "tests" / "test-files" / "largecsv"


def main() -> int:
    csv_file = EXAMPLE_DIR / "parquet_types.txt"
    schema_file = EXAMPLE_DIR / "parquet_types.json"
    output_dir = REPO_ROOT / "output" / "largecsv"

    print("=== Forklift CSV import example ===")
    print(f"Input CSV: {csv_file}")
    print(f"Schema: {schema_file}")
    print(f"Output directory: {output_dir}")

    for path in (csv_file, schema_file):
        if not path.exists():
            print(f"File not found: {path}", file=sys.stderr)
            return 1

    try:
        results = fl.import_csv(
            input_path=csv_file,
            output_path=output_dir,
            schema_file=schema_file,
            header_mode=HeaderMode.PRESENT,  # CSV has headers
            batch_size=50000,  # Process in 50k row batches for efficiency
            encoding="utf-8",
            validate_schema=True,
            create_manifest=True,
            create_metadata=True,
            compression="snappy",
        )
    except Exception as exc:
        print(f"Error processing CSV: {exc}", file=sys.stderr)
        return 1

    print("Processing completed.")
    print(f"Total rows processed: {results.total_rows:,}")
    print(f"Valid rows: {results.valid_rows:,}")
    print(f"Invalid rows: {results.invalid_rows:,}")
    print(f"Execution time: {results.execution_time:.2f} seconds")

    for file_path in results.output_files:
        size = Path(file_path).stat().st_size if Path(file_path).exists() else 0
        print(f"Output file: {Path(file_path).name} ({size:,} bytes)")
    if results.manifest_file:
        print(f"Manifest: {Path(results.manifest_file).name}")
    if results.metadata_file:
        print(f"Metadata: {Path(results.metadata_file).name}")

    if results.errors:
        print("Errors encountered:", file=sys.stderr)
        for error in results.errors:
            print(f"  - {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":  # simple manual smoke test
    sys.exit(main())
