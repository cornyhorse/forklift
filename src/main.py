from __future__ import annotations
from pathlib import Path

# Use the public high-level API
from forklift import read_csv

SAMPLE_PATH = Path("/Users/matt/PycharmProjects/forklift/tests/test-files/largecsv/parquet_types.csv")


def main() -> None:
    if not SAMPLE_PATH.exists():
        raise FileNotFoundError(f"Sample file not found: {SAMPLE_PATH}")
    df = read_csv(SAMPLE_PATH)
    # Show first few rows using underlying polars DataFrame
    print(df.head())


if __name__ == "__main__":  # simple manual smoke test
    main()
