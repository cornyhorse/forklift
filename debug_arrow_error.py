#!/usr/bin/env python3
"""Debug script to test what happens with problematic CSV content."""

import tempfile
from pathlib import Path
import pyarrow as pa
import pyarrow.csv as pv_csv

def test_arrow_with_null_bytes():
    """Test what happens when PyArrow encounters null bytes."""

    # Create problematic file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as tmp_file:
        tmp_file.write("invalid,csv\ndata,with\x00null,bytes")
        problem_file = Path(tmp_file.name)

    try:
        # Configure CSV read options similar to BatchProcessor
        parse_options = pv_csv.ParseOptions(
            delimiter=',',
            quote_char='"',
            escape_char='\\',
            ignore_empty_lines=True,
        )

        read_options = pv_csv.ReadOptions(
            encoding='utf-8',
            skip_rows=0,
            column_names=['col1', 'col2'],
        )

        convert_options = pv_csv.ConvertOptions(
            check_utf8=False,
        )

        print("Testing PyArrow CSV reader with null bytes...")

        with open(problem_file, 'rb') as f:
            csv_reader = pv_csv.open_csv(
                f,
                parse_options=parse_options,
                read_options=read_options,
                convert_options=convert_options,
            )

            # Try to read batches
            batch_count = 0
            while True:
                try:
                    batch = csv_reader.read_next_batch()
                    if batch is None:
                        break
                    print(f"Successfully read batch {batch_count}: {batch}")
                    batch_count += 1
                except StopIteration:
                    break
                except Exception as e:
                    print(f"Exception during batch reading: {type(e).__name__}: {e}")
                    raise

            print(f"Total batches read: {batch_count}")

    except Exception as e:
        print(f"Exception during CSV opening: {type(e).__name__}: {e}")
        raise
    finally:
        problem_file.unlink(missing_ok=True)

if __name__ == "__main__":
    test_arrow_with_null_bytes()
