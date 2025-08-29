import json
from pathlib import Path
import pyarrow.parquet as pq
from forklift.engine.engine import Engine

DATA_DIR = Path(__file__).parent.parent / "test-files" / "largecsv"
CSV_FILE = DATA_DIR / "parquet_types.csv"
SCHEMA_FILE = DATA_DIR / "parquet_types.json"
TOTAL_GENERATED_ROWS = 200_000
SKIP_EVERY = 53
SKIPPED_ROWS = (TOTAL_GENERATED_ROWS - 1) // SKIP_EVERY + 1
EXPECTED_ROWS = TOTAL_GENERATED_ROWS - SKIPPED_ROWS

# Use a relatively small processing_chunk_size and output_chunk_size to exercise streaming code paths

def test_largecsv_chunked(tmp_path: Path):
    assert CSV_FILE.exists(), f"Missing test csv {CSV_FILE}"
    schema = json.loads(SCHEMA_FILE.read_text(encoding="utf-8"))

    eng = Engine(
        input_kind="csv",
        output_kind="parquet",
        schema=schema,
        preprocessors=[],
        delimiter=",",
        encoding_priority=["utf-8"],
        header_mode="auto",
        processing_chunk_size=10_000,
        output_mode="chunked",
        output_chunk_size=10_000,
    )
    eng.run(str(CSV_FILE), str(tmp_path))

    manifest_path = tmp_path / "_manifest.json"
    assert manifest_path.exists(), "manifest not written"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["read"] == EXPECTED_ROWS
    assert manifest["kept"] == EXPECTED_ROWS
    assert manifest["rejected"] == 0

    parquet_out = tmp_path / f"{CSV_FILE.name}.parquet"
    assert parquet_out.exists(), "parquet output missing"
    table = pq.read_table(parquet_out)
    assert table.num_rows == EXPECTED_ROWS
    for col in [
        "bool_col","int32_col","int64_col","decimal_9_2_col","uuid_col","json_col"
    ]:
        assert col in table.schema.names
