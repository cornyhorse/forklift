import json
from pathlib import Path
import pyarrow.parquet as pq
from forklift.engine.engine import Engine

DATA_DIR = Path(__file__).parent.parent / "test-files" / "largecsv"
CSV_FILE = DATA_DIR / "parquet_types.csv"
SCHEMA_FILE = DATA_DIR / "parquet_types.json"
TOTAL_GENERATED_ROWS = 200_000
SKIP_EVERY = 53  # rows where i % 53 == 0 are entirely blank and filtered
SKIPPED_ROWS = (TOTAL_GENERATED_ROWS - 1) // SKIP_EVERY + 1  # includes i=0
EXPECTED_ROWS = TOTAL_GENERATED_ROWS - SKIPPED_ROWS

def test_largecsv_vectorized(tmp_path: Path):
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
        output_mode="vectorized",
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
    # Spot check a few columns exist
    for col in [
        "bool_col","int32_col","int64_col","decimal_9_2_col","uuid_col","json_col"
    ]:
        assert col in table.schema.names
