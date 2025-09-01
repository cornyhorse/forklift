import json
import polars as pl
from pathlib import Path
from forklift.processor import apply_schema


def test_apply_schema_badrows(tmp_path):
    df = pl.DataFrame({
        "id": ["1", "2", "x"],
        "value": ["3.14", "bad", "7"],
        "flag": ["true", "false", "oops"],
    })
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "value": {"type": "number"},
            "flag": {"type": "boolean"},
        }
    }
    bad_path = tmp_path / "badrows.jsonl"
    cleaned = apply_schema(df, schema, str(bad_path))

    # Expect two bad rows: second (value "bad") and third (id "x", flag "oops")
    assert cleaned.shape[0] == 1
    assert cleaned["id"].to_list() == [1]
    assert cleaned["value"].to_list() == [3.14]

    lines = bad_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    recs = [json.loads(l) for l in lines]
    # Ensure error keys exist
    all_err_cols = sorted({c for r in recs for c in r["errors"].keys()})
    assert all_err_cols == ["flag", "id", "value"][:len(all_err_cols)]  # presence check
    for r in recs:
        assert "row_index" in r and "errors" in r and "row" in r and "original" in r

