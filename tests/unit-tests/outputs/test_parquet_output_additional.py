import json
from pathlib import Path
import pyarrow.parquet as pq
import pytest

from forklift.outputs.parquet_output import PQOutput


def test_write_skip_row_branch(tmp_path):
    outdir = tmp_path / "skip_out"
    pqout = PQOutput(dest=str(outdir), schema=None)
    pqout.open()
    pqout.write({"__forklift_skip__": True, "_table": "t1", "value": 1})
    # No kept rows, read incremented once, no rejected
    assert pqout.counters == {"read": 1, "kept": 0, "rejected": 0}
    assert pqout.row_buffers == {}
    pqout.close()
    # Manifest reflects counters
    manifest = json.loads((outdir / "_manifest.json").read_text())
    assert manifest["read"] == 1 and manifest["kept"] == 0 and manifest["rejected"] == 0


def test_validation_failure_triggers_quarantine(tmp_path):
    schema = {"fields": [{"name": "id", "type": "integer"}]}
    outdir = tmp_path / "fail_out"
    pqout = PQOutput(dest=str(outdir), schema=schema)
    pqout.open()
    # Invalid integer -> validation fails -> read increments once, rejected increments once
    pqout.write({"id": "abc"})
    pqout.close()
    assert pqout.counters["read"] == 1
    assert pqout.counters["rejected"] == 1
    assert pqout.counters["kept"] == 0
    q_path = outdir / "_quarantine.jsonl"
    content = q_path.read_text().strip().splitlines()
    assert len(content) == 1
    line_json = json.loads(content[0])
    assert line_json["row"] == {"id": "abc"}
    assert "Field 'id' expected integer" in line_json["error"]


def test_close_with_no_rows_triggers_empty_flush_branch(tmp_path):
    outdir = tmp_path / "empty_flush"
    pqout = PQOutput(dest=str(outdir), schema=None)
    pqout.open()
    # Close without any writes -> _flush_parquet early return covered
    pqout.close()
    assert (outdir / "_manifest.json").exists()
    # No parquet files created
    assert list(outdir.glob("*.parquet")) == []


def test_flush_with_empty_rows_list_continue_branch(tmp_path):
    outdir = tmp_path / "mixed_flush"
    pqout = PQOutput(dest=str(outdir), schema=None)
    pqout.open()
    # Manually seed buffers: one empty list, one with data
    pqout.row_buffers["empty_table"] = []
    pqout.row_buffers["real_table"] = [{"a": 1}, {"a": 2}]
    pqout._flush_parquet()
    # Ensure parquet written for real_table only
    assert (outdir / "real_table.parquet").exists()
    assert not (outdir / "empty_table.parquet").exists()
    pqout.close()
