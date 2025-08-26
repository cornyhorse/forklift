import json
from pathlib import Path
from forklift.outputs.parquet_output import PQOutput
from forklift.types import RowResult
import forklift.outputs.parquet_output as mod

class DummyWriter:
    def __init__(self, dest, schema_hint=None, **opts):
        self.dest = Path(dest)
        self.dest.mkdir(parents=True, exist_ok=True)
        self.closed = False
        self.bad = []
    def write_batch(self, batch): pass
    def write_quarantine(self, rows):
        (self.dest / "_quarantine.jsonl").write_text("\n".join(map(str, rows)))
    def close(self): self.closed = True

def test_quarantine_and_close(tmp_path, monkeypatch):
    # Patch whatever name PQOutput.open() actually references.
    # If your code does "from ..io.parquet_writer import RollingParquetWriter"
    # inside the module, this will still work with raising=False.
    monkeypatch.setattr(mod, "RollingParquetWriter", DummyWriter, raising=False)

    outdir = tmp_path / "out"
    pq = PQOutput(dest=str(outdir), schema={}, opts={})
    pq.open()
    pq.write({"id":"1"})
    rr = RowResult(row={"id":"X"}, error=ValueError("bad"))
    pq.quarantine(rr)
    pq.close()

    assert pq._counts["read"] == 2
    assert pq._counts["kept"] == 1
    assert pq._counts["rejected"] == 1
    assert (outdir / "_quarantine.jsonl").exists()