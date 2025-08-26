from __future__ import annotations
from pathlib import Path
import json
import pytest

try:
    from forklift.engine.engine import Engine
except Exception:
    Engine = None  # type: ignore


@pytest.mark.xfail(Engine is None, reason="Engine not implemented yet", strict=False)
def test_badtsv1_headerless_tsv(tmp_out: Path, data_dir: Path):
    src = data_dir / "badtsv" / "badtsv1.txt"
    schema_path = data_dir / "badtsv" / "badtsv1.json"
    assert src.exists(), src
    assert schema_path.exists(), schema_path

    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    eng = Engine(
        input_kind="csv",  # same input class; delimiter overrides
        output_kind="parquet",
        schema=schema,
        preprocessors=["type_coercion", "footer_filter"],
        delimiter="\t",
        encoding_priority=["utf-8-sig", "utf-8", "latin-1"],
    )
    eng.run(str(src), str(tmp_out))

    manifest = json.loads((tmp_out / "_manifest.json").read_text(encoding="utf-8"))
    # 16 data rows; reject bad amount + bad date; dedupe 1 duplicate pair
    assert manifest["read"] == 16
    assert manifest["kept"] == 13
    assert manifest["rejected"] == 2
