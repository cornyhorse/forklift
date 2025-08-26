import json
from pathlib import Path
from forklift.engine.engine import Engine


def _schema():
    return {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "amt": {"type": "number"}
        },
        "required": ["id", "amt"]
    }


def test_engine_no_preprocessors(tmp_path: Path):
    f = tmp_path / "x.csv"
    f.write_text("id,amt\n1,10\n", encoding="utf-8")
    out = tmp_path / "out"
    eng = Engine(input_kind="csv", output_kind="parquet", schema=_schema(),
                 preprocessors=[], delimiter=",", encoding_priority=["utf-8"])
    eng.run(str(f), str(out))
    m = json.loads((out / "_manifest.json").read_text())
    assert m["read"] == 1 and m["kept"] == 1 and m["rejected"] == 0


def test_engine_rejects_bad_row(tmp_path: Path):
    f = tmp_path / "y.csv"
    # bad amt 'NaNish' -> should be rejected by type coercion to number
    f.write_text("id,amt\n1,10\n2,not_a_number\n", encoding="utf-8")
    out = tmp_path / "out"
    eng = Engine(input_kind="csv", output_kind="parquet", schema=_schema(),
                 preprocessors=["type_coercion"], delimiter=",", encoding_priority=["utf-8"])
    eng.run(str(f), str(out))
    m = json.loads((out / "_manifest.json").read_text())
    assert m["read"] == 2 and m["kept"] == 1 and m["rejected"] == 1
