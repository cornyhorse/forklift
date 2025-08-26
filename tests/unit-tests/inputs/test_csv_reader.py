from pathlib import Path
import json
from forklift.engine.engine import Engine


def test_reader_good_csv(tmp_path: Path, data_dir: Path):
    data = data_dir / "goodcsv" / "good_csv1.txt"
    schema = json.loads((data_dir / "goodcsv" / "good_csv1.json").read_text())
    out = tmp_path / "out"

    eng = Engine(
        input_kind="csv",
        output_kind="parquet",
        schema=schema,
        preprocessors=["type_coercion"],
        delimiter=",",
        encoding_priority=["utf-8"],
    )
    eng.run(str(data), str(out))

    manifest = json.loads((out / "_manifest.json").read_text())
    assert manifest["read"] == 20
    assert manifest["kept"] == 20
    assert manifest["rejected"] == 0


def test_reader_dupe_csv(tmp_path: Path, data_dir: Path):
    data = data_dir / "dupecsv" / "dupe_csv1.txt"
    schema = json.loads((data_dir / "dupecsv" / "dupe_csv1.json").read_text())
    out = tmp_path / "out"

    eng = Engine(
        input_kind="csv",
        output_kind="parquet",
        schema=schema,
        preprocessors=["type_coercion"],
        delimiter=",",
        encoding_priority=["utf-8"],
    )
    eng.run(str(data), str(out))

    manifest = json.loads((out / "_manifest.json").read_text())
    assert manifest["read"] == 24
    assert manifest["kept"] == 24 # 24 because deduplication has not been applied yet.
    assert manifest["rejected"] == 0
