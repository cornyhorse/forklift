from __future__ import annotations
import json
from pathlib import Path
import pyarrow.parquet as pq
from forklift.engine.engine import Engine


def test_goodcsv1_ingest(tmp_out: Path, data_dir: Path):
    sample_dir = data_dir / "goodcsv"
    src = sample_dir / "good_csv1.txt"
    schema_path = sample_dir / "good_csv1.json"

    assert src.exists(), src
    assert schema_path.exists(), schema_path

    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    # Mirror dev helper: include type_coercion preprocessor, delimiter from schema
    xcsv = schema.get("x-csv") or {}
    delimiter = xcsv.get("delimiter", ",")

    eng = Engine(
        input_kind="csv",
        output_kind="parquet",
        schema=schema,
        preprocessors=["type_coercion"],
        delimiter=delimiter,
        encoding_priority=["utf-8-sig", "utf-8", "latin-1"],
        header_mode="auto",
    )
    eng.run(str(src), str(tmp_out))

    # Manifest assertions
    manifest_path = tmp_out / "_manifest.json"
    assert manifest_path.exists(), "_manifest.json missing"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest == {"read": 20, "kept": 20, "rejected": 0}

    # Quarantine assertions (file exists, empty)
    quarantine_path = tmp_out / "_quarantine.jsonl"
    assert quarantine_path.exists(), "_quarantine.jsonl missing"
    q_content = quarantine_path.read_text(encoding="utf-8")
    assert q_content.strip() == "", "Expected no quarantined rows"

    # Parquet file assertions (filename sanitized to basename)
    produced_parquet = tmp_out / "good_csv1.txt.parquet"
    assert produced_parquet.exists(), "Parquet output file missing"

    prod_table = pq.read_table(produced_parquet)

    # Semantic schema assertions based on JSON Schema
    import pyarrow as pa
    expected_fields = {
        "id": pa.int64(),
        "name": pa.string(),
        "email": pa.string(),
        "signup_date": pa.date32(),  # date32 parquet type
        "active": pa.bool_(),
        "amount_usd": pa.float64(),
        "country": pa.string(),
        "status": pa.string(),
        "discount_pct": pa.float64(),
        "notes": pa.string(),
        "_table": pa.string(),
    }
    assert list(expected_fields.keys()) == prod_table.schema.names, "Field name mismatch"
    for fname, ftype in expected_fields.items():
        assert prod_table.schema.field(fname).type == ftype, (
            f"Field {fname} type mismatch: {prod_table.schema.field(fname).type} != {ftype}"  # pragma: no cover
        )

    assert prod_table.num_rows == 20
    ids = set(prod_table.column("id").to_pylist())
    assert ids == set(range(1, 21))
