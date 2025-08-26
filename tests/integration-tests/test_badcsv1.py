# from __future__ import annotations
# from pathlib import Path
# import json
# import pytest
#
# try:
#     from forklift.engine.engine import Engine
# except Exception:
#     Engine = None  # type: ignore
#
#
# @pytest.mark.xfail(Engine is None, reason="Engine not implemented yet", strict=False)
# def test_badcsv1_ingest(tmp_out: Path, data_dir: Path):
#     src = data_dir / "badcsv" / "badcsv1.txt"
#     schema_path = data_dir / "badcsv" / "badcsv1.json"
#     assert src.exists(), src
#     assert schema_path.exists(), schema_path
#
#     schema = json.loads(schema_path.read_text(encoding="utf-8"))
#
#     eng = Engine(
#         input_kind="csv",
#         output_kind="parquet",
#         schema=schema,
#         preprocessors=["type_coercion", "footer_filter"],
#         delimiter=",",
#         encoding_priority=["utf-8-sig", "utf-8", "latin-1"],
#     )
#     eng.run(str(src), str(tmp_out))
#
#     manifest = json.loads((tmp_out / "_manifest.json").read_text(encoding="utf-8"))
#     assert manifest["read"] == 18  # data rows (footer ignored)
#     assert manifest["kept"] == 17
#     assert manifest["rejected"] == 1
#
#     qlines = (tmp_out / "_quarantine" / "rows.jsonl").read_text(encoding="utf-8").splitlines()
#     assert len(qlines) == 1
#     assert "amount" in qlines[0]
