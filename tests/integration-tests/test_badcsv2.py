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
# @pytest.mark.xfail(Engine is None, reason="Engine not implemented yet", strict=False)
# def test_badcsv2_utf8sig_and_dupe_headers(tmp_out: Path, data_dir: Path):
#     src = data_dir / "badcsv" / "badcsv2.txt"
#     schema_path = data_dir / "badcsv" / "badcsv2.json"
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
#     assert manifest["read"] == 7
#     assert manifest["kept"] == 5
#     assert manifest["rejected"] == 2
#
#     q = tmp_out / "_quarantine" / "rows.jsonl"
#     assert q.exists()
#     assert len(q.read_text(encoding="utf-8").splitlines()) == 2