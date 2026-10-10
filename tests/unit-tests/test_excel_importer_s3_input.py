"""import_excel with a workbook and a schema file in S3 (moto).

Excel readers need a seekable local file, so an S3 workbook is copied to a temporary directory
first; the copy keeps the object's file name (output files are named after it) and is removed
afterwards, whether the import succeeds or fails.
"""

from __future__ import annotations

import io
import json
import os
import tempfile

import openpyxl
import pyarrow.parquet as pq
import pytest

from forklift import import_excel

moto = pytest.importorskip("moto")
boto3 = pytest.importorskip("boto3")

SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://github.com/cornyhorse/forklift/schema-standards/test-excel-s3.json",
    "title": "People sheet",
    "type": "object",
    "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
    "x-excel": {"sheets": [{"select": {"name": "people"}}]},
}


def _workbook_bytes():
    workbook = openpyxl.Workbook()
    workbook.active.title = "people"
    for row in [["id", "name"], [1, "Ana"], [2, "Bo"]]:
        workbook.active.append(row)
    workbook.create_sheet("notes").append(["ignored"])
    buffer = io.BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


@pytest.fixture
def store(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with moto.mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="bkt")
        client.put_object(Bucket="bkt", Key="in/book.xlsx", Body=_workbook_bytes())
        client.put_object(Bucket="bkt", Key="in/schema.json", Body=json.dumps(SCHEMA).encode())
        yield client


@pytest.fixture
def scratch_dirs(monkeypatch):
    """Record the temporary directories import_excel creates."""
    created = []
    real = tempfile.TemporaryDirectory

    def recording(*args, **kwargs):
        directory = real(*args, **kwargs)
        created.append(directory.name)
        return directory

    monkeypatch.setattr(tempfile, "TemporaryDirectory", recording)
    return created


class TestWorkbookInS3:
    def test_workbook_and_schema_in_s3_import_like_local_files(
        self, store, tmp_path, scratch_dirs
    ):
        results = import_excel(
            "s3://bkt/in/book.xlsx", tmp_path / "out", schema_file="s3://bkt/in/schema.json"
        )

        assert [p.name for p in (tmp_path / "out").glob("*.parquet")] == ["book_people.parquet"]
        assert pq.read_table(tmp_path / "out" / "book_people.parquet").to_pylist() == [
            {"id": 1, "name": "Ana"},
            {"id": 2, "name": "Bo"},
        ]
        assert results.total_rows == 2
        (scratch,) = scratch_dirs
        assert not os.path.exists(scratch)

    def test_temporary_copy_is_removed_when_the_import_fails(self, store, tmp_path, scratch_dirs):
        store.put_object(Bucket="bkt", Key="in/bad.json", Body=b"[1, 2]")

        with pytest.raises(Exception):
            import_excel(
                "s3://bkt/in/book.xlsx", tmp_path / "out", schema_file="s3://bkt/in/bad.json"
            )

        (scratch,) = scratch_dirs
        assert not os.path.exists(scratch)

    def test_local_schema_file_still_works_with_an_s3_workbook(self, store, tmp_path):
        schema_file = tmp_path / "schema.json"
        schema_file.write_text(json.dumps(SCHEMA))

        results = import_excel("s3://bkt/in/book.xlsx", tmp_path / "out", schema_file=schema_file)

        assert results.total_rows == 2
