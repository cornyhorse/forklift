"""Imports that read from and write to a real S3-compatible store (RustFS).

``TestEndToEnd`` runs the importers against the store with full access. ``TestScopedCredentials``
uses temporary credentials limited by an STS session policy, the way a pipeline should run with
only the access it needs: what forklift can do with read-only credentials, that writes stay
inside the prefix a writer was granted, and that a refused read or write fails without leaving
objects or unfinished uploads behind. Credentials come from the environment
(``AWS_ACCESS_KEY_ID`` ... and ``AWS_ENDPOINT_URL``) exactly as boto3's default chain reads them,
except for SQL exports, which take an explicit ``s3_client``.
"""

from __future__ import annotations

import io
import json
import logging

import openpyxl
import pyarrow.parquet as pq
import pytest
from service_helpers import allow, sql_schema_file

from forklift import import_csv, import_excel, import_sql
from forklift.processors.data_validation.data_validation_processor import (
    BadRowsThresholdExceededError,
)

pytestmark = pytest.mark.services

PEOPLE = "id,name,age\n1,Ana,34\n2,Bo,41\n3,Cy,29\n"
SCHEMA = {
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": "string"},
        "age": {"type": "integer"},
    }
}
# What a writer needs: the upload itself, cleanup of a failed multipart upload, and removal of
# the outputs of an earlier run in the same destination.
WRITE_ACTIONS = ["s3:PutObject", "s3:AbortMultipartUpload", "s3:DeleteObject"]


def _parquet_rows(object_store, bucket, key):
    return pq.read_table(io.BytesIO(object_store.read(bucket, key))).to_pylist()


@pytest.fixture
def people(object_store, bucket):
    object_store.put(bucket, "in/people.csv", PEOPLE)
    object_store.put(bucket, "in/schema.json", json.dumps(SCHEMA))
    return f"s3://{bucket}/in/people.csv"


class TestEndToEnd:
    def test_csv_in_the_store_becomes_parquet_in_the_store(
        self, object_store, bucket, people, s3_environment
    ):
        results = import_csv(
            people, f"s3://{bucket}/out/", schema_file=f"s3://{bucket}/in/schema.json"
        )

        assert results.total_rows == 3
        assert "out/data.parquet" in object_store.keys(bucket, "out/")
        assert _parquet_rows(object_store, bucket, "out/data.parquet") == [
            {"id": 1, "name": "Ana", "age": 34},
            {"id": 2, "name": "Bo", "age": 41},
            {"id": 3, "name": "Cy", "age": 29},
        ]
        assert object_store.unfinished_uploads(bucket) == []

    def test_threshold_failure_keeps_bad_rows_and_discards_data(
        self, object_store, bucket, s3_environment
    ):
        schema = dict(
            SCHEMA,
            **{"x-validation": {"fieldValidations": {"age": {"range": {"min": 0, "max": 150}}}}},
        )
        text = "id,name,age\n" + "".join(f"{i},n{i},{999 if i % 2 else 30}\n" for i in range(20))
        object_store.put(bucket, "in/ages.csv", text)
        object_store.put(bucket, "in/schema.json", json.dumps(schema))

        with pytest.raises(BadRowsThresholdExceededError) as raised:
            import_csv(
                f"s3://{bucket}/in/ages.csv",
                f"s3://{bucket}/out/",
                schema_file=f"s3://{bucket}/in/schema.json",
            )

        keys = object_store.keys(bucket, "out/")
        assert "out/data.parquet" not in keys
        assert raised.value.bad_rows_file == f"s3://{bucket}/out/bad_rows.parquet"
        assert len(_parquet_rows(object_store, bucket, "out/bad_rows.parquet")) == 10
        assert object_store.unfinished_uploads(bucket) == []

    def test_excel_workbook_in_the_store_becomes_one_parquet_file_per_sheet(
        self, object_store, bucket, s3_environment
    ):
        workbook = openpyxl.Workbook()
        workbook.active.title = "people"
        for row in [["id", "name"], [1, "Ana"], [2, "Bo"]]:
            workbook.active.append(row)
        buffer = io.BytesIO()
        workbook.save(buffer)
        object_store.put(bucket, "in/book.xlsx", buffer.getvalue())

        import_excel(f"s3://{bucket}/in/book.xlsx", f"s3://{bucket}/out/")

        (sheet_file,) = [
            key for key in object_store.keys(bucket, "out/") if key.endswith(".parquet")
        ]
        assert _parquet_rows(object_store, bucket, sheet_file) == [
            {"id": 1, "name": "Ana"},
            {"id": 2, "name": "Bo"},
        ]

    def test_sql_table_is_exported_to_the_store(self, object_store, bucket, postgres, tmp_path):
        postgres.admin(
            f"CREATE TABLE {postgres.table('orders')} (id INTEGER, amount DECIMAL(10, 2))",
            f"INSERT INTO {postgres.table('orders')} VALUES (1, 9.50), (2, 12.00)",
        )
        login = postgres.create_user()
        postgres.grant_select(login, "orders")

        results = import_sql(
            postgres.login_connection_string(login),
            f"s3://{bucket}/exports/",
            sql_schema_file(tmp_path, postgres.namespace, ["orders"]),
            s3_client=object_store.streaming_client(),
        )

        assert results.total_rows == 2
        assert [
            r["id"] for r in _parquet_rows(object_store, bucket, "exports/orders.parquet")
        ] == [
            1,
            2,
        ]
        metadata = object_store.read(bucket, "exports/metadata.json").decode()
        assert login.password not in metadata


class TestScopedCredentials:
    def test_read_only_credentials_import_into_a_local_directory(
        self, object_store, bucket, people, s3_environment, tmp_path
    ):
        reader = object_store.scoped_credentials(allow(["s3:GetObject"], f"{bucket}/in/*"))
        with pytest.raises(Exception, match="AccessDenied"):
            object_store.client(reader).list_objects_v2(Bucket=bucket)  # no listing granted
        s3_environment(reader)

        results = import_csv(people, tmp_path / "out", schema_file=f"s3://{bucket}/in/schema.json")

        assert results.total_rows == 3
        assert (tmp_path / "out" / "data.parquet").exists()

    def test_read_only_credentials_cannot_write_to_the_store(
        self, object_store, bucket, people, s3_environment
    ):
        s3_environment(object_store.scoped_credentials(allow(["s3:GetObject"], f"{bucket}/in/*")))

        with pytest.raises(Exception) as raised:
            import_csv(people, f"s3://{bucket}/out/")

        assert "AccessDenied" in f"{type(raised.value).__name__}: {raised.value}"
        assert object_store.keys(bucket, "out/") == []
        assert object_store.unfinished_uploads(bucket) == []

    def test_writes_stay_inside_the_granted_prefix(
        self, object_store, bucket, people, s3_environment
    ):
        s3_environment(
            object_store.scoped_credentials(
                allow(["s3:GetObject"], f"{bucket}/in/*", f"{bucket}/out/team-a/*"),
                allow(WRITE_ACTIONS, f"{bucket}/out/team-a/*"),
            )
        )

        import_csv(people, f"s3://{bucket}/out/team-a/")
        with pytest.raises(Exception, match="AccessDenied"):
            import_csv(people, f"s3://{bucket}/out/team-b/")

        assert "out/team-a/data.parquet" in object_store.keys(bucket, "out/team-a/")
        assert object_store.keys(bucket, "out/team-b/") == []
        assert object_store.unfinished_uploads(bucket) == []

    def test_writer_needs_no_read_access_to_its_own_output_prefix(
        self, object_store, bucket, people, s3_environment
    ):
        s3_environment(
            object_store.scoped_credentials(
                allow(["s3:GetObject"], f"{bucket}/in/*"),
                allow(WRITE_ACTIONS, f"{bucket}/out/*"),  # write-only: no GetObject on out/
            )
        )

        results = import_csv(people, f"s3://{bucket}/out/")

        assert results.errors == []
        assert "out/data.parquet" in object_store.keys(bucket, "out/")
        manifest = json.loads(object_store.read(bucket, "out/manifest.json"))
        assert [entry["file_path"] for entry in manifest["files"]] == ["data.parquet"]

    def test_object_outside_the_granted_prefix_is_refused_not_missing(
        self, object_store, bucket, s3_environment, tmp_path
    ):
        object_store.put(bucket, "restricted/people.csv", PEOPLE)
        s3_environment(object_store.scoped_credentials(allow(["s3:GetObject"], f"{bucket}/in/*")))

        with pytest.raises(Exception) as raised:
            import_csv(f"s3://{bucket}/restricted/people.csv", tmp_path / "out")

        message = f"{type(raised.value).__name__}: {raised.value}"
        assert "AccessDenied" in message or "403" in message
        assert "not found" not in message.lower()
        assert not (tmp_path / "out" / "data.parquet").exists()

    def test_sql_export_with_a_writer_limited_to_its_prefix(
        self, object_store, bucket, postgres, tmp_path
    ):
        postgres.admin(
            f"CREATE TABLE {postgres.table('orders')} (id INTEGER)",
            f"INSERT INTO {postgres.table('orders')} VALUES (1), (2)",
        )
        login = postgres.create_user()
        postgres.grant_select(login, "orders")
        schema = sql_schema_file(tmp_path, postgres.namespace, ["orders"])
        writer = object_store.scoped_credentials(allow(WRITE_ACTIONS, f"{bucket}/exports/*"))

        import_sql(
            postgres.login_connection_string(login),
            f"s3://{bucket}/exports/",
            schema,
            s3_client=object_store.streaming_client(writer),
        )
        with pytest.raises(Exception, match="AccessDenied"):
            import_sql(
                postgres.login_connection_string(login),
                f"s3://{bucket}/elsewhere/",
                schema,
                s3_client=object_store.streaming_client(writer),
            )

        assert "exports/orders.parquet" in object_store.keys(bucket, "exports/")
        assert object_store.keys(bucket, "elsewhere/") == []
        assert object_store.unfinished_uploads(bucket) == []

    def test_wrong_secret_key_fails_without_echoing_it(
        self, object_store, bucket, people, s3_environment, tmp_path, caplog
    ):
        wrong_secret = "wrong-secret-0f9e8d7c6b5a"
        s3_environment(
            {"aws_access_key_id": object_store.access_key, "aws_secret_access_key": wrong_secret}
        )
        caplog.set_level(logging.DEBUG, logger="forklift")

        with pytest.raises(Exception) as raised:
            import_csv(people, tmp_path / "out")

        assert wrong_secret not in str(raised.value)
        assert wrong_secret not in caplog.text
        assert not (tmp_path / "out" / "data.parquet").exists()
