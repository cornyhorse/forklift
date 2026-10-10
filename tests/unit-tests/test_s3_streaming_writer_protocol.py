"""S3 streaming: path normalisation, listing pages and the writer's file protocol/failures.

S3 is moto (in-memory) or a MagicMock client; nothing touches the network.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from forklift.io.s3_streaming import (
    S3Path,
    S3StreamingClient,
    S3StreamingWriter,
    is_s3_path,
    normalize_s3_uri,
)

PART = 5 * 1024 * 1024  # the writer's minimum part size


class _BytesPathLike:
    """An ``os.PathLike`` whose ``__fspath__`` returns bytes."""

    def __fspath__(self):
        return b"s3://bucket/key"


@pytest.fixture
def s3(monkeypatch):
    from moto import mock_aws

    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        monkeypatch.setenv(name, "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    with mock_aws():
        client = S3StreamingClient(
            aws_access_key_id="testing", aws_secret_access_key="testing", region_name="us-east-1"
        )
        client._s3_client.create_bucket(Bucket="bkt")
        yield client


def _mock_client():
    client = MagicMock(name="s3_client")
    client.create_multipart_upload.return_value = {"UploadId": "upload-1"}
    return client


class TestPathNormalisation:
    def test_s3path_is_returned_as_its_uri(self):
        assert normalize_s3_uri(S3Path("s3://bkt/a/b.csv")) == "s3://bkt/a/b.csv"

    def test_bytes_path_is_rejected(self):
        with pytest.raises(TypeError, match="^Unsupported path type: bytes$"):
            normalize_s3_uri(b"s3://bkt/key")

    def test_path_like_returning_bytes_is_not_an_s3_path(self):
        assert isinstance(_BytesPathLike(), os.PathLike)
        assert is_s3_path(_BytesPathLike()) is False


class TestListObjects:
    def test_pages_without_contents_are_skipped(self):
        streaming = S3StreamingClient(region_name="us-east-1")
        streaming._s3_client = MagicMock(name="s3_client")
        paginator = streaming._s3_client.get_paginator.return_value
        paginator.paginate.return_value = iter(
            [{"KeyCount": 0}, {"Contents": [{"Key": "p/a"}, {"Key": "p/b"}]}]
        )

        keys = [obj["Key"] for obj in streaming.list_objects("s3://bkt/p")]

        assert keys == ["p/a", "p/b"]
        paginator.paginate.assert_called_once_with(Bucket="bkt", Prefix="p", MaxKeys=1000)


class TestWriterFileProtocol:
    def test_writer_reports_a_write_only_unseekable_stream(self):
        writer = S3StreamingWriter(_mock_client(), S3Path("s3://bkt/k"), mode="wb")
        assert writer.mode == "wb"
        assert (writer.writable(), writer.readable(), writer.seekable()) == (True, False, False)
        writer.write(b"abc")
        assert writer.flush() is None
        assert writer.tell() == 3

    def test_text_is_refused_in_binary_mode(self):
        writer = S3StreamingWriter(_mock_client(), S3Path("s3://bkt/k"), mode="wb")
        with pytest.raises(ValueError, match="^Cannot write string data in binary mode$"):
            writer.write("text")
        assert writer.tell() == 0


class TestWriterMultipart:
    def test_data_after_the_last_full_part_is_uploaded_as_a_final_part(self, s3):
        payload = os.urandom(PART) + b"tail-bytes"
        writer = S3StreamingWriter(s3._s3_client, S3Path("s3://bkt/big.bin"), part_size=PART)
        with writer:
            writer.write(payload[:PART])  # fills exactly one part, which is uploaded at once
            writer.write(payload[PART:])

        obj = s3._s3_client.get_object(Bucket="bkt", Key="big.bin")
        assert obj["Body"].read() == payload
        assert obj["ETag"].endswith('-2"')  # a two-part multipart object
        uploads = s3._s3_client.list_multipart_uploads(Bucket="bkt").get("Uploads", [])
        assert uploads == []


class TestWriterFailures:
    def test_failed_upload_on_close_aborts_and_reraises(self):
        client = _mock_client()
        error = ClientError({"Error": {"Code": "AccessDenied", "Message": "no"}}, "PutObject")
        client.put_object.side_effect = error
        writer = S3StreamingWriter(client, S3Path("s3://bkt/k"))
        writer.write("data")

        with pytest.raises(ClientError, match="AccessDenied"):
            writer.close()

        assert writer.closed is True
        client.abort_multipart_upload.assert_called_with(
            Bucket="bkt", Key="k", UploadId="upload-1"
        )
        client.complete_multipart_upload.assert_not_called()

    def test_abort_ignores_a_failing_cleanup_request(self):
        client = _mock_client()
        client.abort_multipart_upload.side_effect = ClientError(
            {"Error": {"Code": "NoSuchUpload", "Message": "gone"}}, "AbortMultipartUpload"
        )
        writer = S3StreamingWriter(client, S3Path("s3://bkt/k"))
        writer.write("partial")

        writer.abort()  # does not raise

        assert writer.closed is True
        client.abort_multipart_upload.assert_called_once_with(
            Bucket="bkt", Key="k", UploadId="upload-1"
        )
        client.put_object.assert_not_called()
