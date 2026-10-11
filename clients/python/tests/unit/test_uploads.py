"""upload_file against the fake gateway and its store: one PUT or parts, from paths and file
objects, streamed, checked, retried, and cleaned up when it fails."""

from __future__ import annotations

import base64
import hashlib
import io
import json
import logging
import os

import pytest

from forklift_client import (
    BadRequest,
    Conflict,
    Forbidden,
    NotFound,
    RetryPolicy,
    StoreError,
    TransportError,
    Unauthorized,
    UnexpectedResponse,
    VerificationError,
    _uploads,
)

CSV = b"id,name,age\n1,Ana,34\n2,Bo,41\n3,Cy,\n"  # 35 bytes: one PUT (the fake's threshold: 64)
BIG = b"".join(b"%04d,row\n" % i for i in range(20))  # 180 bytes: 6 parts of 32


class Unseekable(io.RawIOBase):
    """A stream that cannot seek, like a pipe or a socket."""

    def __init__(self, data: bytes):
        self._data = io.BytesIO(data)

    def readable(self):
        return True

    def readinto(self, buffer):
        chunk = self._data.read(len(buffer))
        buffer[: len(chunk)] = chunk
        return len(chunk)


class Growing(io.BytesIO):
    """A file that grows while it is uploaded: its end moves after it was measured."""

    def __init__(self, data: bytes):
        super().__init__(data)
        self.ends = 0

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_END:
            self.ends += 1
            return super().seek(offset, whence) + (0 if self.ends == 1 else 5)
        return super().seek(offset, whence)


def store_puts(fake):
    return fake.sent("PUT store")


class TestSinglePut:
    def test_a_path(self, fake, client, tmp_path):
        path = tmp_path / "people.csv"
        path.write_bytes(CSV)

        upload = client.upload_file(path, classification="sensitive")

        assert upload.status == "complete" and upload.size == len(CSV)
        assert upload.filename == "people.csv" and upload.content_type == "text/csv"
        assert upload.classification == "sensitive"
        assert fake.uploaded(upload.id) == CSV
        (put,) = store_puts(fake)
        assert "authorization" not in put["headers"], "the token never goes to the store"
        assert put["headers"]["content-length"] == str(len(CSV))
        assert (
            put["headers"]["content-md5"] == base64.b64encode(hashlib.md5(CSV).digest()).decode()
        )
        assert put["headers"]["content-type"] == "text/csv"
        (complete,) = fake.sent("POST /uploads/{id}/complete")
        assert json.loads(complete["body"]) == {}

    def test_a_string_path_and_a_declared_sha256(self, fake, client, tmp_path):
        path = tmp_path / "data.bin"
        path.write_bytes(CSV)
        digest = hashlib.sha256(CSV).hexdigest()
        upload = client.upload_file(str(path), filename="renamed.csv", sha256=digest)
        assert upload.filename == "renamed.csv" and upload.declared_sha256 == digest

    def test_a_file_object_from_its_current_position(self, fake, client):
        stream = io.BytesIO(b"junk" + CSV)
        stream.seek(4)
        upload = client.upload_file(stream, filename="people.csv", content_type="")
        assert fake.uploaded(upload.id) == CSV
        assert store_puts(fake)[0]["headers"]["content-type"] == "application/octet-stream"
        assert not stream.closed, "a file object given stays open"

    def test_an_open_file_gives_its_name(self, fake, client, tmp_path):
        path = tmp_path / "people.csv"
        path.write_bytes(CSV)
        with open(path, "rb") as handle:
            upload = client.upload_file(handle, size=len(CSV))
        assert upload.filename == "people.csv"

    def test_a_stream_that_cannot_seek(self, fake, client):
        upload = client.upload_file(Unseekable(CSV), filename="p.csv", size=len(CSV))
        assert fake.uploaded(upload.id) == CSV

    def test_the_tickets_headers_are_sent_and_its_content_type_wins(self, fake, client):
        fake.ticket_headers = {"x-amz-meta-origin": "client", "Content-Type": "text/plain"}
        client.upload_file(io.BytesIO(CSV), filename="p.csv")
        (put,) = store_puts(fake)
        assert put["headers"]["x-amz-meta-origin"] == "client"
        assert put["headers"]["content-type"] == "text/plain"

    def test_a_failed_put_is_retried(self, fake, client):
        fake.fail("PUT store", status=500)
        fake.fail("PUT store", drop="before")
        upload = client.upload_file(io.BytesIO(CSV), filename="p.csv")
        assert upload.status == "complete" and len(store_puts(fake)) == 3

    def test_an_expired_single_put_url_is_not_retried_and_the_upload_is_deleted(
        self, fake, client
    ):
        fake.fail("PUT store", status=403, body=b"<Error><Code>AccessDenied</Code></Error>")
        with pytest.raises(StoreError) as raised:
            client.upload_file(io.BytesIO(CSV), filename="p.csv")
        error = raised.value
        assert error.status == 403 and error.code == "AccessDenied" and not error.transient
        assert str(error).startswith("Uploading p.csv: the object store at 127.0.0.1:")
        assert "X-Amz-Signature" not in str(error), "presigned URLs never reach messages"
        (upload,) = fake.uploads.values()
        assert upload["status"] == "deleted"

    def test_store_errors_past_the_attempts(self, fake, client):
        fake.fail("PUT store", status=503, times=3)
        with pytest.raises(StoreError) as raised:
            client.upload_file(io.BytesIO(CSV), filename="p.csv")
        assert raised.value.transient and len(store_puts(fake)) == 3
        fake.fail("PUT store", drop="before", times=3)
        with pytest.raises(TransportError):
            client.upload_file(io.BytesIO(CSV), filename="p.csv")

    def test_a_tls_failure_is_not_retried(self, fake, client, monkeypatch):
        real = client._transport.request

        def request(method, url, **options):
            if method == "PUT":
                raise TransportError(options["what"], "store", "TLS error", retryable=False)
            return real(method, url, **options)

        monkeypatch.setattr(client._transport, "request", request)
        with pytest.raises(TransportError, match="TLS error"):
            client.upload_file(io.BytesIO(CSV), filename="p.csv")


class TestParts:
    def test_parts_with_url_batches(self, fake, client, tmp_path, monkeypatch):
        monkeypatch.setattr(_uploads, "PART_URLS_PER_REQUEST", 3)
        path = tmp_path / "big.csv"
        path.write_bytes(BIG)

        upload = client.upload_file(path)

        assert upload.status == "complete" and upload.multipart
        assert fake.uploaded(upload.id) == BIG
        asked = [json.loads(e["body"]) for e in fake.sent("POST /uploads/{id}/parts")]
        assert asked == [{"part_numbers": [3, 4, 5]}, {"part_numbers": [6]}]
        puts = store_puts(fake)
        assert [p["headers"]["content-length"] for p in puts] == ["32"] * 5 + ["20"]
        assert all("authorization" not in p["headers"] for p in puts)
        (complete,) = fake.sent("POST /uploads/{id}/complete")
        parts = json.loads(complete["body"])["parts"]
        assert [part["part_number"] for part in parts] == [1, 2, 3, 4, 5, 6]
        assert all(part["etag"].startswith('"') for part in parts)

    def test_parts_of_a_stream_that_cannot_seek(self, fake, client):
        upload = client.upload_file(Unseekable(BIG), filename="big.csv", size=len(BIG))
        assert fake.uploaded(upload.id) == BIG

    def test_a_failed_part_is_sent_again_with_a_fresh_url(self, fake, client):
        client.retry = RetryPolicy(attempts=4, initial_delay=0.001, max_delay=0.002)
        fake.fail("PUT store", status=500)
        fake.fail("PUT store", status=403, body=b"<Error><Code>AccessDenied</Code></Error>")
        fake.fail("PUT store", drop="before")
        upload = client.upload_file(io.BytesIO(BIG), filename="big.csv")
        assert fake.uploaded(upload.id) == BIG
        asked = [json.loads(e["body"]) for e in fake.sent("POST /uploads/{id}/parts")]
        assert asked[:3] == [{"part_numbers": [1]}] * 3, "a fresh URL for each retry of part 1"

    def test_expired_part_urls_are_signed_again(self, fake, client):
        ticket = client.create_upload("big.csv", len(BIG))
        fake.expire_urls()  # the ticket's URLs (parts 1 and 2) answer 403 from now on
        source = _uploads._Source(io.BytesIO(BIG), True, 0, len(BIG), "Uploading big.csv")

        parts = _uploads._put_parts(client, ticket, source)
        client.complete_upload(ticket.upload, parts)

        assert fake.uploaded(ticket.upload.id) == BIG
        assert len(store_puts(fake)) == 8, "parts 1 and 2 twice, the other parts once"
        asked = [json.loads(e["body"]) for e in fake.sent("POST /uploads/{id}/parts")]
        assert asked == [
            {"part_numbers": [1]},
            {"part_numbers": [2]},
            {"part_numbers": [3, 4, 5, 6]},
        ]

    def test_a_part_that_keeps_failing_aborts_the_upload(self, fake, client, caplog):
        fake.fail("PUT store", status=500, times=3)
        with pytest.raises(StoreError):
            client.upload_file(io.BytesIO(BIG), filename="big.csv")
        (upload,) = fake.uploads.values()
        assert upload["status"] == "deleted"

    def test_a_part_answer_without_an_etag(self, fake, client):
        fake.fail("PUT store", status=200, body=b"")
        with pytest.raises(StoreError, match="no ETag in the answer"):
            client.upload_file(io.BytesIO(BIG), filename="big.csv")

    def test_a_ticket_whose_parts_do_not_fit_the_size(self, fake, client):
        ticket = fake._create_upload(body={"filename": "big.csv", "size": len(BIG)})[1]
        ticket["part_count"] = 2
        fake.fail("POST /uploads", status=201, body=ticket)
        with pytest.raises(UnexpectedResponse, match="does not fit 180 bytes"):
            client.upload_file(io.BytesIO(BIG), filename="big.csv")

    def test_a_part_without_a_url(self, fake, client):
        fake.first_parts = 1
        fake.fail("POST /uploads/{id}/parts", status=200, body=[])
        with pytest.raises(UnexpectedResponse, match="gave no URL for the part"):
            client.upload_file(io.BytesIO(BIG), filename="big.csv")

    def test_a_parts_answer_that_is_not_a_list(self, fake, client):
        fake.first_parts = 1
        fake.fail("POST /uploads/{id}/parts", status=200, body={"part_number": 2})
        with pytest.raises(UnexpectedResponse, match="should answer a list"):
            client.upload_file(io.BytesIO(BIG), filename="big.csv")


class TestChecks:
    def test_a_stream_shorter_than_its_size(self, fake, client):
        with pytest.raises(VerificationError, match="ended after 35 of the 40 bytes declared"):
            client.upload_file(Unseekable(CSV), filename="p.csv", size=40)
        (upload,) = fake.uploads.values()
        assert upload["status"] == "deleted"

    def test_a_stream_longer_than_its_size(self, fake, client):
        with pytest.raises(VerificationError, match="more than the 30 bytes declared"):
            client.upload_file(Unseekable(CSV), filename="p.csv", size=30)

    def test_a_file_that_grows(self, fake, client):
        with pytest.raises(VerificationError, match="did it change while it was uploaded"):
            client.upload_file(Growing(CSV), filename="p.csv")

    def test_a_file_that_shrinks(self, fake, client, tmp_path, monkeypatch):
        path = tmp_path / "people.csv"
        path.write_bytes(CSV)
        real = _uploads._Window.md5

        def md5_then_truncate(window):
            digest = real(window)
            os.truncate(path, 10)
            return digest

        monkeypatch.setattr(_uploads._Window, "md5", md5_then_truncate)
        with pytest.raises(VerificationError, match="ended after 10 of the 35 bytes"):
            client.upload_file(path)

    def test_a_declared_size_that_is_not_the_files(self, client):
        with pytest.raises(ValueError, match="size is 3, but the file has 35 bytes"):
            client.upload_file(io.BytesIO(CSV), filename="p.csv", size=3)

    def test_a_stream_that_cannot_seek_needs_its_size(self, client):
        with pytest.raises(ValueError, match="size is required"):
            client.upload_file(Unseekable(CSV), filename="p.csv")

    def test_a_closed_stream_counts_as_one_that_cannot_seek(self, client):
        stream = io.BytesIO(CSV)
        stream.close()
        with pytest.raises(ValueError, match="size is required"):
            client.upload_file(stream, filename="p.csv")

    def test_text_mode_is_refused(self, client, tmp_path):
        path = tmp_path / "people.csv"
        path.write_bytes(CSV)
        with open(path, "r") as handle, pytest.raises(TypeError, match="binary mode"):
            client.upload_file(handle)

    def test_a_stream_without_a_name_needs_a_filename(self, client):
        with pytest.raises(ValueError, match="filename is required"):
            client.upload_file(io.BytesIO(CSV))

    def test_the_gateway_refuses_the_upload(self, fake, client):
        fake.fail(
            "POST /uploads", status=403, body={"detail": "Viewers cannot upload.", "code": "x"}
        )
        with pytest.raises(Forbidden, match="Viewers cannot upload"):
            client.upload_file(io.BytesIO(CSV), filename="p.csv")
        assert fake.sent("DELETE /uploads/{id}") == []

    def test_a_failed_completion_deletes_the_upload(self, fake, client):
        fake.fail("POST /uploads/{id}/complete", status=409, body={"detail": "Size.", "code": "c"})
        with pytest.raises(Conflict):
            client.upload_file(io.BytesIO(CSV), filename="p.csv")
        assert len(fake.sent("DELETE /uploads/{id}")) == 1

    def test_when_deleting_fails_too_the_first_error_is_raised(self, fake, client, caplog):
        fake.fail(
            "POST /uploads/{id}/complete", status=401, body={"detail": "Expired.", "code": "c"}
        )
        fake.fail("DELETE /uploads/{id}", status=404, body={"detail": "Gone.", "code": "n"})
        with pytest.raises(Unauthorized):
            client.upload_file(io.BytesIO(CSV), filename="p.csv")
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert any("and deleting upload" in message for message in warnings)


class TestUploadCalls:
    def test_create_get_list_parts_complete_and_delete(self, fake, client):
        ticket = client.create_upload("big.csv", len(BIG), content_type="text/csv")
        assert ticket.url is None and ticket.part_count == 6 and len(ticket.parts) == 2
        assert ticket.upload.status == "pending" and ticket.method == "PUT"
        assert client.get_upload(ticket.upload.id).multipart
        assert [u.id for u in client.list_uploads()] == [ticket.upload.id]
        urls = client.upload_part_urls(ticket.upload, [3, 4])
        assert [u.part_number for u in urls] == [3, 4]
        with pytest.raises(BadRequest, match="the parts do not match"):
            client.complete_upload(ticket.upload.id, [{"part_number": 1, "etag": '"x"'}])
        client.delete_upload(ticket.upload.id)
        client.delete_upload(ticket.upload.id)  # already deleted: not an error
        with pytest.raises(NotFound):
            client.delete_upload("6f1d7a3e-8a8c-4d0c-9d51-2d1f3bb5a6a1")
