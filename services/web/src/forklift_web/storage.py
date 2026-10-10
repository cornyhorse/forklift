"""Presigned URLs and metadata calls on S3-compatible stores (never object contents).

The gateway signs URLs so that browsers, API clients and workers move data themselves, and it
makes only metadata calls: HEAD (to complete uploads and check artifacts), multipart
bookkeeping, server-side copy (publishing to a destination) and delete (retention). It never
calls GetObject or downloads anything; tests/test_trust_boundary.py enforces that.

URLs are signed for an *audience*, because browsers and workers may reach the store under
different names (``FORKLIFT_S3_PUBLIC_ENDPOINT_URL`` and ``FORKLIFT_S3_WORKER_ENDPOINT_URL``):
a SigV4 signature covers the host, so a URL must be signed for the host its user will call.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Optional
from urllib.parse import quote, urlsplit

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError
from django.conf import settings

from forklift_web.errors import StoreUnavailable

MAX_PRESIGN_SECONDS = 7 * 24 * 3600  # SigV4's limit


class Audience(StrEnum):
    PUBLIC = "public"  # browsers and API clients
    WORKER = "worker"  # worker supervisors and engines
    GATEWAY = "gateway"  # the gateway's own metadata calls


class Purpose(StrEnum):
    UPLOAD = "upload"  # PutObject and multipart uploads
    DOWNLOAD = "download"  # GetObject URLs and HEAD
    DELETE = "delete"  # the retention sweeper


@dataclass(frozen=True)
class ObjectInfo:
    size: int
    etag: str


@dataclass(frozen=True)
class PartInfo:
    """A part the store holds for a pending multipart upload."""

    number: int
    size: int
    etag: str


@dataclass(frozen=True)
class PendingUpload:
    """A multipart upload that was started and neither completed nor aborted."""

    key: str
    upload_id: str
    initiated: datetime


def content_disposition(filename: str) -> str:
    """An ``attachment`` Content-Disposition with an ASCII fallback and the UTF-8 name."""
    fallback = "".join(ch if 32 <= ord(ch) < 127 and ch not in '"\\' else "_" for ch in filename)
    return f"attachment; filename=\"{fallback}\"; filename*=UTF-8''{quote(filename, safe='')}"


def _client_error(operation: str, target: str, error: Exception) -> StoreUnavailable:
    if isinstance(error, ClientError):
        detail = error.response.get("Error", {})
        reason = f"{detail.get('Code', 'error')}: {detail.get('Message', '')}".strip(": ")
    else:
        reason = type(error).__name__
    return StoreUnavailable(
        f"The object store refused or failed {operation} of {target} ({reason})."
    )


class Bucket:
    """One bucket on one S3-compatible endpoint, with credentials per purpose."""

    def __init__(
        self,
        *,
        bucket: str,
        region: str,
        addressing_style: str,
        endpoints: dict,
        credentials: dict,
        prefix: str = "",
        connect_timeout: int = 5,
        read_timeout: int = 30,
        session_token: Optional[str] = None,
    ):
        self.bucket = bucket
        self.prefix = prefix
        self._region = region
        self._addressing_style = addressing_style
        self._endpoints = endpoints
        self._credentials = credentials
        self._session_token = session_token
        self._timeouts = (connect_timeout, read_timeout)
        self._clients: dict = {}
        self._lock = threading.Lock()

    def host(self, audience: Audience) -> Optional[str]:
        """The host name URLs for ``audience`` point at (None for the provider's default)."""
        endpoint = self._endpoints.get(audience)
        return urlsplit(endpoint).hostname if endpoint else None

    def client(self, purpose: Purpose, audience: Audience = Audience.GATEWAY):
        cache_key = (purpose, audience)
        with self._lock:
            client = self._clients.get(cache_key)
            if client is None:
                access_key, secret_key = self._credentials[purpose]
                client = boto3.session.Session().client(
                    "s3",
                    endpoint_url=self._endpoints.get(audience),
                    region_name=self._region,
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key,
                    aws_session_token=self._session_token,
                    config=Config(
                        signature_version="s3v4",
                        s3={"addressing_style": self._addressing_style},
                        connect_timeout=self._timeouts[0],
                        read_timeout=self._timeouts[1],
                        retries={"max_attempts": 3, "mode": "standard"},
                    ),
                )
                self._clients[cache_key] = client
            return client

    # ------------------------------------------------------------------ presigned URLs

    def presign_put(self, key: str, *, expires: int, audience: Audience) -> str:
        return self.client(Purpose.UPLOAD, audience).generate_presigned_url(
            "put_object",
            Params={"Bucket": self.bucket, "Key": key},
            ExpiresIn=min(expires, MAX_PRESIGN_SECONDS),
        )

    def presign_part(
        self, key: str, upload_id: str, part_number: int, *, expires: int, audience: Audience
    ) -> str:
        return self.client(Purpose.UPLOAD, audience).generate_presigned_url(
            "upload_part",
            Params={
                "Bucket": self.bucket,
                "Key": key,
                "UploadId": upload_id,
                "PartNumber": part_number,
            },
            ExpiresIn=min(expires, MAX_PRESIGN_SECONDS),
        )

    def presign_get(
        self, key: str, *, expires: int, audience: Audience, filename: Optional[str] = None
    ) -> str:
        params = {"Bucket": self.bucket, "Key": key}
        if filename:
            params["ResponseContentDisposition"] = content_disposition(filename)
        return self.client(Purpose.DOWNLOAD, audience).generate_presigned_url(
            "get_object", Params=params, ExpiresIn=min(expires, MAX_PRESIGN_SECONDS)
        )

    # ------------------------------------------------------------------ metadata calls

    def head(self, key: str) -> Optional[ObjectInfo]:
        """Size and ETag of ``key``, or None when there is no such object."""
        try:
            response = self.client(Purpose.DOWNLOAD).head_object(Bucket=self.bucket, Key=key)
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise _client_error("HEAD", key, error) from None
        except BotoCoreError as error:
            raise _client_error("HEAD", key, error) from None
        return ObjectInfo(size=response["ContentLength"], etag=response.get("ETag", "").strip('"'))

    def create_multipart(self, key: str) -> str:
        try:
            response = self.client(Purpose.UPLOAD).create_multipart_upload(
                Bucket=self.bucket, Key=key
            )
        except (ClientError, BotoCoreError) as error:
            raise _client_error("starting a multipart upload", key, error) from None
        return response["UploadId"]

    def complete_multipart(self, key: str, upload_id: str, parts: list) -> None:
        try:
            self.client(Purpose.UPLOAD).complete_multipart_upload(
                Bucket=self.bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={
                    "Parts": [
                        {"PartNumber": part["part_number"], "ETag": part["etag"]} for part in parts
                    ]
                },
            )
        except (ClientError, BotoCoreError) as error:
            raise _client_error("completing the multipart upload", key, error) from None

    def abort_multipart(self, key: str, upload_id: str) -> None:
        try:
            self.client(Purpose.DELETE).abort_multipart_upload(
                Bucket=self.bucket, Key=key, UploadId=upload_id
            )
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == "NoSuchUpload":
                return
            raise _client_error("aborting the multipart upload", key, error) from None
        except BotoCoreError as error:
            raise _client_error("aborting the multipart upload", key, error) from None

    def list_parts(self, key: str, upload_id: str) -> Optional[list]:
        """The parts a pending multipart upload holds (PartInfo, by number), or None when
        there is no such pending upload (never started, completed or aborted)."""
        try:
            pages = (
                self.client(Purpose.UPLOAD)
                .get_paginator("list_parts")
                .paginate(Bucket=self.bucket, Key=key, UploadId=upload_id)
            )
            parts = [
                PartInfo(
                    number=part["PartNumber"], size=part["Size"], etag=part["ETag"].strip('"')
                )
                for page in pages
                for part in page.get("Parts", [])
            ]
        except ClientError as error:
            # S3 answers NoSuchUpload for an unknown upload id, RustFS InvalidArgument for one
            # that is not even well-formed
            if error.response.get("Error", {}).get("Code") in {"NoSuchUpload", "InvalidArgument"}:
                return None
            raise _client_error("listing the parts", key, error) from None
        except BotoCoreError as error:
            raise _client_error("listing the parts", key, error) from None
        return sorted(parts, key=lambda part: part.number)

    def list_multipart_uploads(self, prefix: str) -> list:
        """The multipart uploads pending under ``prefix`` (PendingUpload)."""
        try:
            pages = (
                self.client(Purpose.DELETE)
                .get_paginator("list_multipart_uploads")
                .paginate(Bucket=self.bucket, Prefix=prefix)
            )
            return [
                PendingUpload(
                    key=item["Key"], upload_id=item["UploadId"], initiated=item["Initiated"]
                )
                for page in pages
                for item in page.get("Uploads", [])
            ]
        except (ClientError, BotoCoreError) as error:
            raise _client_error("listing the multipart uploads", prefix, error) from None

    def delete(self, key: str) -> None:
        try:
            self.client(Purpose.DELETE).delete_object(Bucket=self.bucket, Key=key)
        except (ClientError, BotoCoreError) as error:
            raise _client_error("DELETE", key, error) from None

    def copy_from(self, source: "Bucket", source_key: str, key: str) -> None:
        """Server-side copy of ``source_key`` in ``source`` to ``key`` here (no data passes
        through the gateway; large objects are copied part by part by the store)."""
        try:
            self.client(Purpose.UPLOAD).copy(
                {"Bucket": source.bucket, "Key": source_key}, self.bucket, key
            )
        except (ClientError, BotoCoreError) as error:
            raise _client_error("copying to", f"{self.bucket}/{key}", error) from None

    def check_access(self) -> None:
        """Raise StoreUnavailable unless the bucket exists and these credentials reach it."""
        try:
            self.client(Purpose.DOWNLOAD).head_bucket(Bucket=self.bucket)
        except (ClientError, BotoCoreError) as error:
            raise _client_error("HEAD", f"bucket {self.bucket}", error) from None


_store: Optional[Bucket] = None
_store_lock = threading.Lock()


def store() -> Bucket:
    """The installation's bucket (uploads/, jobs/, previews/)."""
    global _store
    with _store_lock:
        if _store is None:
            conf = settings.FORKLIFT_STORE
            _store = Bucket(
                bucket=conf["bucket"],
                region=conf["region"],
                addressing_style=conf["addressing_style"],
                endpoints={
                    Audience.GATEWAY: conf["endpoint_url"],
                    Audience.PUBLIC: conf["public_endpoint_url"],
                    Audience.WORKER: conf["worker_endpoint_url"],
                },
                credentials={Purpose(name): pair for name, pair in conf["credentials"].items()},
                connect_timeout=conf["connect_timeout"],
                read_timeout=conf["read_timeout"],
            )
        return _store


def reset_store() -> None:
    """Forget the cached bucket (after settings change, in tests)."""
    global _store
    with _store_lock:
        _store = None


def connection_bucket(config: dict, secrets: dict) -> Bucket:
    """The bucket of an ``s3`` connection, reached with the connection's own credentials.

    Every audience uses the connection's endpoint: it is external to the installation, so
    browsers, workers and the gateway all reach it under the same name.
    """
    endpoint = config.get("endpoint_url") or None
    pair = (secrets.get("access_key_id"), secrets.get("secret_access_key"))
    return Bucket(
        bucket=config["bucket"],
        prefix=config.get("prefix", ""),
        region=config.get("region") or "us-east-1",
        addressing_style=config.get("addressing_style") or "path",
        endpoints={audience: endpoint for audience in Audience},
        credentials={purpose: pair for purpose in Purpose},
        session_token=secrets.get("session_token") or None,
        connect_timeout=settings.FORKLIFT_STORE["connect_timeout"],
        read_timeout=settings.FORKLIFT_STORE["read_timeout"],
    )
