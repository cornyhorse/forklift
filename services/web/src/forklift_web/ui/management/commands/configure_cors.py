"""Set the CORS rule the browser UI needs on the installation's bucket.

Browsers upload straight to the store (PUT to presigned URLs, ``ETag`` read back for each part
of a multipart upload) and fetch previews, reports and generated schemas from it (GET). The
store answers those cross-origin requests only for origins its bucket's CORS rules allow:

    forklift-web configure_cors --origin https://forklift.example.org

This replaces the bucket's CORS rules with one rule for the given origins. Stores that take
CORS settings elsewhere (some only support a global setting) need the same rule there.
"""

from __future__ import annotations

from urllib.parse import urlsplit

from botocore.exceptions import BotoCoreError, ClientError
from django.core.management.base import BaseCommand, CommandError

from forklift_web import storage

METHODS = ["GET", "PUT", "HEAD"]


def checked_origin(value: str) -> str:
    parts = urlsplit(value)
    if parts.scheme not in {"http", "https"} or not parts.hostname or parts.path not in {"", "/"}:
        raise CommandError(
            f"{value!r} is not an origin: give scheme://host[:port], such as "
            "https://forklift.example.org, without a path."
        )
    return f"{parts.scheme}://{parts.netloc}"


def cors_rules(origins: list, max_age: int) -> dict:
    return {
        "CORSRules": [
            {
                "AllowedOrigins": origins,
                "AllowedMethods": METHODS,
                "AllowedHeaders": ["*"],
                "ExposeHeaders": ["ETag"],
                "MaxAgeSeconds": max_age,
            }
        ]
    }


class Command(BaseCommand):
    help = (
        "Allow the UI's origins to upload to (PUT) and read from (GET, HEAD) the installation's "
        "bucket, exposing the ETag header that multipart uploads need. Replaces the bucket's "
        "CORS rules."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--origin",
            action="append",
            required=True,
            help="An origin the UI is served from, e.g. https://forklift.example.org (repeat it "
            "for more than one)",
        )
        parser.add_argument(
            "--max-age",
            type=int,
            default=3600,
            help="Seconds browsers may cache the answer to a preflight request (default 3600)",
        )

    def handle(self, *args, origin, max_age, **options):
        origins = [checked_origin(value) for value in origin]
        bucket = storage.store()
        client = bucket.client(storage.Purpose.UPLOAD)
        try:
            client.put_bucket_cors(
                Bucket=bucket.bucket, CORSConfiguration=cors_rules(origins, max_age)
            )
        except (ClientError, BotoCoreError) as error:
            detail = error.response.get("Error", {}) if isinstance(error, ClientError) else {}
            reason = detail.get("Code") or type(error).__name__
            raise CommandError(
                f"The store refused to set the CORS rules of bucket {bucket.bucket!r} "
                f"({reason}); the credential needs s3:PutBucketCORS, or set the rule in the "
                "store's own configuration."
            ) from None
        self.stdout.write(
            f"Bucket {bucket.bucket!r} now allows {', '.join(METHODS)} from "
            f"{', '.join(origins)} (ETag exposed, preflight cached {max_age} s)."
        )
