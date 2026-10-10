"""Create the platform's bucket and let the UI's origin upload to it (the Compose "init" step).

Browsers PUT uploads and GET downloads straight to the store through presigned URLs, so the
bucket needs a CORS rule for the UI's origin (FORKLIFT_CORS_ORIGINS, comma-separated). Safe to
repeat: an existing bucket is kept and its CORS rule replaced.
"""

import os

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

bucket = os.environ["FORKLIFT_S3_BUCKET"]
origins = [o.strip() for o in os.environ.get("FORKLIFT_CORS_ORIGINS", "").split(",") if o.strip()]
client = boto3.client(
    "s3",
    endpoint_url=os.environ["FORKLIFT_S3_ENDPOINT_URL"],
    region_name=os.environ.get("FORKLIFT_S3_REGION", "us-east-1"),
    aws_access_key_id=os.environ["FORKLIFT_S3_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["FORKLIFT_S3_SECRET_ACCESS_KEY"],
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)

try:
    client.head_bucket(Bucket=bucket)
    print(f"init_store: bucket {bucket} exists")
except ClientError:
    client.create_bucket(Bucket=bucket)
    print(f"init_store: created bucket {bucket}")

if origins:
    client.put_bucket_cors(
        Bucket=bucket,
        CORSConfiguration={
            "CORSRules": [
                {
                    "AllowedOrigins": origins,
                    "AllowedMethods": ["GET", "PUT", "HEAD"],
                    "AllowedHeaders": ["*"],
                    "ExposeHeaders": ["ETag"],
                    "MaxAgeSeconds": 3600,
                }
            ]
        },
    )
    print(f"init_store: CORS for {', '.join(origins)}")
