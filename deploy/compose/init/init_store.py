"""Create the platform's bucket if it does not exist (the Compose "init" step; safe to repeat).

The bucket's CORS rule for the UI's origins is set afterwards by `forklift-web configure_cors`.
"""

import os

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

bucket = os.environ["FORKLIFT_S3_BUCKET"]
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
