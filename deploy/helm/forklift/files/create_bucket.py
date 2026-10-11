"""Create the bundled RustFS's bucket if it does not exist (the store-setup Job; safe to repeat).

Waits up to five minutes for the store to answer: the Job runs right after the install, while
RustFS may still be starting. The bucket's CORS rule is set afterwards by configure_cors.
"""

import os
import sys
import time

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

bucket = os.environ["FORKLIFT_S3_BUCKET"]
client = boto3.client(
    "s3",
    endpoint_url=os.environ["FORKLIFT_S3_ENDPOINT_URL"],
    region_name=os.environ.get("FORKLIFT_S3_REGION", "us-east-1"),
    aws_access_key_id=os.environ["FORKLIFT_S3_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["FORKLIFT_S3_SECRET_ACCESS_KEY"],
    config=Config(
        signature_version="s3v4", s3={"addressing_style": "path"}, retries={"max_attempts": 1}
    ),
)
deadline = time.monotonic() + 300
while True:
    try:
        client.head_bucket(Bucket=bucket)
        print(f"store-setup: bucket {bucket} exists")
        break
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")
        if code not in ("404", "NoSuchBucket", "NotFound"):
            sys.exit(f"store-setup: the store refused HEAD on bucket {bucket} ({code})")
        client.create_bucket(Bucket=bucket)
        print(f"store-setup: created bucket {bucket}")
        break
    except BotoCoreError as error:
        if time.monotonic() > deadline:
            sys.exit(
                f"store-setup: the store did not answer within 5 minutes ({type(error).__name__})"
            )
        print(f"store-setup: waiting for the store ({type(error).__name__})")
        time.sleep(3)
