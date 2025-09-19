# /// script
# requires-python = ">=3.11"
# dependencies = ["boto3>=1.34"]
# ///

import os
import sys

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

endpoint = os.environ.get("BUCKET_ENDPOINT", "http://localhost:9000")
region = os.environ.get("BUCKET_REGION", "us-east-1")
access_key = os.environ.get("BUCKET_ACCESS_KEY", os.environ.get("MINIO_ROOT_USER", "minioadmin"))
secret_key = os.environ.get("BUCKET_SECRET_KEY", os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"))
bucket_name = os.environ.get("BUCKET_NAME", "sapflux-parsed")

session = boto3.session.Session()
s3 = session.resource(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=region,
    config=Config(signature_version="s3v4"),
)

client = s3.meta.client

try:
    client.head_bucket(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' already exists at {endpoint}")
except ClientError as exc:
    error_code = int(exc.response.get("Error", {}).get("Code", "0"))
    if error_code == 404:
        try:
            client.create_bucket(Bucket=bucket_name)
            print(f"Created bucket '{bucket_name}' at {endpoint}")
        except Exception as create_exc:  # noqa: BLE001
            print(f"Failed to create bucket: {create_exc}", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"Failed to check bucket: {exc}", file=sys.stderr)
        sys.exit(1)
