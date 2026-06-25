import boto3
from botocore.exceptions import ClientError


class ObjectStore:
    """The 'getting and writing files' half of the converter: a thin wrapper over an
    S3-compatible object store, kept separate from the ulog -> mcap conversion logic.
    Configured entirely from AWS_* env vars (endpoint, region, credentials)."""

    def __init__(self):
        self._s3 = boto3.client("s3")

    def exists(self, bucket: str, key: str) -> bool:
        try:
            self._s3.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    def list_ulg_keys(self, bucket: str):
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".ulg"):
                    yield obj["Key"]

    def download(self, bucket: str, key: str, dest_path: str) -> None:
        self._s3.download_file(bucket, key, dest_path)

    def upload(self, src_path: str, bucket: str, key: str) -> None:
        self._s3.upload_file(src_path, bucket, key)
