from pathlib import Path

import boto3
from google.cloud import storage

BUCKET_NAME = "stairlight"
COPY_PATH = "tests/sql/gcs/"

gcs_client = storage.Client(credentials=None, project=None)
gcs_bucket = gcs_client.get_bucket(BUCKET_NAME)

s3_client = boto3.resource("s3")
s3_bucket = s3_client.Bucket(BUCKET_NAME)


def get_object_key(path: Path) -> str:
    file_name = str(path).replace(COPY_PATH, "")
    return f"sql/{file_name}"


def upload_to_gcs(path: Path) -> None:
    object_key = get_object_key(path=path)
    blob = gcs_bucket.blob(object_key)
    blob.upload_from_filename(path)


def upload_to_s3(path: Path) -> None:
    object_key = get_object_key(path=path)
    s3_bucket.upload_file(str(path), object_key)


def main():
    for p in Path(COPY_PATH).glob("**/*"):
        if not p.is_file():
            continue
        upload_to_gcs(path=p)
        upload_to_s3(path=p)


if __name__ == "__main__":
    main()
