from pathlib import Path

from google.cloud import storage

BUCKET_NAME = "stairlight"
COPY_PATH = "tests/sql/gcs/"


def main():
    client = storage.Client(credentials=None, project=None)
    bucket = client.get_bucket(BUCKET_NAME)
    for p in Path(COPY_PATH).glob("**/*"):
        if not p.is_file():
            continue
        file_name = str(p).replace(COPY_PATH, "")
        blob = bucket.blob(f"sql/{file_name}")
        blob.upload_from_filename(p)


if __name__ == "__main__":
    main()
