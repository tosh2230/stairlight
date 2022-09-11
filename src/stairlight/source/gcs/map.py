from google.cloud.storage import Blob, Client

from ..config_key import GCS_URI_SCHEME


def get_gcs_blob(uri: str) -> Blob:
    """Get a Google Cloud Storage blob

    Args:
        uri (str): URI

    Returns:
        Blob: Blob
    """
    bucket_name = uri.replace(GCS_URI_SCHEME, "").split("/")[0]
    key = uri.replace(f"{GCS_URI_SCHEME}{bucket_name}/", "")

    client = Client(credentials=None, project=None)
    bucket = client.get_bucket(bucket_name)
    return bucket.blob(key)
