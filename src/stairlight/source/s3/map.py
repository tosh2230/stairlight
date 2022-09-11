import boto3
from mypy_boto3_s3.service_resource import Object, S3ServiceResource

from ..config_key import S3_URI_SCHEME


def get_s3_object(uri: str) -> Object:
    """Get a S3 object

    Args:
        uri (str): URI

    Returns:
        Object: S3 object
    """
    bucket_name = uri.replace(S3_URI_SCHEME, "").split("/")[0]
    key = uri.replace(f"{S3_URI_SCHEME}{bucket_name}/", "")

    s3: S3ServiceResource = boto3.resource("s3")
    return s3.Object(bucket_name=bucket_name, key=key)
