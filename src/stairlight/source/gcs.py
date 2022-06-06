import re
from typing import Iterator, Optional

from google.cloud import storage

from ..config import get_config_value
from ..key import StairlightConfigKey
from .base import Template, TemplateSource, TemplateSourceType
from .controller import GCS_URI_SCHEME


class GcsTemplate(Template):
    def __init__(
        self,
        mapping_config: dict,
        key: str,
        bucket: Optional[str] = None,
        project: Optional[str] = None,
        default_table_prefix: Optional[str] = None,
    ):
        super().__init__(
            mapping_config=mapping_config,
            key=key,
            source_type=TemplateSourceType.GCS,
            bucket=bucket,
            project=project,
            default_table_prefix=default_table_prefix,
        )
        self.uri = self.get_uri()

    def get_uri(self) -> str:
        """Get uri from file path

        Returns:
            str: uri
        """
        return f"{GCS_URI_SCHEME}{self.bucket}/{self.key}"

    def get_template_str(self) -> str:
        """Get template string that read from a file in GCS

        Returns:
            str: Template string
        """
        client = storage.Client(credentials=None, project=self.project)
        bucket = client.get_bucket(self.bucket)
        blob = bucket.blob(self.key)
        return blob.download_as_bytes().decode("utf-8")


class GcsTemplateSource(TemplateSource):
    def __init__(
        self, stairlight_config: dict, mapping_config: dict, source_attributes: dict
    ) -> None:
        super().__init__(
            stairlight_config=stairlight_config, mapping_config=mapping_config
        )
        self.source_attributes = source_attributes
        self.source_type = TemplateSourceType.GCS

    def search_templates(self) -> Iterator[Template]:
        """Search SQL template files from GCS

        Args:
            source (dict): Source attributes of SQL template files

        Yields:
            Iterator[SQLTemplate]: SQL template file attributes
        """
        project = get_config_value(
            key=StairlightConfigKey.Gcs.PROJECT_ID,
            target=self.source_attributes,
            fail_if_not_found=False,
            enable_logging=False,
        )
        bucket = get_config_value(
            key=StairlightConfigKey.Gcs.BUCKET_NAME,
            target=self.source_attributes,
            fail_if_not_found=True,
            enable_logging=False,
        )
        default_table_prefix = get_config_value(
            key=StairlightConfigKey.DEFAULT_TABLE_PREFIX,
            target=self.source_attributes,
            fail_if_not_found=False,
            enable_logging=False,
        )
        regex = get_config_value(
            key=StairlightConfigKey.REGEX,
            target=self.source_attributes,
            fail_if_not_found=True,
            enable_logging=False,
        )

        client = storage.Client(credentials=None, project=project)
        blobs = client.list_blobs(bucket)
        for blob in blobs:
            if (
                not re.fullmatch(
                    rf"{regex}",
                    blob.name,
                )
            ) or self.is_excluded(source_type=self.source_type, key=blob.name):
                self.logger.debug(f"{blob.name} is skipped.")
                continue

            yield GcsTemplate(
                mapping_config=self._mapping_config,
                key=blob.name,
                project=project,
                bucket=bucket,
                default_table_prefix=default_table_prefix,
            )


def get_gcs_blob(gcs_uri: str) -> storage.Blob:
    bucket_name = gcs_uri.replace(GCS_URI_SCHEME, "").split("/")[0]
    key = gcs_uri.replace(f"{GCS_URI_SCHEME}{bucket_name}/", "")

    client = storage.Client(credentials=None, project=None)
    bucket = client.get_bucket(bucket_name)
    return bucket.blob(key)
