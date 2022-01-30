import re
from typing import Iterator, Optional

from google.cloud import storage

from .. import config_key
from .base import Template, TemplateSource, TemplateSourceType

GCS_URI_PREFIX = "gs://"


class GcsTemplate(Template):
    def __init__(
        self,
        mapping_config: dict,
        key: str,
        source_type: Optional[TemplateSourceType] = TemplateSourceType.GCS,
        bucket: Optional[str] = None,
        project: Optional[str] = None,
        default_table_prefix: Optional[str] = None,
    ):
        super().__init__(
            mapping_config=mapping_config,
            key=key,
            source_type=source_type,
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
        return f"{GCS_URI_PREFIX}{self.bucket}/{self.key}"

    def get_template_str(self) -> str:
        """Get template string that read from a file in GCS

        Returns:
            str: Template string
        """
        client = storage.Client(credentials=None, project=self.project)
        bucket = client.get_bucket(self.bucket)
        blob = bucket.blob(self.key)
        return blob.download_as_bytes().decode("utf-8")

    def render(self, params: dict) -> str:
        """Render SQL query string from a jinja template on GCS

        Args:
            params (dict): Jinja paramters

        Returns:
            str: SQL query string
        """
        template_str = self.get_template_str()
        if params:
            results = self.render_by_base_loader(
                template_str=template_str, params=params
            )
        else:
            results = template_str
        return results


class GcsTemplateSource(TemplateSource):
    def __init__(
        self, stairlight_config: dict, mapping_config: dict, source_attributes: dict
    ) -> None:
        super().__init__(
            stairlight_config=stairlight_config, mapping_config=mapping_config
        )
        self.source_attributes = source_attributes
        self.source_type = TemplateSourceType.GCS

    def search_templates_iter(self) -> Iterator[Template]:
        """Search SQL template files from GCS

        Args:
            source (dict): Source attributes of SQL template files

        Yields:
            Iterator[SQLTemplate]: SQL template file attributes
        """
        project = self.source_attributes.get(config_key.PROJECT_ID)
        client = storage.Client(credentials=None, project=project)
        bucket = self.source_attributes.get(config_key.BUCKET_NAME)
        blobs = client.list_blobs(bucket)
        for blob in blobs:
            if (
                not re.fullmatch(
                    rf"{self.source_attributes.get(config_key.REGEX)}",
                    blob.name,
                )
            ) or self.is_excluded(source_type=self.source_type, key=blob.name):
                self.logger.debug(f"{blob.name} is skipped.")
                continue
            yield GcsTemplate(
                mapping_config=self._mapping_config,
                key=blob.name,
                source_type=self.source_type,
                project=project,
                bucket=bucket,
                default_table_prefix=self.source_attributes.get(
                    config_key.DEFAULT_TABLE_PREFIX
                ),
            )
