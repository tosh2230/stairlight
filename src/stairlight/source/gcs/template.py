from __future__ import annotations

import re
from typing import Any, Iterator

from google.cloud import storage

from ..config import ConfigAttributeNotFoundException, MappingConfig, StairlightConfig
from ..controller import GCS_URI_SCHEME
from ..template import Template, TemplateSource, TemplateSourceType
from .config import StairlightConfigIncludeGcs


class GcsTemplate(Template):
    def __init__(
        self,
        mapping_config: MappingConfig,
        key: str,
        bucket: str | None = None,
        project: str | None = None,
        default_table_prefix: str | None = None,
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
        self,
        stairlight_config: StairlightConfig,
        mapping_config: MappingConfig,
        include: StairlightConfigIncludeGcs,
    ) -> None:
        super().__init__(
            stairlight_config=stairlight_config,
            mapping_config=mapping_config,
        )
        self._include = include

    def search_templates(self) -> Iterator[Template]:
        """Search SQL template files from GCS

        Yields:
            Iterator[SQLTemplate]: SQL template file attributes
        """
        project = self._include.ProjectId
        bucket_name = self._include.BucketName

        if not bucket_name:
            raise ConfigAttributeNotFoundException(
                f"BucketName is not found. {self._include}"
            )

        client = storage.Client(credentials=None, project=self._include.ProjectId)
        blobs: Any = client.list_blobs(bucket_name)
        for blob in blobs:
            if self.is_skipped(blob=blob):
                self.logger.debug(f"{blob.name} is skipped.")
                continue

            yield GcsTemplate(
                mapping_config=self._mapping_config,
                key=blob.name,
                project=project,
                bucket=bucket_name,
                default_table_prefix=self._include.DefaultTablePrefix,
            )

    def is_skipped(self, blob: Any) -> bool:
        """Check the target path is skipped or not

        Args:
            blob (Any): Blob

        Returns:
            bool: Is skipped or not
        """
        return not re.fullmatch(
            rf"{self._include.Regex}",
            blob.name,
        ) or self.is_excluded(
            source_type=TemplateSourceType(self._include.TemplateSourceType),
            key=blob.name,
        )
