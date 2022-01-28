import re
from typing import Iterator, Optional

from google.cloud import storage

from .base import TemplateSourceType, Template, TemplateSource


class GcsTemplate(Template):
    def __init__(
        self,
        mapping_config: dict,
        source_type: TemplateSourceType,
        file_path: str,
        bucket: Optional[str] = None,
        project: Optional[str] = None,
        default_table_prefix: Optional[str] = None,
        labels: Optional[dict] = None,
        template_str: Optional[str] = None,
    ):
        super().__init__(
            mapping_config,
            source_type,
            file_path,
            bucket=bucket,
            project=project,
            default_table_prefix=default_table_prefix,
            labels=labels,
            template_str=template_str,
        )
        self.uri = self.get_uri()

    def get_uri(self) -> str:
        """Get uri from file path

        Returns:
            str: uri
        """
        return f"gs://{self.bucket}/{self.file_path}"

    def get_template_str(self) -> str:
        """Get template string that read from a file in GCS

        Returns:
            str: Template string
        """
        client = storage.Client(credentials=None, project=self.project)
        bucket = client.get_bucket(self.bucket)
        blob = bucket.blob(self.file_path)
        return blob.download_as_bytes().decode("utf-8")

    def render(self, params: dict) -> str:
        """Render SQL query string from a jinja template on GCS

        Args:
            params (dict): Jinja paramters

        Returns:
            str: SQL query string
        """
        template_str = self.get_template_str()
        return self.render_by_base_loader(template_str=template_str, params=params)


class GcsTemplateSource(TemplateSource):
    def __init__(
        self, stairlight_config: dict, mapping_config: dict, source_attributes: dict
    ) -> None:
        super().__init__(stairlight_config, mapping_config)
        self.source_attributes = source_attributes
        self.source_type = TemplateSourceType.GCS

    def search_templates_iter(self) -> Iterator[Template]:
        """Search SQL template files from GCS

        Args:
            source (dict): Source attributes of SQL template files

        Yields:
            Iterator[SQLTemplate]: SQL template file attributes
        """
        project = self.source_attributes.get("project")
        client = storage.Client(credentials=None, project=project)
        bucket = self.source_attributes.get("bucket")
        blobs = client.list_blobs(bucket)
        for blob in blobs:
            if (
                not re.fullmatch(rf'{self.source_attributes.get("regex")}', blob.name)
            ) or self.is_excluded(source_type=self.source_type, file_path=blob.name):
                self.logger.debug(f"{blob.name} is skipped.")
                continue
            yield GcsTemplate(
                mapping_config=self._mapping_config,
                source_type=self.source_type,
                file_path=blob.name,
                project=project,
                bucket=bucket,
                default_table_prefix=self.source_attributes.get("default_table_prefix"),
            )
