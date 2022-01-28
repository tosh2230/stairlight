import pathlib
import os
import re
from typing import Iterator, Optional
from jinja2 import Environment, FileSystemLoader

from .base import TemplateSourceType, Template, TemplateSource


class FsTemplate(Template):
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
        return str(pathlib.Path(self.file_path).resolve())

    def get_template_str(self) -> str:
        """Get template string that read from a file in local file system

        Returns:
            str: Template string
        """
        template_str = ""
        with open(self.file_path) as f:
            template_str = f.read()
        return template_str

    def render(self, params: dict) -> str:
        """Render SQL query string from a jinja template on local file system

        Args:
            params (dict): Jinja paramters

        Returns:
            str: SQL query string
        """
        env = Environment(loader=FileSystemLoader(os.path.dirname(self.file_path)))
        jinja_template = env.get_template(os.path.basename(self.file_path))
        return jinja_template.render(params=params)


class FsTemplateSource(TemplateSource):
    def __init__(
        self, stairlight_config: dict, mapping_config: dict, source_attributes: dict
    ) -> None:
        super().__init__(stairlight_config, mapping_config)
        self.source_attributes = source_attributes
        self.source_type = TemplateSourceType.FS

    def search_templates_iter(self) -> Iterator[Template]:
        """Search SQL template files from local file system

        Args:
            source (dict): Source attributes of SQL template files

        Yields:
            Iterator[SQLTemplate]: SQL template file attributes
        """
        path_obj = pathlib.Path(self.source_attributes.get("path"))
        for p in path_obj.glob("**/*"):
            if (
                not re.fullmatch(rf'{self.source_attributes.get("regex")}', str(p))
            ) or self.is_excluded(source_type=self.source_type, file_path=str(p)):
                self.logger.debug(f"{str(p)} is skipped.")
                continue
            yield FsTemplate(
                mapping_config=self._mapping_config,
                source_type=self.source_type,
                file_path=str(p),
                default_table_prefix=self.source_attributes.get("default_table_prefix"),
            )
