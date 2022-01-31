import os
import pathlib
import re
from typing import Iterator, Optional

from jinja2 import Environment, FileSystemLoader

from .. import config_key
from .base import Template, TemplateSource, TemplateSourceType


class FileTemplate(Template):
    def __init__(
        self,
        mapping_config: dict,
        key: str,
        source_type: Optional[TemplateSourceType] = TemplateSourceType.FILE,
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
        return str(pathlib.Path(self.key).resolve())

    def get_template_str(self) -> str:
        """Get template string that read from a file in local file system

        Returns:
            str: Template string
        """
        with open(self.key) as f:
            return f.read()

    def render(self, params: dict) -> str:
        """Render SQL query string from a jinja template on local file system

        Args:
            params (dict): Jinja paramters

        Returns:
            str: SQL query string
        """
        if params:
            env = Environment(loader=FileSystemLoader(os.path.dirname(self.key)))
            jinja_template = env.get_template(os.path.basename(self.key))
            results = jinja_template.render(params)
        else:
            results = self.get_template_str()
        return results


class FileTemplateSource(TemplateSource):
    def __init__(
        self, stairlight_config: dict, mapping_config: dict, source_attributes: dict
    ) -> None:
        super().__init__(stairlight_config, mapping_config)
        self.source_attributes = source_attributes
        self.source_type = TemplateSourceType.FILE

    def search_templates_iter(self) -> Iterator[Template]:
        """Search SQL template files from local file system

        Args:
            source (dict): Source attributes of SQL template files

        Yields:
            Iterator[SQLTemplate]: SQL template file attributes
        """
        path_obj = pathlib.Path(self.source_attributes.get(config_key.FILE_SYSTEM_PATH))
        for p in path_obj.glob("**/*"):
            if (
                not re.fullmatch(
                    rf"{self.source_attributes.get(config_key.REGEX)}",
                    str(p),
                )
            ) or self.is_excluded(source_type=self.source_type, key=str(p)):
                self.logger.debug(f"{str(p)} is skipped.")
                continue
            yield FileTemplate(
                mapping_config=self._mapping_config,
                key=str(p),
                source_type=self.source_type,
                default_table_prefix=self.source_attributes.get(
                    config_key.DEFAULT_TABLE_PREFIX
                ),
            )
