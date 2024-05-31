from __future__ import annotations

import enum
import re
from abc import ABC, abstractmethod
from logging import getLogger
from string import Template as StringTemplate
from typing import Any, Iterator

from jinja2 import BaseLoader, Environment
from jinja2.exceptions import UndefinedError

from src.stairlight.source.config import (
    MappingConfig,
    MappingConfigMappingTable,
    StairlightConfig,
)

logger = getLogger(__name__)


class TemplateSourceType(enum.Enum):
    """Query template source type"""

    FILE = "File"
    GCS = "GCS"
    REDASH = "Redash"
    DBT = "dbt"
    S3 = "S3"

    def __str__(self):
        return self.name


class Template(ABC):
    """Base query template"""

    def __init__(
        self,
        mapping_config: MappingConfig,
        key: str,
        source_type: TemplateSourceType,
        bucket: str | None = None,
        project: str | None = None,
        default_table_prefix: str | None = None,
        data_source_name: str | None = None,
        query_id: int | None = None,
        project_name: str | None = None,
    ):
        """Query template

        Args:
            mapping_config (dict[str, Any]): Mapping configuration
            key (str): A key of query statement.
            source_type (SourceType): Source type.
            bucket (str, optional):
                A name of object storage bucket where query statements saved.
                Defaults to None.
            project (str, optional):
                A project name of Google Cloud where query statements saved.
                Defaults to None.
            default_table_prefix (str, optional):
                If project or dataset that configured table have are omitted,
                it will be complement this prefix. Defaults to None.
            data_source_name (str, optional):
                Data source name. It's only used in a Redash template.
                Defaults to None.
            query_id (int, optional):
                Query id. It's only used in a Redash template.
                Defaults to None.
            project_name (str, optional):
                Project name. It's only used in a DBT template.
                Defaults to None.
        """
        self._mapping_config = mapping_config
        self.key = key
        self.source_type = source_type
        self.bucket = bucket
        self.project = project
        self.default_table_prefix = default_table_prefix
        self.data_source_name = data_source_name
        self.query_id = query_id
        self.project_name = project_name
        self.uri = ""

    def find_mapped_table_attributes(self) -> Iterator[MappingConfigMappingTable]:
        """Get mapped tables as iterator

        Yields:
            Iterator[dict]: Mapped table attributes
        """
        mapping: Any
        for mapping in self._mapping_config.get_mapping():
            found: bool = False
            if mapping.TemplateSourceType in (
                TemplateSourceType.FILE.value,
                TemplateSourceType.DBT.value,
            ):
                if self.key.endswith(mapping.FileSuffix):
                    found = True
            elif mapping.TemplateSourceType in (
                TemplateSourceType.GCS.value,
                TemplateSourceType.S3.value,
            ):
                if self.uri == mapping.Uri:
                    found = True

            if not found:
                continue

            for table_attributes in mapping.get_table():
                yield table_attributes
            break

    @property
    def mapped(self) -> bool:
        """Check if the template is set to mapping configuration

        Returns:
            bool: Is mapped or not
        """
        result = False
        for _ in self.find_mapped_table_attributes():
            result = True
            break
        return result

    @staticmethod
    def get_jinja_params(template_str: str) -> list:
        """get jinja parameters from template string

        Args:
            template_str (str): Template string

        Returns:
            list: Jinja parameters
        """
        jinja_expressions = "".join(
            re.findall("{{[^}]*}}", template_str, re.IGNORECASE)
        )
        return [
            param.strip()
            for param in re.findall("[^{}]+", jinja_expressions, re.IGNORECASE)
        ]

    def render_by_jinja(
        self,
        template_str: str,
        params: dict[str, Any],
    ) -> str:
        """Render query string by jinja2

        Args:
            template_str (str): template string
            params (dict[str, Any]): Jinja parameters

        Raises:
            RenderingTemplateException: class RenderingTemplateException

        Returns:
            str: rendered query string
        """
        if not self.get_jinja_params(template_str=template_str):
            return template_str

        rendered_str: str = template_str
        try:
            env = Environment(loader=BaseLoader())
            template = env.from_string(template_str)
            rendered_str = template.render(params)
        except UndefinedError as undefined_error:
            logger.warning(
                (
                    f"{undefined_error.message}, "
                    f"source_type: {self.source_type}, "
                    f"key: {self.key}"
                )
            )
        return rendered_str

    def render_by_string_template(
        self,
        template_str: str,
        params: dict[str, Any],
    ) -> str:
        """_summary_

        Args:
            template_str (str): template string
            params (dict[str, Any]): mapping dict

        Returns:
            str: rendered query string
        """
        s = StringTemplate(template=template_str)

        try:
            rendered_str = s.substitute(params)
        except KeyError:
            logger.warning(
                (
                    f"Params not found. "
                    f"source_type: {self.source_type}, "
                    f"key: {self.key}",
                    f"params: {params}",
                )
            )
            rendered_str = s.safe_substitute(params)
        except ValueError:
            logger.warning(
                (
                    f"Query rendering failed. "
                    f"source_type: {self.source_type}, "
                    f"key: {self.key}",
                    f"params: {params}",
                )
            )
            rendered_str = template_str
        return rendered_str

    @staticmethod
    def ignore_jinja_params(template_str: str, ignore_params: list[str]) -> str:
        """ignore jinja parameters

        Args:
            template_str (str): template string
            ignore_params (list[str]): ignore parameters

        Returns:
            str: ignored template string
        """
        if not ignore_params:
            return template_str

        ignored_str = template_str
        for ignore_param in ignore_params:
            ignored_str = ignored_str.replace(
                "{{{{ {} }}}}".format(ignore_param), "ignored"
            )
        return ignored_str

    @staticmethod
    def ignore_string_template_params(
        template_str: str, ignore_params: list[str]
    ) -> str:
        """ignore string.Template parameters

        Args:
            template_str (str): template string
            ignore_params (list[str]): ignore parameters

        Returns:
            str: ignored template string
        """
        if not ignore_params:
            return template_str

        s = StringTemplate(template=template_str)
        ignore_mapping = {k: "ignored" for k in ignore_params}
        return s.safe_substitute(ignore_mapping)

    @abstractmethod
    def get_uri(self) -> str:
        """Get uri"""
        pass

    @abstractmethod
    def get_template_str(self) -> str:
        """Get template strings that read from template source"""
        pass

    def render(self, params: dict[str, Any], ignore_params: list[str] = None) -> str:
        """Render a query statement from a jinja template
        Args:
            params (dict[str, Any]): Jinja parameters
            ignore_params (list[str]): Ignore parameters. Defaults to None.
        Returns:
            str: Query statement
        """
        rendered_str = self.get_template_str()
        rendered_str = self.ignore_jinja_params(
            template_str=rendered_str,
            ignore_params=ignore_params,
        )
        rendered_str = self.ignore_string_template_params(
            template_str=rendered_str,
            ignore_params=ignore_params,
        )

        if not params:
            return rendered_str

        rendered_str = self.render_by_jinja(
            template_str=rendered_str,
            params=params,
        )

        rendered_str = self.render_by_string_template(
            template_str=rendered_str,
            params=params,
        )

        return rendered_str


class RenderingTemplateException(Exception):
    """Exception when failing to render jinja templates.

    Args:
        Exception (_type_): Exception
    """

    def __init__(self, *args: object) -> None:
        super().__init__(*args)

    def __str__(self) -> str:
        return super().__str__()


class TemplateSource(ABC):
    """SQL template source"""

    logger = getLogger(__name__)

    def __init__(
        self,
        stairlight_config: StairlightConfig,
        mapping_config: MappingConfig,
        **kwargs,
    ) -> None:
        """SQL template source

        Args:
            stairlight_config (dict[str, Any]): Stairlight configuration
            mapping_config (dict[str, Any]): Mapping configuration
            source_attributes (dict[str, Any]): Template source attributes
        """
        self._stairlight_config = stairlight_config
        self._mapping_config = mapping_config

    @abstractmethod
    def search_templates(self) -> Iterator[Template]:
        """Search query template files

        Yields:
            Iterator[Template]: Attributes of query template files
        """
        pass

    def is_excluded(self, source_type: TemplateSourceType, key: str) -> bool:
        """Check if the specified file is out of scope

        Args:
            source_type (TemplateSourceType): SQL template source type
            key (str): Query template file key

        Returns:
            bool: Return True if the specified file is out of scope
        """
        result = False
        for exclude in self._stairlight_config.get_exclude():
            if source_type == TemplateSourceType(
                exclude.TemplateSourceType
            ) and re.search(rf"{exclude.Regex}", key):
                result = True
                break
        return result
