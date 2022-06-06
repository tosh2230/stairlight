import enum
import re
from abc import ABC
from logging import getLogger
from typing import Iterator, Optional

from jinja2 import BaseLoader, Environment
from jinja2.exceptions import UndefinedError

from ..key import MappingConfigKey, StairlightConfigKey


class TemplateSourceType(enum.Enum):
    """SQL template source type"""

    FILE = "File"
    GCS = "GCS"
    REDASH = "Redash"
    DBT = "dbt"

    def __str__(self):
        return self.name


class Template(ABC):
    """Base SQL template"""

    def __init__(
        self,
        mapping_config: dict,
        key: str,
        source_type: TemplateSourceType,
        bucket: Optional[str] = None,
        project: Optional[str] = None,
        default_table_prefix: Optional[str] = None,
    ):
        """SQL template

        Args:
            mapping_config (dict): Mapping configuration
            key (str): SQL file key
            source_type (SourceType): Source type
            bucket (Optional[str], optional):
                Bucket name where SQL file saved.Defaults to None.
            project (Optional[str], optional):
                Project name where SQL file saved.Defaults to None.
            default_table_prefix (Optional[str], optional):
                If project or dataset that configured table have are omitted,
                it will be complement this prefix. Defaults to None.
        """
        self._mapping_config = mapping_config
        self.key = key
        self.source_type = source_type
        self.bucket = bucket
        self.project = project
        self.default_table_prefix = default_table_prefix
        self.uri = ""

    def find_mapped_table_attributes(self) -> Iterator[dict]:
        """Get mapped tables as iterator

        Yields:
            Iterator[dict]: Mapped table attributes
        """
        for mapping in self._mapping_config.get(MappingConfigKey.MAPPING_SECTION):
            has_suffix = False
            if self.key and mapping.get(MappingConfigKey.File.FILE_SUFFIX):
                has_suffix = self.key.endswith(
                    mapping.get(MappingConfigKey.File.FILE_SUFFIX)
                )
            if has_suffix or self.uri == mapping.get(MappingConfigKey.Gcs.URI):
                for table_attributes in mapping.get(MappingConfigKey.TABLES):
                    yield table_attributes
                break

    def is_mapped(self) -> bool:
        """Check if the template is set to mapping configuration

        Returns:
            bool: Is set or not
        """
        result = False
        for _ in self.find_mapped_table_attributes():
            result = True
            break
        return result

    @staticmethod
    def detect_jinja_params(template_str: str) -> list:
        """Get jinja parameters

        Args:
            template_str (str): File string

        Returns:
            list: Jinja parameters
        """
        jinja_expressions = "".join(
            re.findall("{{[^}]*}}", template_str, re.IGNORECASE)
        )
        return re.findall("[^{} ]+", jinja_expressions, re.IGNORECASE)

    @staticmethod
    def render_by_base_loader(
        source_type: str, key: str, template_str: str, params: dict
    ) -> str:
        """Render query string from template string

        Args:
            source_type (str): source type
            key (str): key path
            template_str (str): template string
            params (dict): parameters

        Raises:
            RenderingTemplateException: class RenderingTemplateException

        Returns:
            str: rendered query string
        """
        try:
            jinja_template = Environment(loader=BaseLoader()).from_string(template_str)
            return jinja_template.render(params)
        except UndefinedError as undefined_error:
            raise RenderingTemplateException(
                (
                    f"{undefined_error.message}, "
                    f"source_type: {source_type}, "
                    f"key: {key}"
                )
            ) from None

    @staticmethod
    def ignore_params_from_template_str(
        template_str: str, ignore_params: "list[str]"
    ) -> str:
        """ignore parameters from template string

        Args:
            template_str (str): template string
            ignore_params (list[str]): ignore parameters

        Returns:
            str: replaced template string
        """
        if not ignore_params:
            ignore_params = []
        replaced_str = template_str
        for ignore_param in ignore_params:
            replaced_str = replaced_str.replace(
                "{{{{ {} }}}}".format(ignore_param), "ignored"
            )
        return replaced_str

    def get_uri(self) -> str:
        """Get uri"""
        return ""

    def get_template_str(self) -> str:
        """Get template strings that read from template source"""
        return ""

    def render(self, params: dict, ignore_params: "list[str]" = None) -> str:
        """Render SQL query string from a jinja template on Redash queries
        Args:
            params (dict): Jinja parameters
            ignore_params (list[str]): Ignore parameters
        Returns:
            str: SQL query string
        """
        template_str = self.get_template_str()
        replaced_template_str = self.ignore_params_from_template_str(
            template_str=template_str,
            ignore_params=ignore_params,
        )
        if params:
            results = self.render_by_base_loader(
                source_type=self.source_type,
                key=self.key,
                template_str=replaced_template_str,
                params=params,
            )
        else:
            results = replaced_template_str
        return results


class RenderingTemplateException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

    def __str__(self) -> str:
        return super().__str__()


class TemplateSource(ABC):
    """SQL template source"""

    logger = getLogger(__name__)

    def __init__(self, stairlight_config: dict, mapping_config: dict) -> None:
        """SQL template source

        Args:
            stairlight_config (dict): Stairlight configuration
            mapping_config (dict): Mapping configuration
        """
        self._stairlight_config = stairlight_config
        self._mapping_config = mapping_config

    def search_templates(self) -> Iterator[Template]:
        """Search SQL template files

        Yields:
            Iterator[SQLTemplate]: SQL template file attributes
        """
        pass

    def is_excluded(self, source_type: TemplateSourceType, key: str) -> bool:
        """Check if the specified file is out of scope

        Args:
            source_type (TemplateSourceType): SQL template source type
            key (str): SQL template file key

        Returns:
            bool: Return True if the specified file is out of scope
        """
        result = False
        exclude_list = self._stairlight_config.get(StairlightConfigKey.EXCLUDE_SECTION)
        if not exclude_list:
            return result
        for exclude in exclude_list:
            if source_type.value == exclude.get(
                StairlightConfigKey.TEMPLATE_SOURCE_TYPE
            ) and re.search(rf"{exclude.get(StairlightConfigKey.REGEX)}", key):
                result = True
                break
        return result
