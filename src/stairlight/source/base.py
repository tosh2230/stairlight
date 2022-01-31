import enum
import re
from logging import getLogger
from typing import Iterator, Optional

from jinja2 import BaseLoader, Environment

from .. import config_key


class TemplateSourceType(enum.Enum):
    """SQL template source type"""

    FILE = "File"
    GCS = "GCS"
    REDASH = "Redash"

    def __str__(self):
        return self.name


class Template:
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

    def get_mapped_table_attributes_iter(self) -> Iterator[dict]:
        """Get mapped tables as iterator

        Yields:
            Iterator[dict]: Mapped table attributes
        """
        for mapping in self._mapping_config.get(
            config_key.MAPPING_CONFIG_MAPPING_SECTION
        ):
            has_suffix = False
            if self.key and mapping.get(config_key.FILE_SUFFIX):
                has_suffix = self.key.endswith(mapping.get(config_key.FILE_SUFFIX))
            if has_suffix or self.uri == mapping.get(config_key.URI):
                for table_attributes in mapping.get(config_key.TABLES):
                    yield table_attributes
                break

    def is_mapped(self) -> bool:
        """Check if the template is set to mapping configuration

        Returns:
            bool: Is set or not
        """
        result = False
        for _ in self.get_mapped_table_attributes_iter():
            result = True
            break
        return result

    @staticmethod
    def get_jinja_params(template_str) -> list:
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
    def render_by_base_loader(template_str: str, params: dict) -> str:
        jinja_template = Environment(loader=BaseLoader()).from_string(template_str)
        return jinja_template.render(params)

    def get_uri(self) -> str:
        """Get uri"""
        return ""

    def get_template_str(self) -> str:
        """Get template strings that read from template source"""
        return ""

    def render(self) -> str:
        """Render SQL query string from a jinja template"""
        return ""


class TemplateSource:
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

    def search_templates_iter(self) -> Iterator[Template]:
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
        exclude_list = self._stairlight_config.get(
            config_key.STAIRLIGHT_CONFIG_EXCLUDE_SECTION
        )
        if not exclude_list:
            return result
        for exclude in exclude_list:
            if source_type.value == exclude.get(
                config_key.TEMPLATE_SOURCE_TYPE
            ) and re.search(rf"{exclude.get(config_key.REGEX)}", key):
                result = True
                break
        return result
