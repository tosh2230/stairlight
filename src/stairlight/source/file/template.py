from __future__ import annotations

import pathlib
import re
from typing import Iterator

from ..config import ConfigAttributeNotFoundException, MappingConfig, StairlightConfig
from ..template import Template, TemplateSource, TemplateSourceType
from .config import StairlightConfigIncludeFile


class FileTemplate(Template):
    def __init__(
        self,
        mapping_config: MappingConfig,
        key: str,
        default_table_prefix: str | None = None,
    ):
        super().__init__(
            mapping_config=mapping_config,
            key=key,
            source_type=TemplateSourceType.FILE,
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


class FileTemplateSource(TemplateSource):
    def __init__(
        self,
        stairlight_config: StairlightConfig,
        mapping_config: MappingConfig,
        include: StairlightConfigIncludeFile,
    ) -> None:
        super().__init__(
            stairlight_config=stairlight_config,
            mapping_config=mapping_config,
        )
        self._include = include

    def search_templates(self) -> Iterator[Template]:
        """Search SQL template files from local file system

        Yields:
            Iterator[Template]: SQL template file attributes
        """
        if not self._include.FileSystemPath:
            raise ConfigAttributeNotFoundException(
                f"FileSystemPath is not found. {self._include}"
            )

        path_obj = pathlib.Path(self._include.FileSystemPath)
        for p in path_obj.glob("**/*"):
            if self.is_skipped(p=p):
                self.logger.debug(f"{str(p)} is skipped.")
                continue

            yield FileTemplate(
                mapping_config=self._mapping_config,
                key=str(p),
                default_table_prefix=self._include.DefaultTablePrefix,
            )

    def is_skipped(self, p: pathlib.Path):
        """Check the target path is skipped or not

        Args:
            p (pathlib.Path): Path

        Returns:
            _type_: Is skipped or not
        """
        return (
            p.is_dir()
            or not re.fullmatch(rf"{self._include.Regex}", str(p))
            or self.is_excluded(
                source_type=TemplateSourceType(self._include.TemplateSourceType),
                key=str(p),
            )
        )
