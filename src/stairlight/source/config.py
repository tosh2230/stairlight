from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Iterator, OrderedDict, Type

from .config_key import MapKey

logger = logging.getLogger()


class ConfigAttributeNotFoundException(Exception):
    def __init__(self, msg: str) -> None:
        self.msg = msg

    def __str__(self) -> str:
        return self.msg


@dataclass
class StairlightConfigInclude:
    TemplateSourceType: str


@dataclass
class StairlightConfigExclude:
    TemplateSourceType: str | None = None
    Regex: str | None = None


@dataclass
class StairlightConfigSettings:
    MappingPrefix: str | None = None


@dataclass
class StairlightConfig:
    Include: list[dict[str, Any]] = field(default_factory=list)
    Exclude: list[dict[str, Any]] = field(default_factory=list)
    Settings: OrderedDict = field(default_factory=OrderedDict)

    @staticmethod
    def select_config_include(source_type: str) -> Type[StairlightConfigInclude]:
        """Select a data class of include section by source type

        Args:
            source_type (str): Source type

        Returns:
            Type[StairlightConfigInclude]: Include section
        """
        from .template import TemplateSourceType

        config_include: Type[StairlightConfigInclude] = StairlightConfigInclude

        # Avoid to occur circular imports
        if source_type == TemplateSourceType.FILE.value:
            from .file.config import StairlightConfigIncludeFile

            config_include = StairlightConfigIncludeFile
        elif source_type == TemplateSourceType.GCS.value:
            from .gcs.config import StairlightConfigIncludeGcs

            config_include = StairlightConfigIncludeGcs
        elif source_type == TemplateSourceType.REDASH.value:
            from .redash.config import StairlightConfigIncludeRedash

            config_include = StairlightConfigIncludeRedash
        elif source_type == TemplateSourceType.DBT.value:
            from .dbt.config import StairlightConfigIncludeDbt

            config_include = StairlightConfigIncludeDbt
        elif source_type == TemplateSourceType.S3.value:
            from .s3.config import StairlightConfigIncludeS3

            config_include = StairlightConfigIncludeS3
        return config_include

    def get_include(self) -> Iterator[StairlightConfigInclude]:
        """Get attributes of a include section

        Yields:
            Iterator[StairlightConfigInclude]: Include section
        """
        for _include in self.Include:
            config = self.select_config_include(
                source_type=str(_include.get(MapKey.TEMPLATE_SOURCE_TYPE))
            )
            yield config(**_include)

    def get_exclude(self) -> Iterator[StairlightConfigExclude]:
        """Get attributes of a exclude section

        Yields:
            Iterator[StairlightConfigExclude]: Exclude section
        """
        for _exclude in self.Exclude:
            yield StairlightConfigExclude(**_exclude)


@dataclass
class MappingConfigGlobal:
    Parameters: dict[str, Any] | None = None


@dataclass
class MappingConfigMappingTable:
    TableName: str
    IgnoreParameters: list[str] | None = None
    Parameters: OrderedDict | None = None
    Labels: dict[str, Any] | None = None


@dataclass
class MappingConfigMapping:
    TemplateSourceType: str
    Tables: list[OrderedDict] = field(default_factory=list)

    def get_table(self) -> Iterator[MappingConfigMappingTable]:
        for _table in self.Tables:
            yield MappingConfigMappingTable(**_table)


@dataclass
class MappingConfigMetadata:
    TableName: str | None = None
    Labels: dict[str, Any] = field(default_factory=dict)


@dataclass
class MappingConfig:
    Global: OrderedDict | None = None
    Mapping: list[OrderedDict] = field(default_factory=list)
    Metadata: list[dict[str, Any]] | None = None

    def get_global(self) -> MappingConfigGlobal:
        """Get a global section

        Returns:
            MappingConfigGlobal: Global section
        """
        if self.Global:
            mapping_config_global = MappingConfigGlobal(**self.Global)
        else:
            mapping_config_global = MappingConfigGlobal()
        return mapping_config_global

    def get_mapping(self) -> Iterator[MappingConfigMapping]:
        """Get a mapping section

        Yields:
            Iterator[MappingConfigMapping]: Mapping section
        """
        for _mapping in self.Mapping:
            mapping_config = self.select_mapping_config(
                source_type=str(_mapping.get(MapKey.TEMPLATE_SOURCE_TYPE))
            )
            yield mapping_config(**_mapping)

    def get_metadata(self) -> Iterator[MappingConfigMetadata]:
        """Get a metadata section

        Yields:
            Iterator[MappingConfigMetadata]: Metadata section
        """
        for _metadata in self.Metadata:
            yield MappingConfigMetadata(**_metadata)

    @staticmethod
    def select_mapping_config(source_type: str) -> Type[MappingConfigMapping]:
        """Select a mapping data class from source type

        Args:
            source_type (str): Source type

        Returns:
            Type[MappingConfigMapping]: Mapping section
        """
        from .template import TemplateSourceType

        mapping_config: Type[MappingConfigMapping] = MappingConfigMapping

        # Avoid to occur circular imports
        if source_type == TemplateSourceType.FILE.value:
            from .file.config import MappingConfigMappingFile

            mapping_config = MappingConfigMappingFile
        elif source_type == TemplateSourceType.GCS.value:
            from .gcs.config import MappingConfigMappingGcs

            mapping_config = MappingConfigMappingGcs
        elif source_type == TemplateSourceType.REDASH.value:
            from .redash.config import MappingConfigMappingRedash

            mapping_config = MappingConfigMappingRedash
        elif source_type == TemplateSourceType.DBT.value:
            from .dbt.config import MappingConfigMappingDbt

            mapping_config = MappingConfigMappingDbt
        elif source_type == TemplateSourceType.S3.value:
            from .s3.config import MappingConfigMappingS3

            mapping_config = MappingConfigMappingS3
        return mapping_config
