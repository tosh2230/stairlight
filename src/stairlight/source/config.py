import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, OrderedDict, Type

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
    TemplateSourceType: str = None
    Regex: str = None


@dataclass
class StairlightConfigSettings:
    MappingPrefix: str = None


@dataclass
class StairlightConfig:
    Include: List[Dict[str, Any]] = field(default_factory=list)
    Exclude: List[Dict[str, Any]] = field(default_factory=list)
    Settings: OrderedDict = field(default_factory=OrderedDict)

    @staticmethod
    def select_config_include(source_type: str) -> Type[StairlightConfigInclude]:
        from .template import TemplateSourceType

        config_include: Type[StairlightConfigInclude] = None

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
        for _include in self.Include:
            config = self.select_config_include(
                source_type=_include.get(MapKey.TEMPLATE_SOURCE_TYPE)
            )
            yield config(**_include)

    def get_exclude(self) -> Iterator[StairlightConfigExclude]:
        for _exclude in self.Exclude:
            yield StairlightConfigExclude(**_exclude)


@dataclass
class MappingConfigGlobal:
    Parameters: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MappingConfigMappingTable:
    TableName: str
    IgnoreParameters: List[str] = field(default_factory=list)
    Parameters: OrderedDict = field(default_factory=OrderedDict)
    Labels: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MappingConfigMapping:
    TemplateSourceType: str
    Tables: List[OrderedDict] = field(default_factory=list)

    def get_table(self) -> Iterator[MappingConfigMappingTable]:
        for _table in self.Tables:
            yield MappingConfigMappingTable(**_table)


@dataclass
class MappingConfigMetadata:
    TableName: str = None
    Labels: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MappingConfig:
    Global: OrderedDict = field(default_factory=OrderedDict)
    Mapping: List[OrderedDict] = field(default_factory=list)
    Metadata: List[Dict[str, Any]] = field(default_factory=list)

    def get_global(self) -> MappingConfigGlobal:
        return MappingConfigGlobal(**self.Global)

    def get_mapping(self) -> Iterator[MappingConfigMapping]:
        for _mapping in self.Mapping:
            mapping_config = self.select_mapping_config(
                source_type=_mapping.get(MapKey.TEMPLATE_SOURCE_TYPE)
            )
            yield mapping_config(**_mapping)

    def get_metadata(self) -> Iterator[MappingConfigMetadata]:
        for _metadata in self.Metadata:
            yield MappingConfigMetadata(**_metadata)

    @staticmethod
    def select_mapping_config(source_type: str) -> Type[MappingConfigMapping]:
        from .template import TemplateSourceType

        mapping_config: Type[MappingConfigMapping] = None

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
