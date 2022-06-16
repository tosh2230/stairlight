import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, OrderedDict, Type

from .config_key import MapKey
from .template import TemplateSourceType

logger = logging.getLogger()


@dataclass
class StairlightConfig:
    Include: List[OrderedDict]
    Exclude: List[OrderedDict]
    Settings: OrderedDict


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
class MappingConfigGlobal:
    Parameters: Dict[str, Any]


@dataclass
class MappingConfigMappingTable:
    TableName: str
    IgnoreParameters: List[str] = field(default_factory=list)
    Parameters: OrderedDict = field(default_factory=OrderedDict)
    Labels: Dict[str, Any] = field(default_factory=OrderedDict)


@dataclass
class MappingConfigMapping:
    TemplateSourceType: str
    Tables: List[OrderedDict]

    def get_table(self) -> Iterator[MappingConfigMappingTable]:
        for _table in self.Tables:
            yield MappingConfigMappingTable(**_table)


@dataclass
class MappingConfigMetadata:
    TableName: str = None
    Labels: Dict[str, Any] = field(default_factory=OrderedDict)


@dataclass
class MappingConfig:
    Global: OrderedDict
    Mapping: List[OrderedDict]
    Metadata: List[OrderedDict]

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
        return mapping_config
