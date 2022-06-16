import logging
from abc import ABC
from dataclasses import dataclass, field
from typing import Any, Dict, List, OrderedDict

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
class MappingConfig:
    Global: OrderedDict
    Mapping: List[OrderedDict]
    Metadata: List[OrderedDict]


@dataclass
class MappingConfigGlobal:
    Parameters: Dict[str, Any]


@dataclass
class MappingConfigMapping:
    TemplateSourceType: str


@dataclass
class MappingConfigMappingTable:
    TableName: str
    IgnoreParameters: List[str]
    Parameters: OrderedDict
    Labels: OrderedDict


@dataclass
class MappingConfigMetadata:
    TableName: str = None
    Labels: OrderedDict = field(default_factory=OrderedDict)


class Key(ABC):
    def __init__(self) -> None:
        super().__init__()

    def __setattr__(self, name: str, value: Any) -> None:
        if name in self.__dict__:
            raise TypeError()
        else:
            self.__setattr__(name=name, value=value)


class StairlightConfigKey(Key):
    INCLUDE_SECTION = "Include"
    EXCLUDE_SECTION = "Exclude"
    SETTING_SECTION = "Settings"

    TEMPLATE_SOURCE_TYPE = "TemplateSourceType"
    DEFAULT_TABLE_PREFIX = "DefaultTablePrefix"
    REGEX = "Regex"

    MAPPING_PREFIX = "MappingPrefix"

    class File(Key):
        FILE_SYSTEM_PATH = "FileSystemPath"

    class Gcs(Key):
        PROJECT_ID = "ProjectId"
        BUCKET_NAME = "BucketName"

    class Redash(Key):
        DATABASE_URL_ENV_VAR = "DatabaseUrlEnvironmentVariable"
        DATA_SOURCE_NAME = "DataSourceName"
        QUERY_IDS = "QueryIds"

    class Dbt(Key):
        PROJECT_DIR = "ProjectDir"
        PROFILES_DIR = "ProfilesDir"
        TARGET = "Target"
        VARS = "Vars"


class MappingConfigKey(Key):
    GLOBAL_SECTION = "Global"
    MAPPING_SECTION = "Mapping"
    METADATA_SECTION = "Metadata"

    TEMPLATE_SOURCE_TYPE = "TemplateSourceType"
    TABLES = "Tables"
    TABLE_NAME = "TableName"
    IGNORE_PARAMETERS = "IgnoreParameters"
    PARAMETERS = "Parameters"
    LABELS = "Labels"

    class File(Key):
        FILE_SUFFIX = "FileSuffix"

    class Gcs(Key):
        URI = "Uri"
        BUCKET_NAME = "BucketName"

    class Redash(Key):
        QUERY_ID = "QueryId"
        DATA_SOURCE_NAME = "DataSourceName"

    class Dbt(Key):
        FILE_SUFFIX = "FileSuffix"
        PROJECT_NAME = "ProjectName"


class DbtProjectKey(Key):
    PROJECT_NAME = "name"
    MODEL_PATHS = "model-paths"
    TARGET_PATH = "target-path"
    PROFILE = "Profile"


class MapKey(Key):
    TABLE_NAME = "TableName"
    TEMPLATE_SOURCE_TYPE = "TemplateSourceType"
    KEY = "Key"
    URI = "Uri"
    LINES = "Lines"
    LINE_NUMBER = "LineNumber"
    LINE_STRING = "LineString"
    BUCKET_NAME = "BucketName"
    LABELS = "Labels"
    DATA_SOURCE_NAME = "DataSourceName"

    TEMPLATE = "Template"
    PARAMETERS = "Parameters"


class ConfigKeyNotFoundException(Exception):
    def __init__(self, msg: str) -> None:
        self.msg = msg

    def __str__(self) -> str:
        return self.msg


def get_config_value(
    key: str,
    target: Dict[Any, Any],
    fail_if_not_found: bool = False,
    enable_logging: bool = False,
) -> Any:
    value = target.get(key)
    if not value:
        msg = f"{key} is not found in the configuration: {target}"
        if fail_if_not_found:
            raise ConfigKeyNotFoundException(msg=msg)
        if enable_logging:
            logger.warning(msg=msg)
    return value
