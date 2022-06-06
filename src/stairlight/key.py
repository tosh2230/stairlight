from abc import ABC
from typing import Any


class Key(ABC):
    def __setattr__(self, __name: str, __value: Any) -> None:
        if __name in self.__dict__:
            raise TypeError()
        else:
            self.__setattr__(__name=__name, __value=__value)


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
        PROJECT_ID = "FileSystemPath"
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
