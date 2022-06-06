from abc import ABC
from typing import Any


class Key(ABC):
    def __setattr__(self, __name: str, __value: Any) -> None:
        if __name in self.__dict__:
            raise TypeError()
        else:
            self.__setattr__(__name=__name, __value=__value)


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
