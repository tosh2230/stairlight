from dataclasses import dataclass, field
from typing import Any, List, OrderedDict

from ..config import MappingConfigMapping, StairlightConfigInclude
from ..template import TemplateSourceType as source_type


@dataclass
class StairlightConfigIncludeS3(StairlightConfigInclude):
    TemplateSourceType: str = source_type.S3.value
    ProjectId: str = None
    BucketName: str = None
    Regex: str = None
    DefaultTablePrefix: str = None


@dataclass
class MappingConfigMappingS3(MappingConfigMapping):
    TemplateSourceType: str = source_type.S3.value
    Uri: str = None
    Tables: List[OrderedDict[str, Any]] = field(default_factory=list)
