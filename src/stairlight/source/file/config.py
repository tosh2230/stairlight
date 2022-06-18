from dataclasses import dataclass, field
from typing import Any, List, OrderedDict

from ..config import MappingConfigMapping, StairlightConfigInclude
from ..template import TemplateSourceType as source_type


@dataclass
class StairlightConfigIncludeFile(StairlightConfigInclude):
    TemplateSourceType: str = source_type.FILE.value
    FileSystemPath: str = None
    Regex: str = None
    DefaultTablePrefix: str = None


@dataclass
class MappingConfigMappingFile(MappingConfigMapping):
    TemplateSourceType: str = source_type.FILE.value
    FileSuffix: str = None
    Tables: List[OrderedDict[str, Any]] = field(default_factory=list)
