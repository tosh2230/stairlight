from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, OrderedDict

from ..config import MappingConfigMapping, StairlightConfigInclude
from ..template import TemplateSourceType as source_type


@dataclass
class StairlightConfigIncludeFile(StairlightConfigInclude):
    TemplateSourceType: str = source_type.FILE.value
    FileSystemPath: str | None = None
    Regex: str | None = None
    DefaultTablePrefix: str | None = None


@dataclass
class MappingConfigMappingFile(MappingConfigMapping):
    TemplateSourceType: str = source_type.FILE.value
    FileSuffix: str | None = None
    Tables: list[OrderedDict[str, Any]] = field(default_factory=list)
