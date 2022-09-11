from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, OrderedDict

from ..config import MappingConfigMapping, StairlightConfigInclude
from ..template import TemplateSourceType as source_type


@dataclass
class StairlightConfigIncludeS3(StairlightConfigInclude):
    TemplateSourceType: str = source_type.S3.value
    ProjectId: str | None = None
    BucketName: str | None = None
    Regex: str | None = None
    DefaultTablePrefix: str | None = None


@dataclass
class MappingConfigMappingS3(MappingConfigMapping):
    TemplateSourceType: str = source_type.S3.value
    Uri: str | None = None
    Tables: list[OrderedDict[str, Any]] = field(default_factory=list)
