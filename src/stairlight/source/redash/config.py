from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, OrderedDict

from ..config import MappingConfigMapping, StairlightConfigInclude
from ..template import TemplateSourceType as source_type


@dataclass
class StairlightConfigIncludeRedash(StairlightConfigInclude):
    TemplateSourceType: str = source_type.REDASH.value
    DatabaseUrlEnvironmentVariable: str = "REDASH_DATABASE_URL"
    DataSourceName: str | None = None
    QueryIds: List[int] = field(default_factory=list)


@dataclass
class MappingConfigMappingRedash(MappingConfigMapping):
    TemplateSourceType: str = source_type.REDASH.value
    QueryId: int | None = None
    DataSourceName: str | None = None
    Tables: List[OrderedDict[str, Any]] = field(default_factory=list)
