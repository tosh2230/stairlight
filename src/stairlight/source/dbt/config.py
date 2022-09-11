from __future__ import annotations

from collections import OrderedDict as ordered_dict
from dataclasses import dataclass, field
from typing import Any, OrderedDict

from ..config import MappingConfigMapping, StairlightConfigInclude
from ..template import TemplateSourceType as source_type


@dataclass
class StairlightConfigIncludeDbt(StairlightConfigInclude):
    TemplateSourceType: str = source_type.DBT.value
    ProjectDir: str | None = None
    ProfilesDir: str | None = None
    Target: str | None = None
    Vars: OrderedDict[str, str] = field(default_factory=ordered_dict)


@dataclass
class MappingConfigMappingDbt(MappingConfigMapping):
    TemplateSourceType: str = source_type.DBT.value
    ProjectName: str | None = None
    FileSuffix: str | None = None
    Tables: list[OrderedDict[str, Any]] = field(default_factory=list)
