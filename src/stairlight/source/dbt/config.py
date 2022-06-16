from collections import OrderedDict as ordered_dict
from dataclasses import dataclass, field
from typing import Any, Dict, List, OrderedDict

from ..config import StairlightConfigInclude, MappingConfigMapping
from ..template import TemplateSourceType as source_type


@dataclass
class StairlightConfigIncludeDbt(StairlightConfigInclude):
    TemplateSourceType: str = source_type.DBT.value
    ProjectDir: str = None
    ProfilesDir: str = None
    Target: str = None
    Vars: OrderedDict[str, str] = field(default_factory=ordered_dict)


@dataclass
class MappingConfigMappingDbt(MappingConfigMapping):
    TemplateSourceType: str = source_type.DBT.value
    ProjectName: str = None
    FileSuffix: str = None
    Tables: List[Dict[str, Any]] = field(default_factory=list)
