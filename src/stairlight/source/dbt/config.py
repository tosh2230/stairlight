from collections import OrderedDict as ordered_dict
from dataclasses import dataclass, field
from typing import OrderedDict

from ..config import StairlightConfigInclude
from ..template import TemplateSourceType as source_type


@dataclass
class StairlightConfigIncludeDbt(StairlightConfigInclude):
    TemplateSourceType: str = source_type.DBT.value
    ProjectDir: str = None
    ProfilesDir: str = None
    Target: str = None
    Vars: OrderedDict[str, str] = field(default_factory=ordered_dict)
