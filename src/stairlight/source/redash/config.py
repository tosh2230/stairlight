from dataclasses import dataclass, field
from typing import List

from ..config import StairlightConfigInclude
from ..template import TemplateSourceType as source_type


@dataclass
class StairlightConfigIncludeRedash(StairlightConfigInclude):
    TemplateSourceType: str = source_type.REDASH.value
    DatabaseUrlEnvironmentVariable: str = "REDASH_DATABASE_URL"
    DataSourceName: str = None
    QueryIds: List[int] = field(default_factory=list)
