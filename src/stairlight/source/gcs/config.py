from dataclasses import dataclass

from ..config import StairlightConfigInclude
from ..template import TemplateSourceType as source_type


@dataclass
class StairlightConfigIncludeGcs(StairlightConfigInclude):
    TemplateSourceType: str = source_type.GCS.value
    ProjectId: str = None
    BucketName: str = None
    Regex: str = None
    DefaultTablePrefix: str = None
