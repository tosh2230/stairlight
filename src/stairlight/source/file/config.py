from dataclasses import dataclass

from ..config import StairlightConfigInclude
from ..template import TemplateSourceType as source_type


@dataclass
class StairlightConfigIncludeFile(StairlightConfigInclude):
    TemplateSourceType: str = source_type.FILE.value
    FileSystemPath: str = None
    Regex: str = None
    DefaultTablePrefix: str = None
