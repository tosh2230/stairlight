import json
import os
from importlib.util import find_spec
from logging import getLogger
from pathlib import Path
from typing import Any, Dict, List, Type

from .config import MappingConfigMapping
from .file.config import MappingConfigMappingFile
from .gcs.config import MappingConfigMappingGcs
from .redash.config import MappingConfigMappingRedash
from .dbt.config import MappingConfigMappingDbt
from .template import Template, TemplateSource, TemplateSourceType

GCS_URI_SCHEME = "gs://"

logger = getLogger(__name__)


def get_template_source_class(template_source_type: str) -> Type[TemplateSource]:
    """Get template source class

    Args:
        template_source_type (str): Template source type

    Returns:
        TemplateSource: Template source class
    """
    template_source: Type[TemplateSource]
    if template_source_type == TemplateSourceType.FILE.value:
        if not find_spec("FileTemplateSource"):
            from .file.template import FileTemplateSource
        template_source = FileTemplateSource
    elif template_source_type == TemplateSourceType.GCS.value:
        if not find_spec("GcsTemplateSource"):
            from .gcs.template import GcsTemplateSource
        template_source = GcsTemplateSource
    elif template_source_type == TemplateSourceType.REDASH.value:
        if not find_spec("RedashTemplateSource"):
            from .redash.template import RedashTemplateSource
        template_source = RedashTemplateSource
    elif template_source_type == TemplateSourceType.DBT.value:
        if not find_spec("DbtTemplateSource"):
            from .dbt.template import DbtTemplateSource
        template_source = DbtTemplateSource
    return template_source


def get_default_table_name(template: Template) -> str:
    default_table_name: str = ""
    if template.source_type == TemplateSourceType.REDASH:
        default_table_name = template.uri
    else:
        default_table_name = Path(template.key).stem
    return default_table_name


def collect_mapping_attributes(
    template: Template,
    tables: List[Dict[str, Any]],
) -> MappingConfigMapping:
    mapping: MappingConfigMapping

    if template.source_type == TemplateSourceType.FILE:
        mapping = MappingConfigMappingFile(
            FileSuffix=template.key,
            Tables=tables,
        )
    elif template.source_type == TemplateSourceType.GCS:
        mapping = MappingConfigMappingGcs(
            Uri=template.uri,
            Tables=tables,
        )
    elif template.source_type == TemplateSourceType.REDASH:
        mapping = MappingConfigMappingRedash(
            QueryId=template.query_id,
            DataSourceName=template.data_source_name,
            Tables=tables,
        )
    elif template.source_type == TemplateSourceType.DBT:
        mapping = MappingConfigMappingDbt(
            ProjectName=template.project_name,
            FileSuffix=template.key,
            Tables=tables,
        )

    return mapping


class SaveMapController:
    def __init__(self, save_file: str, mapped: Dict[str, Any]) -> None:
        self.save_file = save_file
        self._mapped = mapped

    def save(self) -> None:
        if self.save_file.startswith(GCS_URI_SCHEME):
            self._save_map_gcs()
        else:
            self._save_map_fs()

    def _save_map_fs(self) -> None:
        """Save mapped results to file system"""
        with open(self.save_file, "w") as f:
            json.dump(self._mapped, f, indent=2)

    def _save_map_gcs(self) -> None:
        """Save mapped results to Google Cloud Storage"""
        from .gcs.template import get_gcs_blob

        blob = get_gcs_blob(self.save_file)
        blob.upload_from_string(
            data=json.dumps(obj=self._mapped, indent=2),
            content_type="application/json",
        )


class LoadMapController:
    def __init__(self, load_file: str) -> None:
        self.load_file = load_file

    def load(self) -> dict:
        loaded_map = {}
        if self.load_file.startswith(GCS_URI_SCHEME):
            loaded_map = self._load_map_gcs()
        else:
            loaded_map = self._load_map_fs()
        return loaded_map

    def _load_map_fs(self) -> dict:
        """Load mapped results from file system"""
        if not os.path.exists(self.load_file):
            logger.error(f"{self.load_file} is not found.")
            exit()
        with open(self.load_file) as f:
            return json.load(f)

    def _load_map_gcs(self) -> dict:
        """Load mapped results from Google Cloud Storage"""
        from .gcs.template import get_gcs_blob

        blob = get_gcs_blob(self.load_file)
        if not blob.exists():
            logger.error(f"{self.load_file} is not found.")
            exit()
        return json.loads(blob.download_as_string())
