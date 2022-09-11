from __future__ import annotations

import json
import os
from importlib.util import find_spec
from logging import getLogger
from pathlib import Path
from typing import Any, OrderedDict, Type

from .config import MappingConfigMapping
from .config_key import GCS_URI_SCHEME, S3_URI_SCHEME
from .dbt.config import MappingConfigMappingDbt
from .file.config import MappingConfigMappingFile
from .gcs.config import MappingConfigMappingGcs
from .redash.config import MappingConfigMappingRedash
from .s3.config import MappingConfigMappingS3
from .template import Template, TemplateSource, TemplateSourceType

logger = getLogger(__name__)


def get_template_source_class(template_source_type: str) -> Type[TemplateSource]:
    """Get a template source class for dynamic importing

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
    elif template_source_type == TemplateSourceType.S3.value:
        if not find_spec("S3TemplateSource"):
            from .s3.template import S3TemplateSource
        template_source = S3TemplateSource
    return template_source


def get_default_table_name(template: Template) -> str:
    """Get a default table name

    Args:
        template (Template): Query template

    Returns:
        str: Default table name
    """
    default_table_name: str = ""
    if template.source_type == TemplateSourceType.REDASH:
        default_table_name = template.uri
    else:
        default_table_name = Path(template.key).stem
    return default_table_name


def collect_mapping_attributes(
    template: Template,
    tables: list[OrderedDict[str, Any]],
) -> MappingConfigMapping:
    """Collect attributes from a mapping section

    Args:
        template (Template): Query template
        tables (list[OrderedDict[str, Any]]): Tables

    Returns:
        MappingConfigMapping: Attributes of a mapping section
    """
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
    elif template.source_type == TemplateSourceType.S3:
        mapping = MappingConfigMappingS3(
            Uri=template.uri,
            Tables=tables,
        )

    return mapping


class SaveMapController:
    def __init__(self, save_file: str, mapped: dict[str, Any]) -> None:
        self.save_file = save_file
        self._mapped = mapped

    def save(self) -> None:
        """Save mapped results"""
        if self.save_file.startswith(GCS_URI_SCHEME):
            self._save_map_gcs()
        elif self.save_file.startswith(S3_URI_SCHEME):
            self._save_map_s3()
        else:
            self._save_map_file()

    def _save_map_file(self) -> None:
        """Save mapped results to file system"""
        with open(self.save_file, "w") as f:
            json.dump(self._mapped, f, indent=2)

    def _save_map_gcs(self) -> None:
        """Save mapped results to Google Cloud Storage"""
        from google.cloud.storage import Blob

        from .gcs.map import get_gcs_blob

        blob: Blob = get_gcs_blob(uri=self.save_file)
        blob.upload_from_string(
            data=json.dumps(obj=self._mapped, indent=2),
            content_type="application/json",
        )

    def _save_map_s3(self) -> None:
        """Save mapped results to Amazon S3"""
        from mypy_boto3_s3.service_resource import Object

        from .s3.map import get_s3_object

        _object: Object = get_s3_object(uri=self.save_file)
        _ = _object.put(Body=json.dumps(obj=self._mapped, indent=2))


class LoadMapController:
    def __init__(self, load_file: str) -> None:
        self.load_file = load_file

    def load(self) -> dict:
        """Load mapped results

        Returns:
            dict: Loaded map
        """
        loaded_map = {}
        if self.load_file.startswith(GCS_URI_SCHEME):
            loaded_map = self._load_map_gcs()
        elif self.load_file.startswith(S3_URI_SCHEME):
            loaded_map = self._load_map_s3()
        else:
            loaded_map = self._load_map_file()
        return loaded_map

    def _load_map_file(self) -> dict:
        """Load mapped results from file system"""
        if not os.path.exists(self.load_file):
            logger.error(f"{self.load_file} is not found.")
            exit()
        with open(self.load_file) as f:
            return json.load(f)

    def _load_map_gcs(self) -> dict:
        """Load mapped results from Google Cloud Storage"""
        from google.cloud.storage import Blob

        from .gcs.map import get_gcs_blob

        blob: Blob = get_gcs_blob(uri=self.load_file)
        if not blob.exists():
            logger.error(f"{self.load_file} is not found.")
            exit()
        return json.loads(blob.download_as_string())

    def _load_map_s3(self) -> dict:
        """Load mapped results from Amazon S3"""
        from botocore.response import StreamingBody
        from mypy_boto3_s3.service_resource import Object
        from mypy_boto3_s3.type_defs import GetObjectOutputTypeDef

        from .s3.map import get_s3_object

        _object: Object = get_s3_object(uri=self.load_file)
        object_output: GetObjectOutputTypeDef = _object.get()
        body: StreamingBody = object_output["Body"]
        if not body:
            logger.error(f"{self.load_file} is not found.")
            exit()
        return json.loads(body.read().decode("utf-8"))
