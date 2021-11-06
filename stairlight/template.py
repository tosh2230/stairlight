import enum
from logging import getLogger
import pathlib
import re

from google.cloud import storage

logger = getLogger(__name__)


class SourceType(enum.Enum):
    FS = "fs"
    GCS = "gcs"
    S3 = "s3"

    def __str__(self):
        return self.name


class TemplateSource:
    def __init__(self, strl_config, map_config):
        self._strl_config = strl_config
        self._map_config = map_config

    def search(self):
        for source in self._strl_config.get("sources"):
            type = source.get("type")
            if type.casefold() == SourceType.FS.value:
                yield from self.search_fs(source)
            elif type.casefold() == SourceType.GCS.value:
                yield from self.search_gcs(source)
            elif type.casefold() == SourceType.S3.value:
                continue

    def search_fs(self, source):
        path_obj = pathlib.Path(source.get("path"))
        for p in path_obj.glob("**/*"):
            if (
                not re.fullmatch(rf'{source.get("regex")}', str(p))
            ) or self.is_excluded(str(p)):
                logger.debug(f"{str(p)} is skipped.")
                continue
            yield SQLTemplate(
                map_config=self._map_config,
                source_type=SourceType.FS,
                file_path=str(p),
            )

    def search_gcs(self, source):
        project = source.get("project")
        client = storage.Client(credentials=None, project=project)
        bucket = source.get("bucket")
        blobs = client.list_blobs(bucket)
        for blob in blobs:
            if (
                not re.fullmatch(rf'{source.get("regex")}', blob.name)
            ) or self.is_excluded(blob.name):
                logger.debug(f"{blob.name} is skipped.")
                continue
            yield SQLTemplate(
                map_config=self._map_config,
                source_type=SourceType.GCS,
                file_path=blob.name,
                project=project,
                bucket=bucket,
            )

    def is_excluded(self, template_file):
        result = False
        for exclude_file in self._strl_config.get("exclude"):
            if template_file.endswith(exclude_file):
                result = True
                break
        return result


class SQLTemplate:
    def __init__(self, map_config, source_type, file_path, bucket=None, project=None):
        self._map_config = map_config
        self.source_type = source_type
        self.file_path = file_path
        self.bucket = bucket
        self.project = project

    def get_param_list(self):
        param_list = []
        for mapping in self._map_config.get("mapping"):
            if self.file_path.endswith(mapping.get("file_suffix")):
                param_list.append(mapping.get("params"))
        return param_list

    def get_mapped_table(self, params):
        mapped_table = None
        for mapping in self._map_config.get("mapping"):
            if self.file_path.endswith(
                mapping.get("file_suffix")
            ) and params == mapping.get("params"):
                mapped_table = mapping.get("table")
                break
        return mapped_table

    def get_jinja_params(self):
        template_str = ""
        if self.source_type == SourceType.FS:
            template_str = self.get_template_str_fs()
        elif self.source_type == SourceType.GCS:
            template_str = self.get_template_str_gcs()
        elif self.source_type == SourceType.S3:
            pass

        jinja_expressions = "".join(
            re.findall("{{[^}]*}}", template_str, re.IGNORECASE)
        )
        return re.findall("[^{} ]+", jinja_expressions, re.IGNORECASE)

    def get_template_str_fs(self):
        template_str = ""
        with open(self.file_path) as f:
            template_str = f.read()
        return template_str

    def get_template_str_gcs(self):
        client = storage.Client(credentials=None, project=self.project)
        bucket = client.get_bucket(self.bucket)
        blob = bucket.blob(self.file_path)
        return blob.download_as_bytes().decode("utf-8")
