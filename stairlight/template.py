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
        for p in path_obj.glob(source.get("pattern")):
            if self.is_excluded(str(p)):
                logger.debug(f"{str(p)} is skipped.")
                continue
            yield SQLTemplate(
                source_type=SourceType.FS,
                file_path=str(p),
                map_config=self._map_config,
            )

    def search_gcs(self, source):
        client = storage.Client(credentials=None, project=source.get("project"))
        blobs = client.list_blobs(
            source.get("bucket"), prefix=source.get("prefix"), delimiter="/"
        )
        for blob in blobs:
            if self.is_excluded(blob.name) or blob.name == source.get("prefix"):
                logger.debug(f"{blob.name} is skipped.")
                continue
            yield SQLTemplate(
                source_type=SourceType.GCS,
                file_path=blob.name,
                map_config=self._map_config,
            )

    def is_excluded(self, template_file):
        result = False
        for exclude_file in self._strl_config.get("exclude"):
            if template_file.endswith(exclude_file):
                result = True
                break
        return result


class SQLTemplate:
    def __init__(self, source_type, file_path, map_config):
        self.source_type = source_type
        self.file_path = file_path
        self._map_config = map_config

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
        jinja_params = []
        if self.source_type == SourceType.FS:
            with open(self.file_path) as f:
                template_str = f.read()
            jinja_expressions = "".join(
                re.findall("{{[^}]*}}", template_str, re.IGNORECASE)
            )
            jinja_params = re.findall("[^{} ]+", jinja_expressions, re.IGNORECASE)

        return jinja_params
