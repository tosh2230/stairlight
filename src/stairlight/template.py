import enum
import os
import pathlib
import re
from logging import getLogger

from google.cloud import storage
from jinja2 import BaseLoader, Environment, FileSystemLoader

logger = getLogger(__name__)


class SourceType(enum.Enum):
    FS = "fs"
    GCS = "gcs"

    def __str__(self):
        return self.name


class TemplateSource:
    def __init__(self, strl_config, map_config):
        self._strl_config = strl_config
        self._map_config = map_config

    def search(self):
        for source in self._strl_config.get("include"):
            type = source.get("type")
            if type.casefold() == SourceType.FS.value:
                yield from self.search_fs(source)
            elif type.casefold() == SourceType.GCS.value:
                yield from self.search_gcs(source)

    def search_fs(self, source):
        source_type = SourceType.FS
        path_obj = pathlib.Path(source.get("path"))
        for p in path_obj.glob("**/*"):
            if (
                not re.fullmatch(rf'{source.get("regex")}', str(p))
            ) or self.is_excluded(source_type=source_type, file_path=str(p)):
                logger.debug(f"{str(p)} is skipped.")
                continue
            yield SQLTemplate(
                map_config=self._map_config,
                source_type=source_type,
                file_path=str(p),
                default_table_prefix=source.get("default_table_prefix"),
            )

    def search_gcs(self, source):
        source_type = SourceType.GCS
        project = source.get("project")
        client = storage.Client(credentials=None, project=project)
        bucket = source.get("bucket")
        blobs = client.list_blobs(bucket)
        for blob in blobs:
            if (
                not re.fullmatch(rf'{source.get("regex")}', blob.name)
            ) or self.is_excluded(source_type=source_type, file_path=blob.name):
                logger.debug(f"{blob.name} is skipped.")
                continue
            yield SQLTemplate(
                map_config=self._map_config,
                source_type=source_type,
                file_path=blob.name,
                project=project,
                bucket=bucket,
                default_table_prefix=source.get("default_table_prefix"),
            )

    def is_excluded(self, source_type, file_path):
        result = False
        exclude_list = self._strl_config.get("exclude")
        if not exclude_list:
            return result
        for exclude in exclude_list:
            if source_type.value == exclude.get("type") and re.search(
                rf'{exclude.get("regex")}', file_path
            ):
                result = True
                break
        return result


class SQLTemplate:
    def __init__(
        self,
        map_config,
        source_type,
        file_path,
        bucket=None,
        project=None,
        default_table_prefix=None,
    ):
        self._map_config = map_config
        self.source_type = source_type
        self.file_path = file_path
        self.bucket = bucket
        self.project = project
        self.default_table_prefix = default_table_prefix
        self.uri = self.set_uri()

    def set_uri(self):
        uri = ""
        if self.source_type == SourceType.FS:
            p = pathlib.Path(self.file_path)
            uri = str(p.resolve())
        elif self.source_type == SourceType.GCS:
            uri = f"gs://{self.bucket}/{self.file_path}"
        return uri

    def get_mapped_tables(self):
        for mapping in self._map_config.get("mapping"):
            is_suffix = False
            if mapping.get("file_suffix"):
                is_suffix = self.file_path.endswith(mapping.get("file_suffix"))
            if is_suffix or self.uri == mapping.get("uri"):
                for table in mapping.get("tables"):
                    yield table

    def get_param_list(self):
        param_list = []
        for table in self.get_mapped_tables():
            param_list.append(table.get("params"))
        return param_list

    def search_mapped_table(self, params):
        mapped_table = None
        for table in self.get_mapped_tables():
            if table.get("params") == params:
                mapped_table = table.get("table")
                break
        return mapped_table

    def get_jinja_params(self):
        template_str = ""
        if self.source_type == SourceType.FS:
            template_str = self.get_template_str_fs()
        elif self.source_type == SourceType.GCS:
            template_str = self.get_template_str_gcs()

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

    def render(self, params: dict):
        query_str = ""
        if self.source_type == SourceType.FS:
            query_str = self.render_fs(params)
        elif self.source_type == SourceType.GCS:
            query_str = self.render_gcs(params)
        return query_str

    def render_fs(self, params: dict):
        env = Environment(loader=FileSystemLoader(os.path.dirname(self.file_path)))
        jinja_template = env.get_template(os.path.basename(self.file_path))
        return jinja_template.render(params=params)

    def render_gcs(self, params: dict):
        template_str = self.get_template_str_gcs()
        jinja_template = Environment(loader=BaseLoader()).from_string(template_str)
        return jinja_template.render(params=params)
