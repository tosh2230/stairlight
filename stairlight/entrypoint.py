import pathlib
import re

import yaml
from jinja2 import Environment, FileSystemLoader

from stairlight.query_parser import QueryParser


class StairLight:
    def __init__(self, config_file: str, template_dir: str, condition: str):
        self.template_dir = template_dir
        self.condition = condition
        self.__config = self.read_config(config_file)
        self._maps = {}
        self._undefined_files = []
        self._env = Environment(loader=FileSystemLoader(self.template_dir))
        self.create_maps()

    @property
    def maps(self):
        return self._maps

    @property
    def undefined_files(self):
        return self._undefined_files

    @staticmethod
    def read_config(config_file):
        config = {}
        if config_file and config_file.endswith((".yml", ".yaml")):
            with open(config_file) as file:
                config = yaml.load(file, Loader=yaml.SafeLoader)
        return config

    def create_maps(self):
        for template_file in self.search_template_file():
            if self.is_excluded(template_file):
                continue
            param_list = self.get_param_list(template_file)
            if param_list:
                for params in param_list:
                    self.remap(template_file=template_file, params=params)
            else:
                self.remap(template_file=template_file)

    def search_template_file(self):
        path_obj = pathlib.Path(self.template_dir)
        for p in path_obj.glob(self.condition):
            yield str(p)

    def is_excluded(self, sql_file):
        result = False
        for exclude_file in self.__config.get("exclude"):
            if sql_file.endswith(exclude_file):
                result = True
                break
        return result

    def get_param_list(self, template_file):
        params_list = []
        for template in self.__config.get("mapping"):
            if template_file.endswith(template.get("file")):
                params_list = template.get("params")
                break
        return params_list

    def remap(self, template_file: str, params: list = []):
        downstream_tables = self.get_mapped_tables(file=template_file)
        if not downstream_tables:
            self._undefined_files.append(template_file)
            return

        for downstream_table_name in downstream_tables:
            if downstream_table_name not in self._maps:
                self._maps[downstream_table_name] = {}

            query_str = self.render_query(template_file=template_file, params=params)
            query_parser = QueryParser(query_str)

            for upstream_table in query_parser.parse_query():
                upstream_table_name = upstream_table["table_name"]
                self._maps[downstream_table_name][upstream_table_name] = {
                    "file": template_file,
                    "line": upstream_table["line"],
                    "line_str": upstream_table["line_str"],
                }

    def render_query(self, template_file, params):
        template_file_name = re.sub(f"{self.template_dir}/", "", template_file)
        template = self._env.get_template(template_file_name)
        return template.render(params=params)

    def get_mapped_tables(self, file):
        mapped_tables = []
        mapping = self.__config.get("mapping")
        for pair in mapping:
            if file.endswith(pair["file"]):
                mapped_tables.append(pair["table"])
        return mapped_tables
