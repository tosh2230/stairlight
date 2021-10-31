from stairlight.config import MAP_CONFIG, read_config
from stairlight.query import Query
from stairlight.template import Template


class Map:
    def __init__(self) -> None:
        self._map_config = read_config(MAP_CONFIG)
        self.maps = {}
        self.undefined_files = []

    def create(self):
        template = Template()
        for template_file in template.search():
            param_list = self.get_param_list(template_file)
            if param_list:
                for params in param_list:
                    self.remap(template_file=template_file, params=params)
            else:
                self.remap(template_file=template_file)

    def get_param_list(self, template_file):
        params_list = []
        for template in self._map_config.get("mapping"):
            if template_file.endswith(template.get("file_suffix")):
                params_list = template.get("params")
                break
        return params_list

    def remap(self, template_file: str, params: list = []):
        downstream_tables = self.get_mapped_tables(file=template_file)
        if not downstream_tables:
            self.undefined_files.append(template_file)
            return

        query = Query.render(template_file=template_file, params=params)

        for downstream_table_name in downstream_tables:
            if downstream_table_name not in self.maps:
                self.maps[downstream_table_name] = {}

            for upstream_table in query.parse():
                upstream_table_name = upstream_table["table_name"]
                self.maps[downstream_table_name][upstream_table_name] = {
                    "file": template_file,
                    "line": upstream_table["line"],
                    "line_str": upstream_table["line_str"],
                }

    def get_mapped_tables(self, file):
        mapped_tables = []
        mapping = self._map_config.get("mapping")
        for pair in mapping:
            if file.endswith(pair["file_suffix"]):
                mapped_tables.append(pair["table"])
        return mapped_tables
