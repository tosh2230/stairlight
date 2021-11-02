import stairlight.config as config
from stairlight.query import Query
from stairlight.template import Template, get_variables


class Map:
    def __init__(self, maps={}) -> None:
        self._map_config = config.read(config.MAP_CONFIG)
        self.maps = maps
        self.undefined_files = []
        self.template = Template()

    def create(self):
        for template_file in self.template.search():
            param_list = self._get_params(template_file)
            if param_list:
                for params in param_list:
                    self._remap(template_file=template_file, params=params)
            else:
                self._remap(template_file=template_file)

    def _get_params(self, template_file):
        param_list = []
        for template in self._map_config.get("mapping"):
            if template_file.endswith(template.get("file_suffix")):
                param_list.append(template.get("params"))
        return param_list

    def _remap(self, template_file: str, params: dict = {}):
        downstream_table = self._get_table(template_file=template_file, params=params)

        # Grep jinja template variables to add a new configuration
        if not downstream_table:
            self.undefined_files.append(
                {
                    "template_file": template_file,
                    "undefined_variables": get_variables(template_file),
                }
            )
            return

        query = Query.render(template_file=template_file, params=params)

        if downstream_table not in self.maps:
            self.maps[downstream_table] = {}

        for upstream_table in query.parse():
            upstream_table_name = upstream_table["table_name"]
            self.maps[downstream_table][upstream_table_name] = {
                "file": template_file,
                "line": upstream_table["line"],
                "line_str": upstream_table["line_str"],
            }

    def _get_table(self, template_file, params):
        table = None
        for template in self._map_config.get("mapping"):
            if template_file.endswith(
                template.get("file_suffix")
            ) and params == template.get("params"):
                table = template.get("table")
                break
        return table
