from stairlight.query import Query
from stairlight.template import SourceType, SQLTemplate, TemplateSource


class Map:
    def __init__(self, strl_config, map_config, maps={}) -> None:
        self.maps = maps
        self.undefined_files = []
        self.template_source = TemplateSource(
            strl_config=strl_config, map_config=map_config
        )

    def create(self):
        for sql_template in self.template_source.search():
            param_list = sql_template.get_param_list()
            if param_list:
                for params in param_list:
                    self._remap(sql_template=sql_template, params=params)
            else:
                self._remap(sql_template=sql_template)

    def _remap(self, sql_template: SQLTemplate, params: dict = {}):
        downstream_table = sql_template.search_mapped_table(params=params)

        # Grep jinja template variables to suggest new configurations
        if not downstream_table:
            self.undefined_files.append(
                {
                    "sql_template": sql_template,
                    "params": sql_template.get_jinja_params(),
                }
            )
            return

        query_str = sql_template.render(params=params)
        query = Query(query_str=query_str)

        if downstream_table not in self.maps:
            self.maps[downstream_table] = {}

        for upstream_table_attributes in query.parse_upstream():
            upstream_table_name = upstream_table_attributes["table_name"]
            values = {
                "type": sql_template.source_type.value,
                "file": sql_template.file_path,
                "uri": sql_template.uri,
                "line": upstream_table_attributes["line"],
                "line_str": upstream_table_attributes["line_str"],
            }
            if sql_template.source_type == SourceType.GCS:
                values["bucket"] = sql_template.bucket
            self.maps[downstream_table][upstream_table_name] = values
