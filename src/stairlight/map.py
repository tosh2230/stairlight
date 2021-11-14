from .query import Query
from .template import SourceType, SQLTemplate, TemplateSource


class Map:
    def __init__(self, strl_config, map_config, mapped={}) -> None:
        self.mapped = mapped
        self.unmapped = []
        self._template_source = TemplateSource(
            strl_config=strl_config, map_config=map_config
        )

    def collect_undefined(self, sql_template):
        self.unmapped.append(
            {
                "sql_template": sql_template,
                "params": sql_template.get_jinja_params(),
            }
        )

    def write_blank(self):
        for sql_template in self._template_source.search():
            self.collect_undefined(sql_template)

    def write(self):
        for sql_template in self._template_source.search():
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
            self.collect_undefined(sql_template)
            return

        query_str = sql_template.render(params=params)
        query = Query(
            query_str=query_str,
            default_table_prefix=sql_template.default_table_prefix,
        )

        if downstream_table not in self.mapped:
            self.mapped[downstream_table] = {}

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
            self.mapped[downstream_table][upstream_table_name] = values
