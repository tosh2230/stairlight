from .query import Query
from .template import SourceType, SQLTemplate, TemplateSource


class Map:
    """Manages functions related to dependency map objects"""

    def __init__(self, strl_config: dict, map_config: dict, mapped: dict = {}) -> None:
        """Manages functions related to dependency map objects

        Args:
            strl_config (dict): Stairlight configuration
            map_config (dict): Mapping configuration
            mapped (dict, optional):
                Mapped file attributes when a mapping configuration file loaded.
                Defaults to {}.
        """
        self.mapped = mapped
        self.unmapped = []
        self._template_source = TemplateSource(
            strl_config=strl_config, map_config=map_config
        )

    def collect_undefined(self, sql_template: SQLTemplate) -> None:
        """Create a list of unmapped file attributes

        Args:
            sql_template (SQLTemplate): SQL template
        """
        self.unmapped.append(
            {
                "sql_template": sql_template,
                "params": sql_template.get_jinja_params(),
            }
        )

    def write_blank(self) -> None:
        """Create a list of unmapped file attributes, if no mapping configuration set"""
        for sql_template in self._template_source.search():
            self.collect_undefined(sql_template)

    def write(self) -> None:
        """Write a dependency map"""
        for sql_template in self._template_source.search():
            param_list = sql_template.get_param_list()
            if param_list:
                for params in param_list:
                    self._remap(sql_template=sql_template, params=params)
            else:
                self._remap(sql_template=sql_template)

    def _remap(self, sql_template: SQLTemplate, params: dict = {}) -> None:
        """Remap a dependency map

        Args:
            sql_template (SQLTemplate): SQL template
            params (dict, optional): Jinja parameters. Defaults to {}.
        """
        downstairs = sql_template.search_mapped_table(params=params)

        # Grep jinja template variables to suggest new configurations
        if not downstairs:
            self.collect_undefined(sql_template)
            return

        query_str = sql_template.render(params=params)
        query = Query(
            query_str=query_str,
            default_table_prefix=sql_template.default_table_prefix,
        )

        if downstairs not in self.mapped:
            self.mapped[downstairs] = {}

        for upstairs_attributes in query.parse_upstairs():
            upstairs = upstairs_attributes["table_name"]
            values = {
                "type": sql_template.source_type.value,
                "file": sql_template.file_path,
                "uri": sql_template.uri,
                "line": upstairs_attributes["line"],
                "line_str": upstairs_attributes["line_str"],
            }
            if sql_template.source_type == SourceType.GCS:
                values["bucket"] = sql_template.bucket
            self.mapped[downstairs][upstairs] = values
