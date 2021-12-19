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
        self.map_config = map_config
        self._template_source = TemplateSource(
            strl_config=strl_config, map_config=map_config
        )

    def add_unmapped(self, sql_template: SQLTemplate) -> None:
        """add to the list of unmapped files

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
            self.add_unmapped(sql_template)

    def write(self) -> None:
        """Write a dependency map"""
        for sql_template in self._template_source.search():
            if sql_template.is_mapped():
                for table_attributes in sql_template.get_mapped_tables():
                    self._remap(
                        sql_template=sql_template, table_attributes=table_attributes
                    )
            else:
                self.add_unmapped(sql_template)

    def _remap(self, sql_template: SQLTemplate, table_attributes: dict) -> None:
        """Remap a dependency map

        Args:
            sql_template (SQLTemplate): SQL template
            table_attributes (dict): Table attributes from mapping configuration
        """
        query_str = sql_template.render(params=table_attributes.get("params"))
        query = Query(
            query_str=query_str,
            default_table_prefix=sql_template.default_table_prefix,
        )

        downstairs = table_attributes.get("table")
        mapping_labels = table_attributes.get("labels")
        metadata = self.map_config.get("metadata")

        if downstairs not in self.mapped:
            self.mapped[downstairs] = {}

        for upstairs_attributes in query.parse_upstairs():
            upstairs = upstairs_attributes["table_name"]

            if not self.mapped[downstairs].get(upstairs):
                values = {
                    "type": sql_template.source_type.value,
                    "file": sql_template.file_path,
                    "uri": sql_template.uri,
                    "lines": [],
                }
                if sql_template.source_type == SourceType.GCS:
                    values["bucket"] = sql_template.bucket

                metadata_labels = [
                    m.get("labels") for m in metadata if m.get("table") == upstairs
                ]
                if mapping_labels or metadata_labels:
                    values["labels"] = {}
                if mapping_labels:
                    values["labels"] = {**values["labels"], **mapping_labels}
                if metadata_labels:
                    values["labels"] = {**values["labels"], **metadata_labels[0]}

                self.mapped[downstairs][upstairs] = values

            self.mapped[downstairs][upstairs]["lines"].append(
                {
                    "num": upstairs_attributes["line"],
                    "str": upstairs_attributes["line_str"],
                }
            )
