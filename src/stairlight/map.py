from .query import Query
from .template import SourceType, SQLTemplate, TemplateSource


class Map:
    """Manages functions related to dependency map objects"""

    def __init__(
        self, stairlight_config: dict, mapping_config: dict, mapped: dict = {}
    ) -> None:
        """Manages functions related to dependency map objects

        Args:
            stairlight_config (dict): Stairlight configuration
            mapping_config (dict): Mapping configuration
            mapped (dict, optional):
                Mapped file attributes when a mapping configuration file loaded.
                Defaults to {}.
        """
        self.mapped = mapped
        self.unmapped = []
        self.mapping_config = mapping_config
        self._template_source = TemplateSource(
            stairlight_config=stairlight_config, mapping_config=mapping_config
        )

    def add_unmapped(self, sql_template: SQLTemplate, params: list) -> None:
        """add to the list of unmapped files

        Args:
            sql_template (SQLTemplate): SQL template
            params (list): Jinja parameters
        """
        self.unmapped.append(
            {
                "sql_template": sql_template,
                "params": params,
            }
        )

    def find_unmapped_params(
        self, sql_template: SQLTemplate, table_attributes: dict
    ) -> None:
        """find unmapped parameters in mapped files

        Args:
            sql_template (SQLTemplate): SQL template
            table_attributes (dict): Table attributes from mapping configuration
        """
        template_str = sql_template.get_template_file_str()
        template_params = sql_template.get_jinja_params(template_str)
        mapped_params_dict = table_attributes.get("params")
        mapped_params = (
            [f"params.{key}" for key in table_attributes.get("params").keys()]
            if mapped_params_dict
            else {}
        )
        diff_params = list(set(template_params) - set(mapped_params))
        if diff_params:
            self.add_unmapped(sql_template=sql_template, params=diff_params)

    def write(self) -> None:
        """Write a dependency map"""
        for sql_template in self._template_source.search_templates_iter():
            if sql_template.is_mapped():
                for table_attributes in sql_template.get_mapped_tables_iter():
                    self.find_unmapped_params(
                        sql_template=sql_template, table_attributes=table_attributes
                    )
                    self._remap(
                        sql_template=sql_template, table_attributes=table_attributes
                    )
            else:
                template_file_str = sql_template.get_template_file_str()
                params = sql_template.get_jinja_params(template_file_str)
                self.add_unmapped(sql_template=sql_template, params=params)

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
        metadata = self.mapping_config.get("metadata")

        if downstairs not in self.mapped:
            self.mapped[downstairs] = {}

        for upstairs_attributes in query.get_upstairs_attributes_iter():
            upstairs = upstairs_attributes["table_name"]

            if not self.mapped[downstairs].get(upstairs):
                upstairs_values = {
                    "type": sql_template.source_type.value,
                    "file": sql_template.file_path,
                    "uri": sql_template.uri,
                    "lines": [],
                }
                if sql_template.source_type == SourceType.GCS:
                    upstairs_values["bucket"] = sql_template.bucket

                metadata_labels = [
                    m.get("labels") for m in metadata if m.get("table") == upstairs
                ]
                if mapping_labels or metadata_labels:
                    upstairs_values["labels"] = {}
                if mapping_labels:
                    upstairs_values["labels"] = {
                        **upstairs_values["labels"],
                        **mapping_labels,
                    }
                if metadata_labels:
                    upstairs_values["labels"] = {
                        **upstairs_values["labels"],
                        **metadata_labels[0],
                    }

                self.mapped[downstairs][upstairs] = upstairs_values

            self.mapped[downstairs][upstairs]["lines"].append(
                {
                    "num": upstairs_attributes["line"],
                    "str": upstairs_attributes["line_str"],
                }
            )
