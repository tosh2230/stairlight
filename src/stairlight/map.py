from typing import Iterator

from . import config_key
from .query import Query
from .source.base import Template, TemplateSource, TemplateSourceType
from .source.file import FileTemplateSource
from .source.gcs import GcsTemplateSource
from .source.redash import RedashTemplateSource


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
        self.stairlight_config = stairlight_config
        self.mapping_config = mapping_config

    def write(self) -> None:
        for template_source in self.find_template_source(
            stairlight_config=self.stairlight_config,
            mapping_config=self.mapping_config,
        ):
            self.write_by_template_source(template_source=template_source)

    @staticmethod
    def find_template_source(
        stairlight_config: dict, mapping_config: dict
    ) -> Iterator[TemplateSource]:
        for source_attributes in stairlight_config.get(
            config_key.STAIRLIGHT_CONFIG_INCLUDE_SECTION
        ):
            template_source: TemplateSource = None
            template_source_type = source_attributes.get(
                config_key.TEMPLATE_SOURCE_TYPE
            )
            if template_source_type == TemplateSourceType.FILE.value:
                template_source = FileTemplateSource(
                    stairlight_config=stairlight_config,
                    mapping_config=mapping_config,
                    source_attributes=source_attributes,
                )
            elif template_source_type == TemplateSourceType.GCS.value:
                template_source = GcsTemplateSource(
                    stairlight_config=stairlight_config,
                    mapping_config=mapping_config,
                    source_attributes=source_attributes,
                )
            elif template_source_type == TemplateSourceType.REDASH.value:
                template_source = RedashTemplateSource(
                    stairlight_config=stairlight_config,
                    mapping_config=mapping_config,
                    source_attributes=source_attributes,
                )
            else:
                print(f"Template_source not found!: {type}")
                continue
            yield template_source

    def write_by_template_source(self, template_source: TemplateSource) -> None:
        """Write a dependency map"""
        for sql_template in template_source.search_templates_iter():
            if sql_template.is_mapped():
                for table_attributes in sql_template.get_mapped_table_attributes_iter():
                    self.find_unmapped_params(
                        sql_template=sql_template, table_attributes=table_attributes
                    )
                    self.remap(
                        sql_template=sql_template, table_attributes=table_attributes
                    )
            else:
                template_str = sql_template.get_template_str()
                params = sql_template.get_jinja_params(template_str)
                self.add_unmapped_params(sql_template=sql_template, params=params)

    def remap(self, sql_template: Template, table_attributes: dict) -> None:
        """Remap a dependency map

        Args:
            sql_template (Template): SQL template
            table_attributes (dict): Table attributes from mapping configuration
        """
        query_str = sql_template.render(
            params=table_attributes.get(config_key.PARAMETERS)
        )
        query = Query(
            query_str=query_str,
            default_table_prefix=sql_template.default_table_prefix,
        )

        downstairs = table_attributes.get(config_key.TABLE_NAME)
        mapping_labels = table_attributes.get(config_key.LABELS)
        metadata = self.mapping_config.get(config_key.MAPPING_CONFIG_METADATA_SECTION)

        if downstairs not in self.mapped:
            self.mapped[downstairs] = {}

        for upstairs_attributes in query.get_upstairs_attributes_iter():
            upstairs = upstairs_attributes["table_name"]

            if not self.mapped[downstairs].get(upstairs):
                upstairs_values = {
                    "type": sql_template.source_type.value,
                    "file": sql_template.key,
                    "uri": sql_template.uri,
                    "lines": [],
                }
                if sql_template.source_type == TemplateSourceType.GCS:
                    upstairs_values["bucket"] = sql_template.bucket

                metadata_labels = [
                    m.get(config_key.LABELS)
                    for m in metadata
                    if m.get(config_key.TABLE_NAME) == upstairs
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

    def add_unmapped_params(self, sql_template: Template, params: list) -> None:
        """add to the list of unmapped params

        Args:
            sql_template (Template): SQL template
            params (list): Jinja parameters
        """
        self.unmapped.append(
            {
                "sql_template": sql_template,
                "params": params,
            }
        )

    def find_unmapped_params(
        self, sql_template: Template, table_attributes: dict
    ) -> None:
        """find unmapped parameters in mapped files

        Args:
            sql_template (Template): SQL template
            table_attributes (dict): Table attributes from mapping configuration
        """
        template_str = sql_template.get_template_str()
        template_params = sql_template.get_jinja_params(template_str)
        mapped_params_dict = table_attributes.get(config_key.PARAMETERS)
        mapped_params = (
            [f"params.{key}" for key in mapped_params_dict.keys()]
            if mapped_params_dict
            else {}
        )
        diff_params = list(set(template_params) - set(mapped_params))
        print(f"Uri: {sql_template.uri}")
        print(f"template_params: {template_params}")
        print(f"mapped_params_dict: {mapped_params_dict}")
        print(f"mapped_params: {mapped_params}")
        print(f"diff_params: {diff_params}")
        print("")
        if diff_params:
            self.add_unmapped_params(sql_template=sql_template, params=diff_params)
