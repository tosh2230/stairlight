from typing import Iterator

from . import config_key, map_key
from .query import Query
from .source.base import Template, TemplateSource, TemplateSourceType
from .source.file import FileTemplateSource
from .source.gcs import GcsTemplateSource
from .source.redash import RedashTemplateSource


class Map:
    """Manages functions related to dependency map objects"""

    def __init__(
        self, stairlight_config: dict, mapping_config: dict, mapped: dict = None
    ) -> None:
        """Manages functions related to dependency map objects

        Args:
            stairlight_config (dict): Stairlight configuration
            mapping_config (dict): Mapping configuration
            mapped (dict, optional):
                Mapped file attributes when a mapping configuration file loaded.
                Defaults to {}.
        """
        if mapped:
            self.mapped = mapped
        else:
            self.mapped = {}
        self.unmapped: list[dict] = []
        self.stairlight_config = stairlight_config
        self.mapping_config = mapping_config

    def write(self) -> None:
        """Write a dependency map"""
        for template_source in self.find_template_source(
            stairlight_config=self.stairlight_config,
            mapping_config=self.mapping_config,
        ):
            self.write_by_template_source(template_source=template_source)

    @staticmethod
    def find_template_source(
        stairlight_config: dict, mapping_config: dict
    ) -> Iterator[TemplateSource]:
        """find template source

        Args:
            stairlight_config (dict): Stairlight configuration
            mapping_config (dict): Mapping configuration

        Yields:
            Iterator[TemplateSource]: Template source class
        """
        for source_attributes in stairlight_config.get(
            config_key.STAIRLIGHT_CONFIG_INCLUDE_SECTION
        ):
            template_source: TemplateSource = None
            template_source_type = source_attributes.get(
                config_key.TEMPLATE_SOURCE_TYPE
            )
            if template_source_type == TemplateSourceType.FILE.value:
                template_source = FileTemplateSource
            elif template_source_type == TemplateSourceType.GCS.value:
                template_source = GcsTemplateSource
            elif template_source_type == TemplateSourceType.REDASH.value:
                template_source = RedashTemplateSource
            else:
                print(f"Template source is not found: {type}")
                continue
            yield template_source(
                stairlight_config=stairlight_config,
                mapping_config=mapping_config,
                source_attributes=source_attributes,
            )

    def write_by_template_source(self, template_source: TemplateSource) -> None:
        """Write a dependency map"""
        for template in template_source.search_templates_iter():
            if not self.mapping_config:
                self.add_unmapped_params(template=template)
            elif template.is_mapped():
                for table_attributes in template.get_mapped_table_attributes_iter():
                    self.find_unmapped_params(
                        template=template, table_attributes=table_attributes
                    )
                    self.remap(template=template, table_attributes=table_attributes)
            else:
                self.add_unmapped_params(template=template)

    def remap(self, template: Template, table_attributes: dict) -> None:
        """Remap a dependency map

        Args:
            template (Template): SQL template
            table_attributes (dict): Table attributes from mapping configuration
        """
        query_str: str = template.render(
            params=self.get_combined_params(table_attributes)
        )
        query = Query(
            query_str=query_str,
            default_table_prefix=template.default_table_prefix,
        )

        downstairs: str = table_attributes.get(config_key.TABLE_NAME)
        mapping_labels: dict = table_attributes.get(config_key.LABELS)
        metadata: list[str] = self.mapping_config.get(
            config_key.MAPPING_CONFIG_METADATA_SECTION
        )

        if downstairs not in self.mapped:
            self.mapped[downstairs] = {}

        for upstairs_attributes in query.get_upstairs_attributes_iter():
            upstairs = upstairs_attributes[map_key.TABLE_NAME]

            if not self.mapped[downstairs].get(upstairs):
                self.mapped[downstairs][upstairs] = self.create_upstairs_value(
                    template=template,
                    mapping_labels=mapping_labels,
                    metadata=metadata,
                    upstairs=upstairs,
                )

            self.mapped[downstairs][upstairs][map_key.LINES].append(
                {
                    map_key.LINE_NUMBER: upstairs_attributes[map_key.LINE_NUMBER],
                    map_key.LINE_STRING: upstairs_attributes[map_key.LINE_STRING],
                }
            )

    def get_global_params(self) -> dict:
        """get global parameters in mapping.yaml

        Returns:
            dict: global parameters
        """
        global_params: dict = {}
        global_section: dict = self.mapping_config.get(
            config_key.MAPPING_CONFIG_GLOBAL_SECTION
        )
        if config_key.PARAMETERS in global_section:
            global_params = global_section.get(config_key.PARAMETERS)

        return global_params

    def get_combined_params(self, table_attributes: dict) -> dict:
        """return a combination of global parameters and table parameters

        Args:
            table_attributes (dict): table attributes

        Returns:
            dict: combined parameters
        """
        global_params: dict = self.get_global_params()
        table_params: dict = table_attributes.get(config_key.PARAMETERS, {})

        # Table parameters are prioritized over global parameters
        return {**global_params, **table_params}

    @staticmethod
    def create_upstairs_value(
        template: Template,
        mapping_labels: dict,
        metadata: "list[str]",
        upstairs: str,
    ) -> dict:
        """create upstairs table information

        Args:
            template (Template): Template class
            mapping_labels (dict): labels in mapping section
            metadata (list[str]): metadata
            upstairs (str): upstairs table name

        Returns:
            dict: upstairs table information
        """
        metadata_labels = []
        upstairs_values = {
            map_key.TEMPLATE_SOURCE_TYPE: template.source_type.value,
            map_key.KEY: template.key,
            map_key.URI: template.uri,
            map_key.LINES: [],
        }

        if template.source_type == TemplateSourceType.GCS:
            upstairs_values[map_key.BUCKET_NAME] = template.bucket
        elif template.source_type == TemplateSourceType.REDASH:
            upstairs_values[map_key.DATA_SOURCE_NAME] = template.data_source_name

        if metadata:
            metadata_labels = [
                m.get(config_key.LABELS)
                for m in metadata
                if m.get(config_key.TABLE_NAME) == upstairs
            ]
        if mapping_labels or metadata_labels:
            upstairs_values[map_key.LABELS] = {}

        if mapping_labels:
            upstairs_values[map_key.LABELS] = {
                **upstairs_values[map_key.LABELS],
                **mapping_labels,
            }

        if metadata_labels:
            upstairs_values[map_key.LABELS] = {
                **upstairs_values[map_key.LABELS],
                **metadata_labels[0],
            }
        return upstairs_values

    def add_unmapped_params(self, template: Template, params: list = None) -> None:
        """add to the list of unmapped params

        Args:
            template (Template): SQL template
            params (dict, optional): Jinja parameters
        """
        if not params:
            template_str = template.get_template_str()
            params = template.get_jinja_params(template_str)
        self.unmapped.append(
            {
                map_key.TEMPLATE: template,
                map_key.PARAMETERS: params,
            }
        )

    def find_unmapped_params(self, template: Template, table_attributes: dict) -> None:
        """find unmapped parameters in mapped files

        Args:
            template (Template): SQL template
            table_attributes (dict): Table attributes from mapping configuration
        """
        template_str: str = template.get_template_str()
        template_params: list = template.get_jinja_params(template_str)
        if not template_params:
            return

        mapped_params_dict: dict = self.get_combined_params(table_attributes)
        mapped_params: list = combine_nested_dict_keys(d=mapped_params_dict)
        diff_params: list = list(set(template_params) - set(mapped_params))

        if diff_params:
            self.add_unmapped_params(template=template, params=diff_params)


def combine_nested_dict_keys(d: dict, delimiter: str = ".") -> list:
    """combine nested dictionary keys and converts to a list

    Args:
        d (dict): dict

    Returns:
        list: results
    """
    results = []
    for key, value in d.items():
        if isinstance(value, dict):
            recursive_results = combine_nested_dict_keys(d=value)
            for recursive_result in recursive_results:
                concat = key + delimiter + recursive_result
                results.append(concat)
        else:
            results.append(key)
    return results
