from logging import getLogger
from typing import Iterator

from .key import MapKey, MappingConfigKey, StairlightConfigKey
from .query import Query
from .source.base import Template, TemplateSource, TemplateSourceType
from .source.controller import get_template_source_class

logger = getLogger(__name__)


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
            Iterator[TemplateSource]: Template source instance
        """
        source_attributes: dict
        for source_attributes in stairlight_config.get(
            StairlightConfigKey.INCLUDE_SECTION
        ):
            template_source_type: str = source_attributes.get(
                StairlightConfigKey.TEMPLATE_SOURCE_TYPE
            )
            template_source: TemplateSource = get_template_source_class(
                template_source_type=template_source_type
            )
            if not template_source:
                logger.warning(msg=f"Template source is not found: {type}")
                continue
            yield template_source(
                stairlight_config=stairlight_config,
                mapping_config=mapping_config,
                source_attributes=source_attributes,
            )

    def write_by_template_source(self, template_source: TemplateSource) -> None:
        """Write a dependency map by template source"""
        for template in template_source.search_templates():
            if not self.mapping_config:
                self.add_unmapped_params(template=template)
            elif template.is_mapped():
                for table_attributes in template.find_mapped_table_attributes():
                    self.detect_unmapped_params(
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
            params=self.merge_global_params(table_attributes=table_attributes),
            ignore_params=table_attributes.get(MappingConfigKey.IGNORE_PARAMETERS),
        )
        query = Query(
            query_str=query_str,
            default_table_prefix=template.default_table_prefix,
        )

        downstairs: str = table_attributes.get(MappingConfigKey.TABLE_NAME)
        mapping_labels: dict = table_attributes.get(MappingConfigKey.LABELS)
        metadata: list[str] = self.mapping_config.get(MappingConfigKey.METADATA_SECTION)

        if downstairs not in self.mapped:
            self.mapped[downstairs] = {}

        for upstairs_attributes in query.detect_upstairs_attributes():
            upstairs: str = upstairs_attributes[MapKey.TABLE_NAME]

            if not self.mapped[downstairs].get(upstairs):
                self.mapped[downstairs][upstairs] = self.create_upstairs_value(
                    template=template,
                    mapping_labels=mapping_labels,
                    metadata=metadata,
                    upstairs=upstairs,
                )

            self.mapped[downstairs][upstairs][MapKey.LINES].append(
                {
                    MapKey.LINE_NUMBER: upstairs_attributes[MapKey.LINE_NUMBER],
                    MapKey.LINE_STRING: upstairs_attributes[MapKey.LINE_STRING],
                }
            )

    def get_global_params(self, key: str) -> dict:
        """get global parameters in mapping.yaml

        Returns:
            dict: global parameters
        """
        global_params: dict = {}
        global_section: dict = self.mapping_config.get(MappingConfigKey.GLOBAL_SECTION)
        if MappingConfigKey.PARAMETERS in global_section:
            global_params = global_section.get(key)

        return global_params

    def merge_global_params(self, table_attributes: dict) -> dict:
        """return a combination of global parameters and table parameters

        Args:
            table_attributes (dict): table attributes

        Returns:
            dict: combined parameters
        """
        global_params: dict = self.get_global_params(key=MappingConfigKey.PARAMETERS)
        table_params: dict = table_attributes.get(MappingConfigKey.PARAMETERS, {})

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
            MapKey.TEMPLATE_SOURCE_TYPE: template.source_type.value,
            MapKey.KEY: template.key,
            MapKey.URI: template.uri,
            MapKey.LINES: [],
        }

        if template.source_type == TemplateSourceType.GCS:
            upstairs_values[MapKey.BUCKET_NAME] = template.bucket
        elif template.source_type == TemplateSourceType.REDASH:
            upstairs_values[MapKey.DATA_SOURCE_NAME] = template.data_source_name

        if metadata:
            metadata_labels = [
                m.get(MappingConfigKey.LABELS)
                for m in metadata
                if m.get(MappingConfigKey.TABLE_NAME) == upstairs
            ]
        if mapping_labels or metadata_labels:
            upstairs_values[MapKey.LABELS] = {}

        if mapping_labels:
            upstairs_values[MapKey.LABELS] = {
                **upstairs_values[MapKey.LABELS],
                **mapping_labels,
            }

        if metadata_labels:
            upstairs_values[MapKey.LABELS] = {
                **upstairs_values[MapKey.LABELS],
                **metadata_labels[0],
            }
        return upstairs_values

    def add_unmapped_params(
        self, template: Template, params: "list[str]" = None
    ) -> None:
        """add to the list of unmapped params

        Args:
            template (Template): SQL template
            params (dict, optional): Jinja parameters
        """
        if not params:
            template_str = template.get_template_str()
            params = template.detect_jinja_params(template_str=template_str)
        self.unmapped.append(
            {
                MapKey.TEMPLATE: template,
                MapKey.PARAMETERS: params,
            }
        )

    def detect_unmapped_params(
        self, template: Template, table_attributes: dict
    ) -> None:
        """detect unmapped parameters in mapped files

        Args:
            template (Template): SQL template
            table_attributes (dict): Table attributes from mapping configuration
        """
        template_str: str = template.get_template_str()
        template_params: list[str] = template.detect_jinja_params(template_str)
        if not template_params:
            return

        mapped_params_dict: dict = self.merge_global_params(
            table_attributes=table_attributes
        )
        mapped_params: list[str] = create_dict_key_list(d=mapped_params_dict)
        ignore_params: list[str] = table_attributes.get(
            MappingConfigKey.IGNORE_PARAMETERS, []
        )
        diff_params: list[str] = list(
            set(template_params) - set(mapped_params) - set(ignore_params)
        )

        if diff_params:
            self.add_unmapped_params(template=template, params=diff_params)


def create_dict_key_list(d: dict, delimiter: str = ".") -> "list[str]":
    """combine nested dictionary keys and converts to a list

    Args:
        d (dict): dict

    Returns:
        list: results
    """
    results = []
    for key, value in d.items():
        if isinstance(value, dict):
            recursive_results = create_dict_key_list(d=value)
            for recursive_result in recursive_results:
                concat = key + delimiter + recursive_result
                results.append(concat)
        else:
            results.append(key)
    return results
