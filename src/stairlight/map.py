from logging import getLogger
from typing import Any, Dict, Iterator, List, OrderedDict, Type

from .query import Query
from .source.config import (
    MappingConfig,
    MappingConfigGlobal,
    MappingConfigMappingTable,
    StairlightConfig,
)
from .source.config_key import MapKey, MappingConfigKey
from .source.controller import get_template_source_class
from .source.template import Template, TemplateSource, TemplateSourceType

logger = getLogger(__name__)


class Map:
    """Manages functions related to dependency map objects"""

    def __init__(
        self,
        stairlight_config: StairlightConfig,
        mapping_config: MappingConfig,
        mapped: Dict[str, Any] = None,
    ) -> None:
        """Manages functions related to dependency map objects

        Args:
            stairlight_config (StairlightConfig): Stairlight configuration
            mapping_config (MappingConfig): Mapping configuration
            mapped (dict, optional):
                Mapped file attributes when a mapping configuration file loaded.
                Defaults to {}.
        """
        if mapped:
            self.mapped = mapped
        else:
            self.mapped = {}
        self.unmapped: List[dict] = []
        self._stairlight_config = stairlight_config
        self._mapping_config = mapping_config

    def write(self) -> None:
        """Write a dependency map"""
        template_source: TemplateSource
        for template_source in self.find_template_source():
            self.write_by_template_source(template_source=template_source)

    def find_template_source(self) -> Iterator[TemplateSource]:
        """find template source

        Yields:
            Iterator[TemplateSource]: Template source instance
        """
        for include in self._stairlight_config.get_include():
            template_source: Type[TemplateSource] = get_template_source_class(
                template_source_type=include.TemplateSourceType
            )
            if not template_source:
                logger.warning(msg=f"Template source is not found: {type}")
                continue
            yield template_source(
                stairlight_config=self._stairlight_config,
                mapping_config=self._mapping_config,
                **{"include": include},
            )

    def write_by_template_source(self, template_source: TemplateSource) -> None:
        """Write a dependency map by template source"""
        for template in template_source.search_templates():
            if not self._mapping_config:
                self.add_unmapped_params(template=template)
            elif template.is_mapped():
                for table_attributes in template.find_mapped_table_attributes():
                    unmapped_params = self.detect_unmapped_params(
                        template=template, table_attributes=table_attributes
                    )
                    if unmapped_params:
                        self.add_unmapped_params(
                            template=template, params=unmapped_params
                        )
                    self.remap(template=template, table_attributes=table_attributes)
            else:
                self.add_unmapped_params(template=template)

    def remap(
        self, template: Template, table_attributes: MappingConfigMappingTable
    ) -> None:
        """Remap a dependency map

        Args:
            template (Template): SQL template
            table_attributes (MappingConfigMappingTable):
                Table attributes from mapping configuration
        """
        query_str: str = template.render(
            params=self.merge_global_params(table_attributes=table_attributes),
            ignore_params=table_attributes.IgnoreParameters,
        )
        query = Query(
            query_str=query_str,
            default_table_prefix=template.default_table_prefix,
        )

        downstairs: str = table_attributes.TableName
        mapping_labels: Dict[str, Any] = table_attributes.Labels
        metadata: List[Dict[str, Any]] = self._mapping_config.Metadata

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

    def get_global_params(self) -> Dict[str, Any]:
        """get global parameters in mapping.yaml

        Returns:
            dict: global parameters
        """
        _global: MappingConfigGlobal = self._mapping_config.get_global()
        return _global.Parameters

    def merge_global_params(
        self, table_attributes: MappingConfigMappingTable
    ) -> Dict[str, Any]:
        """return a combination of global parameters and table parameters

        Args:
            table_attributes (MappingConfigMappingTable): table attributes

        Returns:
            dict: combined parameters
        """
        global_params: Dict[str, Any] = self.get_global_params()
        table_params: OrderedDict[str, Any] = table_attributes.Parameters

        # Table parameters are prioritized over global parameters
        return {**global_params, **table_params}

    @staticmethod
    def create_upstairs_value(
        template: Template,
        mapping_labels: Dict[str, Any],
        metadata: List[Dict[str, Any]],
        upstairs: str,
    ) -> Dict[str, Any]:
        """create upstairs table information

        Args:
            template (Template): Template class
            mapping_labels (dict): labels in mapping section
            metadata (list[str]): metadata
            upstairs (str): upstairs table name

        Returns:
            dict: upstairs table information
        """
        metadata_labels: List[Dict[str, Any]] = []
        upstairs_values = {
            MapKey.TEMPLATE_SOURCE_TYPE: template.source_type.value,
            MapKey.KEY: template.key,
            MapKey.URI: template.uri,
            MapKey.LINES: [],
        }

        if template.source_type in (TemplateSourceType.GCS, TemplateSourceType.S3):
            upstairs_values[MapKey.BUCKET_NAME] = template.bucket
        elif template.source_type == TemplateSourceType.REDASH:
            upstairs_values[MapKey.DATA_SOURCE_NAME] = template.data_source_name

        if metadata:
            metadata_labels = [
                m.get(MappingConfigKey.LABELS, {})
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

    def add_unmapped_params(self, template: Template, params: List[str] = None) -> None:
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
        self, template: Template, table_attributes: MappingConfigMappingTable
    ) -> List[str]:
        """detect unmapped parameters in mapped files

        Args:
            template (Template): SQL template
            table_attributes (MappingConfigMappingTable):
                Table attributes from mapping configuration
        """
        template_str: str = template.get_template_str()
        template_params: List[str] = template.detect_jinja_params(template_str)
        if not template_params:
            return []

        mapped_params_dict: Dict[str, Any] = self.merge_global_params(
            table_attributes=table_attributes
        )
        mapped_params: List[str] = create_dict_key_list(d=mapped_params_dict)
        ignore_params: List[str] = table_attributes.IgnoreParameters
        unmapped_params: List[str] = list(
            set(template_params) - set(mapped_params) - set(ignore_params)
        )

        return unmapped_params


def create_dict_key_list(d: Dict[str, Any], delimiter: str = ".") -> List[str]:
    """combine nested dictionary keys and converts to a list

    Args:
        d (dict): dict[str, Any]

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
