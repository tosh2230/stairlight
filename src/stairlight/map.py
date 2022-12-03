from __future__ import annotations

from logging import getLogger
from typing import Any, Iterator, OrderedDict, Type

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
        mapped: dict[str, Any] | None = None,
    ) -> None:
        """Manages functions related to dependency map objects

        Args:
            stairlight_config (StairlightConfig): Stairlight configurations.
            mapping_config (MappingConfig):
                Mapping configurations.
            mapped (dict[str, Any], optional):
                Mapped table attributes. Defaults to None.
        """
        if mapped:
            self.mapped = mapped
        else:
            self.mapped = {}
        self.unmapped: list[dict] = []
        self._stairlight_config = stairlight_config
        self._mapping_config = mapping_config

    def write(self) -> None:
        """Write a dependency map"""
        template_source: TemplateSource
        for template_source in self.find_template_source():
            self.write_by_template_source(template_source=template_source)

        self.mapped = {k: v for k, v in self.mapped.items() if v != {}}

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
        """Write a dependency map by template source

        Args:
            template_source (TemplateSource): Template source
        """
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
            template (Template): Query template
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
        mapping_labels: dict[str, Any] = table_attributes.Labels
        if self._mapping_config:
            metadata: list[dict[str, Any]] = self._mapping_config.Metadata

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

    def get_global_params(self) -> dict[str, Any]:
        """get global parameters in mapping.yaml

        Returns:
            dict[str, Any]: Global parameters
        """
        global_params: dict[str, Any] = {}
        if self._mapping_config:
            _global: MappingConfigGlobal = self._mapping_config.get_global()
            if _global.Parameters:
                global_params = _global.Parameters
        return global_params

    def merge_global_params(
        self, table_attributes: MappingConfigMappingTable
    ) -> dict[str, Any]:
        """return a combination of global parameters and table parameters

        Args:
            table_attributes (MappingConfigMappingTable): table attributes

        Returns:
            dict[str, Any]: Combined global parameters
        """
        global_params: dict[str, Any] = self.get_global_params()
        table_params: OrderedDict[str, Any] = (
            table_attributes.Parameters or OrderedDict()
        )

        # Table parameters are prioritized over global parameters
        return {**global_params, **table_params}

    @staticmethod
    def create_upstairs_value(
        template: Template,
        mapping_labels: dict[str, Any],
        metadata: list[dict[str, Any]],
        upstairs: str,
    ) -> dict[str, Any]:
        """create upstairs table information

        Args:
            template (Template): Template class
            mapping_labels (dict[str, Any]): Labels in mapping section
            metadata (list[dict[str, Any]]): Metadata
            upstairs (str): Upstairs table's Name

        Returns:
            dict[str, Any]: upstairs table information
        """
        metadata_labels: list[dict[str, Any]] = []
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

    def add_unmapped_params(
        self, template: Template, params: list[str] | None = None
    ) -> None:
        """add to the list of unmapped params

        Args:
            template (Template): Query template
            params (list[str], optional): Jinja parameters
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
    ) -> list[str]:
        """detect unmapped parameters in mapped files

        Args:
            template (Template): Query template
            table_attributes (MappingConfigMappingTable):
                Table attributes from mapping configuration

        Returns:
            list[str]: Unmapped parameters
        """
        template_str: str = template.get_template_str()
        template_params: list[str] = template.detect_jinja_params(template_str)
        if not template_params:
            return []

        mapped_params_dict: dict[str, Any] = self.merge_global_params(
            table_attributes=table_attributes
        )
        mapped_params: list[str] = create_dict_key_list(d=mapped_params_dict)
        ignore_params: list[str] = table_attributes.IgnoreParameters or []
        unmapped_params: list[str] = list(
            set(template_params) - set(mapped_params) - set(ignore_params)
        )

        return unmapped_params


def create_dict_key_list(d: dict[str, Any], delimiter: str = ".") -> list[str]:
    """combine nested dictionary keys and converts to a list

    Args:
        d (dict[str, Any]): dict

    Returns:
        list[str]: key-combined and list-converted results
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
