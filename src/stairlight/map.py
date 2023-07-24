from __future__ import annotations

from dataclasses import dataclass
from logging import getLogger
from typing import Any, Iterator, OrderedDict, Type

from src.stairlight.query import Query, UpstairTableReference
from src.stairlight.source.config import (
    MappingConfig,
    MappingConfigGlobal,
    MappingConfigMappingTable,
    StairlightConfig,
)
from src.stairlight.source.config_key import MapKey, MappingConfigKey
from src.stairlight.source.controller import get_template_source_class
from src.stairlight.source.template import Template, TemplateSource, TemplateSourceType

logger = getLogger(__name__)


@dataclass
class Stair:
    name: str
    mapped_templates: list[MappedTemplate] | None


@dataclass
class MappedTemplate:
    TemplateSourceType: str
    Key: str
    Uri: str
    Lines: list[dict]
    Labels: dict[str, str] | None = None


@dataclass
class MappedTemplateObjectStorage(MappedTemplate):
    BucketName: str | None = None


@dataclass
class MappedTemplateRedash(MappedTemplate):
    DataSourceType: str | None = None


class Map:
    """Manages functions related to dependency map objects"""

    def __init__(
        self,
        stairlight_config: StairlightConfig,
        mapping_config: MappingConfig,
        mapped: dict[str, dict[str, list[MappedTemplate]] | None] | None = None,
    ) -> None:
        """Manages functions related to dependency map objects

        Args:
            stairlight_config (StairlightConfig): Stairlight configurations.
            mapping_config (MappingConfig):
                Mapping configurations.
            mapped (dict[str, Any], optional):
                Mapped templates. Defaults to None.
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

        self.mapped = {k: v for k, v in self.mapped.items() if v}

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
            # Check if the template is in the mapping_config
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
        current_floor_name: str = table_attributes.TableName
        current_floor_label: dict[str, Any] = table_attributes.Labels
        if self._mapping_config:
            extra_labels: list[dict[str, Any]] = self._mapping_config.ExtraLabels or []

        current_floor_map: dict[str, Any] = self.mapped.get(current_floor_name) or {}
        if not current_floor_map:
            self.mapped[current_floor_name] = {}

        global_params: dict[str, Any] = self.get_global_params()
        params = self.merge_global_params(
            table_attributes=table_attributes, global_params=global_params
        )
        query = Query(
            query_str=template.render(
                params=params,
                ignore_params=table_attributes.IgnoreParameters,
            ),
            default_table_prefix=template.default_table_prefix,
        )

        upstair_table_reference: UpstairTableReference
        for upstair_table_reference in query.detect_upstair_table_reference():
            upstair = Stair(
                name=upstair_table_reference.TableName,
                mapped_templates=current_floor_map.get(
                    upstair_table_reference.TableName, []
                ),
            )

            upstairs_extra_labels = [
                extra_label.get(MappingConfigKey.LABELS, {})
                for extra_label in extra_labels
                if extra_label.get(MappingConfigKey.TABLE_NAME)
                == upstair_table_reference.TableName
            ]
            upstairs_extra_label = (
                upstairs_extra_labels[0] if upstairs_extra_labels else []
            )
            upstair_template = self.create_upstair_template(
                template=template,
                current_floor_label=current_floor_label,
                extra_label=upstairs_extra_label,
            )

            if upstair and upstair.name == upstair_table_reference.TableName:
                upstair = Stair(
                    name=upstair.name,
                    mapped_templates=upstair.mapped_templates + [upstair_template],
                )
            else:
                upstair = Stair(
                    name=upstair_table_reference.TableName,
                    mapped_templates=[upstair_template],
                )

            for i, mapped_template in enumerate(upstair.mapped_templates):
                if (
                    upstair_table_reference.Line not in mapped_template.Lines
                    and upstair.name in upstair_table_reference.Line["LineString"]
                ):
                    upstair.mapped_templates[i].Lines.append(
                        upstair_table_reference.Line
                    )

            self.mapped[current_floor_name][
                str(upstair.name)
            ] = upstair.mapped_templates

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
        self,
        table_attributes: MappingConfigMappingTable,
        global_params: dict[str, Any],
    ) -> dict[str, Any]:
        """return a combination of global parameters and table parameters

        Args:
            table_attributes (MappingConfigMappingTable): table attributes
            global_params (dict[str, Any]): global params

        Returns:
            dict[str, Any]: Combined global parameters
        """
        table_params: OrderedDict[str, Any] = (
            table_attributes.Parameters or OrderedDict()
        )

        # Table parameters are prioritized over global parameters
        return {**global_params, **table_params}

    @staticmethod
    def create_upstair_template(
        template: Template,
        current_floor_label: dict[str, Any],
        extra_label: dict[str, Any],
    ) -> MappedTemplate:
        """create a upstair template

        Args:
            template (Template): Template class
            mapped_labels (dict[str, Any]): Labels in mapping section
            extra_labels (list[dict[str, Any]]): Extra labels
            upstairs (str): Upstairs table's Name

        Returns:
            Table: upstair template
        """
        upstair_labels: dict = {
            **(current_floor_label or {}),
            **(extra_label or {}),
        }

        upstair_template: MappedTemplate
        if template.source_type in (
            TemplateSourceType.GCS,
            TemplateSourceType.S3,
        ):
            upstair_template = MappedTemplateObjectStorage(
                TemplateSourceType=template.source_type.value,
                Key=template.key,
                Uri=template.uri,
                Lines=[],
                Labels=upstair_labels,
                BucketName=template.bucket,
            )
        elif template.source_type == TemplateSourceType.REDASH:
            upstair_template = MappedTemplateRedash(
                TemplateSourceType=template.source_type.value,
                Key=template.key,
                Uri=template.uri,
                Lines=[],
                Labels=upstair_labels,
                DataSourceType=template.data_source_name,
            )
        else:
            upstair_template = MappedTemplate(
                TemplateSourceType=template.source_type.value,
                Key=template.key,
                Uri=template.uri,
                Lines=[],
                Labels=upstair_labels,
            )

        return upstair_template

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

        global_params: dict[str, Any] = self.get_global_params()
        mapped_params_dict: dict[str, Any] = self.merge_global_params(
            table_attributes=table_attributes, global_params=global_params
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
    results: list[str] = []
    for key, value in d.items():
        if isinstance(value, dict):
            results = results + [
                key + delimiter + recursive_result
                for recursive_result in create_dict_key_list(d=value)
            ]
        else:
            results.append(key)
    return results
