from __future__ import annotations

from dataclasses import dataclass
from logging import getLogger
from typing import Any, Iterator, OrderedDict, Type

from src.stairlight.query import Query, UpstairsTableReference
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
    tables: list[Table]


@dataclass
class Table:
    TemplateSourceType: str
    Key: str
    Uri: str
    Lines: list[dict]
    Labels: dict[str, str] | None = None


@dataclass
class TableInObjectStorage(Table):
    BucketName: str | None = None


@dataclass
class TableInRedash(Table):
    DataSourceType: str | None = None


class Map:
    """Manages functions related to dependency map objects"""

    def __init__(
        self,
        stairlight_config: StairlightConfig,
        mapping_config: MappingConfig,
        mapped: dict[str, list[Stair | None]] | None = None,
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

        self.mapped = {k: v for k, v in self.mapped.items() if v != []}

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
        current_floor_name: str = table_attributes.TableName
        current_floor_label: dict[str, Any] = table_attributes.Labels
        if self._mapping_config:
            extra_labels: list[dict[str, Any]] = self._mapping_config.ExtraLabels or []

        if current_floor_name not in self.mapped:
            self.mapped[current_floor_name] = []

        query = Query(
            query_str=template.render(
                params=self.merge_global_params(table_attributes=table_attributes),
                ignore_params=table_attributes.IgnoreParameters,
            ),
            default_table_prefix=template.default_table_prefix,
        )

        upstairs_table_reference: UpstairsTableReference
        for upstairs_table_reference in query.detect_upstairs_table_reference():
            upstairs_extra_labels = [
                extra_label.get(MappingConfigKey.LABELS, {})
                for extra_label in extra_labels
                if extra_label.get(MappingConfigKey.TABLE_NAME)
                == upstairs_table_reference.TableName
            ]
            upstairs_extra_label = (
                upstairs_extra_labels[0] if upstairs_extra_labels else []
            )
            upstairs_table = self.create_upstairs_table(
                template=template,
                current_floor_label=current_floor_label,
                extra_label=upstairs_extra_label,
            )

            self.mapped[current_floor_name].append(
                Stair(
                    name=upstairs_table_reference.TableName,
                    tables=[upstairs_table],
                )
            )

            for upstairs in self.mapped[current_floor_name]:
                for table in upstairs.tables:
                    if table.Key == template.key:
                        table.Lines.append(upstairs_table_reference.Line)

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
    def create_upstairs_table(
        template: Template,
        current_floor_label: dict[str, Any],
        extra_label: dict[str, Any],
    ) -> Table:
        """create upstairs table attributes

        Args:
            template (Template): Template class
            mapped_labels (dict[str, Any]): Labels in mapping section
            extra_labels (list[dict[str, Any]]): Extra labels
            upstairs (str): Upstairs table's Name

        Returns:
            UpstairsTable: upstairs table attributes
        """
        upstairs_labels: dict = {
            **(current_floor_label or {}),
            **(extra_label or {}),
        }

        upstairs_table: Table
        if template.source_type in (
            TemplateSourceType.GCS,
            TemplateSourceType.S3,
        ):
            upstairs_table = TableInObjectStorage(
                TemplateSourceType=template.source_type.value,
                Key=template.key,
                Uri=template.uri,
                Lines=[],
                Labels=upstairs_labels,
                BucketName=template.bucket,
            )
        elif template.source_type == TemplateSourceType.REDASH:
            upstairs_table = TableInRedash(
                TemplateSourceType=template.source_type.value,
                Key=template.key,
                Uri=template.uri,
                Lines=[],
                Labels=upstairs_labels,
                DataSourceType=template.data_source_name,
            )
        else:
            upstairs_table = Table(
                TemplateSourceType=template.source_type.value,
                Key=template.key,
                Uri=template.uri,
                Lines=[],
                Labels=upstairs_labels,
            )

        return upstairs_table

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
    results: list = []
    for key, value in d.items():
        if isinstance(value, dict):
            results = results + [
                key + delimiter + recursive_result
                for recursive_result in create_dict_key_list(d=value)
            ]
        else:
            results.append(key)
    return results
