from __future__ import annotations

import glob
import logging
import re
from collections import OrderedDict
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

import yaml

import src.stairlight.util as sl_util
from src.stairlight.source.config import (
    MappingConfig,
    MappingConfigMapping,
    MappingConfigMappingTable,
    StairlightConfig,
    StairlightConfigExclude,
    StairlightConfigSettings,
)
from src.stairlight.source.config_key import MapKey
from src.stairlight.source.controller import (
    collect_mapping_attributes,
    get_default_table_name,
)
from src.stairlight.source.dbt.config import StairlightConfigIncludeDbt
from src.stairlight.source.file.config import StairlightConfigIncludeFile
from src.stairlight.source.gcs.config import StairlightConfigIncludeGcs
from src.stairlight.source.redash.config import StairlightConfigIncludeRedash
from src.stairlight.source.s3.config import StairlightConfigIncludeS3
from src.stairlight.source.template import Template

logger = logging.getLogger()


class Configurator:
    def __init__(self, dir: str) -> None:
        """Configuration class

        Args:
            dir (str): A directory that Configuration files exists.
        """
        self.dir = dir

    def read_stairlight(self, prefix: str) -> StairlightConfig:
        """Read stairlight configurations from yaml

        Args:
            prefix (str, optional):
                Prefix of the configuration file name.

        Returns:
            StairlightConfig: Stairlight configurations
        """
        config = self.read(prefix=prefix)
        return StairlightConfig(**config)

    def read_mapping_with_regex(self, regex_list: list[str]) -> MappingConfig:
        config: dict[str, Any] = {}
        for regex in regex_list:
            config = sl_util.deep_merge(original=config, add=self.read(pattern=regex))
        return MappingConfig(**config)

    def read_mapping_with_prefix(self, prefix: str) -> MappingConfig:
        """Read mapping configurations from yaml

        Args:
            prefix (str):
                Prefix of the configuration file name.

        Returns:
            MappingConfig: Mapping configurations
        """
        config = self.read(prefix=prefix)
        return MappingConfig(**config)

    def read(self, pattern: str = "", prefix: str = "") -> dict[str, Any]:
        """Read a configuration file

        Args:
            prefix (str): Prefix of configuration file name.

        Returns:
            dict: configurations
        """
        results: dict[str, Any] = {}
        pattern = rf"^{self.dir}/{prefix}\.ya?ml$" if not pattern else pattern
        config_files = [
            p
            for p in glob.glob(f"{self.dir}/**", recursive=False)
            if re.fullmatch(pattern, p)
        ]
        for config_file in config_files:
            with open(config_file) as file:
                config = yaml.safe_load(file)
            results = sl_util.deep_merge(original=results, add=config)
        return results

    def create_stairlight_file(self, prefix: str) -> str:
        """Create a Stairlight template file

        Args:
            prefix (str, optional):
                Prefix of the configuration file name.

        Returns:
            str: Created file name
        """
        template_file_name = f"{self.dir}/{prefix}.yaml"
        with open(template_file_name, "w") as f:
            yaml.add_representer(OrderedDict, self.represent_odict)
            yaml.dump(self.build_stairlight_config(), f)
        return template_file_name

    def create_mapping_file(
        self,
        config: list[dict[str, Any]] | list[str] | dict,
        prefix: str,
    ) -> str:
        """Create a mapping template file

        Args:
            config (list[dict]): config to write
            prefix (str):
                Prefix of file name.

        Returns:
            str: Created file name
        """
        now = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        template_file_name = f"{self.dir}/.{prefix}_{now}.yaml"

        with open(template_file_name, "w") as f:
            yaml.add_representer(
                data_type=OrderedDict, representer=self.represent_odict
            )
            yaml.dump(
                config,
                stream=f,
            )
        return template_file_name

    @staticmethod
    def represent_odict(
        dumper: yaml.Dumper, odict: OrderedDict
    ) -> yaml.nodes.MappingNode:
        """Set for dumping a YAML file in order of OrderedDict

        Args:
            dumper (yaml.Dumper): Dumper
            odict (OrderedDict): Ordered dict

        Returns:
            yaml.nodes.MappingNode: Mapping node
        """
        return dumper.represent_mapping(
            tag="tag:yaml.org,2002:map", mapping=odict.items()
        )

    @staticmethod
    def build_stairlight_config() -> OrderedDict:
        """Create a OrderedDict object for stairlight configurations

        Returns:
            OrderedDict: Template dict for stairlight.yaml
        """
        return OrderedDict(
            asdict(
                StairlightConfig(
                    Include=[
                        OrderedDict(asdict(StairlightConfigIncludeFile())),
                        OrderedDict(asdict(StairlightConfigIncludeGcs())),
                        OrderedDict(asdict(StairlightConfigIncludeRedash())),
                        OrderedDict(asdict(StairlightConfigIncludeDbt())),
                        OrderedDict(asdict(StairlightConfigIncludeS3())),
                    ],
                    Exclude=[OrderedDict(asdict(StairlightConfigExclude()))],
                    Settings=OrderedDict(asdict(StairlightConfigSettings())),
                )
            ),
        )

    def build_mapping_config(
        self, detected_templates: list[dict[str, Any]]
    ) -> OrderedDict:
        """Create a OrderedDict object for mapping configurations

        Args:
            detected_templates (list[dict[str, Any]]):
                Templates that Stairlight detected

        Returns:
            OrderedDict: Template dict for mapping.yaml
        """
        # Mapping section
        mappings: list[MappingConfigMapping] = []
        detected_template: dict[str, Any]
        for detected_template in detected_templates:
            # Tables.Parameters
            parameters: OrderedDict = OrderedDict()

            if MapKey.PARAMETERS in detected_template:
                undefined_params: list[str] = detected_template.get(MapKey.PARAMETERS)
                for undefined_param in undefined_params:
                    splitted_params = undefined_param.split(".")
                    create_nested_dict(keys=splitted_params, results=parameters)

            # To avoid outputting empty fields
            if len(parameters) == 0:
                parameters = None

            template: Template = detected_template.get(MapKey.TEMPLATE)
            mapping_table = MappingConfigMappingTable(
                TableName=get_default_table_name(template=template),
                Parameters=parameters,
            )
            table = OrderedDict(
                asdict(
                    mapping_table,
                    dict_factory=dict_factory,
                )
            )
            mapping: MappingConfigMapping = collect_mapping_attributes(
                template=template,
                tables=[table],
            )
            mappings.append(mapping)

        mapping_config = asdict(
            MappingConfig(
                Mapping=[OrderedDict(asdict(mapping)) for mapping in mappings],
            ),
            dict_factory=dict_factory,
        )
        return OrderedDict(mapping_config)


def dict_factory(d):
    return {k: v for (k, v) in d if v is not None}


def create_nested_dict(
    keys: list[str],
    results: dict[str, Any],
    density: int = 0,
    default_value: Any = None,
) -> None:
    """create nested dict from list

    Args:
        keys (list[str]): a list of dict keys
        results (dict[str, Any]): Nested dict
        density (int, optional): Density of the dict. Defaults to 0.
        default_value (any, optional): Default dict value. Defaults to None.
    """
    key = keys[density]
    if density < len(keys) - 1:
        if key not in results:
            results[key] = {}
        create_nested_dict(keys=keys, results=results[key], density=density + 1)
    else:
        results[key] = default_value
