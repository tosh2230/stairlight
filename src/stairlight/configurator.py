from __future__ import annotations

import glob
import logging
import re
from collections import OrderedDict
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

import yaml

from .source.config import (
    MappingConfig,
    MappingConfigMapping,
    MappingConfigMappingTable,
    StairlightConfig,
    StairlightConfigExclude,
    StairlightConfigSettings,
)
from .source.config_key import MapKey
from .source.controller import collect_mapping_attributes, get_default_table_name
from .source.dbt.config import StairlightConfigIncludeDbt
from .source.file.config import StairlightConfigIncludeFile
from .source.gcs.config import StairlightConfigIncludeGcs
from .source.redash.config import StairlightConfigIncludeRedash
from .source.s3.config import StairlightConfigIncludeS3
from .source.template import Template

logger = logging.getLogger()

STAIRLIGHT_CONFIG_PREFIX_DEFAULT = "stairlight"
MAPPING_CONFIG_PREFIX_DEFAULT = "mapping"


class Configurator:
    def __init__(self, dir: str) -> None:
        """Configuration class

        Args:
            dir (str): A directory that Configuration files exists.
        """
        self.dir = dir

    def read_stairlight(
        self, prefix: str = STAIRLIGHT_CONFIG_PREFIX_DEFAULT
    ) -> StairlightConfig:
        """Read stairlight configurations from yaml

        Args:
            prefix (str, optional):
                Prefix of the configuration file name.
                Defaults to STAIRLIGHT_CONFIG_PREFIX_DEFAULT.

        Returns:
            StairlightConfig: Stairlight configurations
        """
        config = self.read(prefix=prefix)
        return StairlightConfig(**config)

    def read_mapping(
        self, prefix: str = MAPPING_CONFIG_PREFIX_DEFAULT
    ) -> MappingConfig:
        """Read mapping configurations from yaml

        Args:
            prefix (str, optional):
                Prefix of the configuration file name.
                Defaults to MAPPING_CONFIG_PREFIX_DEFAULT.

        Returns:
            MappingConfig: Mapping configurations
        """
        config = self.read(prefix=prefix)
        return MappingConfig(**config)

    def read(self, prefix: str) -> dict[str, Any]:
        """Read a configuration file

        Args:
            prefix (str): Prefix of configuration file name.

        Returns:
            dict: configurations
        """
        config: dict[str, Any] = {}
        pattern = f"^{self.dir}/{prefix}.ya?ml$"
        config_file = [
            p
            for p in glob.glob(f"{self.dir}/**", recursive=False)
            if re.fullmatch(pattern, p)
        ]
        if config_file:
            with open(config_file[0]) as file:
                config = yaml.safe_load(file)
        return config

    def create_stairlight_file(
        self, prefix: str = STAIRLIGHT_CONFIG_PREFIX_DEFAULT
    ) -> str:
        """Create a Stairlight template file

        Args:
            prefix (str, optional):
                Prefix of the configuration file name.
                Defaults to STAIRLIGHT_CONFIG_PREFIX.

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
        unmapped: list[dict[str, Any]],
        prefix: str = MAPPING_CONFIG_PREFIX_DEFAULT,
    ) -> str:
        """Create a mapping template file

        Args:
            unmapped (list[dict]): Unmapped results
            prefix (str, optional):
                Prefix of the configuration file name.
                Defaults to MAPPING_CONFIG_PREFIX.

        Returns:
            str: Created file name
        """
        now = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        template_file_name = f"{self.dir}/{prefix}_{now}.yaml"

        with open(template_file_name, "w") as f:
            yaml.add_representer(
                data_type=OrderedDict, representer=self.represent_odict
            )
            yaml.dump(
                self.build_mapping_config(unmapped_templates=unmapped),
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
        self, unmapped_templates: list[dict[str, Any]]
    ) -> OrderedDict:
        """Create a OrderedDict object for mapping configurations

        Args:
            unmapped_templates (list[dict[str, Any]]):
                Unmapped settings that Stairlight detects

        Returns:
            OrderedDict: Template dict for mapping.yaml
        """
        # Mapping section
        mappings: list[MappingConfigMapping] = []
        unmapped_template: dict[str, Any]
        for unmapped_template in unmapped_templates:
            # Tables.Parameters
            parameters: OrderedDict = OrderedDict()
            template: Template = unmapped_template[MapKey.TEMPLATE]

            if MapKey.PARAMETERS in unmapped_template:
                undefined_params: list[str] = unmapped_template.get(MapKey.PARAMETERS)
                for undefined_param in undefined_params:
                    splitted_params = undefined_param.split(".")
                    create_nested_dict(keys=splitted_params, results=parameters)

            # To avoid outputting empty fields
            if len(parameters) == 0:
                parameters = None

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
        keys (list[str]): Dict keys
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
