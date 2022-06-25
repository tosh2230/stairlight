import glob
import logging
import re
from collections import OrderedDict
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Dict, List

import yaml

from .source.config import (
    MappingConfig,
    MappingConfigGlobal,
    MappingConfigMapping,
    MappingConfigMappingTable,
    MappingConfigMetadata,
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
            path (str): Configuration file path
        """
        self.dir = dir

    def read_stairlight(
        self, prefix: str = STAIRLIGHT_CONFIG_PREFIX_DEFAULT
    ) -> StairlightConfig:
        config = self.read(prefix=prefix)
        return StairlightConfig(**config)

    def read_mapping(
        self, prefix: str = MAPPING_CONFIG_PREFIX_DEFAULT
    ) -> MappingConfig:
        config = self.read(prefix=prefix)
        return MappingConfig(**config)

    def read(self, prefix: str) -> Dict[str, Any]:
        """Read a configuration file

        Args:
            prefix (str): Configuration file name prefix

        Returns:
            dict: Results from reading configuration file
        """
        config: Dict[str, Any] = {}
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
            prefix (str, optional): File prefix. Defaults to STAIRLIGHT_CONFIG_PREFIX.

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
        unmapped: List[Dict[str, Any]],
        prefix: str = MAPPING_CONFIG_PREFIX_DEFAULT,
    ) -> str:
        """Create a mapping template file

        Args:
            unmapped (list[dict]): Unmapped results
            prefix (str, optional): File prefix. Defaults to MAPPING_CONFIG_PREFIX.

        Returns:
            str: Mapping template file
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
        """Create a OrderedDict object for dumping a YAML file
        in order of OrderedDict"""
        return dumper.represent_mapping(
            tag="tag:yaml.org,2002:map", mapping=odict.items()
        )

    @staticmethod
    def build_stairlight_config() -> OrderedDict:
        """Create a OrderedDict object for file 'stairlight.config'

        Returns:
            OrderedDict: stairlight.config template
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
        self, unmapped_templates: List[Dict[str, Any]]
    ) -> OrderedDict:
        """Create a OrderedDict for mapping.yaml

        Args:
            unmapped_templates (list[dict]): unmapped settings that Stairlight detects

        Returns:
            OrderedDict: mapping.yaml template
        """
        # List(instead of Set) because OrderedDict is not hashable
        parameters_set: List[OrderedDict] = []
        global_parameters: Dict[str, Any] = {}

        # Mapping section
        mappings: List[MappingConfigMapping] = []
        unmapped_template: Dict[str, Any]
        for unmapped_template in unmapped_templates:
            # Tables.Parameters
            parameters: OrderedDict = OrderedDict()
            template: Template = unmapped_template[MapKey.TEMPLATE]

            if MapKey.PARAMETERS in unmapped_template:
                undefined_params: List[str] = unmapped_template.get(
                    MapKey.PARAMETERS, []
                )
                for undefined_param in undefined_params:
                    splitted_params = undefined_param.split(".")
                    create_nested_dict(keys=splitted_params, results=parameters)

                if parameters in parameters_set:
                    global_parameters.update(parameters)
                else:
                    parameters_set.append(parameters)

            table = OrderedDict(
                asdict(
                    MappingConfigMappingTable(
                        TableName=get_default_table_name(template=template),
                        Parameters=parameters,
                        IgnoreParameters=[],
                        Labels=OrderedDict(),
                    )
                )
            )
            mapping: MappingConfigMapping = collect_mapping_attributes(
                template=template,
                tables=[table],
            )
            mappings.append(mapping)

        return OrderedDict(
            asdict(
                MappingConfig(
                    Global=OrderedDict(
                        asdict(MappingConfigGlobal(Parameters=global_parameters))
                    ),
                    Mapping=[OrderedDict(asdict(mapping)) for mapping in mappings],
                    Metadata=[OrderedDict(asdict(MappingConfigMetadata()))],
                )
            )
        )


def create_nested_dict(
    keys: List[str],
    results: Dict[str, Any],
    density: int = 0,
    default_value: Any = None,
) -> None:
    """create nested dict from list

    Args:
        keys (list): Dict keys
        results (dict[str, Any]): Nested dict
        density (int, optional): Density. Defaults to 0.
        default_value (any, optional): Default dict value. Defaults to None.
    """
    key = keys[density]
    if density < len(keys) - 1:
        if key not in results:
            results[key] = {}
        create_nested_dict(keys=keys, results=results[key], density=density + 1)
    else:
        results[key] = default_value
