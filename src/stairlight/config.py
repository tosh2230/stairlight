import glob
import logging
import re
from collections import OrderedDict
from copy import deepcopy
from datetime import datetime, timezone

import yaml

from . import config_key, map_key
from .source.base import Template, TemplateSourceType

logger = logging.getLogger()


class Configurator:
    def __init__(self, dir: str) -> None:
        """Configuration class

        Args:
            path (str): Configuration file path
        """
        self.dir = dir

    def read(self, prefix: str) -> dict:
        """Read a configuration file

        Args:
            prefix (str): Configuration file name prefix

        Returns:
            dict: Results from reading configuration file
        """
        config = None
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

    def create_stairlight_template_file(
        self, prefix: str = config_key.STAIRLIGHT_CONFIG_FILE_PREFIX
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

    def create_mapping_template_file(
        self,
        unmapped: "list[dict]",
        prefix: str = config_key.MAPPING_CONFIG_FILE_PREFIX,
    ) -> str:
        """Create a mapping template file

        Args:
            unmapped (list[dict]): Unmapped results
            prefix (str, optional): File prefix. Defaults to MAPPING_CONFIG_PREFIX.

        Returns:
            str: Mapping template file
        """
        now = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        template_file_name = f"{self.dir}/{prefix}_checked_{now}.yaml"

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
        include_section_file = OrderedDict(
            {
                config_key.TEMPLATE_SOURCE_TYPE: TemplateSourceType.FILE.value,
                config_key.FILE_SYSTEM_PATH: None,
                config_key.REGEX: None,
                config_key.DEFAULT_TABLE_PREFIX: None,
            }
        )
        include_section_gcs = OrderedDict(
            {
                config_key.TEMPLATE_SOURCE_TYPE: TemplateSourceType.GCS.value,
                config_key.PROJECT_ID: None,
                config_key.BUCKET_NAME: None,
                config_key.REGEX: None,
                config_key.DEFAULT_TABLE_PREFIX: None,
            }
        )
        return OrderedDict(
            {
                config_key.STAIRLIGHT_CONFIG_INCLUDE_SECTION: [
                    include_section_file,
                    include_section_gcs,
                ],
                config_key.STAIRLIGHT_CONFIG_EXCLUDE_SECTION: [
                    OrderedDict(
                        {
                            config_key.TEMPLATE_SOURCE_TYPE: None,
                            config_key.DEFAULT_TABLE_PREFIX: None,
                        }
                    )
                ],
                config_key.STAIRLIGHT_CONFIG_SETTING_SECTION: {
                    config_key.MAPPING_PREFIX: config_key.MAPPING_CONFIG_FILE_PREFIX
                },
            }
        )

    def build_mapping_config(self, unmapped_templates: "list[dict]") -> OrderedDict:
        """Create a OrderedDict for mapping.config

        Args:
            unmapped (list[dict]): unmapped settings that Stairlight detects

        Returns:
            OrderedDict: mapping.config template
        """
        mapping_config_dict = OrderedDict(
            {
                config_key.MAPPING_CONFIG_GLOBAL_SECTION: [],
                config_key.MAPPING_CONFIG_MAPPING_SECTION: [],
            }
        )

        all_parameters: list[OrderedDict] = []
        global_parameters: dict = {}

        # Mapping section
        unmapped_template: dict
        for unmapped_template in unmapped_templates:
            template: Template = unmapped_template[map_key.TEMPLATE]
            values = OrderedDict(
                {
                    config_key.TEMPLATE_SOURCE_TYPE: template.source_type.value,
                }
            )

            if template.source_type == TemplateSourceType.FILE:
                values[config_key.FILE_SUFFIX] = template.key
            elif template.source_type == TemplateSourceType.GCS:
                values[config_key.URI] = template.uri
                values[config_key.BUCKET_NAME] = template.bucket
            elif template.source_type == TemplateSourceType.REDASH:
                values[config_key.QUERY_ID] = template.query_id
                values[config_key.DATA_SOURCE_NAME] = template.data_source_name

            # Tables
            values[config_key.TABLES] = [OrderedDict({config_key.TABLE_NAME: None})]
            if template.source_type == TemplateSourceType.REDASH:
                values[config_key.TABLES][0][config_key.TABLE_NAME] = template.uri

            # Parameters
            parameters: OrderedDict = None
            if map_key.PARAMETERS in unmapped_template:
                undefined_params: list[str] = unmapped_template.get(map_key.PARAMETERS)
                parameters = OrderedDict()
                for undefined_param in undefined_params:
                    splitted_params = undefined_param.split(".")
                    create_nested_dict(keys=splitted_params, results=parameters)

            if parameters:
                values[config_key.TABLES][0][config_key.PARAMETERS] = parameters
                if parameters in all_parameters:
                    global_parameters.update(parameters)
                else:
                    all_parameters.append(parameters)

            # Labels
            values[config_key.TABLES][0][config_key.LABELS] = OrderedDict(
                {"key": "value"}
            )

            mapping_config_dict[config_key.MAPPING_CONFIG_MAPPING_SECTION].append(
                values
            )

        # Global section
        mapping_config_dict[config_key.MAPPING_CONFIG_GLOBAL_SECTION] = OrderedDict(
            deepcopy({config_key.PARAMETERS: global_parameters})
        )

        # Metadata section
        mapping_config_dict[config_key.MAPPING_CONFIG_METADATA_SECTION] = [
            OrderedDict(
                {
                    config_key.TABLE_NAME: None,
                    config_key.LABELS: OrderedDict({"key": "value"}),
                }
            )
        ]

        return mapping_config_dict


def create_nested_dict(
    keys: list, results: OrderedDict, density: int = 0, default_value: any = None
) -> None:
    """create nested dict from list

    Args:
        keys (list): Dict keys
        results (OrderedDict): Nested dict
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
