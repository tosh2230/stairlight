import glob
import logging
import re
from collections import OrderedDict
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

import yaml

from .key import MapKey, MappingConfigKey, StairlightConfigKey
from .source.base import Template, TemplateSourceType

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
        unmapped: "list[dict]",
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
        include_section_file = OrderedDict(
            {
                StairlightConfigKey.TEMPLATE_SOURCE_TYPE: TemplateSourceType.FILE.value,
                StairlightConfigKey.File.FILE_SYSTEM_PATH: None,
                StairlightConfigKey.REGEX: None,
                StairlightConfigKey.DEFAULT_TABLE_PREFIX: None,
            }
        )
        include_section_gcs = OrderedDict(
            {
                StairlightConfigKey.TEMPLATE_SOURCE_TYPE: TemplateSourceType.GCS.value,
                StairlightConfigKey.Gcs.PROJECT_ID: None,
                StairlightConfigKey.Gcs.BUCKET_NAME: None,
                StairlightConfigKey.REGEX: None,
                StairlightConfigKey.DEFAULT_TABLE_PREFIX: None,
            }
        )
        include_section_redash = OrderedDict(
            {
                StairlightConfigKey.TEMPLATE_SOURCE_TYPE: (
                    TemplateSourceType.REDASH.value
                ),
                StairlightConfigKey.Redash.DATABASE_URL_ENV_VAR: "REDASH_DATABASE_URL",
                StairlightConfigKey.Redash.DATA_SOURCE_NAME: None,
                StairlightConfigKey.Redash.QUERY_IDS: {},
            }
        )
        include_section_dbt = OrderedDict(
            {
                StairlightConfigKey.TEMPLATE_SOURCE_TYPE: TemplateSourceType.DBT.value,
                StairlightConfigKey.Dbt.PROJECT_DIR: None,
                StairlightConfigKey.Dbt.PROFILES_DIR: None,
                StairlightConfigKey.Dbt.TARGET: None,
                StairlightConfigKey.Dbt.VARS: OrderedDict({"key": "value"}),
            }
        )
        return OrderedDict(
            {
                StairlightConfigKey.INCLUDE_SECTION: [
                    include_section_file,
                    include_section_gcs,
                    include_section_redash,
                    include_section_dbt,
                ],
                StairlightConfigKey.EXCLUDE_SECTION: [
                    OrderedDict(
                        {
                            StairlightConfigKey.TEMPLATE_SOURCE_TYPE: None,
                            StairlightConfigKey.DEFAULT_TABLE_PREFIX: None,
                        }
                    )
                ],
                StairlightConfigKey.SETTING_SECTION: {
                    StairlightConfigKey.MAPPING_PREFIX: MAPPING_CONFIG_PREFIX_DEFAULT
                },
            }
        )

    def build_mapping_config(self, unmapped_templates: "list[dict]") -> OrderedDict:
        """Create a OrderedDict for mapping.yaml

        Args:
            unmapped_templates (list[dict]): unmapped settings that Stairlight detects

        Returns:
            OrderedDict: mapping.yaml template
        """
        mapping_config_dict = OrderedDict(
            {
                MappingConfigKey.GLOBAL_SECTION: [],
                MappingConfigKey.MAPPING_SECTION: [],
            }
        )

        # List(instead of Set) because OrderedDict is not hashable
        parameters_set: list[OrderedDict] = []
        global_parameters: dict = {}

        # Mapping section
        unmapped_template: dict
        for unmapped_template in unmapped_templates:
            template: Template = unmapped_template[MapKey.TEMPLATE]
            mapping_values = OrderedDict(
                {
                    MappingConfigKey.TEMPLATE_SOURCE_TYPE: template.source_type.value,
                }
            )
            mapping_values.update(
                self.select_mapping_values_by_template(template=template)
            )

            # Tables
            mapping_values[MappingConfigKey.TABLES] = [
                OrderedDict(
                    {
                        MappingConfigKey.TABLE_NAME: self.get_default_table_name(
                            template=template
                        )
                    }
                )
            ]

            # Parameters
            if MapKey.PARAMETERS in unmapped_template:
                undefined_params: list[str] = unmapped_template.get(MapKey.PARAMETERS)
                parameters = OrderedDict()
                for undefined_param in undefined_params:
                    splitted_params = undefined_param.split(".")
                    create_nested_dict(keys=splitted_params, results=parameters)

                if parameters:
                    mapping_values[MappingConfigKey.TABLES][0][
                        MappingConfigKey.PARAMETERS
                    ] = parameters

                if parameters in parameters_set:
                    global_parameters.update(parameters)
                else:
                    parameters_set.append(parameters)

            # Labels
            mapping_values[MappingConfigKey.TABLES][0][
                MappingConfigKey.LABELS
            ] = OrderedDict({"key": "value"})

            mapping_config_dict[MappingConfigKey.MAPPING_SECTION].append(mapping_values)

        # Global section
        mapping_config_dict[MappingConfigKey.GLOBAL_SECTION] = OrderedDict(
            deepcopy({MappingConfigKey.PARAMETERS: global_parameters})
        )

        # Metadata section
        mapping_config_dict[MappingConfigKey.METADATA_SECTION] = [
            OrderedDict(
                {
                    MappingConfigKey.TABLE_NAME: None,
                    MappingConfigKey.LABELS: OrderedDict({"key": "value"}),
                }
            )
        ]

        return mapping_config_dict

    @staticmethod
    def select_mapping_values_by_template(template: Template) -> dict:
        mapping_values: dict = {}
        if template.source_type == TemplateSourceType.FILE:
            mapping_values[MappingConfigKey.File.FILE_SUFFIX] = template.key
        elif template.source_type == TemplateSourceType.GCS:
            mapping_values[MappingConfigKey.Gcs.URI] = template.uri
            mapping_values[MappingConfigKey.Gcs.BUCKET_NAME] = template.bucket
        elif template.source_type == TemplateSourceType.REDASH:
            mapping_values[MappingConfigKey.Redash.QUERY_ID] = template.query_id
            mapping_values[
                MappingConfigKey.Redash.DATA_SOURCE_NAME
            ] = template.data_source_name
        elif template.source_type == TemplateSourceType.DBT:
            mapping_values[MappingConfigKey.Dbt.PROJECT_NAME] = template.project_name
            mapping_values[MappingConfigKey.Dbt.FILE_SUFFIX] = template.key
        return mapping_values

    @staticmethod
    def get_default_table_name(template: Template) -> str:
        default_table_name: str = None
        if template.source_type == TemplateSourceType.REDASH:
            default_table_name = template.uri
        else:
            default_table_name = Path(template.key).stem
        return default_table_name


def create_nested_dict(
    keys: "list[str]", results: OrderedDict, density: int = 0, default_value: any = None
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


class ConfigKeyNotFoundException(Exception):
    def __init__(self, msg: str) -> None:
        self.msg = msg

    def __str__(self) -> str:
        return self.msg


def get_config_value(
    key: str,
    target: dict,
    fail_if_not_found: bool = False,
    enable_logging: bool = False,
) -> any:
    value = target.get(key)
    if not value:
        msg = f"{key} is not found in the configuration: {target}"
        if fail_if_not_found:
            raise ConfigKeyNotFoundException(msg=msg)
        if enable_logging:
            logger.warning(msg=msg)
    return value
