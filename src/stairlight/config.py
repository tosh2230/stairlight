import glob
import logging
import re
from collections import OrderedDict
from datetime import datetime, timezone

import yaml

from .template import SourceType

MAPPING_CONFIG_PREFIX = "mapping"
STAIRLIGHT_CONFIG_PREFIX = "stairlight"

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
        self, prefix: str = STAIRLIGHT_CONFIG_PREFIX
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
            yaml.dump(self.build_stairlight_template(), f)
        return template_file_name

    def create_mapping_template_file(
        self, unmapped: list, prefix: str = MAPPING_CONFIG_PREFIX
    ) -> str:
        """Create a mapping template file

        Args:
            unmapped (list): Unmapped results
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
            yaml.dump(self.build_mapping_template(unmapped), f)
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
    def build_stairlight_template() -> OrderedDict:
        """Create a OrderedDict object for file 'stairlight.config'

        Returns:
            OrderedDict: stairlight.config template
        """
        return OrderedDict(
            {
                "include": [
                    OrderedDict(
                        {
                            "type": "fs",
                            "path": None,
                            "regex": None,
                            "default_table_prefix": None,
                        }
                    ),
                    OrderedDict(
                        {
                            "type": "gcs",
                            "project": None,
                            "bucket": None,
                            "regex": None,
                            "default_table_prefix": None,
                        }
                    ),
                ],
                "exclude": [OrderedDict({"type": None, "regex": None})],
                "settings": {"mapping_prefix": MAPPING_CONFIG_PREFIX},
            }
        )

    @staticmethod
    def build_mapping_template(unmapped_templates: list) -> OrderedDict:
        """Create a OrderedDict for mapping.config

        Args:
            unmapped (list): unmapped settings that Stairlight detects

        Returns:
            OrderedDict: mapping.config template
        """
        template = OrderedDict({"mapping": []})
        for unmapped_template in unmapped_templates:
            sql_template = unmapped_template["sql_template"]
            values = OrderedDict(
                {
                    "type": sql_template.source_type,
                    "tables": [OrderedDict({"table": None})],
                }
            )

            params = None
            if "params" in unmapped_template:
                undefined_params = unmapped_template.get("params")
                params = OrderedDict({})
                for param in undefined_params:
                    param_str = ".".join(param.split(".")[1:])
                    params[param_str] = None

            if params:
                values["tables"][0]["params"] = params

            values["tables"][0]["labels"] = OrderedDict({"key": "value"})

            if sql_template.source_type in [SourceType.FS]:
                values["file_suffix"] = sql_template.file_path
            elif sql_template.source_type in [SourceType.GCS]:
                values["uri"] = sql_template.uri
                values["bucket"] = sql_template.bucket

            template["mapping"].append(values)

        template["metadata"] = [
            OrderedDict({"table": None, "labels": OrderedDict({"key": "value"})})
        ]

        return template
