import glob
import logging
import re
from collections import OrderedDict
from datetime import datetime, timezone

import yaml

from .template import SourceType

MAP_CONFIG_PREFIX = "mapping"
STRL_CONFIG_PREFIX = "stairlight"

logger = logging.getLogger()


class Configurator:
    def __init__(self, path):
        self.path = path

    def read(self, prefix):
        config = None
        pattern = f"^{self.path}/{prefix}.ya?ml$"
        config_file = [
            p
            for p in glob.glob(f"{self.path}/**", recursive=False)
            if re.fullmatch(pattern, p)
        ]
        if config_file:
            with open(config_file[0]) as file:
                config = yaml.safe_load(file)
        return config

    def create_stairlight_template(self):
        file_name = f"{self.path}/{STRL_CONFIG_PREFIX}.yaml"
        with open(file_name, "w") as f:
            yaml.add_representer(OrderedDict, self.represent_odict)
            yaml.dump(self.build_stairlight_template(), f)
        return file_name

    def create_mapping_template(self, unmapped):
        now = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        file_name = f"{self.path}/{MAP_CONFIG_PREFIX}_{now}.yaml"
        with open(file_name, "w") as f:
            yaml.add_representer(OrderedDict, self.represent_odict)
            yaml.dump(self.build_mapping_template(unmapped), f)
        return file_name

    @staticmethod
    def represent_odict(dumper, instance):
        return dumper.represent_mapping("tag:yaml.org,2002:map", instance.items())

    @staticmethod
    def build_stairlight_template():
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
                "exclude": [
                    OrderedDict(
                        {
                            "type": None,
                            "regex": None,
                        }
                    )
                ],
                "settings": {"mapping_prefix": MAP_CONFIG_PREFIX},
            }
        )

    @staticmethod
    def build_mapping_template(unmapped):
        template = OrderedDict({"mapping": []})
        for unmapped_file in unmapped:
            params = None
            if "params" in unmapped_file:
                undefined_params = unmapped_file.get("params")
                params = OrderedDict({})
                for param in undefined_params:
                    params[param] = None
            sql_template = unmapped_file["sql_template"]
            values = OrderedDict(
                {
                    "file_suffix": sql_template.file_path,
                    "tables": [OrderedDict({"table": None})],
                }
            )
            if params:
                values["tables"][0]["params"] = params
            if sql_template.source_type in [SourceType.GCS]:
                values["uri"] = sql_template.uri
                values["bucket"] = sql_template.bucket
            template["mapping"].append(values)
        return template
