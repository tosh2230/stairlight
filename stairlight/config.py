import glob
import re
from datetime import datetime, timezone

import yaml

from stairlight.template import SourceType

MAP_CONFIG = "mapping"
STRL_CONFIG = "stairlight"


class Configurator:
    def __init__(self, path):
        self.path = path

    def read(self, prefix):
        config = {}
        pattern = f".*{prefix}.ya?ml"
        config_file = [
            p
            for p in glob.glob(f"{self.path}/**", recursive=False)
            if re.search(pattern, p)
        ]
        if config_file:
            with open(config_file[0]) as file:
                config = yaml.safe_load(file)
        return config

    def make_mapping_template(self, undefined_files):
        now = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        file_name = f"{self.path}/mapping_undefined_{now}.yaml"
        with open(file_name, "w") as f:
            yaml.safe_dump(self.build_template_dict(undefined_files), f)
        return file_name

    @staticmethod
    def build_template_dict(undefined_files):
        template = {"mapping": []}
        for undefined_file in undefined_files:
            params = None
            undefined_params = undefined_file.get("params")
            if undefined_params:
                params = {}
                for param in undefined_params:
                    params[param] = None
            sql_template = undefined_file["sql_template"]
            values = {
                "uri": sql_template.uri,
                "file_suffix": sql_template.file_path,
                "tables": {
                    "table": None,
                    "params": params,
                },
            }
            if sql_template.source_type in [SourceType.GCS]:
                values["bucket"] = sql_template.bucket
            template["mapping"].append(values)
        return template
