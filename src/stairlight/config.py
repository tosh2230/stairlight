import glob
import re
from datetime import datetime, timezone

import yaml

from src.stairlight.template import SourceType

MAP_CONFIG_PREFIX = "mapping"
STRL_CONFIG_PREFIX = "stairlight"


class Configurator:
    def __init__(self, path):
        self.path = path

    def read(self, prefix):
        config = None
        pattern = f"^{self.path}{prefix}.ya?ml$"
        config_file = [
            p
            for p in glob.glob(f"{self.path}/**", recursive=False)
            if re.fullmatch(pattern, p)
        ]
        if config_file:
            with open(config_file[0]) as file:
                config = yaml.safe_load(file)
        return config

    def make_mapping_template(self, undefined_files):
        now = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        file_name = f"{self.path}mapping_{now}.yaml"
        with open(file_name, "w") as f:
            yaml.safe_dump(self.build_template_dict(undefined_files), f)
        return file_name

    @staticmethod
    def build_template_dict(undefined_files):
        template = {"mapping": []}
        for undefined_file in undefined_files:
            params = None
            if "params" in undefined_file:
                undefined_params = undefined_file.get("params")
                params = {}
                for param in undefined_params:
                    params[param] = None
            sql_template = undefined_file["sql_template"]
            values = {
                "file_suffix": sql_template.file_path,
                "tables": [{"table": None}],
            }
            if params:
                values["tables"][0]["params"] = params
            if sql_template.source_type in [SourceType.GCS]:
                values["uri"] = sql_template.uri
                values["bucket"] = sql_template.bucket
            template["mapping"].append(values)
        return template
