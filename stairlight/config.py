from datetime import datetime, timezone
import glob
import re
import yaml

MAP_CONFIG = "mapping"
STRL_CONFIG = "stairlight"


class Reader:
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


def make_template(undefined_files):
    now = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    file_name = f"./config/mapping_undefined_{now}.yaml"
    with open(file_name, "w") as f:
        yaml.safe_dump(build_template_dict(undefined_files), f)
    return file_name


def build_template_dict(undefined_files):
    template = {"mapping": []}
    for undefined_file in undefined_files:
        params = None
        undefined_params = undefined_file.get("params")
        if undefined_params:
            params = {}
            for param in undefined_params:
                params[param] = None
        template["mapping"].append(
            {
                "file_suffix": undefined_file.get("template_file"),
                "table": None,
                "params": params,
            }
        )
    return template
