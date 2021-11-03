from datetime import datetime, timezone

import yaml

MAP_CONFIG = "./config/mapping.yaml"
STRL_CONFIG = "./config/stairlight.yaml"


def read(config_file):
    config = {}
    if config_file and config_file.endswith((".yml", ".yaml")):
        with open(config_file) as file:
            config = yaml.load(file, Loader=yaml.SafeLoader)
    return config


def make_template(undefined_files):
    now = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    file_name = f"./config/mapping_undefined_{now}.yaml"
    with open(file_name, "w") as f:
        yaml.dump(build_template_dict(undefined_files), f)
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
