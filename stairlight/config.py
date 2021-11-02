from datetime import datetime, timezone
import yaml

MAP_CONFIG = "./config/mapping.yaml"
SQL_CONFIG = "./config/sql.yaml"


def read(config_file):
    config = {}
    if config_file and config_file.endswith((".yml", ".yaml")):
        with open(config_file) as file:
            config = yaml.load(file, Loader=yaml.SafeLoader)
    return config


def make_template(undefined_files):
    now = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    with open(f"./config/mapping_undefined_{now}.yaml", "w") as f:
        yaml.dump(build_template_dict(undefined_files), f)


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
