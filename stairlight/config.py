import yaml

MAP_CONFIG = "./config/mapping.yaml"
SQL_CONFIG = "./config/sql.yaml"


def read(config_file):
    config = {}
    if config_file and config_file.endswith((".yml", ".yaml")):
        with open(config_file) as file:
            config = yaml.load(file, Loader=yaml.SafeLoader)
    return config
