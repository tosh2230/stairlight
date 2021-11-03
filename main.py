import logging

from stairlight import StairLight
import stairlight.config as config


def set_logging():
    level = fmt = datefmt = None
    config_reader = config.Reader(path="./config/")
    strl_config = config_reader.read(config.STRL_CONFIG).get("logging")
    if strl_config:
        level = strl_config.get("level")
        fmt = strl_config.get("fmt")
        datefmt = strl_config.get("datefmt")
    logging.basicConfig(level=level, format=fmt, datefmt=datefmt)


if __name__ == "__main__":
    set_logging()

    stair_light = StairLight()
    stair_light.all()
    stair_light.up(table_name="PROJECT_D.DATASET_E.TABLE_F", recursive=True)
    stair_light.down(table_name="PROJECT_C.DATASET_C.TABLE_C", recursive=True)
    # stair_light.make_config()
