import logging
import json

from stairlight import StairLight, ResponseType
import stairlight.config as config


def set_logging():
    level = fmt = datefmt = None
    configurator = config.Configurator(path="./config/")
    strl_config = configurator.read(config.STRL_CONFIG).get("logging")
    if strl_config:
        level = strl_config.get("level")
        fmt = strl_config.get("fmt")
        datefmt = strl_config.get("datefmt")
    logging.basicConfig(level=level, format=fmt, datefmt=datefmt)


if __name__ == "__main__":
    set_logging()

    stair_light = StairLight()
    stair_light.set()
    if not stair_light.maps:
        exit()

    print()
    print(
        stair_light.up(
            table_name="PROJECT_D.DATASET_E.TABLE_F", recursive=True, verbose=False
        )
    )

    print()
    print(
        stair_light.up(
            table_name="PROJECT_D.DATASET_E.TABLE_F",
            recursive=True,
            verbose=False,
            response_type=ResponseType.FILE.value,
        )
    )

    print()
    print(
        json.dumps(
            stair_light.up(
                table_name="PROJECT_D.DATASET_E.TABLE_F", recursive=True, verbose=True
            ),
            indent=2,
        )
    )

    print()
    print(
        stair_light.down(
            table_name="PROJECT_C.DATASET_C.TABLE_C", recursive=True, verbose=False
        )
    )

    print()
    print(
        stair_light.down(
            table_name="PROJECT_C.DATASET_C.TABLE_C",
            recursive=True,
            verbose=False,
            response_type=ResponseType.FILE.value,
        )
    )

    print()
    print(
        json.dumps(
            stair_light.down(
                table_name="PROJECT_C.DATASET_C.TABLE_C", recursive=True, verbose=True
            ),
            indent=2,
        )
    )

    # print(stair_light.undefined_files)
    # stair_light.make_config()
