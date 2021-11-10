import logging
import json

from stairlight import StairLight, ResponseType


def set_logging():
    level = "INFO"
    fmt = "%(asctime)s.%(msecs)03d %(filename)s:%(funcName)s:%(lineno)d [%(levelname)s]%(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
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
