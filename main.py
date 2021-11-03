import logging

from stairlight import StairLight

LOG_LEVEL = logging.INFO


def set_log_config():
    fmt = (
        "%(asctime)s.%(msecs)03d %(filename)s:%(funcName)s:%(lineno)d "
        "[%(levelname)s]%(message)s"
    )
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(level=LOG_LEVEL, format=fmt, datefmt=datefmt)


if __name__ == "__main__":
    set_log_config()
    logger = logging.getLogger(__name__)

    stair_light = StairLight()
    stair_light.all()
    stair_light.up(table_name="PROJECT_D.DATASET_E.TABLE_F")
    stair_light.down(table_name="PROJECT_C.DATASET_C.TABLE_C")
    stair_light.make_config()
