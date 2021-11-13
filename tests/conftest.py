import pytest

from src.stairlight.stairlight import StairLight


@pytest.fixture(scope="session")
def stair_light():
    stairlight = StairLight(config_path="./config")
    return stairlight
