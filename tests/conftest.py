import glob
import os

import pytest

from src.stairlight.stairlight import StairLight


@pytest.fixture(scope="session")
def stairlight(save_file="./tests/test_save_map.json"):
    stairlight = StairLight(config_dir="./config", save_file=save_file)
    yield stairlight
    teardown_save(save_file)


def teardown_save(file):
    if os.path.exists(file):
        os.remove(file)


@pytest.fixture
def stairlight_template():
    prefix = "pytest_stairlight"
    yield prefix
    teardown_config(prefix=prefix)


@pytest.fixture
def mapping_template():
    prefix = "pytest_mapping"
    yield prefix
    teardown_config(prefix=prefix)


def teardown_config(prefix):
    rm_files = glob.glob(f"./config/{prefix}*.yaml")
    for file in rm_files:
        os.remove(file)
