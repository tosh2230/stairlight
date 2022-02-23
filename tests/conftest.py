import glob
import os

import pytest

from src.stairlight import config_key
import src.stairlight.config as config
from src.stairlight.map import Map
from src.stairlight.stairlight import StairLight


@pytest.fixture(scope="session")
def configurator():
    return config.Configurator(dir="tests/config")


@pytest.fixture(scope="session")
def stairlight_config(configurator):
    return configurator.read(prefix=config_key.STAIRLIGHT_CONFIG_FILE_PREFIX)


@pytest.fixture(scope="session")
def mapping_config(configurator):
    return configurator.read(prefix=config_key.MAPPING_CONFIG_FILE_PREFIX)


@pytest.fixture(scope="session")
def dependency_map(stairlight_config, mapping_config):
    dependency_map = Map(
        stairlight_config=stairlight_config, mapping_config=mapping_config
    )
    dependency_map.write()
    return dependency_map


@pytest.fixture(scope="session")
def tests_dir():
    return os.path.dirname(os.path.abspath(__file__))


@pytest.fixture(scope="session")
def stairlight_init():
    stairlight = StairLight(config_dir="./tests")
    stairlight.create_map()
    yield stairlight
    teardown_rm_file("./tests/stairlight.yaml")


@pytest.fixture(scope="session")
def stairlight_save(save_file="./tests/test_save_map.json"):
    stairlight = StairLight(config_dir="tests/config", save_file=save_file)
    stairlight.create_map()
    yield stairlight
    teardown_rm_file(save_file)
    teardown_rm_config(prefix="mapping_checked_")


@pytest.fixture(scope="session")
def stairlight_load_and_save():
    load_files = [
        "./tests/results/file.json",
        "./tests/results/gcs.json",
    ]
    save_file = "./tests/results/actual.json"
    stairlight = StairLight(
        config_dir="tests/config", load_files=load_files, save_file=save_file
    )
    stairlight.create_map()
    yield stairlight
    teardown_rm_file(save_file)


def teardown_rm_file(file):
    if os.path.exists(file):
        os.remove(file)


def teardown_rm_config(prefix):
    rm_files = glob.glob(f"tests/config/{prefix}*.yaml")
    for file in rm_files:
        os.remove(file)


@pytest.fixture
def stairlight_template():
    prefix = "pytest_stairlight"
    yield prefix
    teardown_rm_config(prefix=prefix)


@pytest.fixture
def mapping_template():
    prefix = "pytest_mapping"
    yield prefix
    teardown_rm_config(prefix=prefix)
