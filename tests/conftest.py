import glob
import os
import pytest
from typing import Iterator

from src.stairlight import config_key
from src.stairlight.config import Configurator
from src.stairlight.map import Map
from src.stairlight.stairlight import StairLight


@pytest.fixture(scope="session")
def configurator() -> Configurator:
    return Configurator(dir="tests/config")


@pytest.fixture(scope="session")
def stairlight_config(configurator: Configurator) -> dict:
    return configurator.read(prefix=config_key.STAIRLIGHT_CONFIG_FILE_PREFIX)


@pytest.fixture(scope="session")
def mapping_config(configurator: Configurator) -> dict:
    return configurator.read(prefix=config_key.MAPPING_CONFIG_FILE_PREFIX)


@pytest.fixture(scope="session")
def dependency_map(stairlight_config: dict, mapping_config: dict) -> Map:
    dependency_map = Map(
        stairlight_config=stairlight_config, mapping_config=mapping_config
    )
    dependency_map.write()
    return dependency_map


@pytest.fixture(scope="session")
def tests_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))


@pytest.fixture(scope="session")
def stairlight_init() -> Iterator[StairLight]:
    stairlight = StairLight(config_dir="./tests")
    stairlight.create_map()
    yield stairlight
    teardown_rm_file("./tests/stairlight.yaml")


@pytest.fixture(scope="session")
def stairlight_save(save_file="./tests/test_save_map.json") -> Iterator[StairLight]:
    stairlight = StairLight(config_dir="tests/config", save_file=save_file)
    stairlight.create_map()
    yield stairlight
    teardown_rm_file(save_file)
    teardown_rm_config(prefix="mapping_checked_")


@pytest.fixture(scope="session")
def stairlight_load_and_save() -> Iterator[StairLight]:
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


def teardown_rm_file(file: str) -> None:
    if os.path.exists(file):
        os.remove(file)


def teardown_rm_config(prefix: str) -> None:
    rm_files = glob.glob(f"tests/config/{prefix}*.yaml")
    for file in rm_files:
        os.remove(file)


@pytest.fixture
def stairlight_template() -> Iterator[str]:
    prefix = "pytest_stairlight"
    yield prefix
    teardown_rm_config(prefix=prefix)


@pytest.fixture
def mapping_template() -> Iterator[str]:
    prefix = "pytest_mapping"
    yield prefix
    teardown_rm_config(prefix=prefix)
