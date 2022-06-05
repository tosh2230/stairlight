import glob
import os
from typing import Iterator

import pytest

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
    stairlight = StairLight(config_dir="tests/config/test_init")
    stairlight.create_map()
    yield stairlight
    teardown_rm_file(file="tests/config/test_init/stairlight.yaml")


@pytest.fixture(scope="session")
def stairlight_check() -> Iterator[StairLight]:
    stairlight = StairLight(config_dir="tests/config/test_check")
    stairlight.create_map()
    yield stairlight
    teardown_rm_config(pathname="tests/config/test_check/mapping_*.yaml")


@pytest.fixture(scope="session")
def stairlight_check_no_file_found() -> Iterator[StairLight]:
    stairlight = StairLight(config_dir="tests/config/test_check_no_file_found")
    stairlight.create_map()
    yield stairlight


@pytest.fixture(scope="session")
def stairlight_save(save_file="./tests/test_save_map.json") -> Iterator[StairLight]:
    stairlight = StairLight(config_dir="tests/config", save_file=save_file)
    stairlight.create_map()
    yield stairlight
    teardown_rm_file(save_file)
    teardown_rm_config(
        pathname=(
            "tests/config/mapping_[0-9][0-9][0-9][0-9]"
            "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9].yaml"
        )
    )


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


def teardown_rm_config(pathname: str) -> None:
    rm_files = glob.glob(pathname=pathname)
    for file in rm_files:
        os.remove(file)


@pytest.fixture
def stairlight_template_prefix() -> Iterator[str]:
    prefix = "pytest_stairlight"
    yield prefix
    teardown_rm_config(pathname=f"tests/config/{prefix}*.yaml")


@pytest.fixture
def mapping_template_prefix() -> Iterator[str]:
    prefix = "pytest_mapping"
    yield prefix
    teardown_rm_config(pathname=f"tests/config/{prefix}*.yaml")
