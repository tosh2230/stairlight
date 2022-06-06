import glob
import os
from typing import Iterator

import pytest

from src.stairlight.config import (
    Configurator,
    MAPPING_CONFIG_PREFIX_DEFAULT,
    STAIRLIGHT_CONFIG_PREFIX_DEFAULT,
)
from src.stairlight.stairlight import StairLight


@pytest.fixture(scope="session")
def configurator() -> Configurator:
    return Configurator(dir="tests/config")


@pytest.fixture(scope="session")
def stairlight_config(configurator: Configurator) -> dict:
    return configurator.read(prefix=STAIRLIGHT_CONFIG_PREFIX_DEFAULT)


@pytest.fixture(scope="session")
def mapping_config(configurator: Configurator) -> dict:
    return configurator.read(prefix=MAPPING_CONFIG_PREFIX_DEFAULT)


@pytest.fixture(scope="session")
def tests_abspath() -> str:
    return os.path.dirname(os.path.abspath(__file__))


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
def stairlight_template_prefix() -> Iterator[str]:
    prefix = "pytest_stairlight"
    yield prefix
    teardown_rm_config(pathname=f"tests/config/{prefix}*.yaml")


@pytest.fixture(scope="session")
def mapping_template_prefix() -> Iterator[str]:
    prefix = "pytest_mapping"
    yield prefix
    teardown_rm_config(pathname=f"tests/config/{prefix}*.yaml")


def teardown_rm_file(file: str) -> None:
    if os.path.exists(file):
        os.remove(file)


def teardown_rm_config(pathname: str) -> None:
    rm_files = glob.glob(pathname=pathname)
    for file in rm_files:
        os.remove(file)
