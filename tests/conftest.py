import glob
import os
from typing import Iterator

import pytest

from src.stairlight import (
    MAPPING_CONFIG_PREFIX_DEFAULT,
    STAIRLIGHT_CONFIG_PREFIX_DEFAULT,
    StairLight,
)
from src.stairlight.configurator import Configurator
from src.stairlight.source.config import MappingConfig, StairlightConfig


@pytest.fixture(scope="session")
def configurator() -> Configurator:
    return Configurator(dir="tests/config")


@pytest.fixture(scope="session")
def stairlight_config(configurator: Configurator) -> StairlightConfig:
    return configurator.read_stairlight(prefix=STAIRLIGHT_CONFIG_PREFIX_DEFAULT)


@pytest.fixture(scope="session")
def mapping_config(configurator: Configurator) -> MappingConfig:
    return configurator.read_mapping_with_prefix(prefix=MAPPING_CONFIG_PREFIX_DEFAULT)


@pytest.fixture(scope="session")
def mapping_config_single(configurator: Configurator) -> MappingConfig:
    return configurator.read_mapping_with_prefix(prefix="mapping_single")


@pytest.fixture(scope="session")
def tests_abspath() -> str:
    return os.path.dirname(os.path.abspath(__file__))


@pytest.fixture(scope="session")
def stairlight_save() -> Iterator[StairLight]:
    save_file = "./tests/test_save_map.json"
    stairlight = StairLight(config_dir="tests/config", save_file=save_file)
    stairlight.create_map()
    yield stairlight
    teardown_rm_file(save_file)
    teardown_rm_config(
        pathname=(
            "tests/config/.unmapped_[0-9][0-9][0-9][0-9]"
            "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9].yaml"
        )
    )


@pytest.fixture(scope="session")
def stairlight_template_prefix() -> Iterator[str]:
    prefix = "pytest_stairlight"
    yield prefix
    teardown_rm_config(pathname=f"tests/config/{prefix}*.yaml")


@pytest.fixture(scope="session")
def prefix_unmapped() -> Iterator[str]:
    prefix = "unmapped"
    yield prefix
    teardown_rm_config(pathname=f"tests/config/.{prefix}*.yaml")


@pytest.fixture(scope="session")
def prefix_not_found() -> Iterator[str]:
    prefix = "not_found"
    yield prefix
    teardown_rm_config(pathname=f"tests/config/.{prefix}*.yaml")


def teardown_rm_file(file: str) -> None:
    if os.path.exists(file):
        os.remove(file)


def teardown_rm_config(pathname: str) -> None:
    rm_files = glob.glob(pathname=pathname)
    for file in rm_files:
        os.remove(file)
