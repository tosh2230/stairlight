from src.stairlight.config_key import (
    MAPPING_CONFIG_FILE_PREFIX,
    STAIRLIGHT_CONFIG_FILE_PREFIX,
)
import src.stairlight.config as config
from src.stairlight.map import Map


class TestSuccess:
    configurator = config.Configurator(dir="./config")
    stairlight_config = configurator.read(prefix=STAIRLIGHT_CONFIG_FILE_PREFIX)
    mapping_config = configurator.read(prefix=MAPPING_CONFIG_FILE_PREFIX)
    dependency_map = Map(
        stairlight_config=stairlight_config, mapping_config=mapping_config
    )
    dependency_map.write()

    def test_mapped(self):
        assert len(self.dependency_map.mapped) > 0

    def test_unmapped(self):
        assert len(self.dependency_map.unmapped) > 0
