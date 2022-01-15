import src.stairlight.config as config
from src.stairlight.map import Map


class TestSuccess:
    configurator = config.Configurator(dir="./config")
    mapping_config = configurator.read(prefix=config.MAPPING_CONFIG_PREFIX)
    stairlight_config = configurator.read(prefix=config.STAIRLIGHT_CONFIG_PREFIX)
    dependency_map = Map(
        stairlight_config=stairlight_config, mapping_config=mapping_config
    )
    dependency_map.write()

    def test_mapped(self):
        assert len(self.dependency_map.mapped) > 0

    def test_unmapped(self):
        assert len(self.dependency_map.unmapped) > 0
