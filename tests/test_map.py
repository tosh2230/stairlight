import stairlight.config as config
from stairlight.map import Map


class TestSuccess:
    config_reader = config.Reader("./config/")
    map_config = config_reader.read(config.MAP_CONFIG)
    strl_config = config_reader.read(config.STRL_CONFIG)
    dependency_map = Map(map_config=map_config, strl_config=strl_config)
    dependency_map.create()

    def test_maps(self):
        assert len(self.dependency_map.maps) > 0

    def test_undefined_files(self):
        assert len(self.dependency_map.undefined_files) == 3
