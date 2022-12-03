from typing import OrderedDict

from src.stairlight.source.config import MappingConfig


class TestMappingConfigEmpty:
    def test_get_global(self):
        mapping_config = MappingConfig(Mapping=[OrderedDict({})])
        assert mapping_config.get_global()

    def test_get_mapping(self):
        mapping_config = MappingConfig(Mapping=[OrderedDict({})])
        assert mapping_config.get_global()

    def test_get_metadata(self):
        mapping_config = MappingConfig(Mapping=[OrderedDict({})])
        assert mapping_config.get_global()
