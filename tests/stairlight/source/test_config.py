from typing import OrderedDict

from src.stairlight.source.config import MappingConfig


class TestMappingConfigEmpty:
    def test_get_global(self):
        mapping_config = MappingConfig(Mapping=[OrderedDict({})])
        assert mapping_config.get_global()

    def test_get_mapping(self):
        mapping_config = MappingConfig(Mapping=[OrderedDict({})])
        assert mapping_config.get_mapping()

    def test_get_extra_labels(self):
        mapping_config = MappingConfig(Mapping=[OrderedDict({})])
        assert mapping_config.get_extra_labels()
