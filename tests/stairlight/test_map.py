import pytest

from src.stairlight import map_key
import src.stairlight.config as config
from src.stairlight.config_key import (
    MAPPING_CONFIG_FILE_PREFIX,
    STAIRLIGHT_CONFIG_FILE_PREFIX,
)
from src.stairlight.map import Map
from src.stairlight.source.file import (
    FileTemplate,
    TemplateSourceType,
)


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

    @pytest.mark.parametrize(
        "key, expected",
        [
            (
                "tests/sql/main/test_undefined.sql",
                [
                    "params.main_table",
                    "params.sub_table_01",
                    "params.sub_table_02",
                ],
            ),
            (
                "tests/sql/main/test_undefined_part.sql",
                [
                    "params.sub_table_01",
                    "params.sub_table_02",
                ],
            ),
        ],
    )
    def test_find_unmapped_params(self, key, expected):
        actual = None
        for a in self.dependency_map.unmapped:
            sql_template = a.get(map_key.TEMPLATE)
            if sql_template.key == key:
                actual = sorted(a.get(map_key.PARAMETERS))
                break
        assert actual == expected
