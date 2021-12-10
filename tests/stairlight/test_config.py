import os
from collections import OrderedDict

import src.stairlight.config as config
import src.stairlight.template as template


class TestSuccess:
    configurator = config.Configurator(dir="./config")

    def test_read_map(self):
        assert self.configurator.read(prefix=config.MAP_CONFIG_PREFIX)

    def test_read_sql(self):
        assert self.configurator.read(prefix=config.STRL_CONFIG_PREFIX)

    def test_create_stairlight_template(self, stairlight_template):
        file_name = self.configurator.create_stairlight_template(
            prefix=stairlight_template
        )
        assert os.path.exists(file_name)

    def test_create_mapping_template(self, mapping_template):
        file_name = self.configurator.create_mapping_template(
            unmapped=[], prefix=mapping_template
        )
        assert os.path.exists(file_name)

    def test_build_stairlight_template(self):
        stairlight_template = self.configurator.build_stairlight_template()
        assert list(stairlight_template.keys()) == ["include", "exclude", "settings"]

    def test_build_mapping_template(self):
        sql_template = template.SQLTemplate(
            map_config=self.configurator.read(prefix=config.MAP_CONFIG_PREFIX),
            source_type=template.SourceType.FS,
            file_path="tests/sql/main/test_undefined.sql",
        )
        unmapped = [
            {
                "sql_template": sql_template,
                "params": [
                    "params.main_table",
                    "params.sub_table_01",
                    "params.sub_table_02",
                ],
            }
        ]

        value = OrderedDict(
            {
                "file_suffix": sql_template.file_path,
                "tables": [
                    OrderedDict(
                        {
                            "table": None,
                            "params": {
                                "main_table": None,
                                "sub_table_01": None,
                                "sub_table_02": None,
                            },
                        }
                    )
                ],
            }
        )

        expected = {"mapping": [value]}
        actual = self.configurator.build_mapping_template(unmapped=unmapped)
        assert actual == expected
