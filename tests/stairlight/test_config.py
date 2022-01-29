import os
from collections import OrderedDict

import src.stairlight.config as config
import src.stairlight.source.base as base
from src.stairlight import config_key
from src.stairlight.source.file import FileTemplate


class TestSuccess:
    configurator = config.Configurator(dir="./config")

    def test_read_map(self):
        assert self.configurator.read(prefix=config_key.MAPPING_CONFIG_FILE_PREFIX)

    def test_read_sql(self):
        assert self.configurator.read(prefix=config_key.STAIRLIGHT_CONFIG_FILE_PREFIX)

    def test_create_stairlight_template_file(self, stairlight_template):
        file_name = self.configurator.create_stairlight_template_file(
            prefix=stairlight_template
        )
        assert os.path.exists(file_name)

    def test_create_mapping_template_file(self, mapping_template):
        file_name = self.configurator.create_mapping_template_file(
            unmapped=[], prefix=mapping_template
        )
        assert os.path.exists(file_name)

    def test_build_stairlight_template(self):
        stairlight_template = self.configurator.build_stairlight_template()
        assert list(stairlight_template.keys()) == [
            config_key.STAIRLIGHT_CONFIG_INCLUDE_SECTION,
            config_key.STAIRLIGHT_CONFIG_EXCLUDE_SECTION,
            config_key.STAIRLIGHT_CONFIG_SETTING_SECTION,
        ]

    def test_build_mapping_template(self):
        sql_template = FileTemplate(
            mapping_config=self.configurator.read(
                prefix=config_key.MAPPING_CONFIG_FILE_PREFIX
            ),
            source_type=base.TemplateSourceType.FILE,
            key="tests/sql/main/test_undefined.sql",
        )
        unmapped_templates = [
            {
                "sql_template": sql_template,
                "params": [
                    "params.main_table",
                    "params.sub_table_01",
                    "params.sub_table_02",
                ],
            }
        ]

        mapping_value = OrderedDict(
            {
                config_key.TEMPLATE_SOURCE_TYPE: sql_template.source_type.value,
                config_key.TABLES: [
                    OrderedDict(
                        {
                            config_key.TABLE_NAME: None,
                            config_key.PARAMETERS: OrderedDict(
                                {
                                    "main_table": None,
                                    "sub_table_01": None,
                                    "sub_table_02": None,
                                }
                            ),
                            config_key.LABELS: OrderedDict({"key": "value"}),
                        }
                    )
                ],
                config_key.FILE_SUFFIX: sql_template.key,
            }
        )

        metadata_value = OrderedDict(
            {
                config_key.TABLE_NAME: None,
                config_key.LABELS: OrderedDict({"key": "value"}),
            }
        )

        expected = OrderedDict(
            {
                config_key.MAPPING_CONFIG_MAPPING_SECTION: [mapping_value],
                config_key.MAPPING_CONFIG_METADATA_SECTION: [metadata_value],
            }
        )
        actual = self.configurator.build_mapping_template(
            unmapped_templates=unmapped_templates
        )
        assert actual == expected
