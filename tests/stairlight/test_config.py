import os
from collections import OrderedDict

import pytest

from src.stairlight import config_key, map_key
from src.stairlight.config import (
    ConfigKeyNotFoundException,
    get_config_value,
)
from src.stairlight.source.base import TemplateSourceType
from src.stairlight.source.file import FileTemplate
from src.stairlight.source.gcs import GcsTemplate
from src.stairlight.source.redash import RedashTemplate


class TestSuccess:
    @pytest.fixture(scope="class")
    def file_template(self, configurator) -> FileTemplate:
        return FileTemplate(
            mapping_config=configurator.read(
                prefix=config_key.MAPPING_CONFIG_FILE_PREFIX
            ),
            source_type=TemplateSourceType.FILE,
            key="tests/sql/main/test_undefined.sql",
        )

    @pytest.fixture(scope="class")
    def gcs_template(self, configurator) -> GcsTemplate:
        return GcsTemplate(
            mapping_config=configurator.read(
                prefix=config_key.MAPPING_CONFIG_FILE_PREFIX
            ),
            source_type=TemplateSourceType.GCS,
            bucket="stairlight",
            key="sql/one_line/one_line.sql",
        )

    @pytest.fixture(scope="class")
    def redash_template(self, configurator) -> RedashTemplate:
        return RedashTemplate(
            mapping_config=configurator.read(
                prefix=config_key.MAPPING_CONFIG_FILE_PREFIX
            ),
            query_id=5,
            query_name="Copy of (#4) New Query",
            query_str="SELECT * FROM {{ table }}",
            data_source_name="metadata",
        )

    def test_read_map(self, configurator):
        assert configurator.read(prefix=config_key.MAPPING_CONFIG_FILE_PREFIX)

    def test_read_sql(self, configurator):
        assert configurator.read(prefix=config_key.STAIRLIGHT_CONFIG_FILE_PREFIX)

    def test_create_stairlight_template_file(self, configurator, stairlight_template):
        file_name = configurator.create_stairlight_template_file(
            prefix=stairlight_template
        )
        assert os.path.exists(file_name)

    def test_create_mapping_template_file(self, configurator, mapping_template):
        file_name = configurator.create_mapping_template_file(
            unmapped=[], prefix=mapping_template
        )
        assert os.path.exists(file_name)

    def test_build_stairlight_template(self, configurator):
        stairlight_template = configurator.build_stairlight_config()
        assert list(stairlight_template.keys()) == [
            config_key.STAIRLIGHT_CONFIG_INCLUDE_SECTION,
            config_key.STAIRLIGHT_CONFIG_EXCLUDE_SECTION,
            config_key.STAIRLIGHT_CONFIG_SETTING_SECTION,
        ]

    def test_build_mapping_template(self, configurator, file_template):
        unmapped_templates = [
            {
                map_key.TEMPLATE: file_template,
                map_key.PARAMETERS: [
                    "params.main_table",
                    "params.sub_table_01",
                    "params.sub_table_02",
                ],
            }
        ]

        global_value = OrderedDict({config_key.PARAMETERS: {}})
        mapping_value = OrderedDict(
            {
                config_key.TEMPLATE_SOURCE_TYPE: file_template.source_type.value,
                config_key.FILE_SUFFIX: file_template.key,
                config_key.TABLES: [
                    OrderedDict(
                        {
                            config_key.TABLE_NAME: "test_undefined",
                            config_key.PARAMETERS: OrderedDict(
                                {
                                    "params": {
                                        "main_table": None,
                                        "sub_table_01": None,
                                        "sub_table_02": None,
                                    }
                                }
                            ),
                            config_key.LABELS: OrderedDict({"key": "value"}),
                        }
                    )
                ],
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
                config_key.MAPPING_CONFIG_GLOBAL_SECTION: global_value,
                config_key.MAPPING_CONFIG_MAPPING_SECTION: [mapping_value],
                config_key.MAPPING_CONFIG_METADATA_SECTION: [metadata_value],
            }
        )
        actual = configurator.build_mapping_config(
            unmapped_templates=unmapped_templates
        )
        assert actual == expected

    def test_get_config_value(self):
        actual = get_config_value(key="a", target={"a": "c"})
        expected = "c"
        assert actual == expected

    def test_get_mapping_values_by_template_type_file(
        self, configurator, file_template
    ):
        actual = configurator.get_mapping_values_by_template_type(
            template=file_template
        )
        expected = {config_key.FILE_SUFFIX: "tests/sql/main/test_undefined.sql"}
        assert actual == expected

    def test_get_mapping_values_by_template_type_gcs(self, configurator, gcs_template):
        actual = configurator.get_mapping_values_by_template_type(template=gcs_template)
        expected = {
            config_key.URI: "gs://stairlight/sql/one_line/one_line.sql",
            config_key.BUCKET_NAME: "stairlight",
        }
        assert actual == expected

    def test_get_mapping_values_by_template_type_redash(
        self, configurator, redash_template
    ):
        actual = configurator.get_mapping_values_by_template_type(
            template=redash_template
        )
        expected = {
            config_key.QUERY_ID: 5,
            config_key.DATA_SOURCE_NAME: "metadata",
        }
        assert actual == expected


class TestFailure:
    def test_get_config_value(self):
        with pytest.raises(ConfigKeyNotFoundException):
            _ = get_config_value(key="a", target={"b": "c"}, fail_if_not_found=True)
